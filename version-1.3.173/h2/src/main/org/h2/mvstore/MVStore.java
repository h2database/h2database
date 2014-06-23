/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.beans.ExceptionListener;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.h2.compress.CompressLZF;
import org.h2.compress.Compressor;
import org.h2.engine.Constants;
import org.h2.mvstore.cache.CacheLongKeyLIRS;
import org.h2.mvstore.cache.FilePathCache;
import org.h2.mvstore.type.StringDataType;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathCrypt;
import org.h2.store.fs.FilePathNio;
import org.h2.util.MathUtils;
import org.h2.util.New;

/*

File format:
header: (blockSize) bytes
header: (blockSize) bytes
[ chunk ] *
(there are two headers for security at the beginning of the file,
and there is a header after each chunk)
header:
H:3,...

TODO:

TestMVStoreDataLoss

TransactionStore:
- support reading the undo log

MVStore:
- rolling docs review: at "Features"
- additional test async write / read algorithm for speed and errors
- move setters to the builder, except for setRetainVersion, setReuseSpace,
    and settings that are persistent (setStoreVersion)
- test meta rollback: it is changed after save; could rollback break it?
- automated 'kill process' and 'power failure' test
- update checkstyle
- maybe split database into multiple files, to speed up compact
- auto-compact from time to time and on close
- test and possibly improve compact operation (for large dbs)
- possibly split chunk metadata into immutable and mutable
- compact: avoid processing pages using a counting bloom filter
- defragment (re-creating maps, specially those with small pages)
- chunk header: store changed chunk data as row; maybe after the root
- chunk checksum (header, last page, 2 bytes per page?)
- is there a better name for the file header,
-- if it's no longer always at the beginning of a file? store header?
- on insert, if the child page is already full, don't load and modify it
-- split directly (specially for leaves with one large entry)
- maybe let a chunk point to a list of potential next chunks
-- (so no fixed location header is needed)
- support stores that span multiple files (chunks stored in other files)
- triggers (can be implemented with a custom map)
- store number of write operations per page (maybe defragment
-- if much different than count)
- r-tree: nearest neighbor search
- chunk metadata: do not store default values
- support maps without values (just existence of the key)
- support maps without keys (counted b-tree features)
- use a small object cache (StringCache), test on Android
- dump values
- map split / merge (fast if no overlap)
- auto-save if there are too many changes (required for StreamStore)
- StreamStore optimization: avoid copying bytes
- unlimited transaction size
- MVStoreTool.shrink to shrink a store (create, copy, rename, delete)
-- and for MVStore on Windows, auto-detect renamed file
- ensure data is overwritten eventually if the system doesn't have a timer
- SSD-friendly write (always in blocks of 4 MB / 1 second?)
- close the file on out of memory or disk write error (out of disk space or so)
- implement a sharded map (in one store, multiple stores)
-- to support concurrent updates and writes, and very large maps
- implement an off-heap file system
- remove change cursor, or add support for writing to branches
- support pluggable logging or remove log
- maybe add an optional finalizer and exit hook
    to store committed changes
- to save space when persisting very small transactions,
-- use a transaction log where only the deltas are stored
- serialization for lists, sets, sets, sorted sets, maps, sorted maps
- maybe rename 'rollback' to 'revert' to distinguish from transactions
- support other compression algorithms (deflate, LZ4,...)
- only retain the last version, unless explicitly set (setRetainVersion)
- support opening (existing) maps by id
- more consistent null handling (keys/values sometimes may be null)
- logging mechanism, specially for operations in a background thread

*/

/**
 * A persistent storage for maps.
 */
public class MVStore {

    /**
     * Whether assertions are enabled.
     */
    public static final boolean ASSERT = false;

    /**
     * The block size (physical sector size) of the disk. The file header is
     * written twice, one copy in each block, to ensure it survives a crash.
     */
    static final int BLOCK_SIZE = 4 * 1024;

    private static final int FORMAT_WRITE = 1;
    private static final int FORMAT_READ = 1;

    /**
     * The background thread, if any.
     */
    volatile Thread backgroundThread;

    private boolean closed;

    private final String fileName;
    private final char[] filePassword;

    private int pageSize = 6 * 1024;

    private FileChannel file;
    private FileLock fileLock;
    private long fileSize;
    private long rootChunkStart;

    /**
     * The cache. The default size is 16 MB, and the average size is 2 KB. It is
     * split in 16 segments. The stack move distance is 2% of the expected
     * number of entries.
     */
    private final CacheLongKeyLIRS<Page> cache;

    private int lastChunkId;

    /**
     * The map of chunks.
     */
    private final ConcurrentHashMap<Integer, Chunk> chunks =
            new ConcurrentHashMap<Integer, Chunk>();

    /**
     * The free spaces between the chunks. The first block to use is block 2
     * (the first two blocks are the file header).
     */
    private FreeSpaceBitSet freeSpace = new FreeSpaceBitSet(2, BLOCK_SIZE);

    /**
     * The map of temporarily removed pages. The key is the unsaved version, the
     * value is the map of chunks. The maps of chunks contains the number of
     * freed entries per chunk. Access is synchronized.
     */
    private final HashMap<Long, HashMap<Integer, Chunk>> freedPages = New.hashMap();

    private MVMapConcurrent<String, String> meta;

    private final ConcurrentHashMap<Integer, MVMap<?, ?>> maps =
            new ConcurrentHashMap<Integer, MVMap<?, ?>>();

    private HashMap<String, String> fileHeader = New.hashMap();

    private ByteBuffer writeBuffer;

    private boolean readOnly;

    private int lastMapId;

    private volatile boolean reuseSpace = true;
    private long retainVersion = -1;

    private final Compressor compressor = new CompressLZF();

    private final boolean compress;

    private long currentVersion;

    /**
     * The version of the last stored chunk.
     */
    private long lastStoredVersion;
    private int fileReadCount;
    private int fileWriteCount;
    private int unsavedPageCount;
    private int maxUnsavedPages;

    /**
     * The time the store was created, in milliseconds since 1970.
     */
    private long creationTime;
    private int retentionTime = 45000;

    private long lastStoreTime;

    /**
     * To which version to roll back when opening the store after a crash.
     */
    private long lastCommittedVersion;

    /**
     * The earliest chunk to retain, if any.
     */
    private Chunk retainChunk;

    /**
     * The version of the current store operation (if any).
     */
    private volatile long currentStoreVersion = -1;

    private volatile boolean metaChanged;

    /**
     * The delay in milliseconds to automatically store changes.
     */
    private int writeDelay = 1000;

    private ExceptionListener backgroundExceptionListener;

    MVStore(HashMap<String, Object> config) {
        String f = (String) config.get("fileName");
        if (f != null && f.indexOf(':') < 0) {
            // NIO is used, unless a different file system is specified
            // the following line is to ensure the NIO file system is compiled
            FilePathNio.class.getName();
            f = "nio:" + f;
        }
        this.fileName = f;
        this.readOnly = config.containsKey("readOnly");
        this.compress = config.containsKey("compress");
        if (fileName != null) {
            Object o = config.get("cacheSize");
            int mb = o == null ? 16 : (Integer) o;
            int maxMemoryBytes = mb * 1024 * 1024;
            int averageMemory = pageSize / 2;
            int segmentCount = 16;
            int stackMoveDistance = maxMemoryBytes / averageMemory * 2 / 100;
            cache = new CacheLongKeyLIRS<Page>(
                    maxMemoryBytes, averageMemory, segmentCount, stackMoveDistance);
            filePassword = (char[]) config.get("encrypt");
            o = config.get("writeBufferSize");
            mb = o == null ? 4 : (Integer) o;
            int writeBufferSize =  mb * 1024 * 1024;
            maxUnsavedPages = writeBufferSize / pageSize;
            o = config.get("writeDelay");
            writeDelay = o == null ? 1000 : (Integer) o;
        } else {
            cache = null;
            filePassword = null;
        }
    }

    /**
     * Open a store in exclusive mode. For a file-based store, the parent
     * directory must already exist.
     *
     * @param fileName the file name (null for in-memory)
     * @return the store
     */
    public static MVStore open(String fileName) {
        HashMap<String, Object> config = New.hashMap();
        config.put("fileName", fileName);
        MVStore s = new MVStore(config);
        s.open();
        return s;
    }

    /**
     * Open an old, stored version of a map.
     *
     * @param version the version
     * @param mapId the map id
     * @param template the template map
     * @return the read-only map
     */
    @SuppressWarnings("unchecked")
    <T extends MVMap<?, ?>> T  openMapVersion(long version, int mapId, MVMap<?, ?> template) {
        MVMap<String, String> oldMeta = getMetaMap(version);
        String r = oldMeta.get("root." + mapId);
        long rootPos = r == null ? 0 : Long.parseLong(r);
        MVMap<?, ?> m = template.openReadOnly();
        m.setRootPos(rootPos, version);
        return (T) m;
    }

    /**
     * Open a map with the default settings. The map is automatically create if
     * it does not yet exist. If a map with this name is already open, this map
     * is returned.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @return the map
     */
    public <K, V> MVMap<K, V> openMap(String name) {
        return openMap(name, new MVMap.Builder<K, V>());
    }

    /**
     * Open a map with the given builder. The map is automatically create if it
     * does not yet exist. If a map with this name is already open, this map is
     * returned.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @param builder the map builder
     * @return the map
     */
    public <M extends MVMap<K, V>, K, V> M openMap(String name, MVMap.MapBuilder<M, K, V> builder) {
        checkOpen();
        String x = meta.get("name." + name);
        int id;
        long root;
        HashMap<String, String> c;
        M map;
        if (x != null) {
            id = Integer.parseInt(x);
            @SuppressWarnings("unchecked")
            M old = (M) maps.get(id);
            if (old != null) {
                return old;
            }
            map = builder.create();
            String config = meta.get("map." + x);
            c = DataUtils.parseMap(config);
            c.put("id", x);
            map.init(this, c);
            String r = meta.get("root." + id);
            root = r == null ? 0 : Long.parseLong(r);
        } else {
            c = New.hashMap();
            id = ++lastMapId;
            c.put("id", Integer.toString(id));
            c.put("createVersion", Long.toString(currentVersion));
            map = builder.create();
            map.init(this, c);
            meta.put("map." + id, map.asString(name));
            meta.put("name." + name, Integer.toString(id));
            markMetaChanged();
            root = 0;
        }
        map.setRootPos(root, -1);
        maps.put(id, map);
        return map;
    }

    /**
     * Get the metadata map. This data is for informational purposes only. The
     * data is subject to change in future versions. The data should not be
     * modified (doing so may corrupt the store).
     * <p>
     * It contains the following entries:
     *
     * <pre>
     * name.{name} = {mapId}
     * map.{mapId} = {map metadata}
     * root.{mapId} = {root position}
     * chunk.{chunkId} = {chunk metadata}
     * </pre>
     *
     * @return the metadata map
     */
    public MVMap<String, String> getMetaMap() {
        checkOpen();
        return meta;
    }

    private MVMap<String, String> getMetaMap(long version) {
        Chunk c = getChunkForVersion(version);
        DataUtils.checkArgument(c != null, "Unknown version {0}", version);
        c = readChunkHeader(c.start);
        MVMap<String, String> oldMeta = meta.openReadOnly();
        oldMeta.setRootPos(c.metaRootPos, version);
        return oldMeta;
    }

    private Chunk getChunkForVersion(long version) {
        for (int chunkId = lastChunkId;; chunkId--) {
            Chunk x = chunks.get(chunkId);
            if (x == null || x.version < version) {
                return null;
            } else if (x.version == version) {
                return x;
            }
        }
    }

    /**
     * Remove a map.
     *
     * @param id the map id
     */
    void removeMap(int id) {
        String name = getMapName(id);
        markMetaChanged();
        meta.remove("map." + id);
        meta.remove("name." + name);
        meta.remove("root." + id);
        maps.remove(id);
    }

    /**
     * Mark a map as changed (containing unsaved changes).
     *
     * @param map the map
     */
    void markChanged(MVMap<?, ?> map) {
        if (map == meta) {
            metaChanged = true;
        }
    }

    private void markMetaChanged() {
        // changes in the metadata alone are usually not detected, as the meta
        // map is changed after storing
        markChanged(meta);
    }

    /**
     * Open the store.
     *
     * @throws IllegalStateException if the file is corrupt, or an exception
     *             occurred while opening
     * @throws IllegalArgumentException if the directory does not exist
     */
    void open() {
        meta = new MVMapConcurrent<String, String>(StringDataType.INSTANCE, StringDataType.INSTANCE);
        HashMap<String, String> c = New.hashMap();
        c.put("id", "0");
        c.put("createVersion", Long.toString(currentVersion));
        meta.init(this, c);
        if (fileName == null) {
            return;
        }
        FilePath parent = FilePath.get(fileName).getParent();
        if (!parent.exists()) {
            throw DataUtils.newIllegalArgumentException("Directory does not exist: {0}", parent);
        }
        try {
            if (readOnly) {
                openFile();
            } else if (!openFile()) {
                readOnly = true;
                openFile();
            }
        } finally {
            if (filePassword != null) {
                Arrays.fill(filePassword, (char) 0);
            }
        }
        lastStoreTime = getTime();
        String r = meta.get("rollbackOnOpen");
        if (r != null) {
            long rollback = Long.parseLong(r);
            rollbackTo(rollback);
        }
        this.lastCommittedVersion = currentVersion;
        setWriteDelay(writeDelay);
    }

    /**
     * Try to open the file in read or write mode.
     *
     * @return if opening the file was successful, and false if the file could
     *          not be opened in write mode because the write file format it too
     *          high (in which case the file can be opened in read-only mode)
     * @throw IllegalStateException if the file could not be opened
     *          because of an IOException or file format error
     */
    private boolean openFile() {
        IllegalStateException exception;
        try {
            log("file open");
            FilePath f = FilePath.get(fileName);
            if (f.exists() && !f.canWrite()) {
                readOnly = true;
            }
            file = f.open(readOnly ? "r" : "rw");
            if (filePassword != null) {
                byte[] password = FilePathCrypt.getPasswordBytes(filePassword);
                file = new FilePathCrypt.FileCrypt(fileName, password, file);
            }
            file = FilePathCache.wrap(file);
            try {
                if (readOnly) {
                    fileLock = file.tryLock(0, Long.MAX_VALUE, true);
                } else {
                    fileLock = file.tryLock();
                }
            } catch (OverlappingFileLockException e) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_LOCKED, "The file is locked: {0}", fileName, e);
            }
            if (fileLock == null) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_LOCKED, "The file is locked: {0}", fileName);
            }
            fileSize = file.size();
            if (fileSize == 0) {
                creationTime = 0;
                creationTime = getTime();
                lastStoreTime = creationTime;
                fileHeader.put("H", "3");
                fileHeader.put("blockSize", "" + BLOCK_SIZE);
                fileHeader.put("format", "" + FORMAT_WRITE);
                fileHeader.put("creationTime", "" + creationTime);
                writeFileHeader();
            } else {
                readFileHeader();
                int formatWrite = Integer.parseInt(fileHeader.get("format"));
                String x = fileHeader.get("formatRead");
                int formatRead = x == null ? formatWrite : Integer.parseInt(x);
                if (formatRead > FORMAT_READ) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_UNSUPPORTED_FORMAT,
                            "The file format {0} is larger than the supported format {1}",
                            formatRead, FORMAT_READ);
                }
                if (formatWrite > FORMAT_WRITE) {
                    readOnly = true;
                    file.close();
                    return false;
                }
                if (rootChunkStart > 0) {
                    readMeta();
                }
            }
            exception = null;
        } catch (IOException e) {
            exception = DataUtils.newIllegalStateException(
                    DataUtils.ERROR_READING_FAILED,
                    "Could not open file {0}", fileName, e);
        } catch (IllegalStateException e) {
            exception = e;
        }
        if (exception != null) {
            try {
                closeFile(false);
            } catch (Exception e2) {
                // ignore
            }
            throw exception;
        }
        return true;
    }


    private void readMeta() {
        Chunk header = readChunkHeader(rootChunkStart);
        lastChunkId = header.id;
        chunks.put(header.id, header);
        meta.setRootPos(header.metaRootPos, -1);
        // load chunks in reverse order, because data about previous chunks
        // might only be available in later chunks
        // if this is a performance problem when there are many
        // chunks, the id of the previous / next chunk might need to
        // be maintained
        for (int id = lastChunkId; id >= 0; id--) {
            String s = meta.get("chunk." + id);
            if (s == null) {
                continue;
            }
            Chunk c = Chunk.fromString(s);
            if (c.id == header.id) {
                c.start = header.start;
                c.length = header.length;
                c.metaRootPos = header.metaRootPos;
                c.pageCount = header.pageCount;
                c.pageCountLive = header.pageCountLive;
                c.maxLength = header.maxLength;
                c.maxLengthLive = header.maxLengthLive;
            }
            lastChunkId = Math.max(c.id, lastChunkId);
            chunks.put(c.id, c);
            if (c.pageCountLive == 0) {
                // remove this chunk in the next save operation
                registerFreePage(currentVersion, c.id, 0, 0);
            }
        }
        // rebuild the free space list
        freeSpace.clear();
        for (Chunk c : chunks.values()) {
            if (c.start == Long.MAX_VALUE) {
                continue;
            }
            int len = MathUtils.roundUpInt(c.length, BLOCK_SIZE) + BLOCK_SIZE;
            freeSpace.markUsed(c.start, len);
        }
    }

    private void readFileHeader() {
        // we don't have a valid header yet
        currentVersion = -1;
        // read the last block of the file, and then two first blocks
        ByteBuffer buff = ByteBuffer.allocate(3 * BLOCK_SIZE);
        buff.limit(BLOCK_SIZE);
        fileReadCount++;
        DataUtils.readFully(file, fileSize - BLOCK_SIZE, buff);
        buff.limit(3 * BLOCK_SIZE);
        buff.position(BLOCK_SIZE);
        fileReadCount++;
        DataUtils.readFully(file, 0, buff);
        for (int i = 0; i < 3 * BLOCK_SIZE; i += BLOCK_SIZE) {
            String s = new String(buff.array(), i, BLOCK_SIZE, Constants.UTF8)
                    .trim();
            HashMap<String, String> m;
            try {
                m = DataUtils.parseMap(s);
            } catch (IllegalArgumentException e) {
                continue;
            }
            String f = m.remove("fletcher");
            if (f == null) {
                continue;
            }
            int check;
            try {
                check = (int) Long.parseLong(f, 16);
            } catch (NumberFormatException e) {
                check = -1;
            }
            s = s.substring(0, s.lastIndexOf("fletcher") - 1);
            byte[] bytes = s.getBytes(Constants.UTF8);
            int checksum = DataUtils.getFletcher32(bytes, bytes.length / 2 * 2);
            if (check != checksum) {
                continue;
            }
            long version = Long.parseLong(m.get("version"));
            if (version > currentVersion) {
                fileHeader = m;
                rootChunkStart = Long.parseLong(m.get("rootChunk"));
                creationTime = Long.parseLong(m.get("creationTime"));
                currentVersion = version;
                lastMapId = Integer.parseInt(m.get("lastMapId"));
            }
        }
        if (currentVersion < 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT, "File header is corrupt: {0}", fileName);
        }
        lastStoredVersion = -1;
    }

    private byte[] getFileHeaderBytes() {
        StringBuilder buff = new StringBuilder();
        fileHeader.put("lastMapId", "" + lastMapId);
        fileHeader.put("rootChunk", "" + rootChunkStart);
        fileHeader.put("version", "" + currentVersion);
        DataUtils.appendMap(buff, fileHeader);
        byte[] bytes = buff.toString().getBytes(Constants.UTF8);
        int checksum = DataUtils.getFletcher32(bytes, bytes.length / 2 * 2);
        DataUtils.appendMap(buff, "fletcher", Integer.toHexString(checksum));
        bytes = buff.toString().getBytes(Constants.UTF8);
        if (bytes.length > BLOCK_SIZE) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "File header too large: {0}", buff);
        }
        return bytes;
    }

    private void writeFileHeader() {
        byte[] bytes = getFileHeaderBytes();
        ByteBuffer header = ByteBuffer.allocate(2 * BLOCK_SIZE);
        header.put(bytes);
        header.position(BLOCK_SIZE);
        header.put(bytes);
        header.rewind();
        fileWriteCount++;
        DataUtils.writeFully(file,  0, header);
        fileSize = Math.max(fileSize, 2 * BLOCK_SIZE);
    }

    /**
     * Close the file and the store. If there are any committed but unsaved
     * changes, they are written to disk first. If any temporary data was
     * written but not committed, this is rolled back. All open maps are closed.
     */
    public void close() {
        if (closed) {
            return;
        }
        if (!readOnly) {
            if (hasUnsavedChanges()) {
                rollbackTo(lastCommittedVersion);
                store(false);
            }
        }
        closeFile(true);
    }

    /**
     * Close the file and the store, without writing anything.
     * This will stop the background thread.
     */
    public void closeImmediately() {
        closeFile(false);
    }

    private void closeFile(boolean shrinkIfPossible) {
        if (closed) {
            return;
        }
        closed = true;
        if (file == null) {
            return;
        }
        // can not synchronize on this yet, because
        // the thread also synchronized on this, which
        // could result in a deadlock
        stopBackgroundThread();
        synchronized (this) {
            try {
                if (shrinkIfPossible) {
                    shrinkFileIfPossible(0);
                }
                log("file close");
                if (fileLock != null) {
                    fileLock.release();
                    fileLock = null;
                }
                file.close();
                for (MVMap<?, ?> m : New.arrayList(maps.values())) {
                    m.close();
                }
                meta = null;
                chunks.clear();
                freeSpace.clear();
                cache.clear();
                maps.clear();
            } catch (Exception e) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_WRITING_FAILED,
                        "Closing failed for file {0}", fileName, e);
            } finally {
                file = null;
            }
        }
    }

    /**
     * Get the chunk for the given position.
     *
     * @param pos the position
     * @return the chunk
     */
    Chunk getChunk(long pos) {
        return chunks.get(DataUtils.getPageChunkId(pos));
    }

    /**
     * Increment the current version, without committing the changes.
     *
     * @return the new version
     */
    public long incrementVersion() {
        return ++currentVersion;
    }

    /**
     * Commit the changes. This method marks the changes as committed and
     * increments the version.
     * <p>
     * Unless the write delay is set to 0, this method does not write to the
     * file. Instead, data is written after the delay, manually by calling the
     * store method, when the write buffer is full, or when closing the store.
     *
     * @return the new version
     */
    public long commit() {
        long v = ++currentVersion;
        lastCommittedVersion = v;
        if (writeDelay == 0) {
            store(false);
        }
        return v;
    }

    /**
     * Commit all changes and persist them to disk. This method does nothing if
     * there are no unsaved changes, otherwise it increments the current version
     * and stores the data (for file based stores).
     * <p>
     * One store operation may run at any time.
     *
     * @return the new version (incremented if there were changes)
     */
    public long store() {
        checkOpen();
        return store(false);
    }

    /**
     * Store changes. Changes that are marked as temporary are rolled back after
     * a restart.
     *
     * @param temp whether the changes are only temporary (not committed), and
     *        should be rolled back after a crash
     * @return the new version (incremented if there were changes)
     */
    private synchronized long store(boolean temp) {
        if (closed) {
            return currentVersion;
        }
        if (currentStoreVersion >= 0) {
            // store is possibly called within store, if the meta map changed
            return currentVersion;
        }
        if (!hasUnsavedChanges()) {
            return currentVersion;
        }
        if (readOnly) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_WRITING_FAILED, "This store is read-only");
        }
        int currentUnsavedPageCount = unsavedPageCount;
        long storeVersion = currentStoreVersion = currentVersion;
        long version = incrementVersion();

        if (file == null) {
            return version;
        }
        long time = getTime();
        lastStoreTime = time;
        if (temp) {
            meta.put("rollbackOnOpen", Long.toString(lastCommittedVersion));
            // find the oldest chunk to retain
            long minVersion = Long.MAX_VALUE;
            Chunk minChunk = null;
            for (Chunk c : chunks.values()) {
                if (c.version < minVersion) {
                    minVersion = c.version;
                    minChunk = c;
                }
            }
            retainChunk = minChunk;
        } else {
            lastCommittedVersion = version;
            meta.remove("rollbackOnOpen");
            retainChunk = null;
        }
        // the last chunk was not completely correct in the last store()
        // this needs to be updated now (it's better not to update right after
        // storing, because that would modify the meta map again)
        Chunk lastChunk = chunks.get(lastChunkId);
        if (lastChunk != null) {
            meta.put("chunk." + lastChunk.id, lastChunk.asString());
            // never go backward in time
            time = Math.max(lastChunk.time, time);
        }
        Chunk c;
        c = new Chunk(++lastChunkId);
        c.maxLength = Long.MAX_VALUE;
        c.maxLengthLive = Long.MAX_VALUE;
        c.start = Long.MAX_VALUE;
        c.length = Integer.MAX_VALUE;
        c.time = time;
        c.version = version;
        chunks.put(c.id, c);
        meta.put("chunk." + c.id, c.asString());
        ArrayList<MVMap<?, ?>> list = New.arrayList(maps.values());
        ArrayList<MVMap<?, ?>> changed = New.arrayList();
        for (MVMap<?, ?> m : list) {
            if (m != meta) {
                long v = m.getVersion();
                if (v >= 0 && m.getVersion() >= lastStoredVersion) {
                    MVMap<?, ?> r = m.openVersion(storeVersion);
                    r.waitUntilWritten(r.getRoot());
                    if (r.getRoot().getPos() == 0) {
                        changed.add(r);
                    }
                }
            }
        }
        for (MVMap<?, ?> m : changed) {
            Page p = m.getRoot();
            if (p.getTotalCount() == 0) {
                meta.put("root." + m.getId(), "0");
            } else {
                meta.put("root." + m.getId(), String.valueOf(Integer.MAX_VALUE));
            }
        }
        Set<Chunk> removedChunks = applyFreedPages(storeVersion, time);
        ByteBuffer buff;
        if (writeBuffer != null) {
            buff = writeBuffer;
            buff.clear();
        } else {
            buff = ByteBuffer.allocate(1024 * 1024);
        }
        // need to patch the header later
        c.writeHeader(buff);
        c.maxLength = 0;
        c.maxLengthLive = 0;
        for (MVMap<?, ?> m : changed) {
            Page p = m.getRoot();
            if (p.getTotalCount() > 0) {
                buff = p.writeUnsavedRecursive(c, buff);
                long root = p.getPos();
                meta.put("root." + m.getId(), "" + root);
            }
        }

        meta.put("chunk." + c.id, c.asString());

        // this will modify maxLengthLive, but
        // the correct value is written in the chunk header
        buff = meta.getRoot().writeUnsavedRecursive(c, buff);

        int chunkLength = buff.position();

        // round to the next block,
        // and one additional block for the file header
        int length = MathUtils.roundUpInt(chunkLength, BLOCK_SIZE) + BLOCK_SIZE;
        if (length > buff.capacity()) {
            buff = DataUtils.ensureCapacity(buff, length - buff.capacity());
        }
        buff.limit(length);

        long fileSizeUsed = getFileSizeUsed();
        long filePos;
        if (reuseSpace) {
            filePos = freeSpace.allocate(length);
        } else {
            filePos = fileSizeUsed;
            freeSpace.markUsed(fileSizeUsed, length);
        }
        boolean storeAtEndOfFile = filePos + length >= fileSizeUsed;

        // free up the space of unused chunks now
        for (Chunk x : removedChunks) {
            int len = MathUtils.roundUpInt(x.length, BLOCK_SIZE) + BLOCK_SIZE;
            freeSpace.free(x.start, len);
        }

        c.start = filePos;
        c.length = chunkLength;
        c.metaRootPos = meta.getRoot().getPos();
        buff.position(0);
        c.writeHeader(buff);
        rootChunkStart = filePos;
        revertTemp(storeVersion);

        buff.position(buff.limit() - BLOCK_SIZE);
        byte[] header = getFileHeaderBytes();
        buff.put(header);
        // fill the header with zeroes
        buff.put(new byte[BLOCK_SIZE - header.length]);

        buff.position(0);
        fileWriteCount++;
        DataUtils.writeFully(file, filePos, buff);

        fileSize = Math.max(fileSize, filePos + buff.position());
        if (buff.capacity() <= 4 * 1024 * 1024) {
            writeBuffer = buff;
        }

        // overwrite the header if required
        if (!storeAtEndOfFile) {
            writeFileHeader();
            shrinkFileIfPossible(1);
        }

        for (MVMap<?, ?> m : changed) {
            Page p = m.getRoot();
            if (p.getTotalCount() > 0) {
                p.writeEnd();
            }
        }
        meta.getRoot().writeEnd();

        // some pages might have been changed in the meantime (in the newest version)
        unsavedPageCount = Math.max(0, unsavedPageCount - currentUnsavedPageCount);
        currentStoreVersion = -1;
        metaChanged = false;
        lastStoredVersion = storeVersion;
        return version;
    }

    private boolean canOverwriteChunk(Chunk c, long time) {
        if (c.time + retentionTime > time) {
            return false;
        }
        Chunk r = retainChunk;
        if (r != null && c.version > r.version) {
            return false;
        }
        return true;
    }

    private long getTime() {
        return System.currentTimeMillis() - creationTime;
    }

    /**
     * Apply the freed pages to the chunk metadata. The metadata is updated, but
     * freed chunks are not removed yet.
     *
     * @param storeVersion apply up to the given version
     * @return the set of freed chunks (might be empty)
     */
    private Set<Chunk> applyFreedPages(long storeVersion, long time) {
        Set<Chunk> removedChunks = New.hashSet();
        synchronized (freedPages) {
            while (true) {
                ArrayList<Chunk> modified = New.arrayList();
                for (Iterator<Long> it = freedPages.keySet().iterator(); it.hasNext();) {
                    long v = it.next();
                    if (v > storeVersion) {
                        continue;
                    }
                    Map<Integer, Chunk> freed = freedPages.get(v);
                    for (Chunk f : freed.values()) {
                        Chunk c = chunks.get(f.id);
                        c.maxLengthLive += f.maxLengthLive;
                        c.pageCountLive += f.pageCountLive;
                        if (c.pageCountLive < 0) {
                            throw DataUtils.newIllegalStateException(
                                    DataUtils.ERROR_INTERNAL,
                                    "Corrupt page count {0}", c.pageCountLive);
                        }
                        if (c.maxLengthLive < 0) {
                            throw DataUtils.newIllegalStateException(
                                    DataUtils.ERROR_INTERNAL,
                                    "Corrupt max length {0}", c.maxLengthLive);
                        }
                        if (c.pageCount == 0 && c.maxLengthLive > 0) {
                            throw DataUtils.newIllegalStateException(
                                    DataUtils.ERROR_INTERNAL,
                                    "Corrupt max length {0}", c.maxLengthLive);
                        }
                        modified.add(c);
                    }
                    it.remove();
                }
                for (Chunk c : modified) {
                    if (c.maxLengthLive == 0) {
                        if (canOverwriteChunk(c, time)) {
                            removedChunks.add(c);
                            chunks.remove(c.id);
                            meta.remove("chunk." + c.id);
                        } else {
                            // remove this chunk in the next save operation
                            registerFreePage(storeVersion + 1, c.id, 0, 0);
                        }
                    } else {
                        meta.put("chunk." + c.id, c.asString());
                    }
                }
                if (modified.size() == 0) {
                    break;
                }
            }
        }
        return removedChunks;
    }

    /**
     * Shrink the file if possible, and if at least a given percentage can be
     * saved.
     *
     * @param minPercent the minimum percentage to save
     */
    private void shrinkFileIfPossible(int minPercent) {
        long used = getFileSizeUsed();
        if (used >= fileSize) {
            return;
        }
        if (minPercent > 0 && fileSize - used < BLOCK_SIZE) {
            return;
        }
        int savedPercent = (int) (100 - (used * 100 / fileSize));
        if (savedPercent < minPercent) {
            return;
        }
        try {
            file.truncate(used);
        } catch (IOException e) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_WRITING_FAILED,
                    "Could not truncate file {0} to size {1}",
                    fileName, used, e);
        }
        fileSize = used;
    }

    private long getFileSizeUsed() {
        long size = 2 * BLOCK_SIZE;
        for (Chunk c : chunks.values()) {
            if (c.start == Long.MAX_VALUE) {
                continue;
            }
            long x = c.start + c.length;
            size = Math.max(size, MathUtils.roundUpLong(x, BLOCK_SIZE) + BLOCK_SIZE);
        }
        return size;
    }

    /**
     * Check whether there are any unsaved changes.
     *
     * @return if there are any changes
     */
    public boolean hasUnsavedChanges() {
        checkOpen();
        if (metaChanged) {
            return true;
        }
        for (MVMap<?, ?> m : maps.values()) {
            if (!m.isClosed()) {
                long v = m.getVersion();
                if (v >= 0 && v >= lastStoredVersion) {
                    return true;
                }
            }
        }
        return false;
    }

    private Chunk readChunkHeader(long start) {
        fileReadCount++;
        ByteBuffer buff = ByteBuffer.allocate(40);
        DataUtils.readFully(file, start, buff);
        buff.rewind();
        return Chunk.fromHeader(buff, start);
    }

    /**
     * Try to reduce the file size. Chunks with a low number of live items will
     * be re-written. If the current fill rate is higher than the target fill
     * rate, no optimization is done.
     *
     * @param fillRate the minimum percentage of live entries
     * @return if anything was written
     */
    public boolean compact(int fillRate) {
        checkOpen();
        if (chunks.size() == 0) {
            // avoid division by 0
            return false;
        }
        long maxLengthSum = 0, maxLengthLiveSum = 0;
        for (Chunk c : chunks.values()) {
            maxLengthSum += c.maxLength;
            maxLengthLiveSum += c.maxLengthLive;
        }
        if (maxLengthSum <= 0) {
            // avoid division by 0
            maxLengthSum = 1;
        }
        int percentTotal = (int) (100 * maxLengthLiveSum / maxLengthSum);
        if (percentTotal > fillRate) {
            return false;
        }

        // calculate the average max length
        int averageMaxLength = (int) (maxLengthSum / chunks.size());

        long time = getTime();

        // the 'old' list contains the chunks we want to free up
        ArrayList<Chunk> old = New.arrayList();
        for (Chunk c : chunks.values()) {
            if (canOverwriteChunk(c, time)) {
                int age = lastChunkId - c.id + 1;
                c.collectPriority = c.getFillRate() / age;
                old.add(c);
            }
        }
        if (old.size() == 0) {
            return false;
        }

        // sort the list, so the first entry should be collected first
        Collections.sort(old, new Comparator<Chunk>() {
            @Override
            public int compare(Chunk o1, Chunk o2) {
                return new Integer(o1.collectPriority).compareTo(o2.collectPriority);
            }
        });

        // find out up to were in the old list we need to move
        // try to move one (average sized) chunk
        long moved = 0;
        Chunk move = null;
        for (Chunk c : old) {
            if (move != null && moved + c.maxLengthLive > averageMaxLength) {
                break;
            }
            log(" chunk " + c.id + " " + c.getFillRate() + "% full; prio=" + c.collectPriority);
            moved += c.maxLengthLive;
            move = c;
        }

        // remove the chunks we want to keep from this list
        boolean remove = false;
        for (Iterator<Chunk> it = old.iterator(); it.hasNext();) {
            Chunk c = it.next();
            if (move == c) {
                remove = true;
            } else if (remove) {
                it.remove();
            }
        }

        // iterate over all the pages in the old pages
        for (Chunk c : old) {
            copyLive(c, old);
        }

        store();
        return true;
    }

    private void copyLive(Chunk chunk, ArrayList<Chunk> old) {
        ByteBuffer buff = ByteBuffer.allocate(chunk.length);
        DataUtils.readFully(file, chunk.start, buff);
        Chunk.fromHeader(buff, chunk.start);
        int chunkLength = chunk.length;
        markMetaChanged();
        while (buff.position() < chunkLength) {
            int start = buff.position();
            int pageLength = buff.getInt();
            buff.getShort();
            int mapId = DataUtils.readVarInt(buff);
            @SuppressWarnings("unchecked")
            MVMap<Object, Object> map = (MVMap<Object, Object>) getMap(mapId);
            if (map == null) {
                buff.position(start + pageLength);
                continue;
            }
            buff.position(start);
            Page page = new Page(map, 0);
            page.read(buff, chunk.id, buff.position(), chunk.length);
            for (int i = 0; i < page.getKeyCount(); i++) {
                Object k = page.getKey(i);
                Page p = map.getPage(k);
                if (p == null) {
                    // was removed later - ignore
                    // or the chunk no longer exists
                } else if (p.getPos() < 0) {
                    // temporarily changed - ok
                    // TODO move old data if there is an uncommitted change?
                } else {
                    Chunk c = getChunk(p.getPos());
                    if (old.contains(c)) {
                        log("       move key:" + k + " chunk:" + c.id);
                        Object value = map.remove(k);
                        map.put(k, value);
                    }
                }
            }
        }
    }

    private MVMap<?, ?> getMap(int mapId) {
        if (mapId == 0) {
            return meta;
        }
        return maps.get(mapId);
    }

    /**
     * Read a page.
     *
     * @param map the map
     * @param pos the page position
     * @return the page
     */
    Page readPage(MVMap<?, ?> map, long pos) {
        Page p = cache.get(pos);
        if (p == null) {
            Chunk c = getChunk(pos);
            if (c == null) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Chunk {0} not found",
                        DataUtils.getPageChunkId(pos));
            }
            long filePos = c.start;
            filePos += DataUtils.getPageOffset(pos);
            fileReadCount++;
            p = Page.read(file, map, pos, filePos, fileSize);
            cache.put(pos, p, p.getMemory());
        }
        return p;
    }

    /**
     * Remove a page.
     *
     * @param map the map the page belongs to
     * @param pos the position of the page
     */
    void removePage(MVMap<?, ?> map, long pos) {
        // we need to keep temporary pages,
        // to support reading old versions and rollback
        if (pos == 0) {
            // the value could be smaller than 0 because
            // in some cases a page is allocated without a store
            unsavedPageCount = Math.max(0, unsavedPageCount - 1);
            return;
        }
        // this could result in a cache miss
        // if the operation is rolled back,
        // but we don't optimize for rollback
        cache.remove(pos);
        Chunk c = getChunk(pos);
        long version = currentVersion;
        if (map == meta && currentStoreVersion >= 0) {
            // if the meta map is modified while storing,
            // then this freed page needs to be registered
            // with the stored chunk, so that the old chunk
            // can be re-used
            version = currentStoreVersion;
        }
        registerFreePage(version, c.id, DataUtils.getPageMaxLength(pos), 1);
    }

    private void registerFreePage(long version, int chunkId, long maxLengthLive, int pageCount) {
        synchronized (freedPages) {
            HashMap<Integer, Chunk>freed = freedPages.get(version);
            if (freed == null) {
                freed = New.hashMap();
                freedPages.put(version, freed);
            }
            Chunk f = freed.get(chunkId);
            if (f == null) {
                f = new Chunk(chunkId);
                freed.put(chunkId, f);
            }
            f.maxLengthLive -= maxLengthLive;
            f.pageCountLive -= pageCount;
        }
    }

    /**
     * Log the string, if logging is enabled.
     *
     * @param string the string to log
     */
    void log(String string) {
        // TODO logging
        // System.out.println(string);
    }

    /**
     * Set the amount of memory a page should contain at most, in bytes. Larger
     * pages are split. The default is 6 KB. This is not a limit in the page
     * size (pages with one entry can get larger), it is just the point where
     * pages are split.
     *
     * @param pageSize the page size
     */
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Get the page size, in bytes.
     *
     * @return the page size
     */
    public int getPageSize() {
        return pageSize;
    }

    Compressor getCompressor() {
        return compressor;
    }

    boolean getCompress() {
        return compress;
    }

    public boolean getReuseSpace() {
        return reuseSpace;
    }

    /**
     * Whether empty space in the file should be re-used. If enabled, old data
     * is overwritten (default). If disabled, writes are appended at the end of
     * the file.
     * <p>
     * This setting is specially useful for online backup. To create an online
     * backup, disable this setting, then copy the file (starting at the
     * beginning of the file). In this case, concurrent backup and write
     * operations are possible (obviously the backup process needs to be faster
     * than the write operations).
     *
     * @param reuseSpace the new value
     */
    public void setReuseSpace(boolean reuseSpace) {
        this.reuseSpace = reuseSpace;
    }

    public int getRetentionTime() {
        return retentionTime;
    }

    /**
     * How long to retain old, persisted chunks, in milliseconds. Chunks that
     * are older may be overwritten once they contain no live data. The default
     * is 45000 (45 seconds). It is assumed that a file system and hard disk
     * will flush all write buffers within this time. Using a lower value might
     * be dangerous, unless the file system and hard disk flush the buffers
     * earlier. To manually flush the buffers, use
     * <code>MVStore.getFile().force(true)</code>, however please note that
     * according to various tests this does not always work as expected.
     * <p>
     * This setting is not persisted.
     *
     * @param ms how many milliseconds to retain old chunks (0 to overwrite them
     *        as early as possible)
     */
    public void setRetentionTime(int ms) {
        this.retentionTime = ms;
    }

    /**
     * Which version to retain in memory. If not set, all versions back to the
     * last stored version are retained.
     *
     * @param retainVersion the oldest version to retain
     */
    public void setRetainVersion(long retainVersion) {
        this.retainVersion = retainVersion;
    }

    public long getRetainVersion() {
        long v = retainVersion;
        if (currentStoreVersion >= -1) {
            v = Math.min(v, currentStoreVersion);
        }
        return v;
    }

    /**
     * Set the listener to be used for exceptions that occur in the background
     * thread.
     *
     * @param backgroundExceptionListener the listener
     */
    public void setBackgroundExceptionListener(
            ExceptionListener backgroundExceptionListener) {
        this.backgroundExceptionListener = backgroundExceptionListener;
    }

    public ExceptionListener getBackgroundExceptionListener() {
        return backgroundExceptionListener;
    }

    /**
     * Check whether all data can be read from this version. This requires that
     * all chunks referenced by this version are still available (not
     * overwritten).
     *
     * @param version the version
     * @return true if all data can be read
     */
    private boolean isKnownVersion(long version) {
        if (version > currentVersion || version < 0) {
            return false;
        }
        if (version == currentVersion || chunks.size() == 0) {
            // no stored data
            return true;
        }
        // need to check if a chunk for this version exists
        Chunk c = getChunkForVersion(version);
        if (c == null) {
            return false;
        }
        // also, all chunks referenced by this version
        // need to be available in the file
        MVMap<String, String> oldMeta = getMetaMap(version);
        if (oldMeta == null) {
            return false;
        }
        for (Iterator<String> it = oldMeta.keyIterator("chunk."); it.hasNext();) {
            String chunkKey = it.next();
            if (!chunkKey.startsWith("chunk.")) {
                break;
            }
            if (!meta.containsKey(chunkKey)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the estimated number of unsaved pages. The returned value is not
     * accurate, specially after rollbacks, but can be used to estimate the
     * memory usage for unsaved data.
     *
     * @return the number of unsaved pages
     */
    public int getUnsavedPageCount() {
        return unsavedPageCount;
    }

    /**
     * Increment the number of unsaved pages.
     */
    void registerUnsavedPage() {
        unsavedPageCount++;
    }

    /**
     * This method is called before writing to a map.
     */
    void beforeWrite() {
        if (currentStoreVersion >= 0) {
            // store is possibly called within store, if the meta map changed
            return;
        }
        if (unsavedPageCount > maxUnsavedPages && maxUnsavedPages > 0) {
            store(true);
        }
    }

    /**
     * Get the store version. The store version is usually used to upgrade the
     * structure of the store after upgrading the application. Initially the
     * store version is 0, until it is changed.
     *
     * @return the store version
     */
    public int getStoreVersion() {
        checkOpen();
        String x = meta.get("setting.storeVersion");
        return x == null ? 0 : Integer.parseInt(x);
    }

    /**
     * Update the store version.
     *
     * @param version the new store version
     */
    public void setStoreVersion(int version) {
        checkOpen();
        markMetaChanged();
        meta.put("setting.storeVersion", Integer.toString(version));
    }

    /**
     * Revert to the beginning of the given version. All later changes (stored
     * or not) are forgotten. All maps that were created later are closed. A
     * rollback to a version before the last stored version is immediately
     * persisted. Rollback to version 0 means all data is removed.
     *
     * @param version the version to revert to
     */
    public synchronized void rollbackTo(long version) {
        checkOpen();
        if (version == 0) {
            // special case: remove all data
            for (MVMap<?, ?> m : maps.values()) {
                m.close();
            }
            meta.clear();
            chunks.clear();
            freeSpace.clear();
            maps.clear();
            synchronized (freedPages) {
                freedPages.clear();
            }
            currentVersion = version;
            metaChanged = false;
            return;
        }
        DataUtils.checkArgument(
                isKnownVersion(version),
                "Unknown version {0}", version);
        for (MVMap<?, ?> m : maps.values()) {
            m.rollbackTo(version);
        }
        synchronized (freedPages) {
            for (long v = currentVersion; v >= version; v--) {
                if (freedPages.size() == 0) {
                    break;
                }
                freedPages.remove(v);
            }
        }
        meta.rollbackTo(version);
        metaChanged = false;
        boolean loadFromFile = false;
        Chunk last = chunks.get(lastChunkId);
        if (last != null) {
            if (last.version >= version) {
                revertTemp(version);
                loadFromFile = true;
                do {
                    last = chunks.remove(lastChunkId);
                    int len = MathUtils.roundUpInt(last.length, BLOCK_SIZE) + BLOCK_SIZE;
                    freeSpace.free(last.start, len);
                    lastChunkId--;
                } while (last.version > version && chunks.size() > 0);
                rootChunkStart = last.start;
                writeFileHeader();
                // need to write the header at the end of the file as well,
                // so that the old end header is not used
                byte[] bytes = getFileHeaderBytes();
                ByteBuffer header = ByteBuffer.allocate(BLOCK_SIZE);
                header.put(bytes);
                header.rewind();
                fileWriteCount++;
                DataUtils.writeFully(file,  fileSize, header);
                fileSize += BLOCK_SIZE;
                readFileHeader();
                readMeta();
            }
        }
        for (MVMap<?, ?> m : New.arrayList(maps.values())) {
            int id = m.getId();
            if (m.getCreateVersion() >= version) {
                m.close();
                maps.remove(id);
            } else {
                if (loadFromFile) {
                    String r = meta.get("root." + id);
                    long root = r == null ? 0 : Long.parseLong(r);
                    m.setRootPos(root, -1);
                }
            }
        }
        this.currentVersion = version;
    }

    private void revertTemp(long storeVersion) {
        synchronized (freedPages) {
            for (Iterator<Long> it = freedPages.keySet().iterator(); it.hasNext();) {
                long v = it.next();
                if (v > storeVersion) {
                    continue;
                }
                it.remove();
            }
        }
        for (MVMap<?, ?> m : maps.values()) {
            m.removeUnusedOldVersions();
        }
    }

    /**
     * Get the current version of the data. When a new store is created, the
     * version is 0.
     *
     * @return the version
     */
    public long getCurrentVersion() {
        return currentVersion;
    }

    /**
     * Get the last committed version.
     *
     * @return the version
     */
    public long getCommittedVersion() {
        return lastCommittedVersion;
    }

    /**
     * Get the number of file write operations since this store was opened.
     *
     * @return the number of write operations
     */
    public int getFileWriteCount() {
        return fileWriteCount;
    }

    /**
     * Get the number of file read operations since this store was opened.
     *
     * @return the number of read operations
     */
    public int getFileReadCount() {
        return fileReadCount;
    }

    /**
     * Get the file name, or null for in-memory stores.
     *
     * @return the file name
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Get the file header. This data is for informational purposes only. The
     * data is subject to change in future versions. The data should not be
     * modified (doing so may corrupt the store).
     *
     * @return the file header
     */
    public Map<String, String> getFileHeader() {
        return fileHeader;
    }

    /**
     * Get the file instance in use, if a file is used. The application may read
     * from the file (for example for online backup), but not write to it or
     * truncate it.
     *
     * @return the file, or null
     */
    public FileChannel getFile() {
        checkOpen();
        return file;
    }

    private void checkOpen() {
        if (closed) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_CLOSED, "This store is closed");
        }
    }

    /**
     * Rename a map.
     *
     * @param map the map
     * @param newName the new name
     */
    void renameMap(MVMap<?, ?> map, String newName) {
        checkOpen();
        DataUtils.checkArgument(map != meta,
                "Renaming the meta map is not allowed");
        if (map.getName().equals(newName)) {
            return;
        }
        DataUtils.checkArgument(
                !meta.containsKey("name." + newName),
                "A map named {0} already exists", newName);
        int id = map.getId();
        String oldName = getMapName(id);
        markMetaChanged();
        meta.remove("map." + id);
        meta.remove("name." + oldName);
        meta.put("map." + id, map.asString(newName));
        meta.put("name." + newName, Integer.toString(id));
    }

    /**
     * Get the name of the given map.
     *
     * @param id the map id
     * @return the name
     */
    String getMapName(int id) {
        String m = meta.get("map." + id);
        return DataUtils.parseMap(m).get("name");
    }

    /**
     * Store all unsaved changes, if there are any that are committed.
     */
    void storeInBackground() {
        if (closed || unsavedPageCount == 0) {
            return;
        }
        // could also store when there are many unsaved pages,
        // but according to a test it doesn't really help
        if (lastCommittedVersion >= currentVersion) {
            return;
        }
        long time = getTime();
        if (time <= lastStoreTime + writeDelay) {
            return;
        }
        try {
            store(true);
        } catch (Exception e) {
            if (backgroundExceptionListener != null) {
                backgroundExceptionListener.exceptionThrown(e);
            }
        }
    }

    /**
     * Set the read cache size in MB.
     *
     * @param mb the cache size in MB.
     */
    public void setCacheSize(long mb) {
        if (cache != null) {
            cache.setMaxMemory(mb * 1024 * 1024);
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isClosed() {
        return closed;
    }

    private void stopBackgroundThread() {
        if (backgroundThread == null) {
            return;
        }
        Thread t = backgroundThread;
        backgroundThread = null;
        synchronized (this) {
            notify();
        }
        try {
            t.join();
        } catch (Exception e) {
            // ignore
        }
    }

    public void setWriteDelay(int value) {
        writeDelay = value;
        stopBackgroundThread();
        // start the background thread if needed
        if (value > 0) {
            int sleep = Math.max(1, value / 10);
            Writer w = new Writer(this, sleep);
            Thread t = new Thread(w, "MVStore writer " + fileName);
            t.setDaemon(true);
            t.start();
            backgroundThread = t;
        }
    }

    public int getWriteDelay() {
        return writeDelay;
    }

    /**
     * A background writer to automatically store changes from time to time.
     */
    private static class Writer implements Runnable {

        private final MVStore store;
        private final int sleep;

        Writer(MVStore store, int sleep) {
            this.store = store;
            this.sleep = sleep;
        }

        @Override
        public void run() {
            while (store.backgroundThread != null) {
                synchronized (store) {
                    try {
                        store.wait(sleep);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                store.storeInBackground();
            }
        }

    }

    /**
     * A builder for an MVStore.
     */
    public static class Builder {

        private final HashMap<String, Object> config = New.hashMap();

        private Builder set(String key, Object value) {
            config.put(key, value);
            return this;
        }

        /**
         * Use the following file name. If the file does not exist, it is
         * automatically created. The parent directory already must exist.
         *
         * @param fileName the file name
         * @return this
         */
        public Builder fileName(String fileName) {
            return set("fileName", fileName);
        }

        /**
         * Encrypt / decrypt the file using the given password. This method has
         * no effect for in-memory stores. The password is passed as a char
         * array so that it can be cleared as soon as possible. Please note
         * there is still a small risk that password stays in memory (due to
         * Java garbage collection). Also, the hashed encryption key is kept in
         * memory as long as the file is open.
         *
         * @param password the password
         * @return this
         */
        public Builder encryptionKey(char[] password) {
            return set("encrypt", password);
        }

        /**
         * Open the file in read-only mode. In this case, a shared lock will be
         * acquired to ensure the file is not concurrently opened in write mode.
         * <p>
         * If this option is not used, the file is locked exclusively.
         * <p>
         * Please note a store may only be opened once in every JVM (no matter
         * whether it is opened in read-only or read-write mode), because each file
         * may be locked only once in a process.
         *
         * @return this
         */
        public Builder readOnly() {
            return set("readOnly", 1);
        }

        /**
         * Set the read cache size in MB. The default is 16 MB.
         *
         * @param mb the cache size in megabytes
         * @return this
         */
        public Builder cacheSize(int mb) {
            return set("cacheSize", mb);
        }

        /**
         * Compress data before writing using the LZF algorithm. This will save
         * about 50% of the disk space, but will slow down read and write
         * operations slightly.
         * <p>
         * This setting only affects writes; it is not necessary to enable
         * compression when reading, even if compression was enabled when
         * writing.
         *
         * @return this
         */
        public Builder compressData() {
            return set("compress", 1);
        }

        /**
         * Set the size of the write buffer, in MB (for file-based stores).
         * Changes are automatically stored if the buffer grows larger than
         * this. However, unless the changes are committed later on, they are
         * rolled back when opening the store.
         * <p>
         * The default is 4 MB.
         * <p>
         * When the value is set to 0 or lower, data is never automatically
         * stored.
         *
         * @param mb the write buffer size, in megabytes
         * @return this
         */
        public Builder writeBufferSize(int mb) {
            return set("writeBufferSize", mb);
        }

        /**
         * Set the maximum delay in milliseconds to store committed changes (for
         * file-based stores).
         * <p>
         * The default is 1000, meaning committed changes are stored after at
         * most one second.
         * <p>
         * When the value is set to -1, committed changes are only written when
         * calling the store method. When the value is set to 0, committed
         * changes are immediately written on a commit, but please note this
         * decreases performance and does still not guarantee the disk will
         * actually write the data.
         *
         * @param millis the maximum delay
         * @return this
         */
        public Builder writeDelay(int millis) {
            return set("writeDelay", millis);
        }

        /**
         * Open the store.
         *
         * @return the opened store
         */
        public MVStore open() {
            MVStore s = new MVStore(config);
            s.open();
            return s;
        }

        @Override
        public String toString() {
            return DataUtils.appendMap(new StringBuilder(), config).toString();
        }

        /**
         * Read the configuration from a string.
         *
         * @param s the string representation
         * @return the builder
         */
        public static Builder fromString(String s) {
            HashMap<String, String> config = DataUtils.parseMap(s);
            Builder builder = new Builder();
            builder.config.putAll(config);
            return builder;
        }

    }

}
