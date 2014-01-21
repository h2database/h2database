/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.h2.compress.CompressLZF;
import org.h2.compress.Compressor;
import org.h2.mvstore.cache.CacheLongKeyLIRS;
import org.h2.mvstore.type.StringDataType;
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

Documentation
- rolling docs review: at "Transactions"
- better document that writes are in background thread
- better document how to do non-unique indexes
- document pluggable store and OffHeapStore

TestMVStoreDataLoss

MVTableEngine:
- verify tests don't use the PageStore
- test and possibly allow MVCC & MULTI_THREADED
- maybe enable MVCC by default (but allow to disable it)
- use StreamStore to avoid deadlocks
- config options for compression and page size (maybe combined)

TransactionStore:

MVStore:
- automated 'kill process' and 'power failure' test
- update checkstyle
- feature to auto-compact from time to time and on close
- test and possibly improve compact operation (for large dbs)
- possibly split chunk metadata into immutable and mutable
- compact: avoid processing pages using a counting bloom filter
- defragment (re-creating maps, specially those with small pages)
- chunk header: store changed chunk data as row; maybe after the root
- chunk checksum (header, last page, 2 bytes per page?)
- maybe let a chunk point to a list of potential next chunks
    (so no fixed location header is needed), similar to a skip list
- store number of write operations per page (maybe defragment
    if much different than count)
- r-tree: nearest neighbor search
- use a small object value cache (StringCache), test on Android
    for default serialization
- MVStoreTool.dump: dump values (using a callback)
- ensure data is overwritten eventually if the system doesn't have a
    real-time clock (Raspberry Pi) and if there are few writes per startup
- SSD-friendly write (always in blocks of 4 MB / 1 second?)
- close the file on out of memory or disk write error (out of disk space or so)
- implement a sharded map (in one store, multiple stores)
    to support concurrent updates and writes, and very large maps
- to save space when persisting very small transactions,
    use a transaction log where only the deltas are stored
- serialization for lists, sets, sets, sorted sets, maps, sorted maps
- maybe rename 'rollback' to 'revert' to distinguish from transactions
- support other compression algorithms (deflate, LZ4,...)
- support opening (existing) maps by id
- more consistent null handling (keys/values sometimes may be null)
- autocommit (to avoid having to call commit,
    as it could be called too often or it is easily forgotten)
- remove features that are not really needed; simplify the code
    possibly using a separate layer or tools
    (retainVersion?)
- rename "store" to "save", as "store" is used in "storeVersion"
- MVStoreTool.dump should dump the data if possible;
    possibly using a callback for serialization
- optional pluggable checksum mechanism (per page), which
    requires that everything is a page (including headers)
- rename setStoreVersion to setDataVersion or similar
- to save space for small chunks, combine the last partial
    block with the header
- temporary file storage
- simple rollback method (rollback to last committed version)
- MVMap to implement SortedMap, then NavigableMap
- Test with OSGi
- storage that splits database into multiple files,
    to speed up compact and allow using trim
    (by truncating / deleting empty files)
- add new feature to the file system API to avoid copying data
    (reads that returns a ByteBuffer instead of writing into one)
    for memory mapped files and off-heap storage
- do we need to store a dummy chunk entry in the chunk itself?
    currently yes, as some fields are not set in the chunk header
- support log structured merge style operations (blind writes)
    using one map per level plus bloom filter
- have a strict call order MVStore -> MVMap -> Page -> FileStore
- autocommit mode (default) and manual mode
- manual mode: combine commit and store;
    rollback only to chunk
- rename writeDelay to commitDelay, default 1 s
- rollback() to rollback to the latest commit; throws exception
    in autocommit mode
- fix documentation (including examples)
- autocommit commits, stores, and compacts from time to time;
    the background thread should wait at least 90% of the
    configured write delay to store changes
- currently, uncommitted changes are stored if there are many transient changes,
    and rolled back when opening - is this really needed?
- compact* should also store uncommitted changes (if there are any)
- write a LSM-tree (log structured merge tree) utility on top of the MVStore

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
     * The block size (physical sector size) of the disk. The store header is
     * written twice, one copy in each block, to ensure it survives a crash.
     */
    static final int BLOCK_SIZE = 4 * 1024;

    private static final int FORMAT_WRITE = 1;
    private static final int FORMAT_READ = 1;

    /**
     * The background thread, if any.
     */
    volatile BackgroundWriterThread backgroundWriterThread;

    private volatile boolean reuseSpace = true;

    private boolean closed;

    private FileStore fileStore;

    private final int pageSplitSize;

    private long rootChunkStart;

    /**
     * The page cache. The default size is 16 MB, and the average size is 2 KB.
     * It is split in 16 segments. The stack move distance is 2% of the expected
     * number of entries.
     */
    private CacheLongKeyLIRS<Page> cache;

    private int lastChunkId;

    /**
     * The map of chunks.
     */
    private final ConcurrentHashMap<Integer, Chunk> chunks = new ConcurrentHashMap<Integer, Chunk>();

    /**
     * The map of temporarily freed storage space caused by freed pages. The key
     * is the unsaved version, the value is the map of chunks. The maps contains
     * the number of freed entries per chunk. Access is synchronized.
     */
    private final ConcurrentHashMap<Long, HashMap<Integer, Chunk>> freedPageSpace =
            new ConcurrentHashMap<Long, HashMap<Integer, Chunk>>();

    /**
     * The metadata map. Write access to this map needs to be synchronized on
     * the store.
     */
    private MVMapConcurrent<String, String> meta;

    private final ConcurrentHashMap<Integer, MVMap<?, ?>> maps = new ConcurrentHashMap<Integer, MVMap<?, ?>>();

    private HashMap<String, String> storeHeader = New.hashMap();

    private WriteBuffer writeBuffer;

    private int lastMapId;

    private int versionsToKeep = 5;

    /**
     * Whether to compress new pages. Even if disabled, the store may contain
     * (old) compressed pages.
     */
    private final boolean compress;

    private final Compressor compressor = new CompressLZF();

    private final UncaughtExceptionHandler backgroundExceptionHandler;

    private long currentVersion;

    /**
     * The version of the last stored chunk.
     */
    private long lastStoredVersion;

    /**
     * The estimated number of unsaved pages (this number may not be completely
     * accurate, because it may be changed concurrently, and because temporary
     * pages are counted)
     */
    private int unsavedPageCount;
    private int autoCommitPageCount;
    private boolean saveNeeded;

    /**
     * The time the store was created, in milliseconds since 1970.
     */
    private long creationTime;
    private int retentionTime;

    private long lastCommitTime;

    /**
     * The earliest chunk to retain, if any.
     */
    private Chunk retainChunk;

    /**
     * The version of the current store operation (if any).
     */
    private volatile long currentStoreVersion = -1;

    private Thread currentStoreThread;

    private volatile boolean metaChanged;

    /**
     * The delay in milliseconds to automatically commit and write changes.
     */
    private int autoCommitDelay;

    /**
     * Create and open the store.
     *
     * @param config the configuration to use
     * @throws IllegalStateException if the file is corrupt, or an exception
     *             occurred while opening
     * @throws IllegalArgumentException if the directory does not exist
     */
    MVStore(HashMap<String, Object> config) {
        this.compress = config.containsKey("compress");
        String fileName = (String) config.get("fileName");
        Object o = config.get("pageSplitSize");
        if (o == null) {
            pageSplitSize = fileName == null ? 4 * 1024 : 16 * 1024;
        } else {
            pageSplitSize = (Integer) o;
        }
        o = config.get("backgroundExceptionHandler");
        this.backgroundExceptionHandler = (UncaughtExceptionHandler) o;
        meta = new MVMapConcurrent<String, String>(StringDataType.INSTANCE,
                StringDataType.INSTANCE);
        HashMap<String, String> c = New.hashMap();
        c.put("id", "0");
        c.put("createVersion", Long.toString(currentVersion));
        meta.init(this, c);
        fileStore = (FileStore) config.get("fileStore");
        if (fileName == null && fileStore == null) {
            cache = null;
            return;
        }
        if (fileStore == null) {
            fileStore = new FileStore();
        }
        retentionTime = fileStore.getDefaultRetentionTime();
        boolean readOnly = config.containsKey("readOnly");
        o = config.get("cacheSize");
        int mb = o == null ? 16 : (Integer) o;
        if (mb > 0) {
            int maxMemoryBytes = mb * 1024 * 1024;
            int averageMemory = Math.max(10, pageSplitSize / 2);
            int segmentCount = 16;
            int stackMoveDistance = maxMemoryBytes / averageMemory * 2 / 100;
            cache = new CacheLongKeyLIRS<Page>(maxMemoryBytes, averageMemory,
                    segmentCount, stackMoveDistance);
        }
        o = config.get("autoCommitBufferSize");
        int kb = o == null ? 512 : (Integer) o;
        // 19 KB memory is about 1 KB storage
        int autoCommitBufferSize = kb * 1024 * 19;
        int div = pageSplitSize;
        autoCommitPageCount = autoCommitBufferSize / (div == 0 ? 1 : div);
        char[] encryptionKey = (char[]) config.get("encryptionKey");
        try {
            fileStore.open(fileName, readOnly, encryptionKey);
            if (fileStore.size() == 0) {
                creationTime = 0;
                creationTime = getTime();
                lastCommitTime = creationTime;
                storeHeader.put("H", "3");
                storeHeader.put("blockSize", "" + BLOCK_SIZE);
                storeHeader.put("format", "" + FORMAT_WRITE);
                storeHeader.put("creationTime", "" + creationTime);
                writeStoreHeader();
            } else {
                readStoreHeader();
                long format = DataUtils.parseLong(storeHeader.get("format"), 0);
                if (format > FORMAT_WRITE && !fileStore.isReadOnly()) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_UNSUPPORTED_FORMAT,
                            "The write format {0} is larger than the supported format {1}, " +
                            "and the file was not opened in read-only mode",
                            format, FORMAT_WRITE);
                }
                format = DataUtils.parseLong(storeHeader.get("formatRead"), format);
                if (format > FORMAT_READ) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_UNSUPPORTED_FORMAT,
                            "The read format {0} is larger than the supported format {1}",
                            format, FORMAT_READ);
                }
                if (rootChunkStart > 0) {
                    readMeta();
                }
            }
        } catch (IllegalStateException e) {
            try {
                closeStore(false);
            } catch (Exception e2) {
                // ignore
            }
            throw e;
        } finally {
            if (encryptionKey != null) {
                Arrays.fill(encryptionKey, (char) 0);
            }
        }
        lastCommitTime = getTime();

        // setAutoCommitDelay starts the thread, but only if
        // the parameter is different from the old value
        o = config.get("autoCommitDelay");
        int delay = o == null ? 1000 : (Integer) o;
        setAutoCommitDelay(delay);
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
        return new MVStore(config);
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
    <T extends MVMap<?, ?>> T openMapVersion(long version, int mapId,
            MVMap<?, ?> template) {
        MVMap<String, String> oldMeta = getMetaMap(version);
        String r = oldMeta.get("root." + mapId);
        long rootPos = DataUtils.parseLong(r, 0);
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
    public synchronized <M extends MVMap<K, V>, K, V> M openMap(String name, MVMap.MapBuilder<M, K, V> builder) {
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
            markMetaChanged();
            meta.put("map." + id, map.asString(name));
            meta.put("name." + name, Integer.toString(id));
            root = 0;
        }
        map.setRootPos(root, -1);
        maps.put(id, map);
        return map;
    }

    /**
     * Get the set of all map names.
     *
     * @return the set of names
     */
    public synchronized Set<String> getMapNames() {
        HashSet<String> set = New.hashSet();
        checkOpen();
        for (Iterator<String> it = meta.keyIterator("name."); it.hasNext();) {
            String x = it.next();
            if (!x.startsWith("name.")) {
                break;
            }
            set.add(x.substring("name.".length()));
        }
        return set;
    }

    /**
     * Get the metadata map. This data is for informational purposes only. The
     * data is subject to change in future versions.
     * <p>
     * The data in this map should not be modified (changing system data may
     * corrupt the store). If modifications are needed, they need be
     * synchronized on the store.
     * <p>
     * The metadata map contains the following entries:
     * <pre>
     * chunk.{chunkId} = {chunk metadata}
     * name.{name} = {mapId}
     * map.{mapId} = {map metadata}
     * root.{mapId} = {root position}
     * setting.storeVersion = {version}
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
            if (x == null) {
                return null;
            } else if (x.version <= version) {
                return x;
            }
        }
    }

    /**
     * Check whether a given map exists.
     *
     * @param name the map name
     * @return true if it exists
     */
    public boolean hasMap(String name) {
        return meta.containsKey("name." + name);
    }

    private void markMetaChanged() {
        // changes in the metadata alone are usually not detected, as the meta
        // map is changed after storing
        metaChanged = true;
    }

    private synchronized void readMeta() {
        chunks.clear();
        Chunk header = readChunkHeader(rootChunkStart);
        if (header.start == Long.MAX_VALUE) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Chunk {0} is invalid", header.id);
        }
        lastChunkId = header.id;
        chunks.put(header.id, header);
        meta.setRootPos(header.metaRootPos, -1);
        String s = meta.get("chunk." + lastChunkId);
        Chunk h2 = Chunk.fromString(s);
        h2.start = header.start;
        h2.length = header.length;
        h2.metaRootPos = header.metaRootPos;
        h2.pageCount = header.pageCount;
        h2.pageCountLive = header.pageCountLive;
        h2.maxLength = header.maxLength;
        h2.maxLengthLive = header.maxLengthLive;
        chunks.put(header.id, h2);
        // we can load the chunk in any order,
        // because loading chunk metadata
        // might recursively load another chunk
        for (Iterator<String> it = meta.keyIterator("chunk."); it.hasNext();) {
            s = it.next();
            if (!s.startsWith("chunk.")) {
                break;
            }
            s = meta.get(s);
            Chunk c = Chunk.fromString(s);
            if (!chunks.containsKey(c.id)) {
                if (c.start == Long.MAX_VALUE) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_FILE_CORRUPT,
                            "Chunk {0} is invalid", c.id);
                }
                chunks.put(c.id, c);
            }
        }
        // build the free space list
        for (Chunk c : chunks.values()) {
            if (c.pageCountLive == 0) {
                // remove this chunk in the next save operation
                registerFreePage(currentVersion, c.id, 0, 0);
            }
            int len = MathUtils.roundUpInt(c.length, BLOCK_SIZE) + BLOCK_SIZE;
            fileStore.markUsed(c.start, len);
        }
    }

    private void readStoreHeader() {
        // we don't have a valid header yet
        currentVersion = -1;
        // we don't know which chunk is the newest
        long newestChunk = -1;
        // read the last block of the file, and then the two first blocks
        ByteBuffer buffLastBlock = fileStore.readFully(fileStore.size()
                - BLOCK_SIZE, BLOCK_SIZE);
        ByteBuffer buffFirst2Blocks = fileStore.readFully(0, BLOCK_SIZE * 2);
        ByteBuffer buff = ByteBuffer.allocate(3 * BLOCK_SIZE);
        buff.put(buffLastBlock);
        buff.put(buffFirst2Blocks);
        for (int i = 0; i < 3 * BLOCK_SIZE; i += BLOCK_SIZE) {
            String s = new String(buff.array(), i, BLOCK_SIZE, DataUtils.UTF8)
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
                continue;
            }
            s = s.substring(0, s.lastIndexOf("fletcher") - 1);
            byte[] bytes = s.getBytes(DataUtils.UTF8);
            int checksum = DataUtils.getFletcher32(bytes, bytes.length / 2 * 2);
            if (check != checksum) {
                continue;
            }
            long chunk = Long.parseLong(m.get("chunk"));
            if (chunk > newestChunk) {
                newestChunk = chunk;
                storeHeader = m;
                rootChunkStart = Long.parseLong(m.get("rootChunk"));
                creationTime = Long.parseLong(m.get("creationTime"));
                lastMapId = Integer.parseInt(m.get("lastMapId"));
                currentVersion = Long.parseLong(m.get("version"));
            }
        }
        if (currentVersion < 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Store header is corrupt: {0}", fileStore);
        }
        setWriteVersion(currentVersion);
        lastStoredVersion = -1;
    }

    private byte[] getStoreHeaderBytes() {
        StringBuilder buff = new StringBuilder();
        storeHeader.put("lastMapId", "" + lastMapId);
        storeHeader.put("chunk", "" + lastChunkId);
        storeHeader.put("rootChunk", "" + rootChunkStart);
        storeHeader.put("version", "" + currentVersion);
        DataUtils.appendMap(buff, storeHeader);
        byte[] bytes = buff.toString().getBytes(DataUtils.UTF8);
        int checksum = DataUtils.getFletcher32(bytes, bytes.length / 2 * 2);
        DataUtils.appendMap(buff, "fletcher", Integer.toHexString(checksum));
        bytes = buff.toString().getBytes(DataUtils.UTF8);
        if (bytes.length > BLOCK_SIZE) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "Store header too large: {0}", buff);
        }
        return bytes;
    }

    private void writeStoreHeader() {
        byte[] bytes = getStoreHeaderBytes();
        ByteBuffer header = ByteBuffer.allocate(2 * BLOCK_SIZE);
        header.put(bytes);
        header.position(BLOCK_SIZE);
        header.put(bytes);
        header.rewind();
        fileStore.writeFully(0, header);
    }

    /**
     * Close the file and the store. Unsaved changes are written to disk first.
     */
    public void close() {
        if (closed) {
            return;
        }
        if (fileStore != null && !fileStore.isReadOnly()) {
            stopBackgroundThread();
            if (hasUnsavedChanges()) {
                commitAndSave();
            }
        }
        closeStore(true);
    }

    /**
     * Close the file and the store, without writing anything. This will stop
     * the background thread. This method ignores all errors.
     */
    public void closeImmediately() {
        try {
            closeStore(false);
        } catch (Exception e) {
            if (backgroundExceptionHandler != null) {
                backgroundExceptionHandler.uncaughtException(null, e);
            }
        }
    }

    private void closeStore(boolean shrinkIfPossible) {
        if (closed) {
            return;
        }
        // can not synchronize on this yet, because
        // the thread also synchronized on this, which
        // could result in a deadlock
        stopBackgroundThread();
        closed = true;
        if (fileStore == null) {
            return;
        }
        synchronized (this) {
            if (shrinkIfPossible) {
                shrinkFileIfPossible(0);
            }
            // release memory early - this is important when called
            // because of out of memory
            cache = null;
            for (MVMap<?, ?> m : New.arrayList(maps.values())) {
                m.close();
            }
            meta = null;
            chunks.clear();
            maps.clear();
            try {
                fileStore.close();
            } finally {
                fileStore = null;
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
        int chunkId = DataUtils.getPageChunkId(pos);
        Chunk c = chunks.get(chunkId);
        if (c == null) {
            if (!Thread.holdsLock(this)) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_INTERNAL,
                        "Unsynchronized metadata read");
            }
            String s = meta.get("chunk." + chunkId);
            if (s == null) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Chunk {0} not found", chunkId);
            }
            c = Chunk.fromString(s);
            if (c.start == Long.MAX_VALUE) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Chunk {0} is invalid", chunkId);
            }
            chunks.put(c.id, c);
        }
        return c;
    }

    private void setWriteVersion(long version) {
        for (MVMap<?, ?> map : maps.values()) {
            map.setWriteVersion(version);
        }
        meta.setWriteVersion(version);
    }

    /**
     * Commit the changes.
     * <p>
     * For in-memory stores, this method increments the version.
     * <p>
     * For persistent stores, it also writes changes to disk. It does nothing if
     * there are no unsaved changes, and returns the old version. It is not
     * necessary to call this method when auto-commit is enabled (the default
     * setting), as in this case it is automatically called from time to time or
     * when enough changes have accumulated. However, it may still be called to
     * flush all changes to disk.
     *
     * @return the new version
     */
    public long commit() {
        if (fileStore != null) {
            return commitAndSave();
        }
        long v = ++currentVersion;
        setWriteVersion(v);
        return v;
    }

    /**
     * Commit all changes and persist them to disk. This method does nothing if
     * there are no unsaved changes, otherwise it increments the current version
     * and stores the data (for file based stores).
     * <p>
     * At most one store operation may run at any time.
     *
     * @return the new version (incremented if there were changes)
     */
    private synchronized long commitAndSave() {
        if (closed) {
            return currentVersion;
        }
        if (fileStore == null) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_WRITING_FAILED,
                    "This is an in-memory store");
        }
        if (currentStoreVersion >= 0) {
            // store is possibly called within store, if the meta map changed
            return currentVersion;
        }
        if (!hasUnsavedChanges()) {
            return currentVersion;
        }
        if (fileStore.isReadOnly()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_WRITING_FAILED, "This store is read-only");
        }
        try {
            currentStoreVersion = currentVersion;
            currentStoreThread = Thread.currentThread();
            return storeNow();
        } finally {
            // in any case reset the current store version,
            // to allow closing the store
            currentStoreVersion = -1;
            currentStoreThread = null;
        }
    }

    private long storeNow() {
        int currentUnsavedPageCount = unsavedPageCount;
        long storeVersion = currentStoreVersion;
        long version = ++currentVersion;
        setWriteVersion(version);
        long time = getTime();
        lastCommitTime = time;
        retainChunk = null;

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
            m.setWriteVersion(version);
            long v = m.getVersion();
            if (m.getCreateVersion() > storeVersion) {
                // the map was created after storing started
                continue;
            }
            if (v >= 0 && v >= lastStoredVersion) {
                m.waitUntilWritten(storeVersion);
                MVMap<?, ?> r = m.openVersion(storeVersion);
                if (r.getRoot().getPos() == 0) {
                    changed.add(r);
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
        Set<Chunk> removedChunks = applyFreedSpace(storeVersion, time);
        WriteBuffer buff = getWriteBuffer();
        // need to patch the header later
        c.writeHeader(buff);
        c.maxLength = 0;
        c.maxLengthLive = 0;
        for (MVMap<?, ?> m : changed) {
            Page p = m.getRoot();
            if (p.getTotalCount() > 0) {
                p.writeUnsavedRecursive(c, buff);
                long root = p.getPos();
                meta.put("root." + m.getId(), "" + root);
            }
        }
        meta.put("chunk." + c.id, c.asString());
        meta.setWriteVersion(version);

        // this will (again) modify maxLengthLive, but
        // the correct value is written in the chunk header
        Page metaRoot = meta.getRoot();
        metaRoot.writeUnsavedRecursive(c, buff);

        int chunkLength = buff.position();

        // round to the next block,
        // and one additional block for the store header
        int length = MathUtils.roundUpInt(chunkLength, BLOCK_SIZE) + BLOCK_SIZE;
        buff.limit(length);

        // free up the space of unused chunks now
        for (Chunk x : removedChunks) {
            int len = MathUtils.roundUpInt(x.length, BLOCK_SIZE) + BLOCK_SIZE;
            fileStore.free(x.start, len);
        }

        // the length of the file that is still in use
        // (not necessarily the end of the file)
        long end = getFileLengthInUse();
        long filePos;
        if (reuseSpace) {
            filePos = fileStore.allocate(length);
        } else {
            filePos = end;
            fileStore.markUsed(end, length);
        }
        // end is not necessarily the end of the file
        boolean storeAtEndOfFile = filePos + length >= fileStore.size();

        c.start = filePos;
        c.length = chunkLength;
        c.metaRootPos = metaRoot.getPos();
        buff.position(0);
        c.writeHeader(buff);
        rootChunkStart = filePos;
        revertTemp(storeVersion);

        buff.position(buff.limit() - BLOCK_SIZE);
        byte[] header = getStoreHeaderBytes();
        buff.put(header);
        // fill the header with zeroes
        buff.put(new byte[BLOCK_SIZE - header.length]);

        buff.position(0);
        fileStore.writeFully(filePos, buff.getBuffer());

        releaseWriteBuffer(buff);

        // overwrite the header if required
        if (!storeAtEndOfFile) {
            writeStoreHeader();
            shrinkFileIfPossible(1);
        }

        for (MVMap<?, ?> m : changed) {
            Page p = m.getRoot();
            if (p.getTotalCount() > 0) {
                p.writeEnd();
            }
        }
        metaRoot.writeEnd();

        // some pages might have been changed in the meantime (in the newest
        // version)
        unsavedPageCount = Math.max(0, unsavedPageCount
                - currentUnsavedPageCount);

        metaChanged = false;
        lastStoredVersion = storeVersion;

        return version;
    }

    /**
     * Get a buffer for writing. This caller must synchronize on the store
     * before calling the method and until after using the buffer.
     *
     * @return the buffer
     */
    private WriteBuffer getWriteBuffer() {
        WriteBuffer buff;
        if (writeBuffer != null) {
            buff = writeBuffer;
            buff.clear();
        } else {
            buff = new WriteBuffer();
        }
        return buff;
    }

    /**
     * Release a buffer for writing. This caller must synchronize on the store
     * before calling the method and until after using the buffer.
     *
     * @param buff the buffer than can be re-used
     */
    private void releaseWriteBuffer(WriteBuffer buff) {
        if (buff.capacity() <= 4 * 1024 * 1024) {
            writeBuffer = buff;
        }
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
     * Apply the freed space to the chunk metadata. The metadata is updated, but
     * freed chunks are not removed yet.
     *
     * @param storeVersion apply up to the given version
     * @return the set of completely freed chunks (might be empty)
     */
    private Set<Chunk> applyFreedSpace(long storeVersion, long time) {
        Set<Chunk> removedChunks = New.hashSet();
        while (true) {
            ArrayList<Chunk> modified = New.arrayList();
            Iterator<Entry<Long, HashMap<Integer, Chunk>>> it;
            it = freedPageSpace.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Long, HashMap<Integer, Chunk>> e = it.next();
                long v = e.getKey();
                if (v > storeVersion) {
                    continue;
                }
                HashMap<Integer, Chunk> freed = e.getValue();
                for (Chunk f : freed.values()) {
                    Chunk c = chunks.get(f.id);
                    if (c == null) {
                        // already removed
                        continue;
                    }
                    // no need to synchronize, as old entries
                    // are not concurrently modified
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
                        meta.put("chunk." + c.id, c.asString());
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
        return removedChunks;
    }

    /**
     * Shrink the file if possible, and if at least a given percentage can be
     * saved.
     *
     * @param minPercent the minimum percentage to save
     */
    private void shrinkFileIfPossible(int minPercent) {
        long end = getFileLengthInUse();
        long fileSize = fileStore.size();
        if (end >= fileSize) {
            return;
        }
        if (minPercent > 0 && fileSize - end < BLOCK_SIZE) {
            return;
        }
        int savedPercent = (int) (100 - (end * 100 / fileSize));
        if (savedPercent < minPercent) {
            return;
        }
        fileStore.truncate(end);
    }

    /**
     * Get the position of the last used byte.
     *
     * @return the position
     */
    private long getFileLengthInUse() {
        long size = 2 * BLOCK_SIZE;
        for (Chunk c : chunks.values()) {
            long x = c.start + c.length;
            size = Math.max(size, MathUtils.roundUpLong(x, BLOCK_SIZE)
                    + BLOCK_SIZE);
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
                if (v >= 0 && v > lastStoredVersion) {
                    return true;
                }
            }
        }
        return false;
    }

    private Chunk readChunkHeader(long start) {
        ByteBuffer buff = fileStore.readFully(start, 40);
        return Chunk.fromHeader(buff, start);
    }

    /**
     * Compact the store by moving all chunks next to each other, if there is
     * free space between chunks. This might temporarily double the file size.
     * Chunks are overwritten irrespective of the current retention time. Before
     * overwriting chunks and before resizing the file, syncFile() is called.
     *
     * @return if anything was written
     */
    public synchronized boolean compactMoveChunks() {
        checkOpen();
        if (chunks.size() == 0) {
            // nothing to do
            return false;
        }
        int oldRetentionTime = retentionTime;
        retentionTime = 0;
        long time = getTime();
        ArrayList<Chunk> free = New.arrayList();
        for (Chunk c : chunks.values()) {
            if (c.maxLengthLive == 0) {
                if (canOverwriteChunk(c, time)) {
                    free.add(c);
                }
            }
        }
        for (Chunk c : free) {
            chunks.remove(c.id);
            markMetaChanged();
            meta.remove("chunk." + c.id);
            int length = MathUtils.roundUpInt(c.length, BLOCK_SIZE)
                    + BLOCK_SIZE;
            fileStore.free(c.start, length);
        }
        if (fileStore.getFillRate() == 100) {
            return false;
        }
        long firstFree = fileStore.getFirstFree();
        ArrayList<Chunk> move = New.arrayList();
        for (Chunk c : chunks.values()) {
            if (c.start > firstFree) {
                move.add(c);
            }
        }
        for (Chunk c : move) {
            WriteBuffer buff = getWriteBuffer();
            int length = MathUtils.roundUpInt(c.length, BLOCK_SIZE)
                    + BLOCK_SIZE;
            buff.limit(length);
            ByteBuffer buff2 = fileStore.readFully(c.start, length);
            buff.put(buff2);
            long end = getFileLengthInUse();
            fileStore.markUsed(end, length);
            fileStore.free(c.start, length);
            c.start = end;
            buff.position(0);
            c.writeHeader(buff);
            buff.position(buff.limit() - BLOCK_SIZE);
            byte[] header = getStoreHeaderBytes();
            buff.put(header);
            // fill the header with zeroes
            buff.put(new byte[BLOCK_SIZE - header.length]);
            buff.position(0);
            fileStore.writeFully(end, buff.getBuffer());
            releaseWriteBuffer(buff);
            markMetaChanged();
            meta.put("chunk." + c.id, c.asString());
        }
        boolean oldReuse = reuseSpace;

        // update the metadata (store at the end of the file)
        reuseSpace = false;
        commitAndSave();

        sync();

        // now re-use the empty space
        reuseSpace = true;
        for (Chunk c : move) {
            if (!chunks.containsKey(c.id)) {
                // already removed during the
                // previous store operation
                continue;
            }
            WriteBuffer buff = getWriteBuffer();
            int length = MathUtils.roundUpInt(c.length, BLOCK_SIZE)
                    + BLOCK_SIZE;
            buff.limit(length);
            ByteBuffer buff2 = fileStore.readFully(c.start, length);
            buff.put(buff2);
            long pos = fileStore.allocate(length);
            fileStore.free(c.start, length);
            buff.position(0);
            c.start = pos;
            c.writeHeader(buff);
            buff.position(buff.limit() - BLOCK_SIZE);
            byte[] header = getStoreHeaderBytes();
            buff.put(header);
            // fill the header with zeroes
            buff.put(new byte[BLOCK_SIZE - header.length]);
            buff.position(0);
            fileStore.writeFully(pos, buff.getBuffer());
            releaseWriteBuffer(buff);
            markMetaChanged();
            meta.put("chunk." + c.id, c.asString());
        }

        // update the metadata (within the file)
        commitAndSave();
        sync();
        shrinkFileIfPossible(0);

        reuseSpace = oldReuse;
        retentionTime = oldRetentionTime;

        return true;
    }

    /**
     * Force all stored changes to be written to the storage. The default
     * implementation calls FileChannel.force(true).
     */
    public void sync() {
        fileStore.sync();
    }

    /**
     * Try to reduce the file size by re-writing partially full chunks. Chunks
     * with a low number of live items are re-written. If the current fill rate
     * is higher than the target fill rate, nothing is done.
     * <p>
     * Only data of open maps can be moved. For maps that are not open, the old
     * chunk is still referenced. Therefore, it is recommended to open all maps
     * before calling this method.
     *
     * @param fillRate the minimum percentage of live entries
     * @return if anything was written
     */
    public synchronized boolean compact(int fillRate) {
        checkOpen();
        if (chunks.size() == 0) {
            // nothing to do
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
        // the fill rate of all chunks combined
        int totalChunkFillRate = (int) (100 * maxLengthLiveSum / maxLengthSum);

        if (totalChunkFillRate > fillRate) {
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
                return new Integer(o1.collectPriority)
                        .compareTo(o2.collectPriority);
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

        commitAndSave();
        return true;
    }

    private void copyLive(Chunk chunk, ArrayList<Chunk> old) {
        ByteBuffer buff = fileStore.readFully(chunk.start, chunk.length);
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
                // pages of maps that are not open or that have been removed
                // later on are not moved (for maps that are not open, the live
                // counter is not decremented, so the chunk is not removed)
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
                } else if (p.getPos() == 0) {
                    // temporarily changed - ok
                    // TODO move old data if there is an uncommitted change?
                } else {
                    Chunk c = getChunk(p.getPos());
                    if (old.contains(c)) {
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
        if (pos == 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT, "Position 0");
        }
        Page p = cache == null ? null : cache.get(pos);
        if (p == null) {
            Chunk c = getChunk(pos);
            long filePos = c.start;
            filePos += DataUtils.getPageOffset(pos);
            if (filePos < 0) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Negative position {0}", filePos);
            }
            p = Page.read(fileStore, map, pos, filePos, fileStore.size());
            cachePage(pos, p, p.getMemory());
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

        // This could result in a cache miss if the operation is rolled back,
        // but we don't optimize for rollback.
        // We could also keep the page in the cache, as somebody could read it.
        if (cache != null) {
            cache.remove(pos);
        }

        Chunk c = getChunk(pos);
        long version = currentVersion;
        if (map == meta && currentStoreVersion >= 0) {
            if (Thread.currentThread() == currentStoreThread) {
                // if the meta map is modified while storing,
                // then this freed page needs to be registered
                // with the stored chunk, so that the old chunk
                // can be re-used
                version = currentStoreVersion;
            }
        }
        registerFreePage(version, c.id, DataUtils.getPageMaxLength(pos), 1);
    }

    private void registerFreePage(long version, int chunkId,
            long maxLengthLive, int pageCount) {
        HashMap<Integer, Chunk> freed = freedPageSpace.get(version);
        if (freed == null) {
            freed = New.hashMap();
            HashMap<Integer, Chunk> f2 = freedPageSpace.putIfAbsent(version,
                    freed);
            if (f2 != null) {
                freed = f2;
            }
        }
        // synchronize, because pages could be freed concurrently
        synchronized (freed) {
            Chunk f = freed.get(chunkId);
            if (f == null) {
                f = new Chunk(chunkId);
                freed.put(chunkId, f);
            }
            f.maxLengthLive -= maxLengthLive;
            f.pageCountLive -= pageCount;
        }
    }

    Compressor getCompressor() {
        return compressor;
    }

    boolean getCompress() {
        return compress;
    }

    public int getPageSplitSize() {
        return pageSplitSize;
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
     * are older may be overwritten once they contain no live data.
     * <p>
     * The default value is 45000 (45 seconds) when using the default file
     * store. It is assumed that a file system and hard disk will flush all
     * write buffers within this time. Using a lower value might be dangerous,
     * unless the file system and hard disk flush the buffers earlier. To
     * manually flush the buffers, use
     * <code>MVStore.getFile().force(true)</code>, however please note that
     * according to various tests this does not always work as expected
     * depending on the operating system and hardware.
     * <p>
     * This setting is not persisted.
     *
     * @param ms how many milliseconds to retain old chunks (0 to overwrite them
     *            as early as possible)
     */
    public void setRetentionTime(int ms) {
        this.retentionTime = ms;
    }

    /**
     * How many versions to retain for in-memory stores. If not set, 5 versions
     * are retained.
     *
     * @param count the number of versions to keep
     */
    public void setVersionsToKeep(int count) {
        this.versionsToKeep = count;
    }

    /**
     * Get the oldest version to retain in memory.
     *
     * @return the version
     */
    public long getVersionsToKeep() {
        return versionsToKeep;
    }

    /**
     * Get the oldest version to retain in memory, which is the manually set
     * retain version, or the current store version (whatever is older).
     *
     * @return the version
     */
    long getOldestVersionToKeep() {
        long v = currentVersion;
        if (fileStore == null) {
            return v - versionsToKeep;
        }
        long storeVersion = currentStoreVersion;
        if (storeVersion > -1) {
            v = Math.min(v, storeVersion);
        }
        return v;
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
     * Increment the number of unsaved pages.
     */
    void registerUnsavedPage() {
        int count = ++unsavedPageCount;
        if (count > autoCommitPageCount && autoCommitPageCount > 0) {
            saveNeeded = true;
        }
    }

    /**
     * This method is called before writing to a map.
     */
    void beforeWrite() {
        if (saveNeeded) {
            saveNeeded = false;
            commitAndSave();
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
    public synchronized void setStoreVersion(int version) {
        checkOpen();
        markMetaChanged();
        meta.put("setting.storeVersion", Integer.toString(version));
    }

    /**
     * Revert to the beginning of the current version, reverting all uncommitted
     * changes.
     */
    public void rollback() {
        rollbackTo(currentVersion);
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
            if (fileStore != null) {
                fileStore.clear();
            }
            maps.clear();
            freedPageSpace.clear();
            currentVersion = version;
            setWriteVersion(version);
            metaChanged = false;
            return;
        }
        DataUtils.checkArgument(
                isKnownVersion(version),
                "Unknown version {0}", version);
        for (MVMap<?, ?> m : maps.values()) {
            m.rollbackTo(version);
        }
        for (long v = currentVersion; v >= version; v--) {
            if (freedPageSpace.size() == 0) {
                break;
            }
            freedPageSpace.remove(v);
        }
        meta.rollbackTo(version);
        metaChanged = false;
        boolean loadFromFile = false;
        // get the largest chunk with a version
        // higher or equal the requested version
        int removeChunksNewerThan = -1;
        for (int chunkId = lastChunkId;; chunkId--) {
            Chunk x = chunks.get(chunkId);
            if (x == null) {
                break;
            } else if (x.version >= version) {
                removeChunksNewerThan = x.id;
            }
        }
        if (removeChunksNewerThan >= 0 && lastChunkId > removeChunksNewerThan) {
            revertTemp(version);
            loadFromFile = true;
            Chunk last = null;
            while (true) {
                last = chunks.get(lastChunkId);
                if (last == null) {
                    break;
                } else if (last.id <= removeChunksNewerThan) {
                    break;
                }
                chunks.remove(lastChunkId);
                int len = MathUtils.roundUpInt(last.length, BLOCK_SIZE) + BLOCK_SIZE;
                fileStore.free(last.start, len);
                lastChunkId--;
            }
            rootChunkStart = last.start;
            writeStoreHeader();
            // need to write the header at the end of the file as well,
            // so that the old end header is not used
            byte[] bytes = getStoreHeaderBytes();
            ByteBuffer header = ByteBuffer.allocate(BLOCK_SIZE);
            header.put(bytes);
            header.rewind();
            fileStore.writeFully(fileStore.size(), header);
            readStoreHeader();
            readMeta();
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
        // rollback might have rolled back the stored chunk metadata as well
        Chunk c = chunks.get(lastChunkId - 1);
        if (c != null) {
            meta.put("chunk." + c.id, c.asString());
        }
        currentVersion = version;
        setWriteVersion(version);
    }

    private void revertTemp(long storeVersion) {
        for (Iterator<Long> it = freedPageSpace.keySet().iterator(); it.hasNext();) {
            long v = it.next();
            if (v > storeVersion) {
                continue;
            }
            it.remove();
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
     * Get the file store.
     *
     * @return the file store
     */
    public FileStore getFileStore() {
        return fileStore;
    }

    /**
     * Get the store header. This data is for informational purposes only. The
     * data is subject to change in future versions. The data should not be
     * modified (doing so may corrupt the store).
     *
     * @return the store header
     */
    public Map<String, String> getStoreHeader() {
        return storeHeader;
    }

    private void checkOpen() {
        if (closed) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED,
                    "This store is closed");
        }
    }

    /**
     * Rename a map.
     *
     * @param map the map
     * @param newName the new name
     */
    public synchronized void renameMap(MVMap<?, ?> map, String newName) {
        checkOpen();
        DataUtils.checkArgument(map != meta,
                "Renaming the meta map is not allowed");
        int id = map.getId();
        String oldName = getMapName(id);
        if (oldName.equals(newName)) {
            return;
        }
        DataUtils.checkArgument(
                !meta.containsKey("name." + newName),
                "A map named {0} already exists", newName);
        markMetaChanged();
        meta.remove("name." + oldName);
        meta.put("map." + id, map.asString(newName));
        meta.put("name." + newName, Integer.toString(id));
    }

    /**
     * Remove a map.
     *
     * @param map the map
     */
    public synchronized void removeMap(MVMap<?, ?> map) {
        checkOpen();
        DataUtils.checkArgument(map != meta,
                "Removing the meta map is not allowed");
        map.clear();
        int id = map.getId();
        String name = getMapName(id);
        markMetaChanged();
        meta.remove("map." + id);
        meta.remove("name." + name);
        meta.remove("root." + id);
        maps.remove(id);
    }

    /**
     * Get the name of the given map.
     *
     * @param id the map id
     * @return the name, or null if not found
     */
    public synchronized String getMapName(int id) {
        String m = meta.get("map." + id);
        return m == null ? null : DataUtils.parseMap(m).get("name");
    }

    /**
     * Commit and save all changes, if there are any.
     */
    void commitInBackground() {
        if (unsavedPageCount == 0 || closed) {
            return;
        }

        // could also store when there are many unsaved pages,
        // but according to a test it doesn't really help

        long time = getTime();
        if (time <= lastCommitTime + autoCommitDelay) {
            return;
        }
        if (!hasUnsavedChanges()) {
            return;
        }
        try {
            commitAndSave();
        } catch (Exception e) {
            if (backgroundExceptionHandler != null) {
                backgroundExceptionHandler.uncaughtException(null, e);
            }
        }
    }

    /**
     * Set the read cache size in MB.
     *
     * @param mb the cache size in MB.
     */
    public void setCacheSize(int mb) {
        if (cache != null) {
            cache.setMaxMemory((long) mb * 1024 * 1024);
        }
    }

    public boolean isClosed() {
        return closed;
    }

    private void stopBackgroundThread() {
        BackgroundWriterThread t = backgroundWriterThread;
        if (t == null) {
            return;
        }
        backgroundWriterThread = null;
        synchronized (t.sync) {
            t.sync.notifyAll();
        }
        try {
            t.join();
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Set the maximum delay in milliseconds to auto-commit changes.
     * <p>
     * To disable auto-commit, set the value to 0. In this case, changes are
     * only committed when explicitly calling commit.
     * <p>
     * The default is 1000, meaning all changes are committed after at most one
     * second.
     *
     * @param millis the maximum delay
     */
    public void setAutoCommitDelay(int millis) {
        if (autoCommitDelay == millis) {
            return;
        }
        autoCommitDelay = millis;
        if (fileStore == null) {
            return;
        }
        stopBackgroundThread();
        // start the background thread if needed
        if (millis > 0) {
            int sleep = Math.max(1, millis / 10);
            BackgroundWriterThread t = new BackgroundWriterThread(this, sleep, fileStore.toString());
            t.start();
            backgroundWriterThread = t;
        }
    }

    /**
     * Get the auto-commit delay.
     *
     * @return the delay in milliseconds, or 0 if auto-commit is disabled.
     */
    public int getAutoCommitDelay() {
        return autoCommitDelay;
    }

    /**
     * Get the maximum number of unsaved pages. If this number is exceeded,
     * unsaved changes are stored to disk.
     *
     * @return the number of maximum unsaved pages
     */
    public int getAutoCommitPageCount() {
        return autoCommitPageCount;
    }

    /**
     * Get the estimated number of unsaved pages. If the value exceeds the
     * auto-commit page count, the changes are committed.
     * <p>
     * The returned value may not be completely accurate, but can be used to
     * estimate the memory usage for unsaved data.
     *
     * @return the number of unsaved pages
     */
    public int getUnsavedPageCount() {
        return unsavedPageCount;
    }

    /**
     * Put the page in the cache.
     *
     * @param pos the page position
     * @param page the page
     * @param memory the memory used
     */
    void cachePage(long pos, Page page, int memory) {
        if (cache != null) {
            cache.put(pos, page, memory);
        }
    }

    /**
     * Get the amount of memory used for caching, in MB.
     *
     * @return the amount of memory used for caching
     */
    public int getCacheSizeUsed() {
        if (cache == null) {
            return 0;
        }
        return (int) (cache.getUsedMemory() / 1024 / 1024);
    }

    /**
     * Get the maximum cache size, in MB.
     *
     * @return the cache size
     */
    public int getCacheSize() {
        if (cache == null) {
            return 0;
        }
        return (int) (cache.getMaxMemory() / 1024 / 1024);
    }

    /**
     * A background writer thread to automatically store changes from time to time.
     */
    private static class BackgroundWriterThread extends Thread {

        public final Object sync = new Object();
        private final MVStore store;
        private final int sleep;

        BackgroundWriterThread(MVStore store, int sleep, String fileStoreName) {
            super("MVStore background writer " + fileStoreName);
            this.store = store;
            this.sleep = sleep;
            setDaemon(true);
        }

        @Override
        public void run() {
            while (true) {
                Thread t = store.backgroundWriterThread;
                if (t == null) {
                    break;
                }
                synchronized (sync) {
                    try {
                        sync.wait(sleep);
                    } catch (InterruptedException e) {
                        continue;
                    }
                }
                store.commitInBackground();
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
         * Disable auto-commit, by setting the auto-commit delay and auto-commit
         * buffer size to 0.
         *
         * @return this
         */
        public Builder autoCommitDisabled() {
            // we have a separate config option so that
            // no thread is started if the write delay is 0
            // (if we only had a setter in the MVStore,
            // the thread would need to be started in any case)
            set("autoCommitBufferSize", 0);
            return set("autoCommitDelay", 0);
        }

        /**
         * Set the size of the write buffer, in KB (for file-based stores).
         * Unless auto-commit is disabled, changes are automatically saved if
         * there are more than this amount of changes.
         * <p>
         * The default is 512 KB.
         * <p>
         * When the value is set to 0 or lower, data is not automatically
         * stored.
         *
         * @param kb the write buffer size, in kilobytes
         * @return this
         */
        public Builder autoCommitBufferSize(int kb) {
            return set("autoCommitBufferSize", kb);
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
            return set("encryptionKey", password);
        }

        /**
         * Open the file in read-only mode. In this case, a shared lock will be
         * acquired to ensure the file is not concurrently opened in write mode.
         * <p>
         * If this option is not used, the file is locked exclusively.
         * <p>
         * Please note a store may only be opened once in every JVM (no matter
         * whether it is opened in read-only or read-write mode), because each
         * file may be locked only once in a process.
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
         * Set the amount of memory a page should contain at most, in bytes,
         * before it is split. The default is 16 KB for persistent stores and 4
         * KB for in-memory stores. This is not a limit in the page size, as
         * pages with one entry can get larger. It is just the point where pages
         * that contain more than one entry are split.
         *
         * @param pageSplitSize the page size
         * @return this
         */
        public Builder pageSplitSize(int pageSplitSize) {
            return set("pageSplitSize", pageSplitSize);
        }

        /**
         * Set the listener to be used for exceptions that occur when writing in
         * the background thread.
         *
         * @param exceptionHandler the handler
         * @return this
         */
        public Builder backgroundExceptionHandler(
                Thread.UncaughtExceptionHandler exceptionHandler) {
            return set("backgroundExceptionHandler", exceptionHandler);
        }

        /**
         * Use the provided file store instead of the default one.
         *
         * @param store the file store
         * @return this
         */
        public Builder fileStore(FileStore store) {
            return set("fileStore", store);
        }

        /**
         * Open the store.
         *
         * @return the opened store
         */
        public MVStore open() {
            return new MVStore(config);
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
