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

TODO:

Documentation
- rolling docs review: at "Transactions"
- better document that writes are in background thread
- better document how to do non-unique indexes
- document pluggable store and OffHeapStore

MVTableEngine:
- test and possibly allow MVCC & MULTI_THREADED
- maybe enable MVCC by default (but allow to disable it)
- test with MVStore.ASSERT enabled

TransactionStore:
- ability to disable the transaction log,
    if there is only one connection

MVStore:

- maybe change the length code to have lower gaps
- data kept in stream store if transaction is not committed?

- automated 'kill process' and 'power failure' test
- test and possibly improve compact operation (for large dbs)
- possibly split chunk metadata into immutable and mutable
- compact: avoid processing pages using a counting bloom filter
- defragment (re-creating maps, specially those with small pages)
- store number of write operations per page (maybe defragment
    if much different than count)
- r-tree: nearest neighbor search
- use a small object value cache (StringCache), test on Android
    for default serialization
- MVStoreTool.dump: dump values (using a callback)
- close the file on out of memory or disk write error (out of disk space or so)
- implement a sharded map (in one store, multiple stores)
    to support concurrent updates and writes, and very large maps
- to save space when persisting very small transactions,
    use a transaction log where only the deltas are stored
- serialization for lists, sets, sets, sorted sets, maps, sorted maps
- maybe rename 'rollback' to 'revert' to distinguish from transactions
- support other compression algorithms (deflate, LZ4,...)
- remove features that are not really needed; simplify the code
    possibly using a separate layer or tools
    (retainVersion?)
- rename "store" to "save", as "store" is used in "storeVersion"
- MVStoreTool.dump should dump the data if possible;
    possibly using a callback for serialization
- optional pluggable checksum mechanism (per page), which
    requires that everything is a page (including headers)
- rename setStoreVersion to setDataVersion or similar
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
- support log structured merge style operations (blind writes)
    using one map per level plus bloom filter
- have a strict call order MVStore -> MVMap -> Page -> FileStore
- autocommit commits, stores, and compacts from time to time;
    the background thread should wait at least 90% of the
    configured write delay to store changes
- compact* should also store uncommitted changes (if there are any)
- write a LSM-tree (log structured merge tree) utility on top of the MVStore
- StreamStore: split blocks similar to rsync crypto, where the split is made
    "if the sum of the past 8196 bytes divides by 4096 with zero remainder"
- LIRS cache: maybe remove 'mask' field, and dynamically grow the arrays
- chunk metadata: maybe split into static and variable,
    or use a small page size for metadata
- data type "string": maybe use prefix compression for keys
- test chunk id rollover
- feature to auto-compact from time to time and on close
- compact very small chunks

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

    /**
     * The page cache. The default size is 16 MB, and the average size is 2 KB.
     * It is split in 16 segments. The stack move distance is 2% of the expected
     * number of entries.
     */
    private CacheLongKeyLIRS<Page> cache;

    /**
     * The newest chunk. If nothing was stored yet, this field is not set.
     */
    private Chunk lastChunk;

    /**
     * The map of chunks.
     */
    private final ConcurrentHashMap<Integer, Chunk> chunks = 
            new ConcurrentHashMap<Integer, Chunk>();

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

    private final ConcurrentHashMap<Integer, MVMap<?, ?>> maps = 
            new ConcurrentHashMap<Integer, MVMap<?, ?>>();

    private HashMap<String, Object> fileHeader = New.hashMap();

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
     * The version of the last stored chunk, or -1 if nothing was stored so far.
     */
    private long lastStoredVersion;

    /**
     * The estimated number of average-sized unsaved pages. This number may not
     * be completely accurate, because it may be changed concurrently, and
     * because temporary pages are counted.
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
        HashMap<String, Object> c = New.hashMap();
        c.put("id", 0);
        c.put("createVersion", currentVersion);
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
                creationTime = getTime();
                lastCommitTime = creationTime;
                fileHeader.put("H", 2);
                fileHeader.put("blockSize", BLOCK_SIZE);
                fileHeader.put("format", FORMAT_WRITE);
                fileHeader.put("created", creationTime);
                writeFileHeader();
            } else {
                readFileHeader();
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
        long rootPos = getRootPos(oldMeta, mapId);
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
    public synchronized <M extends MVMap<K, V>, K, V> M openMap(
            String name, MVMap.MapBuilder<M, K, V> builder) {
        checkOpen();
        String x = meta.get("name." + name);
        int id;
        long root;
        HashMap<String, Object> c;
        M map;
        if (x != null) {
            id = DataUtils.parseHexInt(x);
            @SuppressWarnings("unchecked")
            M old = (M) maps.get(id);
            if (old != null) {
                return old;
            }
            map = builder.create();
            String config = meta.get("map." + x);
            c = New.hashMap();
            c.putAll(DataUtils.parseMap(config));
            c.put("id", id);
            map.init(this, c);
            root = getRootPos(meta, id);
        } else {
            c = New.hashMap();
            id = ++lastMapId;
            c.put("id", id);
            c.put("createVersion", currentVersion);
            map = builder.create();
            map.init(this, c);
            markMetaChanged();
            x = Integer.toHexString(id);
            meta.put("map." + x, map.asString(name));
            meta.put("name." + name, x);
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
        c = readChunkHeader(c.block);
        MVMap<String, String> oldMeta = meta.openReadOnly();
        oldMeta.setRootPos(c.metaRootPos, version);
        return oldMeta;
    }

    private Chunk getChunkForVersion(long version) {
        Chunk c = lastChunk;
        while (true) {
            if (c == null || c.version <= version) {
                return c;
            }
            c = chunks.get(c.id - 1);
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

    private synchronized void readFileHeader() {
        boolean validHeader = false;
        // we don't know yet which chunk and version are the newest
        long newestVersion = -1;
        long chunkBlock = -1;
        // read the first two blocks
        ByteBuffer fileHeaderBlocks = fileStore.readFully(0, 2 * BLOCK_SIZE);
        byte[] buff = new byte[BLOCK_SIZE];
        for (int i = 0; i <= BLOCK_SIZE; i += BLOCK_SIZE) {
            fileHeaderBlocks.get(buff);
            // the following can fail for various reasons
            try {
                String s = new String(buff, 0, BLOCK_SIZE, DataUtils.LATIN).trim();
                HashMap<String, String> m = DataUtils.parseMap(s);
                int blockSize = DataUtils.readHexInt(m, "blockSize", BLOCK_SIZE);
                if (blockSize != BLOCK_SIZE) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_UNSUPPORTED_FORMAT,
                            "Block size {0} is currently not supported",
                            blockSize);
                }
                int check = DataUtils.readHexInt(m, "fletcher", 0);
                m.remove("fletcher");
                s = s.substring(0, s.lastIndexOf("fletcher") - 1);
                byte[] bytes = s.getBytes(DataUtils.LATIN);
                int checksum = DataUtils.getFletcher32(bytes, bytes.length / 2 * 2);
                if (check != checksum) {
                    continue;
                }
                long version = DataUtils.readHexLong(m, "version", 0);
                if (version > newestVersion) {
                    newestVersion = version;
                    fileHeader.putAll(m);
                    chunkBlock = DataUtils.readHexLong(m, "block", 0);
                    creationTime = DataUtils.readHexLong(m, "created", 0);
                    validHeader = true;
                }
            } catch (Exception e) {
                continue;
            }
        }
        if (!validHeader) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Store header is corrupt: {0}", fileStore);
        }
        long format = DataUtils.readHexLong(fileHeader, "format", 1);
        if (format > FORMAT_WRITE && !fileStore.isReadOnly()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The write format {0} is larger than the supported format {1}, " +
                    "and the file was not opened in read-only mode",
                    format, FORMAT_WRITE);
        }
        format = DataUtils.readHexLong(fileHeader, "formatRead", format);
        if (format > FORMAT_READ) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The read format {0} is larger than the supported format {1}",
                    format, FORMAT_READ);
        }
        lastStoredVersion = -1;
        chunks.clear();
        long now = System.currentTimeMillis();
        // calculate the year (doesn't have to be exact;
        // we assume 365.25 days per year, * 4 = 1461)
        int year =  1970 + (int) (now / (1000L * 60 * 60 * 6 * 1461));
        if (year < 2014) {
            // if the year is before 2014,
            // we assume the system doesn't have a real-time clock,
            // and we set the creationTime to the past, so that
            // existing chunks are overwritten
            creationTime = now - fileStore.getDefaultRetentionTime();
        } else if (now < creationTime) {
            // the system time was set to the past:
            // we change the creation time
            creationTime = now;
            fileHeader.put("created", creationTime);
        }

        Chunk footer = readChunkFooter(fileStore.size());
        if (footer != null) {
            if (footer.version > newestVersion) {
                newestVersion = footer.version;
                chunkBlock = footer.block;
            }
        }
        if (chunkBlock <= 0) {
            // no chunk
            return;
        }

        // read the chunk header and footer,
        // and follow the chain of next chunks
        lastChunk = null;
        while (true) {
            Chunk header;
            try {
                header = readChunkHeader(chunkBlock);
            } catch (Exception e) {
                // invalid chunk header: ignore, but stop
                break;
            }
            if (header.version < newestVersion) {
                // we have reached the end
                break;
            }
            footer = readChunkFooter((chunkBlock + header.len) * BLOCK_SIZE);
            if (footer == null || footer.id != header.id) {
                // invalid chunk footer, or the wrong one
                break;
            }
            lastChunk = header;
            newestVersion = header.version;
            if (header.next == 0 || header.next >= fileStore.size() / BLOCK_SIZE) {
                // no (valid) next
                break;
            }
            chunkBlock = header.next;
        }
        if (lastChunk == null) {
            // no valid chunk
            return;
        }
        lastMapId = lastChunk.mapId;
        currentVersion = lastChunk.version;
        setWriteVersion(currentVersion);
        chunks.put(lastChunk.id, lastChunk);
        meta.setRootPos(lastChunk.metaRootPos, -1);

        // load the chunk metadata: we can load in any order,
        // because loading chunk metadata might recursively load another chunk
        for (Iterator<String> it = meta.keyIterator("chunk."); it.hasNext();) {
            String s = it.next();
            if (!s.startsWith("chunk.")) {
                break;
            }
            s = meta.get(s);
            Chunk c = Chunk.fromString(s);
            if (!chunks.containsKey(c.id)) {
                if (c.block == Long.MAX_VALUE) {
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
            long start = c.block * BLOCK_SIZE;
            int length = c.len * BLOCK_SIZE;
            fileStore.markUsed(start, length);
        }
    }

    /**
     * Try to read a chunk footer.
     *
     * @param end the end of the chunk
     * @return the chunk, or null if not successful
     */
    private Chunk readChunkFooter(long end) {
        // the following can fail for various reasons
        try {
            // read the chunk footer of the last block of the file
            ByteBuffer lastBlock = fileStore.readFully(
                    end - Chunk.FOOTER_LENGTH, Chunk.FOOTER_LENGTH);
            byte[] buff = new byte[Chunk.FOOTER_LENGTH];
            lastBlock.get(buff);
            String s = new String(buff, DataUtils.LATIN).trim();
            HashMap<String, String> m = DataUtils.parseMap(s);
            int check = DataUtils.readHexInt(m, "fletcher", 0);
            m.remove("fletcher");
            s = s.substring(0, s.lastIndexOf("fletcher") - 1);
            byte[] bytes = s.getBytes(DataUtils.LATIN);
            int checksum = DataUtils.getFletcher32(bytes, bytes.length / 2 * 2);
            if (check == checksum) {
                int chunk = DataUtils.readHexInt(m, "chunk", 0);
                Chunk c = new Chunk(chunk);
                c.version = DataUtils.readHexLong(m, "version", 0);
                c.block = DataUtils.readHexLong(m, "block", 0);
                return c;
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    private void writeFileHeader() {
        StringBuilder buff = new StringBuilder();
        if (lastChunk != null) {
            fileHeader.put("block", lastChunk.block);
            fileHeader.put("chunk", lastChunk.id);
            fileHeader.put("version", lastChunk.version);
        }
        DataUtils.appendMap(buff, fileHeader);
        byte[] bytes = buff.toString().getBytes(DataUtils.LATIN);
        int checksum = DataUtils.getFletcher32(bytes, bytes.length / 2 * 2);
        DataUtils.appendMap(buff, "fletcher", checksum);
        buff.append("\n");
        bytes = buff.toString().getBytes(DataUtils.LATIN);
        ByteBuffer header = ByteBuffer.allocate(2 * BLOCK_SIZE);
        header.put(bytes);
        header.position(BLOCK_SIZE);
        header.put(bytes);
        header.rewind();
        write(0, header);
    }

    private void write(long pos, ByteBuffer buffer) {
        try {
            fileStore.writeFully(pos, buffer);
        } catch (IllegalStateException e) {
            closeImmediately();
            throw e;
        }
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
    private Chunk getChunk(long pos) {
        int chunkId = DataUtils.getPageChunkId(pos);
        Chunk c = chunks.get(chunkId);
        if (c == null) {
            if (!Thread.holdsLock(this)) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_INTERNAL,
                        "Unsynchronized metadata read");
            }
            String s = meta.get(Chunk.getMetaKey(chunkId));
            if (s == null) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Chunk {0} not found", chunkId);
            }
            c = Chunk.fromString(s);
            if (c.block == Long.MAX_VALUE) {
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

        // the last chunk was not stored before and needs to be set now (it's
        // better not to update right after storing, because that would modify
        // the meta map again)
        int lastChunkId;
        if (lastChunk == null) {
            lastChunkId = 0;
        } else {
            lastChunkId = lastChunk.id;
            meta.put(Chunk.getMetaKey(lastChunkId), lastChunk.asString());
            // never go backward in time
            time = Math.max(lastChunk.time, time);
        }
        int newChunkId = lastChunkId;
        do {
            newChunkId = (newChunkId + 1) % Chunk.MAX_ID;
        } while (chunks.containsKey(newChunkId));
        Chunk c = new Chunk(newChunkId);

        c.pageCount = Integer.MAX_VALUE;
        c.pageCountLive = Integer.MAX_VALUE;
        c.maxLen = Long.MAX_VALUE;
        c.maxLenLive = Long.MAX_VALUE;
        c.metaRootPos = Long.MAX_VALUE;
        c.block = Long.MAX_VALUE;
        c.len = Integer.MAX_VALUE;
        c.time = time;
        c.version = version;
        c.mapId = lastMapId;
        c.next = Long.MAX_VALUE;
        chunks.put(c.id, c);
        // force a metadata update
        meta.put(Chunk.getMetaKey(c.id), c.asString());
        meta.remove(Chunk.getMetaKey(c.id));
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
            String key = "root." + Long.toHexString(m.getId());
            if (p.getTotalCount() == 0) {
                meta.put(key, "0");
            } else {
                meta.put(key, Long.toHexString(Long.MAX_VALUE));
            }
        }
        Set<Chunk> removedChunks = applyFreedSpace(storeVersion, time);
        WriteBuffer buff = getWriteBuffer();
        // need to patch the header later
        c.writeChunkHeader(buff, 0);
        int headerLength = buff.position();
        c.pageCount = 0;
        c.pageCountLive = 0;
        c.maxLen = 0;
        c.maxLenLive = 0;
        for (MVMap<?, ?> m : changed) {
            Page p = m.getRoot();
            if (p.getTotalCount() > 0) {
                p.writeUnsavedRecursive(c, buff);
                long root = p.getPos();
                String key = "root." + Long.toHexString(m.getId());
                meta.put(key, Long.toHexString(root));
            }
        }
        meta.setWriteVersion(version);

        Page metaRoot = meta.getRoot();
        metaRoot.writeUnsavedRecursive(c, buff);

        int chunkLength = buff.position();

        // add the store header and round to the next block
        int length = MathUtils.roundUpInt(chunkLength +
                Chunk.FOOTER_LENGTH, BLOCK_SIZE);
        buff.limit(length);

        // the length of the file that is still in use
        // (not necessarily the end of the file)
        long end = getFileLengthInUse();
        long filePos;
        if (reuseSpace) {
            filePos = fileStore.allocate(length);
        } else {
            filePos = end;
        }
        // end is not necessarily the end of the file
        boolean storeAtEndOfFile = filePos + length >= fileStore.size();

        // free up the space of unused chunks now
        // (after allocating space for this chunk, so that
        // no data is lost if writing this chunk fails)
        for (Chunk x : removedChunks) {
            long start = x.block * BLOCK_SIZE;
            int len = x.len * BLOCK_SIZE;
            fileStore.free(start, len);
        }

        if (!reuseSpace) {
            // we can not mark it earlier, because it
            // might have been allocated by one of the
            // removed chunks
            fileStore.markUsed(end, length);
        }

        c.block = filePos / BLOCK_SIZE;
        c.len = length / BLOCK_SIZE;
        c.metaRootPos = metaRoot.getPos();
        // calculate and set the likely next position
        if (reuseSpace) {
            int predictBlocks = c.len;
            long predictedNextStart = fileStore.allocate(predictBlocks * BLOCK_SIZE);
            fileStore.free(predictedNextStart, predictBlocks * BLOCK_SIZE);
            c.next = predictedNextStart / BLOCK_SIZE;
        } else {
            // just after this chunk
            c.next = 0;
        }
        buff.position(0);
        c.writeChunkHeader(buff, headerLength);
        revertTemp(storeVersion);

        buff.position(buff.limit() - Chunk.FOOTER_LENGTH);
        buff.put(c.getFooterBytes());

        buff.position(0);
        write(filePos, buff.getBuffer());
        releaseWriteBuffer(buff);

        // whether we need to write the file header
        boolean needHeader = false;
        if (!storeAtEndOfFile) {
            if (lastChunk == null) {
                needHeader = true;
            } else if (lastChunk.next != c.block) {
                // the last prediction did not matched
                needHeader = true;
            } else {
                long headerVersion = DataUtils.readHexLong(fileHeader, "version", 0);
                if (lastChunk.version - headerVersion > 20) {
                    // we write after at least 20 entries
                    needHeader = true;
                } else {
                    int chunkId = DataUtils.readHexInt(fileHeader, "chunk", 0);
                    while (true) {
                        Chunk old = chunks.get(chunkId);
                        if (old == null) {
                            // one of the chunks in between
                            // was removed
                            needHeader = true;
                            break;
                        }
                        if (chunkId == lastChunk.id) {
                            break;
                        }
                        chunkId++;
                    }
                }

//                    }
//                    while (chunkId <= lastChunk.id) {
//                        if (chunks.get(chunkId) == null) {
//                            // one of the chunks in between
//                            // was removed
//                            needHeader = true;
//                            break;
//                        }
//                        chunkId++;
//                    }
//                }
            }
        }

        lastChunk = c;
        if (needHeader) {
            writeFileHeader();
        }
        if (!storeAtEndOfFile) {
            // may only shrink after the file header was written
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
                    c.maxLenLive += f.maxLenLive;
                    c.pageCountLive += f.pageCountLive;
                    if (c.pageCountLive < 0) {
                        throw DataUtils.newIllegalStateException(
                                DataUtils.ERROR_INTERNAL,
                                "Corrupt page count {0}", c.pageCountLive);
                    }
                    if (c.maxLenLive < 0) {
                        throw DataUtils.newIllegalStateException(
                                DataUtils.ERROR_INTERNAL,
                                "Corrupt max length {0}", c.maxLenLive);
                    }
                    if (c.pageCount == 0 && c.maxLenLive > 0) {
                        throw DataUtils.newIllegalStateException(
                                DataUtils.ERROR_INTERNAL,
                                "Corrupt max length {0}", c.maxLenLive);
                    }
                    modified.add(c);
                }
                it.remove();
            }
            for (Chunk c : modified) {
                if (c.maxLenLive == 0) {
                    if (canOverwriteChunk(c, time)) {
                        removedChunks.add(c);
                        chunks.remove(c.id);
                        meta.remove(Chunk.getMetaKey(c.id));
                    } else {
                        meta.put(Chunk.getMetaKey(c.id), c.asString());
                        // remove this chunk in the next save operation
                        registerFreePage(storeVersion + 1, c.id, 0, 0);
                    }
                } else {
                    meta.put(Chunk.getMetaKey(c.id), c.asString());
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
            if (c.len != Integer.MAX_VALUE) {
                long x = (c.block + c.len) * BLOCK_SIZE;
                size = Math.max(size, x);
            }
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

    private Chunk readChunkHeader(long block) {
        long p = block * BLOCK_SIZE;
        ByteBuffer buff = fileStore.readFully(p, Chunk.MAX_HEADER_LENGTH);
        return Chunk.readChunkHeader(buff, p);
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
        if (lastChunk == null) {
            // nothing to do
            return false;
        }
        int oldRetentionTime = retentionTime;
        retentionTime = 0;
        long time = getTime();
        ArrayList<Chunk> free = New.arrayList();
        for (Chunk c : chunks.values()) {
            if (c.maxLenLive == 0) {
                if (canOverwriteChunk(c, time)) {
                    free.add(c);
                }
            }
        }
        for (Chunk c : free) {
            chunks.remove(c.id);
            markMetaChanged();
            meta.remove(Chunk.getMetaKey(c.id));
            long start = c.block * BLOCK_SIZE;
            int length = c.len * BLOCK_SIZE;
            fileStore.free(start, length);
        }
        if (fileStore.getFillRate() == 100) {
            return false;
        }
        long firstFree = fileStore.getFirstFree() / BLOCK_SIZE;
        ArrayList<Chunk> move = New.arrayList();
        for (Chunk c : chunks.values()) {
            if (c.block > firstFree) {
                move.add(c);
            }
        }
        for (Chunk c : move) {
            WriteBuffer buff = getWriteBuffer();
            long start = c.block * BLOCK_SIZE;
            int length = c.len * BLOCK_SIZE;
            buff.limit(length);
            ByteBuffer readBuff = fileStore.readFully(start, length);
            Chunk.readChunkHeader(readBuff, start);
            int chunkHeaderLen = readBuff.position();
            buff.position(chunkHeaderLen);
            buff.put(readBuff);
            long end = getFileLengthInUse();
            fileStore.markUsed(end, length);
            fileStore.free(start, length);
            c.block = end / BLOCK_SIZE;
            c.next = 0;
            buff.position(0);
            c.writeChunkHeader(buff, chunkHeaderLen);
            buff.position(length - Chunk.FOOTER_LENGTH);
            buff.put(lastChunk.getFooterBytes());
            buff.position(0);
            write(end, buff.getBuffer());
            releaseWriteBuffer(buff);
            markMetaChanged();
            meta.put(Chunk.getMetaKey(c.id), c.asString());
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
            long start = c.block * BLOCK_SIZE;
            int length = c.len * BLOCK_SIZE;
            buff.limit(length);
            ByteBuffer readBuff = fileStore.readFully(start, length);
            Chunk.readChunkHeader(readBuff, 0);
            int chunkHeaderLen = readBuff.position();
            buff.position(chunkHeaderLen);
            buff.put(readBuff);
            long pos = fileStore.allocate(length);
            fileStore.free(start, length);
            buff.position(0);
            c.block = pos / BLOCK_SIZE;
            c.writeChunkHeader(buff, chunkHeaderLen);
            buff.position(length - Chunk.FOOTER_LENGTH);
            buff.put(lastChunk.getFooterBytes());
            buff.position(0);
            write(pos, buff.getBuffer());
            releaseWriteBuffer(buff);
            markMetaChanged();
            meta.put(Chunk.getMetaKey(c.id), c.asString());
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
     * Try to increase the fill rate by re-writing partially full chunks. Chunks
     * with a low number of live items are re-written.
     * <p>
     * If the current fill rate is higher than the target fill rate, nothing is
     * done. If not at least a minimum amount of space can be saved, nothing is
     * done.
     * <p>
     * Please note this method will not necessarily reduce the file size, as
     * empty chunks are not overwritten.
     * <p>
     * Only data of open maps can be moved. For maps that are not open, the old
     * chunk is still referenced. Therefore, it is recommended to open all maps
     * before calling this method.
     *
     * @param targetFillRate the minimum percentage of live entries
     * @param minSaving the minimum amount of saved space
     * @return if a chunk was re-written
     */
    public synchronized boolean compact(int targetFillRate, int minSaving) {
        checkOpen();
        if (lastChunk == null) {
            // nothing to do
            return false;
        }

        // calculate the fill rate
        long maxLengthSum = 0;
        long maxLengthLiveSum = 0;
        for (Chunk c : chunks.values()) {
            maxLengthSum += c.maxLen;
            maxLengthLiveSum += c.maxLenLive;
        }
        // the fill rate of all chunks combined
        if (maxLengthSum <= 0) {
            // avoid division by 0
            maxLengthSum = 1;
        }
        int fillRate = (int) (100 * maxLengthLiveSum / maxLengthSum);
        if (fillRate >= targetFillRate) {
            return false;
        }

        long time = getTime();

        // the 'old' list contains the chunks we want to free up
        ArrayList<Chunk> old = New.arrayList();
        Chunk last = chunks.get(lastChunk.id);
        for (Chunk c : chunks.values()) {
            if (canOverwriteChunk(c, time)) {
                long age = last.version - c.version + 1;
                c.collectPriority = (int) (c.getFillRate() / age);
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
        long saved = 0;
        Chunk move = null;
        for (Chunk c : old) {
            long save = c.maxLen - c.maxLenLive;
            if (move != null) {
                if (saved > minSaving) {
                    break;
                }
            }
            saved += save;
            move = c;
        }
        if (saved < minSaving) {
            return false;
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
        long start = chunk.block * BLOCK_SIZE;
        int length = chunk.len * BLOCK_SIZE;
        ByteBuffer buff = fileStore.readFully(start, length);
        Chunk.readChunkHeader(buff, start);
        int pagesRemaining = chunk.pageCount;
        markMetaChanged();
        while (pagesRemaining-- > 0) {
            int offset = buff.position();
            int pageLength = buff.getInt();
            if (pageLength <= 0) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT, "Page length {0}", pageLength);
            }
            buff.getShort();
            int mapId = DataUtils.readVarInt(buff);
            @SuppressWarnings("unchecked")
            MVMap<Object, Object> map = (MVMap<Object, Object>) getMap(mapId);
            if (map == null) {
                // pages of maps that are not open or that have been removed
                // later on are not moved (for maps that are not open, the live
                // counter is not decremented, so the chunk is not removed)
                buff.position(offset + pageLength);
                continue;
            }
            buff.position(offset);
            Page page = new Page(map, 0);
            page.read(buff, chunk.id, buff.position(), length);
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
            long filePos = c.block * BLOCK_SIZE;
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
     * @param memory the memory usage
     */
    void removePage(MVMap<?, ?> map, long pos, int memory) {
        // we need to keep temporary pages,
        // to support reading old versions and rollback
        if (pos == 0) {
            // the value could be smaller than 0 because
            // in some cases a page is allocated,
            // but never stored
            int count = 1 + memory / pageSplitSize;
            unsavedPageCount = Math.max(0, unsavedPageCount - count);
            return;
        }

        // This could result in a cache miss if the operation is rolled back,
        // but we don't optimize for rollback.
        // We could also keep the page in the cache, as somebody
        // could still read it (reading the old version).
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
            f.maxLenLive -= maxLengthLive;
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
     * How many versions to retain for in-memory stores. If not set, 5 old
     * versions are retained.
     *
     * @param count the number of versions to keep
     */
    public void setVersionsToKeep(int count) {
        this.versionsToKeep = count;
    }

    /**
     * Get the oldest version to retain in memory (for in-memory stores).
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
            return v - versionsToKeep + 1;
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
     *
     * @param memory the memory usage of the page
     */
    void registerUnsavedPage(int memory) {
        int count = 1 + memory / pageSplitSize;
        unsavedPageCount += count;
        int newValue = unsavedPageCount;
        if (newValue > autoCommitPageCount && autoCommitPageCount > 0) {
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
        return x == null ? 0 : DataUtils.parseHexInt(x);
    }

    /**
     * Update the store version.
     *
     * @param version the new store version
     */
    public synchronized void setStoreVersion(int version) {
        checkOpen();
        markMetaChanged();
        meta.put("setting.storeVersion", Integer.toHexString(version));
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
        Chunk removeChunksNewerThan = null;
        Chunk c = lastChunk;
        while (true) {
            if (c == null || c.version < version) {
                break;
            }
            removeChunksNewerThan = c;
            c = chunks.get(c.id - 1);
        }
        Chunk last = lastChunk;
        if (removeChunksNewerThan != null && 
                last.version > removeChunksNewerThan.version) {
            revertTemp(version);
            loadFromFile = true;
            while (true) {
                last = lastChunk;
                if (last == null) {
                    break;
                } else if (last.version <= removeChunksNewerThan.version) {
                    break;
                }
                chunks.remove(lastChunk.id);
                long start = last.block * BLOCK_SIZE;
                int length = last.len * BLOCK_SIZE;
                fileStore.free(start, length);
                // need to overwrite the chunk,
                // so it can not be used
                WriteBuffer buff = getWriteBuffer();
                buff.limit(length);
                // buff.clear() does not set the data
                Arrays.fill(buff.getBuffer().array(), (byte) 0);
                write(start, buff.getBuffer());
                releaseWriteBuffer(buff);
                lastChunk = chunks.get(lastChunk.id - 1);
            }
            writeFileHeader();
            readFileHeader();
        }
        for (MVMap<?, ?> m : New.arrayList(maps.values())) {
            int id = m.getId();
            if (m.getCreateVersion() >= version) {
                m.close();
                maps.remove(id);
            } else {
                if (loadFromFile) {
                    m.setRootPos(getRootPos(meta, id), -1);
                }
            }

        }
        // rollback might have rolled back the stored chunk metadata as well
        if (lastChunk != null) {
            c = chunks.get(lastChunk.id - 1);
            if (c != null) {
                meta.put(Chunk.getMetaKey(c.id), c.asString());
            }
        }
        currentVersion = version;
        setWriteVersion(version);
    }

    private static long getRootPos(MVMap<String, String> map, int mapId) {
        String root = map.get("root." + Integer.toHexString(mapId));
        return root == null ? 0 : DataUtils.parseHexLong(root);
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
    public Map<String, Object> getStoreHeader() {
        return fileHeader;
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
        String x = Integer.toHexString(id);
        meta.remove("name." + oldName);
        meta.put("map." + x, map.asString(newName));
        meta.put("name." + newName, x);
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
        String x = Integer.toHexString(id);
        meta.remove("map." + x);
        meta.remove("name." + name);
        meta.remove("root." + x);
        maps.remove(id);
    }

    /**
     * Get the name of the given map.
     *
     * @param id the map id
     * @return the name, or null if not found
     */
    public synchronized String getMapName(int id) {
        String m = meta.get("map." + Integer.toHexString(id));
        return m == null ? null : DataUtils.parseMap(m).get("name");
    }

    /**
     * Commit and save all changes, if there are any.
     */
    void commitInBackground() {
        if (unsavedPageCount == 0 || closed) {
            return;
        }

        // could also commit when there are many unsaved pages,
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
        if (Thread.currentThread() == t) {
            // within the thread itself - can not join
            return;
        }
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
            BackgroundWriterThread t = 
                    new BackgroundWriterThread(this, sleep, 
                            fileStore.toString());
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
         * no effect for in-memory stores. The password is passed as a
         * char array so that it can be cleared as soon as possible. Please note
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
         * Use the provided file store instead of the default one. Please note
         * that any kind of store (including an off-heap store) is considered a
         * "persistence", while an "in-memory store" means objects are not
         * persisted and fully kept in the JVM heap.
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
