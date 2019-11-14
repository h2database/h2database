/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import static org.h2.mvstore.MVMap.INITIAL_VERSION;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.h2.compress.CompressDeflate;
import org.h2.compress.CompressLZF;
import org.h2.compress.Compressor;
import org.h2.mvstore.cache.CacheLongKeyLIRS;
import org.h2.util.MathUtils;
import org.h2.util.Utils;

/*

TODO:

Documentation
- rolling docs review: at "Metadata Map"
- better document that writes are in background thread
- better document how to do non-unique indexes
- document pluggable store and OffHeapStore

TransactionStore:
- ability to disable the transaction log,
    if there is only one connection

MVStore:
- better and clearer memory usage accounting rules
    (heap memory versus disk memory), so that even there is
    never an out of memory
    even for a small heap, and so that chunks
    are still relatively big on average
- make sure serialization / deserialization errors don't corrupt the file
- test and possibly improve compact operation (for large dbs)
- automated 'kill process' and 'power failure' test
- defragment (re-creating maps, specially those with small pages)
- store number of write operations per page (maybe defragment
    if much different than count)
- r-tree: nearest neighbor search
- use a small object value cache (StringCache), test on Android
    for default serialization
- MVStoreTool.dump should dump the data if possible;
    possibly using a callback for serialization
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
- optional pluggable checksum mechanism (per page), which
    requires that everything is a page (including headers)
- rename "store" to "save", as "store" is used in "storeVersion"
- rename setStoreVersion to setDataVersion, setSchemaVersion or similar
- temporary file storage
- simple rollback method (rollback to last committed version)
- MVMap to implement SortedMap, then NavigableMap
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
    with blind writes and/or a bloom filter that
    internally uses regular maps and merge sort
- chunk metadata: maybe split into static and variable,
    or use a small page size for metadata
- data type "string": maybe use prefix compression for keys
- test chunk id rollover
- feature to auto-compact from time to time and on close
- compact very small chunks
- Page: to save memory, combine keys & values into one array
    (also children & counts). Maybe remove some other
    fields (childrenCount for example)
- Support SortedMap for MVMap
- compact: copy whole pages (without having to open all maps)
- maybe change the length code to have lower gaps
- test with very low limits (such as: short chunks, small pages)
- maybe allow to read beyond the retention time:
    when compacting, move live pages in old chunks
    to a map (possibly the metadata map) -
    this requires a change in the compaction code, plus
    a map lookup when reading old data; also, this
    old data map needs to be cleaned up somehow;
    maybe using an additional timeout
*/

/**
 * A persistent storage for maps.
 */
public class MVStore implements AutoCloseable
{
    // The following are attribute names (keys) in store header map
    private static final String HDR_H = "H";
    private static final String HDR_BLOCK_SIZE = "blockSize";
    private static final String HDR_FORMAT = "format";
    private static final String HDR_CREATED = "created";
    private static final String HDR_FORMAT_READ = "formatRead";
    private static final String HDR_CHUNK = "chunk";
    private static final String HDR_BLOCK = "block";
    private static final String HDR_VERSION = "version";
    private static final String HDR_CLEAN = "clean";
    private static final String HDR_FLETCHER = "fletcher";


    /**
     * The block size (physical sector size) of the disk. The store header is
     * written twice, one copy in each block, to ensure it survives a crash.
     */
    static final int BLOCK_SIZE = 4 * 1024;

    private static final int FORMAT_WRITE = 1;
    private static final int FORMAT_READ = 1;

    /**
     * Store is open.
     */
    private static final int STATE_OPEN = 0;

    /**
     * Store is about to close now, but is still operational.
     * Outstanding store operation by background writer or other thread may be in progress.
     * New updates must not be initiated, unless they are part of a closing procedure itself.
     */
    private static final int STATE_STOPPING = 1;

    /**
     * Store is closing now, and any operation on it may fail.
     */
    private static final int STATE_CLOSING = 2;

    /**
     * Store is closed.
     */
    private static final int STATE_CLOSED = 3;

    /**
     * Lock which governs access to major store operations: store(), close(), ...
     * It should used in a non-reentrant fashion.
     * It serves as a replacement for synchronized(this), except it allows for
     * non-blocking lock attempts.
     */
    private final ReentrantLock storeLock = new ReentrantLock(true);

    /**
     * Reference to a background thread, which is expected to be running, if any.
     */
    private final AtomicReference<BackgroundWriterThread> backgroundWriterThread = new AtomicReference<>();

    private volatile boolean reuseSpace = true;

    private volatile int state;

    private final FileStore fileStore;

    private final boolean fileStoreIsProvided;

    private final int pageSplitSize;

    private final int keysPerPage;

    /**
     * The page cache. The default size is 16 MB, and the average size is 2 KB.
     * It is split in 16 segments. The stack move distance is 2% of the expected
     * number of entries.
     */
    private final CacheLongKeyLIRS<Page> cache;

    /**
     * The newest chunk. If nothing was stored yet, this field is not set.
     */
    private Chunk lastChunk;

    /**
     * The map of chunks.
     */
    private final ConcurrentHashMap<Integer, Chunk> chunks = new ConcurrentHashMap<>();

    private final Queue<RemovedPageInfo> removedPages = new PriorityBlockingQueue<>();

    private final Deque<Chunk> deadChunks = new ArrayDeque<>();

    private long updateCounter = 0;
    private long updateAttemptCounter = 0;

    /**
     * The metadata map. Write access to this map needs to be done under storeLock.
     */
    private final MVMap<String, String> meta;

    private final ConcurrentHashMap<Integer, MVMap<?, ?>> maps = new ConcurrentHashMap<>();

    private final HashMap<String, Object> storeHeader = new HashMap<>();

    private WriteBuffer writeBuffer;

    private final AtomicInteger lastMapId = new AtomicInteger();

    private int versionsToKeep = 5;

    /**
     * The compression level for new pages (0 for disabled, 1 for fast, 2 for
     * high). Even if disabled, the store may contain (old) compressed pages.
     */
    private final int compressionLevel;

    private Compressor compressorFast;

    private Compressor compressorHigh;

    private final boolean recoveryMode;

    private final UncaughtExceptionHandler backgroundExceptionHandler;

    private volatile long currentVersion;

    /**
     * The version of the last stored chunk, or -1 if nothing was stored so far.
     */
    private volatile long lastStoredVersion = INITIAL_VERSION;

    /**
     * Oldest store version in use. All version beyond this can be safely dropped
     */
    private final AtomicLong oldestVersionToKeep = new AtomicLong();

    /**
     * Ordered collection of all version usage counters for all versions starting
     * from oldestVersionToKeep and up to current.
     */
    private final Deque<TxCounter> versions = new LinkedList<>();

    /**
     * Counter of open transactions for the latest (current) store version
     */
    private volatile TxCounter currentTxCounter = new TxCounter(currentVersion);

    /**
     * The estimated memory used by unsaved pages. This number is not accurate,
     * also because it may be changed concurrently, and because temporary pages
     * are counted.
     */
    private int unsavedMemory;
    private final int autoCommitMemory;
    private volatile boolean saveNeeded;

    /**
     * The time the store was created, in milliseconds since 1970.
     */
    private long creationTime;

    /**
     * How long to retain old, persisted chunks, in milliseconds. For larger or
     * equal to zero, a chunk is never directly overwritten if unused, but
     * instead, the unused field is set. If smaller zero, chunks are directly
     * overwritten if unused.
     */
    private int retentionTime;

    private long lastCommitTime;

    /**
     * The version of the current store operation (if any).
     */
    private volatile long currentStoreVersion = -1;

    private volatile boolean metaChanged;

    /**
     * The delay in milliseconds to automatically commit and write changes.
     */
    private int autoCommitDelay;

    private final int autoCompactFillRate;
    private long autoCompactLastFileOpCount;

    private volatile IllegalStateException panicException;

    private long lastTimeAbsolute;


    /**
     * Create and open the store.
     *
     * @param config the configuration to use
     * @throws IllegalStateException if the file is corrupt, or an exception
     *             occurred while opening
     * @throws IllegalArgumentException if the directory does not exist
     */
    MVStore(Map<String, Object> config) {
        recoveryMode = config.containsKey("recoveryMode");
        compressionLevel = DataUtils.getConfigParam(config, "compress", 0);
        String fileName = (String) config.get("fileName");
        FileStore fileStore = (FileStore) config.get("fileStore");
        fileStoreIsProvided = fileStore != null;
        if(fileStore == null && fileName != null) {
            fileStore = new FileStore();
        }
        this.fileStore = fileStore;

        int pgSplitSize = 48; // for "mem:" case it is # of keys
        CacheLongKeyLIRS.Config cc = null;
        if (this.fileStore != null) {
            int mb = DataUtils.getConfigParam(config, "cacheSize", 16);
            if (mb > 0) {
                cc = new CacheLongKeyLIRS.Config();
                cc.maxMemory = mb * 1024L * 1024L;
                Object o = config.get("cacheConcurrency");
                if (o != null) {
                    cc.segmentCount = (Integer)o;
                }
            }
            pgSplitSize = 16 * 1024;
        }
        if (cc != null) {
            cache = new CacheLongKeyLIRS<>(cc);
        } else {
            cache = null;
        }

        pgSplitSize = DataUtils.getConfigParam(config, "pageSplitSize", pgSplitSize);
        // Make sure pages will fit into cache
        if (cache != null && pgSplitSize > cache.getMaxItemSize()) {
            pgSplitSize = (int)cache.getMaxItemSize();
        }
        pageSplitSize = pgSplitSize;
        keysPerPage = DataUtils.getConfigParam(config, "keysPerPage", 48);
        backgroundExceptionHandler =
                (UncaughtExceptionHandler)config.get("backgroundExceptionHandler");
        meta = new MVMap<>(this);
        if (this.fileStore != null) {
            retentionTime = this.fileStore.getDefaultRetentionTime();
            // 19 KB memory is about 1 KB storage
            int kb = Math.max(1, Math.min(19, Utils.scaleForAvailableMemory(64))) * 1024;
            kb = DataUtils.getConfigParam(config, "autoCommitBufferSize", kb);
            autoCommitMemory = kb * 1024;
            autoCompactFillRate = DataUtils.getConfigParam(config, "autoCompactFillRate", 90);
            char[] encryptionKey = (char[]) config.get("encryptionKey");
            try {
                if (!fileStoreIsProvided) {
                    boolean readOnly = config.containsKey("readOnly");
                    this.fileStore.open(fileName, readOnly, encryptionKey);
                }
                if (this.fileStore.size() == 0) {
                    creationTime = getTimeAbsolute();
                    lastCommitTime = creationTime;
                    storeHeader.put(HDR_H, 2);
                    storeHeader.put(HDR_BLOCK_SIZE, BLOCK_SIZE);
                    storeHeader.put(HDR_FORMAT, FORMAT_WRITE);
                    storeHeader.put(HDR_CREATED, creationTime);
                    writeStoreHeader();
                } else {
                    // there is no need to lock store here, since it is not opened yet,
                    // just to make some assertions happy, when they ensure single-threaded access
                    storeLock.lock();
                    try {
                        readStoreHeader();
                    } finally {
                        storeLock.unlock();
                    }
                }
            } catch (IllegalStateException e) {
                panic(e);
            } finally {
                if (encryptionKey != null) {
                    Arrays.fill(encryptionKey, (char) 0);
                }
            }
            lastCommitTime = getTimeSinceCreation();

            scrubMetaMap();

            // setAutoCommitDelay starts the thread, but only if
            // the parameter is different from the old value
            int delay = DataUtils.getConfigParam(config, "autoCommitDelay", 1000);
            setAutoCommitDelay(delay);
        } else {
            autoCommitMemory = 0;
            autoCompactFillRate = 0;
        }
    }

    private void scrubMetaMap() {
        Set<String> keysToRemove = new HashSet<>();

        // ensure that there is only one name mapped to this id
        // this could be a leftover of an unfinished map rename
        for (Iterator<String> it = meta.keyIterator(DataUtils.META_NAME); it.hasNext();) {
            String key = it.next();
            if (!key.startsWith(DataUtils.META_NAME)) {
                break;
            }
            String mapName = key.substring(DataUtils.META_NAME.length());
            int mapId = DataUtils.parseHexInt(meta.get(key));
            String realMapName = getMapName(mapId);
            if(!mapName.equals(realMapName)) {
                keysToRemove.add(key);
            }
        }

        // remove roots of non-existent maps (leftover after unfinished map removal)
        for (Iterator<String> it = meta.keyIterator(DataUtils.META_ROOT); it.hasNext();) {
            String key = it.next();
            if (!key.startsWith(DataUtils.META_ROOT)) {
                break;
            }
            String mapIdStr = key.substring(key.lastIndexOf('.') + 1);
            if(!meta.containsKey(DataUtils.META_MAP + mapIdStr)) {
                meta.remove(key);
                markMetaChanged();
                keysToRemove.add(key);
            }
        }

        for (String key : keysToRemove) {
            meta.remove(key);
            markMetaChanged();
        }

        for (Iterator<String> it = meta.keyIterator(DataUtils.META_MAP); it.hasNext();) {
            String key = it.next();
            if (!key.startsWith(DataUtils.META_MAP)) {
                break;
            }
            String mapName = DataUtils.getMapName(meta.get(key));
            String mapIdStr = key.substring(DataUtils.META_MAP.length());
            // ensure that last map id is not smaller than max of any existing map ids
            int mapId = DataUtils.parseHexInt(mapIdStr);
            if (mapId > lastMapId.get()) {
                lastMapId.set(mapId);
            }
            // each map should have a proper name
            if(!mapIdStr.equals(meta.get(DataUtils.META_NAME + mapName))) {
                meta.put(DataUtils.META_NAME + mapName, mapIdStr);
                markMetaChanged();
            }
        }
    }

    private void panic(IllegalStateException e) {
        if (isOpen()) {
            handleException(e);
            panicException = e;
            closeImmediately();
        }
        throw e;
    }

    public IllegalStateException getPanicException() {
        return panicException;
    }

    /**
     * Open a store in exclusive mode. For a file-based store, the parent
     * directory must already exist.
     *
     * @param fileName the file name (null for in-memory)
     * @return the store
     */
    public static MVStore open(String fileName) {
        HashMap<String, Object> config = new HashMap<>();
        config.put("fileName", fileName);
        return new MVStore(config);
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
     * @param <M> the map type
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @param builder the map builder
     * @return the map
     */
    public <M extends MVMap<K, V>, K, V> M openMap(String name, MVMap.MapBuilder<M, K, V> builder) {
        int id = getMapId(name);
        M map;
        if (id >= 0) {
            map = openMap(id, builder);
            assert builder.getKeyType() == null || map.getKeyType().getClass().equals(builder.getKeyType().getClass());
            assert builder.getValueType() == null || map.getValueType().getClass().equals(builder.getValueType()
                    .getClass());
        } else {
            HashMap<String, Object> c = new HashMap<>();
            id = lastMapId.incrementAndGet();
            assert getMap(id) == null;
            c.put("id", id);
            c.put("createVersion", currentVersion);
            map = builder.create(this, c);
            String x = Integer.toHexString(id);
            meta.put(MVMap.getMapKey(id), map.asString(name));
            meta.put(DataUtils.META_NAME + name, x);
            map.setRootPos(0, lastStoredVersion);
            markMetaChanged();
            @SuppressWarnings("unchecked")
            M existingMap = (M) maps.putIfAbsent(id, map);
            if (existingMap != null) {
                map = existingMap;
            }
        }
        return map;
    }

    private <M extends MVMap<K, V>, K, V> M openMap(int id, MVMap.MapBuilder<M, K, V> builder) {
        storeLock.lock();
        try {
            @SuppressWarnings("unchecked")
            M map = (M) getMap(id);
            if (map == null) {
                String configAsString = meta.get(MVMap.getMapKey(id));
                HashMap<String, Object> config;
                if (configAsString != null) {
                    config = new HashMap<String, Object>(DataUtils.parseMap(configAsString));
                } else {
                    config = new HashMap<>();
                }
                config.put("id", id);
                map = builder.create(this, config);
                long root = getRootPos(meta, id);
                map.setRootPos(root, lastStoredVersion);
                maps.put(id, map);
            }
            return map;
        } finally {
            storeLock.unlock();
        }
    }

    /**
     * Get map by id.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param id map id
     * @return Map
     */
    public <K, V> MVMap<K,V> getMap(int id) {
        checkOpen();
        @SuppressWarnings("unchecked")
        MVMap<K, V> map = (MVMap<K, V>) maps.get(id);
        return map;
    }

    /**
     * Get the set of all map names.
     *
     * @return the set of names
     */
    public Set<String> getMapNames() {
        HashSet<String> set = new HashSet<>();
        checkOpen();
        for (Iterator<String> it = meta.keyIterator(DataUtils.META_NAME); it.hasNext();) {
            String x = it.next();
            if (!x.startsWith(DataUtils.META_NAME)) {
                break;
            }
            String mapName = x.substring(DataUtils.META_NAME.length());
            set.add(mapName);
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
        long block = c.block;
        c = readChunkHeader(block);
        MVMap<String, String> oldMeta = meta.openReadOnly(c.metaRootPos, version);
        return oldMeta;
    }

    private Chunk getChunkForVersion(long version) {
        Chunk newest = null;
        for (Chunk c : chunks.values()) {
            if (c.version <= version) {
                if (newest == null || c.id > newest.id) {
                    newest = c;
                }
            }
        }
        return newest;
    }

    /**
     * Check whether a given map exists.
     *
     * @param name the map name
     * @return true if it exists
     */
    public boolean hasMap(String name) {
        return meta.containsKey(DataUtils.META_NAME + name);
    }

    /**
     * Check whether a given map exists and has data.
     *
     * @param name the map name
     * @return true if it exists and has data.
     */
    public boolean hasData(String name) {
        return hasMap(name) && getRootPos(meta, getMapId(name)) != 0;
    }

    private void markMetaChanged() {
        // changes in the metadata alone are usually not detected, as the meta
        // map is changed after storing
        metaChanged = true;
    }

    private void readStoreHeader() {
        Chunk newest = null;
        boolean assumeCleanShutdown = true;
        boolean validStoreHeader = false;
        // find out which chunk and version are the newest
        // read the first two blocks
        ByteBuffer fileHeaderBlocks = fileStore.readFully(0, 2 * BLOCK_SIZE);
        byte[] buff = new byte[BLOCK_SIZE];
        for (int i = 0; i <= BLOCK_SIZE; i += BLOCK_SIZE) {
            fileHeaderBlocks.get(buff);
            // the following can fail for various reasons
            try {
                HashMap<String, String> m = DataUtils.parseChecksummedMap(buff);
                if (m == null) {
                    assumeCleanShutdown = false;
                    continue;
                }
                long version = DataUtils.readHexLong(m, HDR_VERSION, 0);
                // if both header blocks do agree on version
                // we'll continue on happy path - assume that previous shutdown was clean
                assumeCleanShutdown = assumeCleanShutdown && (newest == null || version == newest.version);
                if (newest == null || version > newest.version) {
                    validStoreHeader = true;
                    storeHeader.putAll(m);
                    creationTime = DataUtils.readHexLong(m, HDR_CREATED, 0);
                    int chunkId = DataUtils.readHexInt(m, HDR_CHUNK, 0);
                    long block = DataUtils.readHexLong(m, HDR_BLOCK, 0);
                    Chunk test = readChunkHeaderAndFooter(block, chunkId);
                    if (test != null) {
                        newest = test;
                    }
                }
            } catch (Exception ignore) {
                assumeCleanShutdown = false;
            }
        }

        if (!validStoreHeader) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Store header is corrupt: {0}", fileStore);
        }
        int blockSize = DataUtils.readHexInt(storeHeader, HDR_BLOCK_SIZE, BLOCK_SIZE);
        if (blockSize != BLOCK_SIZE) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "Block size {0} is currently not supported",
                    blockSize);
        }
        long format = DataUtils.readHexLong(storeHeader, HDR_FORMAT, 1);
        if (format > FORMAT_WRITE && !fileStore.isReadOnly()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The write format {0} is larger " +
                    "than the supported format {1}, " +
                    "and the file was not opened in read-only mode",
                    format, FORMAT_WRITE);
        }
        format = DataUtils.readHexLong(storeHeader, HDR_FORMAT_READ, format);
        if (format > FORMAT_READ) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The read format {0} is larger " +
                    "than the supported format {1}",
                    format, FORMAT_READ);
        }

        assumeCleanShutdown = assumeCleanShutdown && newest != null
                && DataUtils.readHexInt(storeHeader, HDR_CLEAN, 0) != 0
                && !recoveryMode;
        lastStoredVersion = INITIAL_VERSION;
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
            storeHeader.put(HDR_CREATED, creationTime);
        }

        long fileSize = fileStore.size();
        long blocksInStore = fileSize / BLOCK_SIZE;

        Comparator<Chunk> chunkComparator = new Comparator<Chunk>() {
            @Override
            public int compare(Chunk one, Chunk two) {
                int result = Long.compare(two.version, one.version);
                if (result == 0) {
                    // out of two copies of the same chunk we prefer the one
                    // close to the beginning of file (presumably later version)
                    result = Long.compare(one.block, two.block);
                }
                return result;
            }
        };

        if (!assumeCleanShutdown) {
            Chunk tailChunk = discoverChunk(blocksInStore);
            if (tailChunk != null) {
                blocksInStore = tailChunk.block; // for a possible full scan later on
                if (newest == null || tailChunk.version > newest.version) {
                    newest = tailChunk;
                }
            }
        }

        Map<Long, Chunk> validChunksByLocation = new HashMap<>();
        if (newest != null) {
            // read the chunk header and footer,
            // and follow the chain of next chunks
            while (true) {
                validChunksByLocation.put(newest.block, newest);
                if (newest.next == 0 || newest.next >= blocksInStore) {
                    // no (valid) next
                    break;
                }
                Chunk test = readChunkHeaderAndFooter(newest.next, newest.id + 1);
                if (test == null || test.version <= newest.version) {
                    break;
                }
                // if shutdown was really clean then chain should be empty
                assumeCleanShutdown = false;
                newest = test;
            }
        }

        if (assumeCleanShutdown) {
            setLastChunk(newest);
            // quickly check latest 20 chunks referenced in meta table
            Queue<Chunk> chunksToVerify = new PriorityQueue<>(20, Collections.reverseOrder(chunkComparator));
            try {
                // load the chunk metadata: although meta's root page resides in the lastChunk,
                // traversing meta map might recursively load another chunk(s)
                Cursor<String, String> cursor = meta.cursor(DataUtils.META_CHUNK);
                while (cursor.hasNext() && cursor.next().startsWith(DataUtils.META_CHUNK)) {
                    Chunk c = Chunk.fromString(cursor.getValue());
                    assert c.version <= currentVersion;
                    // might be there already, due to meta traversal
                    // see readPage() ... getChunkIfFound()
                    chunks.putIfAbsent(c.id, c);
                    chunksToVerify.offer(c);
                    if (chunksToVerify.size() == 20) {
                        chunksToVerify.poll();
                    }
                }
                Chunk c;
                while (assumeCleanShutdown && (c = chunksToVerify.poll()) != null) {
                    assumeCleanShutdown = readChunkHeaderAndFooter(c.block, c.id) != null;
                }
            } catch(IllegalStateException ignored) {
                assumeCleanShutdown = false;
            }
        }

        if (!assumeCleanShutdown) {
            boolean quickRecovery = false;
            if (!recoveryMode) {
                // now we know, that previous shutdown did not go well and file
                // is possibly corrupted but there is still hope for a quick
                // recovery

                // this collection will hold potential candidates for lastChunk to fall back to,
                // in order from the most to least likely
                Chunk[] lastChunkCandidates = validChunksByLocation.values().toArray(new Chunk[0]);
                Arrays.sort(lastChunkCandidates, chunkComparator);
                Map<Integer, Chunk> validChunksById = new HashMap<>();
                for (Chunk chunk : lastChunkCandidates) {
                    validChunksById.put(chunk.id, chunk);
                }
                quickRecovery = findLastChunkWithCompleteValidChunkSet(lastChunkCandidates, validChunksByLocation,
                        validChunksById, false);
            }

            if (!quickRecovery) {
                // scan whole file and try to fetch chunk header and/or footer out of every block
                // matching pairs with nothing in-between are considered as valid chunk
                long block = blocksInStore;
                Chunk tailChunk;
                while ((tailChunk = discoverChunk(block)) != null) {
                    block = tailChunk.block;
                    validChunksByLocation.put(block, tailChunk);
                }

                // this collection will hold potential candidates for lastChunk to fall back to,
                // in order from the most to least likely
                Chunk[] lastChunkCandidates = validChunksByLocation.values().toArray(new Chunk[0]);
                Arrays.sort(lastChunkCandidates, chunkComparator);
                Map<Integer, Chunk> validChunksById = new HashMap<>();
                for (Chunk chunk : lastChunkCandidates) {
                    validChunksById.put(chunk.id, chunk);
                }
                findLastChunkWithCompleteValidChunkSet(lastChunkCandidates, validChunksByLocation,
                        validChunksById, true);
            }
        }

        fileStore.clear();
        // build the free space list
        for (Chunk c : chunks.values()) {
            if (c.isSaved()) {
                long start = c.block * BLOCK_SIZE;
                int length = c.len * BLOCK_SIZE;
                fileStore.markUsed(start, length);
            }
            if (!c.isLive()) {
                deadChunks.offer(c);
            }
        }
        assert validateFileLength("on open");
        setWriteVersion(currentVersion);
        if (lastStoredVersion == INITIAL_VERSION) {
            lastStoredVersion = currentVersion - 1;
        }
    }

    private boolean findLastChunkWithCompleteValidChunkSet(Chunk[] lastChunkCandidates,
            Map<Long, Chunk> validChunksByLocation,
            Map<Integer, Chunk> validChunksById,
            boolean afterFullScan) {
        // Try candidates for "last chunk" in order from newest to oldest
        // until suitable is found. Suitable one should have meta map
        // where all chunk references point to valid locations.
        for (Chunk chunk : lastChunkCandidates) {
            boolean verified = true;
            try {
                setLastChunk(chunk);
                // load the chunk metadata: although meta's root page resides in the lastChunk,
                // traversing meta map might recursively load another chunk(s)
                Cursor<String, String> cursor = meta.cursor(DataUtils.META_CHUNK);
                while (cursor.hasNext() && cursor.next().startsWith(DataUtils.META_CHUNK)) {
                    Chunk c = Chunk.fromString(cursor.getValue());
                    assert c.version <= currentVersion;
                    // might be there already, due to meta traversal
                    // see readPage() ... getChunkIfFound()
                    Chunk test = chunks.putIfAbsent(c.id, c);
                    if (test != null) {
                        c = test;
                    }
                    assert chunks.get(c.id) == c;
                    if ((test = validChunksByLocation.get(c.block)) == null || test.id != c.id) {
                        if ((test = validChunksById.get(c.id)) != null) {
                            // We do not have a valid chunk at that location,
                            // but there is a copy of same chunk from original
                            // location.
                            // Chunk header at original location does not have
                            // any dynamic (occupancy) metadata, so it can't be
                            // used here as is, re-point our chunk to original
                            // location instead.
                            c.block = test.block;
                        } else if (!c.isLive()) {
                            // we can just remove entry from meta, referencing to this chunk,
                            // but store maybe R/O, and it's not properly started yet,
                            // so lets make this chunk "dead" and taking no space,
                            // and it will be automatically removed later.
                            c.block = Long.MAX_VALUE;
                            c.len = Integer.MAX_VALUE;
                            if (c.unused == 0) {
                                c.unused = creationTime;
                            }
                            if (c.unusedAtVersion == 0) {
                                c.unusedAtVersion = INITIAL_VERSION;
                            }
                        } else if (afterFullScan || readChunkHeaderAndFooter(c.block, c.id) == null) {
                            // chunk reference is invalid
                            // this "last chunk" candidate is not suitable
                            verified = false;
                            break;
                        }
                    }
                }
            } catch(Exception ignored) {
                verified = false;
            }
            if (verified) {
                return true;
            }
        }
        return false;
    }

    private void setLastChunk(Chunk last) {
        chunks.clear();
        lastChunk = last;
        if (last == null) {
            // no valid chunk
            lastMapId.set(0);
            currentVersion = 0;
            lastStoredVersion = INITIAL_VERSION;
            meta.setRootPos(0, INITIAL_VERSION);
        } else {
            lastMapId.set(last.mapId);
            currentVersion = last.version;
            chunks.put(last.id, last);
            lastStoredVersion = currentVersion - 1;
            meta.setRootPos(last.metaRootPos, lastStoredVersion);
        }
    }

    /**
     * Discover a valid chunk, searching file backwards from the given block
     *
     * @param block to start search from (found chunk footer should be no
     *            further than block-1)
     * @return valid chunk or null if none found
     */
    private Chunk discoverChunk(long block) {
        long candidateLocation = Long.MAX_VALUE;
        Chunk candidate = null;
        while (true) {
            if (block == candidateLocation) {
                return candidate;
            }
            if (block == 2) { // number of blocks occupied by headers
                return null;
            }
            Chunk test = readChunkFooter(block);
            if (test != null) {
                // if we encounter chunk footer (with or without corresponding header)
                // in the middle of prospective chunk, stop considering it
                candidateLocation = Long.MAX_VALUE;
                test = readChunkHeaderOptionally(test.block, test.id);
                if (test != null) {
                    // if that footer has a corresponding header,
                    // consider them as a new candidate for a valid chunk
                    candidate = test;
                    candidateLocation = test.block;
                }
            }

            // if we encounter chunk header without corresponding footer
            // (due to incomplete write?) in the middle of prospective
            // chunk, stop considering it
            if (--block > candidateLocation && readChunkHeaderOptionally(block) != null) {
                candidateLocation = Long.MAX_VALUE;
            }
        }
    }


    /**
     * Read a chunk header and footer, and verify the stored data is consistent.
     *
     * @param block the block
     * @param expectedId of the chunk
     * @return the chunk, or null if the header or footer don't match or are not
     *         consistent
     */
    private Chunk readChunkHeaderAndFooter(long block, int expectedId) {
        Chunk header = readChunkHeaderOptionally(block, expectedId);
        if (header != null) {
            Chunk footer = readChunkFooter(block + header.len);
            if (footer == null || footer.id != expectedId || footer.block != header.block) {
                return null;
            }
        }
        return header;
    }

    /**
     * Try to read a chunk footer.
     *
     * @param block the index of the next block after the chunk
     * @return the chunk, or null if not successful
     */
    private Chunk readChunkFooter(long block) {
        // the following can fail for various reasons
        try {
            // read the chunk footer of the last block of the file
            long pos = block * BLOCK_SIZE - Chunk.FOOTER_LENGTH;
            if(pos < 0) {
                return null;
            }
            ByteBuffer lastBlock = fileStore.readFully(pos, Chunk.FOOTER_LENGTH);
            byte[] buff = new byte[Chunk.FOOTER_LENGTH];
            lastBlock.get(buff);
            HashMap<String, String> m = DataUtils.parseChecksummedMap(buff);
            if (m != null) {
                int chunk = DataUtils.readHexInt(m, HDR_CHUNK, 0);
                Chunk c = new Chunk(chunk);
                c.version = DataUtils.readHexLong(m, HDR_VERSION, 0);
                c.block = DataUtils.readHexLong(m, HDR_BLOCK, 0);
                return c;
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    private void writeStoreHeader() {
        StringBuilder buff = new StringBuilder(112);
        if (lastChunk != null) {
            storeHeader.put(HDR_BLOCK, lastChunk.block);
            storeHeader.put(HDR_CHUNK, lastChunk.id);
            storeHeader.put(HDR_VERSION, lastChunk.version);
        }
        DataUtils.appendMap(buff, storeHeader);
        byte[] bytes = buff.toString().getBytes(StandardCharsets.ISO_8859_1);
        int checksum = DataUtils.getFletcher32(bytes, 0, bytes.length);
        DataUtils.appendMap(buff, HDR_FLETCHER, checksum);
        buff.append('\n');
        bytes = buff.toString().getBytes(StandardCharsets.ISO_8859_1);
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
            panic(e);
        }
    }

    /**
     * Close the file and the store. Unsaved changes are written to disk first.
     */
    @Override
    public void close() {
        closeStore(true, 0);
    }

    /**
     * Close the file and the store. Unsaved changes are written to disk first,
     * and compaction (up to a specified number of milliseconds) is attempted.
     *
     * @param allowedCompactionTime the allowed time for compaction (in
     *            milliseconds)
     */
    public void close(long allowedCompactionTime) {
        closeStore(true, allowedCompactionTime);
    }

    /**
     * Close the file and the store, without writing anything.
     * This will try to stop the background thread (without waiting for it).
     * This method ignores all errors.
     */
    public void closeImmediately() {
        try {
            closeStore(false, 0);
        } catch (Throwable e) {
            handleException(e);
        }
    }

    private void closeStore(boolean normalShutdown, long allowedCompactionTime) {
        // If any other thead have already initiated closure procedure,
        // isClosed() would wait until closure is done and then  we jump out of the loop.
        // This is a subtle difference between !isClosed() and isOpen().
        while (!isClosed()) {
            stopBackgroundThread(normalShutdown);
            storeLock.lock();
            try {
                if (state == STATE_OPEN) {
                    state = STATE_STOPPING;
                    try {
                        try {
                            if (normalShutdown && fileStore != null && !fileStore.isReadOnly()) {
                                for (MVMap<?, ?> map : maps.values()) {
                                    if (map.isClosed()) {
                                        deregisterMapRoot(map.getId());
                                    }
                                }
                                setRetentionTime(0);
                                commit();
                                if (allowedCompactionTime > 0) {
                                    compactFile(allowedCompactionTime);
                                } else if (allowedCompactionTime < 0) {
                                    doMaintenance(autoCompactFillRate);
                                }
                                shrinkFileIfPossible(0);
                                storeHeader.put(HDR_CLEAN, 1);
                                writeStoreHeader();
                                sync();
                                assert validateFileLength("on close");
                            }

                            state = STATE_CLOSING;

                            // release memory early - this is important when called
                            // because of out of memory
                            clearCaches();
                            for (MVMap<?, ?> m : new ArrayList<>(maps.values())) {
                                m.close();
                            }
                            chunks.clear();
                            maps.clear();
                        } finally {
                            if (fileStore != null && !fileStoreIsProvided) {
                                fileStore.close();
                            }
                        }
                    } finally {
                        state = STATE_CLOSED;
                    }
                }
            } finally {
                storeLock.unlock();
            }
        }
    }

    /**
     * Read a page of data into a ByteBuffer.
     *
     * @param pos page pos
     * @param expectedMapId expected map id for the page
     * @return ByteBuffer containing page data.
     */
    private ByteBuffer readBufferForPage(long pos, int expectedMapId) {
        return getChunk(pos).readBufferForPage(fileStore, pos, expectedMapId);
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
            checkOpen();
            String s = meta.get(Chunk.getMetaKey(chunkId));
            if (s == null) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_CHUNK_NOT_FOUND,
                        "Chunk {0} not found", chunkId);
            }
            c = Chunk.fromString(s);
            if (!c.isSaved()) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Chunk {0} is invalid", chunkId);
            }
            chunks.put(c.id, c);
        }
        return c;
    }

    private void setWriteVersion(long version) {
        for (Iterator<MVMap<?, ?>> iter = maps.values().iterator(); iter.hasNext(); ) {
            MVMap<?, ?> map = iter.next();
            assert map != meta;
            if (map.setWriteVersion(version) == null) {
                iter.remove();
            }
        }
        meta.setWriteVersion(version);
        onVersionChange(version);
    }

    /**
     * Unlike regular commit this method returns immediately if there is commit
     * in progress on another thread, otherwise it acts as regular commit.
     *
     * This method may return BEFORE this thread changes are actually persisted!
     *
     * @return the new version (incremented if there were changes)
     */
    public long tryCommit() {
        // we need to prevent re-entrance, which may be possible,
        // because meta map is modified within storeNow() and that
        // causes beforeWrite() call with possibility of going back here
        if ((!storeLock.isHeldByCurrentThread() || currentStoreVersion < 0) &&
                storeLock.tryLock()) {
            try {
                store();
            } finally {
                storeLock.unlock();
            }
        }
        return currentVersion;
    }

    /**
     * Commit the changes.
     * <p>
     * This method does nothing if there are no unsaved changes,
     * otherwise it increments the current version
     * and stores the data (for file based stores).
     * <p>
     * It is not necessary to call this method when auto-commit is enabled (the default
     * setting), as in this case it is automatically called from time to time or
     * when enough changes have accumulated. However, it may still be called to
     * flush all changes to disk.
     * <p>
     * At most one store operation may run at any time.
     *
     * @return the new version (incremented if there were changes)
     */
    public long commit() {
        // we need to prevent re-entrance, which may be possible,
        // because meta map is modified within storeNow() and that
        // causes beforeWrite() call with possibility of going back here
        if(!storeLock.isHeldByCurrentThread() || currentStoreVersion < 0) {
            storeLock.lock();
            try {
                store();
            } finally {
                storeLock.unlock();
            }
        }
        return currentVersion;
    }

    private void store() {
        store(0, reuseSpace ? 0 : getAfterLastBlock());
    }

    private void store(long reservedLow, long reservedHigh) {
        assert storeLock.isHeldByCurrentThread();
        if (isOpenOrStopping()) {
            if (hasUnsavedChanges()) {
                dropUnusedChunks();
                try {
                    currentStoreVersion = currentVersion;
                    if (fileStore == null) {
                        lastStoredVersion = currentVersion;
                        //noinspection NonAtomicOperationOnVolatileField
                        ++currentVersion;
                        setWriteVersion(currentVersion);
                        metaChanged = false;
                    } else {
                        if (fileStore.isReadOnly()) {
                            throw DataUtils.newIllegalStateException(
                                    DataUtils.ERROR_WRITING_FAILED, "This store is read-only");
                        }
                        try {
                            storeNow(reservedLow, reservedHigh);
                        } catch (IllegalStateException e) {
                            panic(e);
                        } catch (Throwable e) {
                            panic(DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "{0}", e.toString(),
                                    e));
                        }
                    }
                } finally {
                    // in any case reset the current store version,
                    // to allow closing the store
                    currentStoreVersion = -1;
                }
            }
        }
    }

    private void storeNow(long reservedLow, long reservedHigh) {
        long time = getTimeSinceCreation();
        int currentUnsavedPageCount = unsavedMemory;
        long storeVersion = currentStoreVersion;
        long version = ++currentVersion;
        lastCommitTime = time;

        // the metadata of the last chunk was not stored so far, and needs to be
        // set now (it's better not to update right after storing, because that
        // would modify the meta map again)
        int lastChunkId;
        if (lastChunk == null) {
            lastChunkId = 0;
        } else {
            lastChunkId = lastChunk.id;
            meta.put(Chunk.getMetaKey(lastChunkId), lastChunk.asString());
            markMetaChanged();
            // never go backward in time
            time = Math.max(lastChunk.time, time);
        }
        int newChunkId = lastChunkId;
        while (true) {
            newChunkId = (newChunkId + 1) & Chunk.MAX_ID;
            Chunk old = chunks.get(newChunkId);
            if (old == null) {
                break;
            }
            if (!old.isSaved()) {
                IllegalStateException e = DataUtils.newIllegalStateException(
                        DataUtils.ERROR_INTERNAL,
                        "Last block {0} not stored, possibly due to out-of-memory", old);
                panic(e);
            }
        }
        Chunk c = new Chunk(newChunkId);
        c.pageCount = 0;
        c.pageCountLive = 0;
        c.maxLen = 0;
        c.maxLenLive = 0;
        c.metaRootPos = Long.MAX_VALUE;
        c.block = Long.MAX_VALUE;
        c.len = Integer.MAX_VALUE;
        c.time = time;
        c.version = version;
        c.next = Long.MAX_VALUE;
        chunks.put(c.id, c);
        ArrayList<Page> changed = new ArrayList<>();
        for (Iterator<MVMap<?, ?>> iter = maps.values().iterator(); iter.hasNext(); ) {
            MVMap<?, ?> map = iter.next();
            RootReference rootReference = map.setWriteVersion(version);
            if (rootReference == null) {
                iter.remove();
            } else if (map.getCreateVersion() <= storeVersion && // if map was created after storing started, skip it
                    !map.isVolatile() &&
                    map.hasChangesSince(lastStoredVersion)) {
                assert rootReference.version <= version : rootReference.version + " > " + version;
                Page rootPage = rootReference.root;
                if (!rootPage.isSaved() ||
                        // after deletion previously saved leaf
                        // may pop up as a root, but we still need
                        // to save new root pos in meta
                        rootPage.isLeaf()) {
                    changed.add(rootPage);
                }
            }
        }
        WriteBuffer buff = getWriteBuffer();
        // need to patch the header later
        c.writeChunkHeader(buff, 0);
        int headerLength = buff.position() + 44;
        buff.position(headerLength);
        for (Page p : changed) {
            String key = MVMap.getMapRootKey(p.getMapId());
            if (p.getTotalCount() == 0) {
                meta.remove(key);
            } else {
                p.writeUnsavedRecursive(c, buff);
                long root = p.getPos();
                meta.put(key, Long.toHexString(root));
            }
        }

        acceptChunkOccupancyChanges(time, version);

        RootReference metaRootReference = meta.setWriteVersion(version);
        assert metaRootReference != null;
        assert metaRootReference.version == version : metaRootReference.version + " != " + version;
        metaChanged = false;

        acceptChunkOccupancyChanges(time, version);

        onVersionChange(version);

        Page metaRoot = metaRootReference.root;
        metaRoot.writeUnsavedRecursive(c, buff);

        // last allocated map id should be captured after the meta map was saved, because
        // this will ensure that concurrently created map, which made it into meta before save,
        // will have it's id reflected in mapid field of currently written chunk
        c.mapId = lastMapId.get();

        int chunkLength = buff.position();

        // add the store header and round to the next block
        int length = MathUtils.roundUpInt(chunkLength +
                Chunk.FOOTER_LENGTH, BLOCK_SIZE);
        buff.limit(length);

        long filePos = fileStore.allocate(length, reservedLow, reservedHigh);
        c.block = filePos / BLOCK_SIZE;
        c.len = length / BLOCK_SIZE;
        assert validateFileLength(c.asString());
        c.metaRootPos = metaRoot.getPos();
        // calculate and set the likely next position
        if (reservedLow > 0 || reservedHigh == reservedLow) {
            c.next = fileStore.predictAllocation(c.len, 0, 0);
        } else {
            // just after this chunk
            c.next = 0;
        }
        assert c.pageCountLive == c.pageCount : c;
        buff.position(0);
        c.writeChunkHeader(buff, headerLength);

        buff.position(buff.limit() - Chunk.FOOTER_LENGTH);
        buff.put(c.getFooterBytes());

        buff.position(0);
        write(filePos, buff.getBuffer());
        releaseWriteBuffer(buff);

        // whether we need to write the store header
        boolean writeStoreHeader = false;
        // end of the used space is not necessarily the end of the file
        boolean storeAtEndOfFile = filePos + length >= fileStore.size();
        if (!storeAtEndOfFile) {
            if (lastChunk == null) {
                writeStoreHeader = true;
            } else if (lastChunk.next != c.block) {
                // the last prediction did not matched
                writeStoreHeader = true;
            } else {
                long headerVersion = DataUtils.readHexLong(storeHeader, HDR_VERSION, 0);
                if (lastChunk.version - headerVersion > 20) {
                    // we write after at least every 20 versions
                    writeStoreHeader = true;
                } else {
                    int chunkId = DataUtils.readHexInt(storeHeader, HDR_CHUNK, 0);
                    while (true) {
                        Chunk old = chunks.get(chunkId);
                        if (old == null) {
                            // one of the chunks in between
                            // was removed
                            writeStoreHeader = true;
                            break;
                        }
                        if (chunkId == lastChunk.id) {
                            break;
                        }
                        chunkId++;
                    }
                }
            }
        }

        if (storeHeader.remove(HDR_CLEAN) != null) {
            writeStoreHeader = true;
        }

        lastChunk = c;
        if (writeStoreHeader) {
            writeStoreHeader();
        }
        if (!storeAtEndOfFile) {
            // may only shrink after the store header was written
            shrinkFileIfPossible(1);
        }
        for (Page p : changed) {
            p.writeEnd();
        }
        metaRoot.writeEnd();

        // some pages might have been changed in the meantime (in the newest
        // version)
        saveNeeded = false;
        unsavedMemory = Math.max(0, unsavedMemory - currentUnsavedPageCount);
        lastStoredVersion = storeVersion;
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

    private static boolean canOverwriteChunk(Chunk c, long oldestVersionToKeep) {
        return !c.isLive() && c.unusedAtVersion < oldestVersionToKeep;
    }

    private boolean isSeasonedChunk(Chunk chunk, long time) {
        return retentionTime < 0 || chunk.time + retentionTime <= time;
    }

    private long getTimeSinceCreation() {
        return Math.max(0, getTimeAbsolute() - creationTime);
    }

    private long getTimeAbsolute() {
        long now = System.currentTimeMillis();
        if (lastTimeAbsolute != 0 && now < lastTimeAbsolute) {
            // time seems to have run backwards - this can happen
            // when the system time is adjusted, for example
            // on a leap second
            now = lastTimeAbsolute;
        } else {
            lastTimeAbsolute = now;
        }
        return now;
    }

    /**
     * Apply the freed space to the chunk metadata. The metadata is updated, but
     * completely free chunks are not removed from the set of chunks, and the
     * disk space is not yet marked as free. They are queued instead and wait until
     * their usage is over.
     */
    private void acceptChunkOccupancyChanges(long time, long version) {
        Set<Chunk> modifiedChunks = new HashSet<>();
        while (true) {
            RemovedPageInfo rpi;
            while ((rpi = removedPages.peek()) != null && rpi.version < version) {
                rpi = removedPages.poll();  // could be different from the peeked one
                assert rpi != null;         // since nobody else retrieves from queue
                assert rpi.version < version : rpi + " < " + version;
                int chunkId = rpi.getPageChunkId();
                Chunk chunk = chunks.get(chunkId);
                assert chunk != null;
                modifiedChunks.add(chunk);
                if (chunk.accountForRemovedPage(rpi.getPageLength(), rpi.isPinned(), time, rpi.version)) {
                    deadChunks.offer(chunk);
                }
            }
            if (modifiedChunks.isEmpty()) {
                return;
            }
            for (Chunk chunk : modifiedChunks) {
                int chunkId = chunk.id;
                meta.put(Chunk.getMetaKey(chunkId), chunk.asString());
            }
            markMetaChanged();
            modifiedChunks.clear();
        }
    }

    /**
     * Shrink the file if possible, and if at least a given percentage can be
     * saved.
     *
     * @param minPercent the minimum percentage to save
     */
    private void shrinkFileIfPossible(int minPercent) {
        if (fileStore.isReadOnly()) {
            return;
        }
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
        if (isOpenOrStopping()) {
            sync();
        }
        fileStore.truncate(end);
    }

    /**
     * Get the position right after the last used byte.
     *
     * @return the position
     */
    private long getFileLengthInUse() {
        long result = fileStore.getFileLengthInUse();
        assert result == measureFileLengthInUse() : result + " != " + measureFileLengthInUse();
        return result;
    }

    /**
     * Get the index of the first block after last occupied one.
     * It marks the beginning of the last (infinite) free space.
     *
     * @return block index
     */
    private long getAfterLastBlock() {
        return fileStore.getAfterLastBlock();
    }

    private long measureFileLengthInUse() {
        long size = 2;
        for (Chunk c : chunks.values()) {
            if (c.isSaved()) {
                size = Math.max(size, c.block + c.len);
            }
        }
        return size * BLOCK_SIZE;
    }

    /**
     * Check whether there are any unsaved changes.
     *
     * @return if there are any changes
     */
    public boolean hasUnsavedChanges() {
        if (metaChanged) {
            return true;
        }
        for (MVMap<?, ?> m : maps.values()) {
            if (!m.isClosed()) {
                if(m.hasChangesSince(lastStoredVersion)) {
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

    private Chunk readChunkHeaderOptionally(long block) {
        try {
            Chunk chunk = readChunkHeader(block);
            return chunk.block != block ? null : chunk;
        } catch (Exception ignore) {
            return null;
        }
    }

    private Chunk readChunkHeaderOptionally(long block, int expectedId) {
        Chunk chunk = readChunkHeaderOptionally(block);
        return chunk == null || chunk.id != expectedId ? null : chunk;
    }

    /**
     * Compact by moving all chunks next to each other.
     */
    public void compactMoveChunks() {
        compactMoveChunks(100, Long.MAX_VALUE);
    }

    /**
     * Compact the store by moving all chunks next to each other, if there is
     * free space between chunks. This might temporarily increase the file size.
     * Chunks are overwritten irrespective of the current retention time. Before
     * overwriting chunks and before resizing the file, syncFile() is called.
     *
     * @param targetFillRate do nothing if the file store fill rate is higher
     *            than this
     * @param moveSize the number of bytes to move
     */
    private void compactMoveChunks(int targetFillRate, long moveSize) {
        storeLock.lock();
        try {
            checkOpen();
            if (lastChunk != null && reuseSpace) {
                int oldRetentionTime = retentionTime;
                boolean oldReuse = reuseSpace;
                try {
                    retentionTime = -1;
                    if (getFillRate() <= targetFillRate) {
                        compactMoveChunks(moveSize);
                    }
                } finally {
                    reuseSpace = oldReuse;
                    retentionTime = oldRetentionTime;
                }
            }
        } finally {
            storeLock.unlock();
        }
    }

    private boolean compactMoveChunks(long moveSize) {
        dropUnusedChunks();
        long start = fileStore.getFirstFree() / BLOCK_SIZE;
        Iterable<Chunk> chunksToMove = findChunksToMove(start, moveSize);
        if (chunksToMove == null) {
            return false;
        }
        compactMoveChunks(chunksToMove);
        return true;
    }

    private Iterable<Chunk> findChunksToMove(long startBlock, long moveSize) {
        long maxBlocksToMove = moveSize / BLOCK_SIZE;
        Iterable<Chunk> result = null;
        if (maxBlocksToMove > 0) {
            PriorityQueue<Chunk> queue = new PriorityQueue<>(chunks.size() / 2 + 1,
                    new Comparator<Chunk>() {
                        @Override
                        public int compare(Chunk o1, Chunk o2) {
                            // instead of selection just closest to beginning of the file,
                            // pick smaller chunk(s) which sit in between bigger holes
                            int res = Integer.compare(o2.collectPriority, o1.collectPriority);
                            if (res != 0) {
                                return res;
                            }
                            return Long.signum(o2.block - o1.block);
                        }
                    });
            long size = 0;
            for (Chunk chunk : chunks.values()) {
                if (chunk.isSaved() && chunk.block > startBlock) {
                    chunk.collectPriority = getMovePriority(chunk);
                    queue.offer(chunk);
                    size += chunk.len;
                    while (size > maxBlocksToMove) {
                        Chunk removed = queue.poll();
                        if (removed == null) {
                            break;
                        }
                        size -= removed.len;
                    }
                }
            }
            if (!queue.isEmpty()) {
                ArrayList<Chunk> list = new ArrayList<>(queue);
                Collections.sort(list, Chunk.PositionComparator.INSTANCE);
                result = list;
            }
        }
        return result;
    }

    private int getMovePriority(Chunk chunk) {
        return fileStore.getMovePriority((int)chunk.block);
    }

    private void compactMoveChunks(Iterable<Chunk> move) {
        assert storeLock.isHeldByCurrentThread();
        if (move != null) {
            assert lastChunk != null;
            // this will ensure better recognition of the last chunk
            // in case of power failure, since we are going to move older chunks
            // to the end of the file
            writeStoreHeader();
            sync();

            Iterator<Chunk> iterator = move.iterator();
            assert iterator.hasNext();
            long leftmostBlock = iterator.next().block;
            long originalBlockCount = getAfterLastBlock();
            // we need to ensure that chunks moved within the following loop
            // do not overlap with space just released by chunks moved before them,
            // hence the need to reserve this area [leftmostBlock, originalBlockCount)
            for (Chunk chunk : move) {
                moveChunk(chunk, leftmostBlock, originalBlockCount);
            }
            // update the metadata (hopefully within the file)
            store(leftmostBlock, originalBlockCount);
            sync();

            Chunk chunkToMove = lastChunk;
            long postEvacuationBlockCount = getAfterLastBlock();

            boolean chunkToMoveIsAlreadyInside = chunkToMove.block < leftmostBlock;
            boolean movedToEOF = !chunkToMoveIsAlreadyInside;
            // move all chunks, which previously did not fit before reserved area
            // now we can re-use previously reserved area [leftmostBlock, originalBlockCount),
            // but need to reserve [originalBlockCount, postEvacuationBlockCount)
            for (Chunk c : move) {
                if (c.block >= originalBlockCount &&
                        moveChunk(c, originalBlockCount, postEvacuationBlockCount)) {
                    assert c.block < originalBlockCount;
                    movedToEOF = true;
                }
            }
            assert postEvacuationBlockCount >= getAfterLastBlock();

            if (movedToEOF) {
                boolean moved = moveChunkInside(chunkToMove, originalBlockCount);

                // store a new chunk with updated metadata (hopefully within a file)
                store(originalBlockCount, postEvacuationBlockCount);
                sync();
                // if chunkToMove did not fit within originalBlockCount (move is
                // false), and since now previously reserved area
                // [originalBlockCount, postEvacuationBlockCount) also can be
                // used, lets try to move that chunk into this area, closer to
                // the beginning of the file
                long lastBoundary = moved || chunkToMoveIsAlreadyInside ?
                                        postEvacuationBlockCount : chunkToMove.block;
                moved = !moved && moveChunkInside(chunkToMove, lastBoundary);
                if (moveChunkInside(lastChunk, lastBoundary) || moved) {
                    store(lastBoundary, -1);
                }
            }

            shrinkFileIfPossible(0);
            sync();
        }
    }

    private boolean moveChunkInside(Chunk chunkToMove, long boundary) {
        boolean res = chunkToMove.block >= boundary &&
                fileStore.predictAllocation(chunkToMove.len, boundary, -1) < boundary &&
                moveChunk(chunkToMove, boundary, -1);
        assert !res || chunkToMove.block + chunkToMove.len <= boundary;
        return res;
    }

    /**
     * Move specified chunk into free area of the file. "Reserved" area
     * specifies file interval to be avoided, when un-allocated space will be
     * chosen for a new chunk's location.
     *
     * @param chunk to move
     * @param reservedAreaLow low boundary of reserved area, inclusive
     * @param reservedAreaHigh high boundary of reserved area, exclusive
     * @return true if block was moved, false otherwise
     */
    private boolean moveChunk(Chunk chunk, long reservedAreaLow, long reservedAreaHigh) {
        // ignore if already removed during the previous store operations
        // those are possible either as explicit commit calls
        // or from meta map updates at the end of this method
        if (!chunks.containsKey(chunk.id)) {
            return false;
        }
        WriteBuffer buff = getWriteBuffer();
        long start = chunk.block * BLOCK_SIZE;
        int length = chunk.len * BLOCK_SIZE;
        buff.limit(length);
        ByteBuffer readBuff = fileStore.readFully(start, length);
        Chunk chunkFromFile = Chunk.readChunkHeader(readBuff, start);
        int chunkHeaderLen = readBuff.position();
        buff.position(chunkHeaderLen);
        buff.put(readBuff);
        long pos = fileStore.allocate(length, reservedAreaLow, reservedAreaHigh);
        long block = pos / BLOCK_SIZE;
        // in the absence of a reserved area,
        // block should always move closer to the beginning of the file
        assert reservedAreaHigh > 0 || block <= chunk.block : block + " " + chunk;
        buff.position(0);
        // can not set chunk's new block/len until it's fully written at new location,
        // because concurrent reader can pick it up prematurely,
        // also occupancy accounting fields should not leak into header
        chunkFromFile.block = block;
        chunkFromFile.next = 0;
        chunkFromFile.writeChunkHeader(buff, chunkHeaderLen);
        buff.position(length - Chunk.FOOTER_LENGTH);
        buff.put(chunkFromFile.getFooterBytes());
        buff.position(0);
        write(pos, buff.getBuffer());
        releaseWriteBuffer(buff);
        fileStore.free(start, length);
        chunk.block = block;
        chunk.next = 0;
        meta.put(Chunk.getMetaKey(chunk.id), chunk.asString());
        markMetaChanged();
        return true;
    }

    /**
     * Force all stored changes to be written to the storage. The default
     * implementation calls FileChannel.force(true).
     */
    public void sync() {
        checkOpen();
        FileStore f = fileStore;
        if (f != null) {
            f.sync();
        }
    }

    /**
     * Compact store file, that is, compact blocks that have a low
     * fill rate, and move chunks next to each other. This will typically
     * shrink the file. Changes are flushed to the file, and old
     * chunks are overwritten.
     *
     * @param maxCompactTime the maximum time in milliseconds to compact
     */
    public void compactFile(long maxCompactTime) {
        setRetentionTime(0);
        long start = System.nanoTime();
        while (compact(95, 16 * 1024 * 1024)) {
            sync();
            compactMoveChunks(95, 16 * 1024 * 1024);
            long time = System.nanoTime() - start;
            if (time > TimeUnit.MILLISECONDS.toNanos(maxCompactTime)) {
                break;
            }
        }
    }

    /**
     * Try to increase the fill rate by re-writing partially full chunks. Chunks
     * with a low number of live items are re-written.
     * <p>
     * If the current fill rate is higher than the target fill rate, nothing is
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
     * @param write the minimum number of bytes to write
     * @return if a chunk was re-written
     */
    public boolean compact(int targetFillRate, int write) {
        if (reuseSpace && lastChunk != null) {
            checkOpen();
            if (targetFillRate > 0 && getChunksFillRate() < targetFillRate) {
                // We can't wait forever for the lock here,
                // because if called from the background thread,
                // it might go into deadlock with concurrent database closure
                // and attempt to stop this thread.
                try {
                    if (storeLock.tryLock(10, TimeUnit.MILLISECONDS)) {
                        try {
                            return rewriteChunks(write);
                        } finally {
                            storeLock.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return false;
    }

    private boolean rewriteChunks(int writeLimit) {
        TxCounter txCounter = registerVersionUsage();
        try {
            Iterable<Chunk> old = findOldChunks(writeLimit);
            if (old != null) {
                HashSet<Integer> idSet = createIdSet(old);
                return !idSet.isEmpty() && compactRewrite(idSet) > 0;
            }
        } finally {
            deregisterVersionUsage(txCounter);
        }
        return false;
    }

    /**
     * Get the current fill rate (percentage of used space in the file). Unlike
     * the fill rate of the store, here we only account for chunk data; the fill
     * rate here is how much of the chunk data is live (still referenced). Young
     * chunks are considered live.
     *
     * @return the fill rate, in percent (100 is completely full)
     */
    public int getChunksFillRate() {
        long maxLengthSum = 1;
        long maxLengthLiveSum = 1;
        for (Chunk c : chunks.values()) {
            assert c.maxLen >= 0;
            maxLengthSum += c.maxLen;
            maxLengthLiveSum += c.maxLenLive;
        }
        // the fill rate of all chunks combined
        int fillRate = (int) (100 * maxLengthLiveSum / maxLengthSum);
        return fillRate;
    }

    private int getProjectedFillRate() {
        int vacatedBlocks = 0;
        long maxLengthSum = 1;
        long maxLengthLiveSum = 1;
        long time = getTimeSinceCreation();
        for (Chunk c : chunks.values()) {
            assert c.maxLen >= 0;
            if (isRewritable(c, time)) {
                assert c.maxLenLive >= c.maxLenLive;
                vacatedBlocks += c.len;
                maxLengthSum += c.maxLen;
                maxLengthLiveSum += c.maxLenLive;
            }
        }
        int additionalBlocks  = (int) (vacatedBlocks * maxLengthLiveSum / maxLengthSum);
        int fillRate = fileStore.getProjectedFillRate(vacatedBlocks - additionalBlocks);
        return fillRate;
    }

    public int getFillRate() {
        return fileStore.getFillRate();
    }

    private Iterable<Chunk> findOldChunks(int writeLimit) {
        assert lastChunk != null;
        long time = getTimeSinceCreation();

        // the queue will contain chunks we want to free up
        PriorityQueue<Chunk> queue = new PriorityQueue<>(this.chunks.size() / 4 + 1,
                new Comparator<Chunk>() {
                    @Override
                    public int compare(Chunk o1, Chunk o2) {
                        int comp = Integer.compare(o2.collectPriority, o1.collectPriority);
                        if (comp == 0) {
                            comp = Long.compare(o2.maxLenLive, o2.maxLenLive);
                        }
                        return comp;
                    }
                });

        long totalSize = 0;
        long latestVersion = lastChunk.version + 1;
        for (Chunk chunk : chunks.values()) {
            // only look at chunk older than the retention time
            // (it's possible to compact chunks earlier, but right
            // now we don't do that)
            if (isRewritable(chunk, time)) {
                long age = latestVersion - chunk.version;
                chunk.collectPriority = (int) (chunk.getFillRate() * 1000 / age);
                totalSize += chunk.maxLenLive;
                queue.offer(chunk);
                while (totalSize > writeLimit) {
                    Chunk removed = queue.poll();
                    if (removed == null) {
                        break;
                    }
                    totalSize -= removed.maxLenLive;
                }
            }
        }

        return queue.isEmpty() ? null : queue;
    }

    private boolean isRewritable(Chunk chunk, long time) {
        return chunk.isRewritable() && isSeasonedChunk(chunk, time);
    }

    private int compactRewrite(Set<Integer> set) {
        assert storeLock.isHeldByCurrentThread();
        // this will ensure better recognition of the last chunk
        // in case of power failure, since we are going to move older chunks
        // to the end of the file
        writeStoreHeader();
        sync();

        int rewrittenPageCount = 0;
        storeLock.unlock();
        try {
            for (MVMap<?, ?> map : maps.values()) {
                if (!map.isClosed() && !map.isSingleWriter()) {
                    try {
                        rewrittenPageCount += map.rewrite(set);
                    } catch(IllegalStateException ex) {
                        if (!map.isClosed()) {
                            throw ex;
                        }
                    }
                }
            }
            int rewriteMetaCount = meta.rewrite(set);
            if (rewriteMetaCount > 0) {
                markMetaChanged();
                rewrittenPageCount += rewriteMetaCount;
            }
        } finally {
            storeLock.lock();
        }
        commit();
        assert validateRewrite(set);
        return rewrittenPageCount;
    }

    private boolean validateRewrite(Set<Integer> set) {
        for (Integer chunkId : set) {
            Chunk chunk = chunks.get(chunkId);
            if (chunk != null && chunk.isLive()) {
                int pageCountLive = chunk.pageCountLive;
                RemovedPageInfo[] removedPageInfos = removedPages.toArray(new RemovedPageInfo[0]);
                for (RemovedPageInfo rpi : removedPageInfos) {
                    if (rpi.getPageChunkId() == chunk.id) {
                        --pageCountLive;
                    }
                }
                if (pageCountLive != 0) {
                    for (String mapName : getMapNames()) {
                        if (!mapName.startsWith("undoLog") && hasData(mapName)) {   // non-singleWriter map has data
                            int mapId = getMapId(mapName);
                            if (!maps.containsKey(mapId)) { // map is not open
                                // all bets are off
                                return true;
                            }
                        }
                    }
                    assert pageCountLive != 0 : chunk + " " + Arrays.toString(removedPageInfos);
                }
            }
        }
        return true;
    }

    private static HashSet<Integer> createIdSet(Iterable<Chunk> toCompact) {
        HashSet<Integer> set = new HashSet<>();
        for (Chunk c : toCompact) {
            set.add(c.id);
        }
        return set;
    }

    /**
     * Read a page.
     *
     * @param map the map
     * @param pos the page position
     * @return the page
     */
    Page readPage(MVMap<?, ?> map, long pos) {
        try {
            if (!DataUtils.isPageSaved(pos)) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT, "Position 0");
            }
            Page p = cache == null ? null : cache.get(pos);
            if (p == null) {
                ByteBuffer buff = readBufferForPage(pos, map.getId());
                try {
                    p = Page.read(buff, pos, map);
                } catch (Exception e) {
                    throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                            "Unable to read the page at position {0}", pos, e);
                }
                cachePage(p);
            }
            return p;
        } catch (IllegalStateException e) {
            if (recoveryMode) {
                return map.createEmptyLeaf();
            }
            throw e;
        }
    }

    /**
     * Remove a page.
     *
     * @param pos the position of the page
     * @param version at which page was removed
     * @param pinned whether page is considered pinned
     */
    void accountForRemovedPage(long pos, long version, boolean pinned) {
        assert DataUtils.isPageSaved(pos);
        RemovedPageInfo rpi = new RemovedPageInfo(pos, pinned, version);
        removedPages.add(rpi);
    }

    Compressor getCompressorFast() {
        if (compressorFast == null) {
            compressorFast = new CompressLZF();
        }
        return compressorFast;
    }

    Compressor getCompressorHigh() {
        if (compressorHigh == null) {
            compressorHigh = new CompressDeflate();
        }
        return compressorHigh;
    }

    int getCompressionLevel() {
        return compressionLevel;
    }

    public int getPageSplitSize() {
        return pageSplitSize;
    }

    public int getKeysPerPage() {
        return keysPerPage;
    }

    public long getMaxPageSize() {
        return cache == null ? Long.MAX_VALUE : cache.getMaxItemSize() >> 4;
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
     * The retention time needs to be long enough to allow reading old chunks
     * while traversing over the entries of a map.
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
     * Get the oldest version to retain.
     * We keep at least number of previous versions specified by "versionsToKeep"
     * configuration parameter (default 5).
     * Previously it was used only in case of non-persistent MVStore.
     * Now it's honored in all cases (although H2 always sets it to zero).
     * Oldest version determination also takes into account calls (de)registerVersionUsage(),
     * an will not release the version, while version is still in use.
     *
     * @return the version
     */
    long getOldestVersionToKeep() {
        long v = oldestVersionToKeep.get();
        v = Math.max(v - versionsToKeep, INITIAL_VERSION);
        if (fileStore != null) {
            long storeVersion = lastStoredVersion;
            if (storeVersion != INITIAL_VERSION && storeVersion < v) {
                v = storeVersion;
            }
        }
        return v;
    }

    private void setOldestVersionToKeep(long oldestVersionToKeep) {
        boolean success;
        do {
            long current = this.oldestVersionToKeep.get();
            // Oldest version may only advance, never goes back
            success = oldestVersionToKeep <= current ||
                        this.oldestVersionToKeep.compareAndSet(current, oldestVersionToKeep);
        } while (!success);
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
        if (version == currentVersion || chunks.isEmpty()) {
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
        try {
            for (Iterator<String> it = oldMeta.keyIterator(DataUtils.META_CHUNK); it.hasNext();) {
                String chunkKey = it.next();
                if (!chunkKey.startsWith(DataUtils.META_CHUNK)) {
                    break;
                }
                if (!meta.containsKey(chunkKey)) {
                    String s = oldMeta.get(chunkKey);
                    Chunk c2 = Chunk.fromString(s);
                    Chunk test = readChunkHeaderAndFooter(c2.block, c2.id);
                    if (test == null) {
                        return false;
                    }
                }
            }
        } catch (IllegalStateException e) {
            // the chunk missing where the metadata is stored
            return false;
        }
        return true;
    }

    /**
     * Adjust amount of "unsaved memory" meaning amount of RAM occupied by pages
     * not saved yet to the file. This is the amount which triggers auto-commit.
     *
     * @param memory adjustment
     */
    public void registerUnsavedMemory(int memory) {
        // this counter was intentionally left unprotected against race
        // condition for performance reasons
        // TODO: evaluate performance impact of atomic implementation,
        //       since updates to unsavedMemory are largely aggregated now
        unsavedMemory += memory;
        int newValue = unsavedMemory;
        if (newValue > autoCommitMemory && autoCommitMemory > 0) {
            saveNeeded = true;
        }
    }

    boolean isSaveNeeded() {
        return saveNeeded;
    }

    /**
     * This method is called before writing to a map.
     *
     * @param map the map
     */
    void beforeWrite(MVMap<?, ?> map) {
        if (saveNeeded && fileStore != null && isOpenOrStopping() &&
                // condition below is to prevent potential deadlock,
                // because we should never seek storeLock while holding
                // map root lock
                (storeLock.isHeldByCurrentThread() || !map.getRoot().isLockedByCurrentThread()) &&
                // to avoid infinite recursion via store() -> dropUnusedChunks() -> meta.remove()
                map != meta) {

            saveNeeded = false;
            // check again, because it could have been written by now
            if (unsavedMemory > autoCommitMemory && autoCommitMemory > 0) {
                // if unsaved memory creation rate is to high,
                // some back pressure need to be applied
                // to slow things down and avoid OOME
                if (3 * unsavedMemory > 4 * autoCommitMemory && !map.isSingleWriter()) {
                    commit();
                } else {
                    tryCommit();
                }
            }
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
    public void setStoreVersion(int version) {
        storeLock.lock();
        try {
            checkOpen();
            markMetaChanged();
            meta.put("setting.storeVersion", Integer.toHexString(version));
        } finally {
            storeLock.unlock();
        }
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
    public void rollbackTo(long version) {
        storeLock.lock();
        try {
            checkOpen();
            if (version == 0) {
                // special case: remove all data
                meta.setInitialRoot(meta.createEmptyLeaf(), INITIAL_VERSION);
                deadChunks.clear();
                removedPages.clear();
                chunks.clear();
                clearCaches();
                if (fileStore != null) {
                    fileStore.clear();
                }
                lastChunk = null;
                versions.clear();
                currentVersion = version;
                setWriteVersion(version);
                metaChanged = false;
                lastStoredVersion = INITIAL_VERSION;
                for (MVMap<?, ?> m : maps.values()) {
                    m.close();
                }
                return;
            }
            DataUtils.checkArgument(
                    isKnownVersion(version),
                    "Unknown version {0}", version);

            TxCounter txCounter;
            while ((txCounter = versions.peekLast()) != null && txCounter.version >= version) {
                versions.removeLast();
            }
            currentTxCounter = new TxCounter(version);

            meta.rollbackTo(version);
            metaChanged = false;
            // find out which chunks to remove,
            // and which is the newest chunk to keep
            // (the chunk list can have gaps)
            ArrayList<Integer> remove = new ArrayList<>();
            Chunk keep = null;
            for (Chunk c : chunks.values()) {
                if (c.version > version) {
                    remove.add(c.id);
                } else if (keep == null || keep.version < c.version) {
                    keep = c;
                }
            }
            if (!remove.isEmpty()) {
                // remove the youngest first, so we don't create gaps
                // (in case we remove many chunks)
                Collections.sort(remove, Collections.reverseOrder());
                for (int id : remove) {
                    Chunk c = chunks.remove(id);
                    if (c != null) {
                        long start = c.block * BLOCK_SIZE;
                        int length = c.len * BLOCK_SIZE;
                        freeFileSpace(start, length);
                        // overwrite the chunk,
                        // so it is not be used later on
                        WriteBuffer buff = getWriteBuffer();
                        buff.limit(length);
                        // buff.clear() does not set the data
                        Arrays.fill(buff.getBuffer().array(), (byte) 0);
                        write(start, buff.getBuffer());
                        releaseWriteBuffer(buff);
                        // only really needed if we remove many chunks, when writes are
                        // re-ordered - but we do it always, because rollback is not
                        // performance critical
                        sync();
                    }
                }
                lastChunk = keep;
                writeStoreHeader();
                readStoreHeader();
            }
            deadChunks.clear();
            removedPages.clear();
            clearCaches();
            currentVersion = version;
            if (lastStoredVersion == INITIAL_VERSION) {
                lastStoredVersion = currentVersion - 1;
            }
            for (MVMap<?, ?> m : new ArrayList<>(maps.values())) {
                int id = m.getId();
                if (m.getCreateVersion() >= version) {
                    m.close();
                    maps.remove(id);
                } else {
                    if (!m.rollbackRoot(version)) {
                        m.setRootPos(getRootPos(meta, id), version);
                    }
                }
            }
        } finally {
            storeLock.unlock();
        }
    }

    private void clearCaches() {
        if (cache != null) {
            cache.clear();
        }
    }

    private static long getRootPos(MVMap<String, String> map, int mapId) {
        String root = map.get(MVMap.getMapRootKey(mapId));
        return root == null ? 0 : DataUtils.parseHexLong(root);
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
        return storeHeader;
    }

    private void checkOpen() {
        if (!isOpenOrStopping()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED,
                    "This store is closed", panicException);
        }
    }

    /**
     * Rename a map.
     *
     * @param map the map
     * @param newName the new name
     */
    public void renameMap(MVMap<?, ?> map, String newName) {
        checkOpen();
        DataUtils.checkArgument(map != meta,
                "Renaming the meta map is not allowed");
        int id = map.getId();
        String oldName = getMapName(id);
        if (oldName != null && !oldName.equals(newName)) {
            String idHexStr = Integer.toHexString(id);
            // at first create a new name as an "alias"
            String existingIdHexStr = meta.putIfAbsent(DataUtils.META_NAME + newName, idHexStr);
            // we need to cope with the case of previously unfinished rename
            DataUtils.checkArgument(
                    existingIdHexStr == null || existingIdHexStr.equals(idHexStr),
                    "A map named {0} already exists", newName);
            // switch roles of a new and old names - old one is an alias now
            meta.put(MVMap.getMapKey(id), map.asString(newName));
            // get rid of the old name completely
            meta.remove(DataUtils.META_NAME + oldName);
            markMetaChanged();
        }
    }

    /**
     * Remove a map from the current version of the store.
     *
     * @param map the map to remove
     */
    public void removeMap(MVMap<?, ?> map) {
        storeLock.lock();
        try {
            checkOpen();
            DataUtils.checkArgument(map != meta,
                    "Removing the meta map is not allowed");
            RootReference rootReference = map.clearIt();
            map.close();

            updateCounter += rootReference.updateCounter;
            updateAttemptCounter += rootReference.updateAttemptCounter;

            int id = map.getId();
            String name = getMapName(id);
            if (meta.remove(MVMap.getMapKey(id)) != null) {
                markMetaChanged();
            }
            if (meta.remove(DataUtils.META_NAME + name) != null) {
                markMetaChanged();
            }
        } finally {
            storeLock.unlock();
        }
    }

    /**
     * Performs final stage of map removal - delete root location info from the meta table.
     * Map is supposedly closed and anonymous and has no outstanding usage by now.
     *
     * @param mapId to deregister
     */
    void deregisterMapRoot(int mapId) {
        if (meta.remove(MVMap.getMapRootKey(mapId)) != null) {
            markMetaChanged();
        }
    }

    /**
     * Remove map by name.
     *
     * @param name the map name
     */
    public void removeMap(String name) {
        int id = getMapId(name);
        if(id > 0) {
            MVMap<?, ?> map = getMap(id);
            if (map == null) {
                map = openMap(name);
            }
            removeMap(map);
        }
    }

    /**
     * Get the name of the given map.
     *
     * @param id the map id
     * @return the name, or null if not found
     */
    public String getMapName(int id) {
        checkOpen();
        String m = meta.get(MVMap.getMapKey(id));
        return m == null ? null : DataUtils.getMapName(m);
    }

    private int getMapId(String name) {
        String m = meta.get(DataUtils.META_NAME + name);
        return m == null ? -1 : DataUtils.parseHexInt(m);
    }

    /**
     * Commit and save all changes, if there are any, and compact the store if
     * needed.
     */
    void writeInBackground() {
        try {
            if (!isOpenOrStopping() || isReadOnly()) {
                return;
            }

            // could also commit when there are many unsaved pages,
            // but according to a test it doesn't really help

            long time = getTimeSinceCreation();
            if (time > lastCommitTime + autoCommitDelay) {
                tryCommit();
                if (autoCompactFillRate < 0) {
                    compact(-getTargetFillRate(), autoCommitMemory);
                }
            }
            int targetFillRate;
            int projectedFillRate;
            if (isIdle()) {
                doMaintenance(autoCompactFillRate);
            } else if (fileStore.isFragmented()) {
                if (storeLock.tryLock(10, TimeUnit.MILLISECONDS)) {
                    try {
                        compactMoveChunks(autoCommitMemory * 4);
                    } finally {
                        storeLock.unlock();
                    }
                }
            } else if (lastChunk != null && getFillRate() > (targetFillRate = getTargetFillRate())
                    && (projectedFillRate = getProjectedFillRate()) < targetFillRate) {
                if (storeLock.tryLock(10, TimeUnit.MILLISECONDS)) {
                    try {
                        int writeLimit = autoCommitMemory * targetFillRate / Math.max(projectedFillRate, 1);
                        if (rewriteChunks(writeLimit)) {
                            dropUnusedChunks();
                        }
                    } finally {
                        storeLock.unlock();
                    }
                }
            }
            autoCompactLastFileOpCount = fileStore.getWriteCount() + fileStore.getReadCount();
        } catch (InterruptedException ignore) {
        } catch (Throwable e) {
            handleException(e);
            if (backgroundExceptionHandler == null) {
                throw e;
            }
        }
    }

    private void doMaintenance(int targetFillRate) {
        if (autoCompactFillRate > 0 && lastChunk != null && reuseSpace) {
            try {
                int lastProjectedFillRate = -1;
                for (int cnt = 0; cnt < 5; cnt++) {
                    int fillRate = getFillRate();
                    int projectedFillRate = fillRate;
                    if (fillRate > targetFillRate) {
                        projectedFillRate = getProjectedFillRate();
                        if (projectedFillRate > targetFillRate || projectedFillRate <= lastProjectedFillRate) {
                            break;
                        }
                    }
                    lastProjectedFillRate = projectedFillRate;
                    // We can't wait forever for the lock here,
                    // because if called from the background thread,
                    // it might go into deadlock with concurrent database closure
                    // and attempt to stop this thread.
                    if (!storeLock.tryLock(10, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                    try {
                        int writeLimit = autoCommitMemory * targetFillRate / Math.max(projectedFillRate, 1);
                        if (projectedFillRate < fillRate) {
                            if ((!rewriteChunks(writeLimit) || dropUnusedChunks() == 0) && cnt > 0) {
                                break;
                            }
                        }
                        if (!compactMoveChunks(writeLimit)) {
                            break;
                        }
                    } finally {
                        storeLock.unlock();
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private int getTargetFillRate() {
        int targetRate = autoCompactFillRate;
        // use a lower fill rate if there were any file operations since the last time
        if (!isIdle()) {
            targetRate /= 3;
        }
        return targetRate;
    }

    private boolean isIdle() {
        return autoCompactLastFileOpCount == fileStore.getWriteCount() + fileStore.getReadCount();
    }

    private void handleException(Throwable ex) {
        if (backgroundExceptionHandler != null) {
            try {
                backgroundExceptionHandler.uncaughtException(Thread.currentThread(), ex);
            } catch(Throwable ignore) {
                if (ex != ignore) { // OOME may be the same
                    ex.addSuppressed(ignore);
                }
            }
        }
    }

    /**
     * Set the read cache size in MB.
     *
     * @param mb the cache size in MB.
     */
    public void setCacheSize(int mb) {
        final long bytes = (long) mb * 1024 * 1024;
        if (cache != null) {
            cache.setMaxMemory(bytes);
            cache.clear();
        }
    }

    private boolean isOpen() {
        return state == STATE_OPEN;
    }

    /**
     * Determine that store is open, or wait for it to be closed (by other thread)
     * @return true if store is open, false otherwise
     */
    public boolean isClosed() {
        if (isOpen()) {
            return false;
        }
        storeLock.lock();
        try {
            assert state == STATE_CLOSED;
            return true;
        } finally {
            storeLock.unlock();
        }
    }

    private boolean isOpenOrStopping() {
        return state <= STATE_STOPPING;
    }

    private void stopBackgroundThread(boolean waitForIt) {
        // Loop here is not strictly necessary, except for case of a spurious failure,
        // which should not happen with non-weak flavour of CAS operation,
        // but I've seen it, so just to be safe...
        BackgroundWriterThread t;
        while ((t = backgroundWriterThread.get()) != null) {
            if (backgroundWriterThread.compareAndSet(t, null)) {
                // if called from within the thread itself - can not join
                if (t != Thread.currentThread()) {
                    synchronized (t.sync) {
                        t.sync.notifyAll();
                    }

                    if (waitForIt) {
                        try {
                            t.join();
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                }
                break;
            }
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
        if (fileStore == null || fileStore.isReadOnly()) {
            return;
        }
        stopBackgroundThread(true);
        // start the background thread if needed
        if (millis > 0 && isOpen()) {
            int sleep = Math.max(1, millis / 10);
            BackgroundWriterThread t =
                    new BackgroundWriterThread(this, sleep,
                            fileStore.toString());
            if (backgroundWriterThread.compareAndSet(null, t)) {
                t.start();
            }
        }
    }

    public boolean isBackgroundThread() {
        return Thread.currentThread() == backgroundWriterThread.get();
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
     * Get the maximum memory (in bytes) used for unsaved pages. If this number
     * is exceeded, unsaved changes are stored to disk.
     *
     * @return the memory in bytes
     */
    public int getAutoCommitMemory() {
        return autoCommitMemory;
    }

    /**
     * Get the estimated memory (in bytes) of unsaved data. If the value exceeds
     * the auto-commit memory, the changes are committed.
     * <p>
     * The returned value is an estimation only.
     *
     * @return the memory in bytes
     */
    public int getUnsavedMemory() {
        return unsavedMemory;
    }

    /**
     * Put the page in the cache.
     * @param page the page
     */
    void cachePage(Page page) {
        if (cache != null) {
            cache.put(page.getPos(), page, page.getMemory());
        }
    }

    /**
     * Get the amount of memory used for caching, in MB.
     * Note that this does not include the page chunk references cache, which is
     * 25% of the size of the page cache.
     *
     * @return the amount of memory used for caching
     */
    public int getCacheSizeUsed() {
        if (cache == null) {
            return 0;
        }
        return (int) (cache.getUsedMemory() >> 20);
    }

    /**
     * Get the maximum cache size, in MB.
     * Note that this does not include the page chunk references cache, which is
     * 25% of the size of the page cache.
     *
     * @return the cache size
     */
    public int getCacheSize() {
        if (cache == null) {
            return 0;
        }
        return (int) (cache.getMaxMemory() >> 20);
    }

    /**
     * Get the cache.
     *
     * @return the cache
     */
    public CacheLongKeyLIRS<Page> getCache() {
        return cache;
    }

    /**
     * Whether the store is read-only.
     *
     * @return true if it is
     */
    public boolean isReadOnly() {
        return fileStore != null && fileStore.isReadOnly();
    }

    public int getCacheHitRatio() {
        if (cache == null) {
            return 0;
        }
        long hits = cache.getHits();
        return (int) (100 * hits / (hits + cache.getMisses() + 1));
    }

    public double getUpdateFailureRatio() {
        long updateCounter = this.updateCounter;
        long updateAttemptCounter = this.updateAttemptCounter;
        RootReference rootReference = meta.getRoot();
        updateCounter += rootReference.updateCounter;
        updateAttemptCounter += rootReference.updateAttemptCounter;
        for (MVMap<?, ?> map : maps.values()) {
            RootReference root = map.getRoot();
            updateCounter += root.updateCounter;
            updateAttemptCounter += root.updateAttemptCounter;
        }
        return updateAttemptCounter == 0 ? 0 : 1 - ((double)updateCounter / updateAttemptCounter);
    }

    /**
     * Register opened operation (transaction).
     * This would increment usage counter for the current version.
     * This version (and all after it) should not be dropped until all
     * transactions involved are closed and usage counter goes to zero.
     * @return TxCounter to be decremented when operation finishes (transaction closed).
     */
    public TxCounter registerVersionUsage() {
        TxCounter txCounter;
        while(true) {
            txCounter = currentTxCounter;
            if(txCounter.incrementAndGet() > 0) {
                return txCounter;
            }
            // The only way for counter to be negative
            // if it was retrieved right before onVersionChange()
            // and now onVersionChange() is done.
            // This version is eligible for reclamation now
            // and should not be used here, so restore count
            // not to upset accounting and try again with a new
            // version (currentTxCounter should have changed).
            assert txCounter != currentTxCounter : txCounter;
            txCounter.decrementAndGet();
        }
    }

    /**
     * De-register (close) completed operation (transaction).
     * This will decrement usage counter for the corresponding version.
     * If counter reaches zero, that version (and all unused after it)
     * can be dropped immediately.
     *
     * @param txCounter to be decremented, obtained from registerVersionUsage()
     */
    public void deregisterVersionUsage(TxCounter txCounter) {
        if(txCounter != null) {
            if(txCounter.decrementAndGet() <= 0) {
                if (storeLock.isHeldByCurrentThread()) {
                    dropUnusedVersions();
                } else if (storeLock.tryLock()) {
                    try {
                        dropUnusedVersions();
                    } finally {
                        storeLock.unlock();
                    }
                }
            }
        }
    }

    private void onVersionChange(long version) {
        TxCounter txCounter = currentTxCounter;
        assert txCounter.get() >= 0;
        versions.add(txCounter);
        currentTxCounter = new TxCounter(version);
        txCounter.decrementAndGet();
        dropUnusedVersions();
    }

    private void dropUnusedVersions() {
        assert storeLock.isHeldByCurrentThread();
        TxCounter txCounter;
        while ((txCounter = versions.peek()) != null
                && txCounter.get() < 0) {
            versions.poll();
        }
        setOldestVersionToKeep((txCounter != null ? txCounter : currentTxCounter).version);
    }

    private int dropUnusedChunks() {
        assert storeLock.isHeldByCurrentThread();
        int count = 0;
        if (!deadChunks.isEmpty()) {
            long oldestVersionToKeep = getOldestVersionToKeep();
            long time = getTimeSinceCreation();
            Chunk chunk;
            while ((chunk = deadChunks.poll()) != null &&
                    (isSeasonedChunk(chunk, time) && canOverwriteChunk(chunk, oldestVersionToKeep) ||
                            // if chunk is not ready yet, put it back and exit
                            // since this deque is inbounded, offerFirst() always return true
                            !deadChunks.offerFirst(chunk))) {

                if (chunks.remove(chunk.id) != null) {
                    if (meta.remove(Chunk.getMetaKey(chunk.id)) != null) {
                        markMetaChanged();
                    }
                    if (chunk.isSaved()) {
                        freeChunkSpace(chunk);
                    }
                    ++count;
                }
            }
        }
        return count;
    }

    private void freeChunkSpace(Chunk chunk) {
        long start = chunk.block * BLOCK_SIZE;
        int length = chunk.len * BLOCK_SIZE;
        freeFileSpace(start, length);
    }

    private void freeFileSpace(long start, int length) {
        fileStore.free(start, length);
        assert validateFileLength(start + ":" + length);
    }

    private boolean validateFileLength(String msg) {
        assert fileStore.getFileLengthInUse() == measureFileLengthInUse() :
                fileStore.getFileLengthInUse() + " != " + measureFileLengthInUse() + " " + msg;
        return true;
    }

    /**
     * Class TxCounter is a simple data structure to hold version of the store
     * along with the counter of open transactions,
     * which are still operating on this version.
     */
    public static final class TxCounter {

        /**
         * Version of a store, this TxCounter is related to
         */
        public final long version;

        /**
         * Counter of outstanding operation on this version of a store
         */
        private volatile int counter;

        private static final AtomicIntegerFieldUpdater<TxCounter> counterUpdater =
                                        AtomicIntegerFieldUpdater.newUpdater(TxCounter.class, "counter");


        TxCounter(long version) {
            this.version = version;
        }

        int get() {
            return counter;
        }

        /**
         * Increment and get the counter value.
         *
         * @return the new value
         */
        int incrementAndGet() {
            return counterUpdater.incrementAndGet(this);
        }

        /**
         * Decrement and get the counter values.
         *
         * @return the new value
         */
        int decrementAndGet() {
            return counterUpdater.decrementAndGet(this);
        }

        @Override
        public String toString() {
            return "v=" + version + " / cnt=" + counter;
        }
    }

    /**
     * A background writer thread to automatically store changes from time to
     * time.
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
            while (store.isBackgroundThread()) {
                synchronized (sync) {
                    try {
                        sync.wait(sleep);
                    } catch (InterruptedException ignore) {
                    }
                }
                if (!store.isBackgroundThread()) {
                    break;
                }
                store.writeInBackground();
            }
        }
    }

    private static class RemovedPageInfo implements Comparable<RemovedPageInfo>
    {
        final long version;
        final int removedPageInfo;

        RemovedPageInfo(long pagePos, boolean pinned, long version) {
            this.removedPageInfo = createRemovedPageInfo(pagePos, pinned);
            this.version = version;
        }

        @Override
        public int compareTo(RemovedPageInfo other) {
            return Long.compare(version, other.version);
        }

        int getPageChunkId() {
            return removedPageInfo >>> 6;
        }

        int getPageLength() {
            return DataUtils.decodePageLength((removedPageInfo >> 1) & 0x1F);
        }

        /**
         * Find out if removed page was pinned (can not be evacuated to a new chunk).
         * @return true if page has been pinned
         */
        boolean isPinned() {
            return (removedPageInfo & 1) == 1;
        }

        /**
         * Transforms saved page position into removed page info, by eliminating page offset
         * and replacing "page type" bit with "pinned page" flag.
         * 0    "pinned" flag
         * 1-5  encoded page length
         * 6-31 chunk id
         * @param pagePos of the saved page
         * @param isPinned whether page belong to a "single writer" map
         * @return removed page info that contains chunk id, page length and pinned flag
         */
        private static int createRemovedPageInfo(long pagePos, boolean isPinned) {
            int result = ((int) (pagePos >>> 32)) & ~0x3F | ((int) pagePos) & 0x3E;
            if (isPinned) {
                result |= 1;
            }
            return result;
        }

        @Override
        public String toString() {
            return "RemovedPageInfo{" +
                    "version=" + version +
                    ", chunk=" + getPageChunkId() +
                    ", len=" + getPageLength() +
                    (isPinned() ? ", pinned" : "") +
                    '}';
        }
    }

    /**
     * A builder for an MVStore.
     */
    public static final class Builder {

        private final HashMap<String, Object> config;

        private Builder(HashMap<String, Object> config) {
            this.config = config;
        }

        /**
         * Creates new instance of MVStore.Builder.
         */
        public Builder() {
            config = new HashMap<>();
        }

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
            //set("autoCommitBufferSize", 0);
            return set("autoCommitDelay", 0);
        }

        /**
         * Set the size of the write buffer, in KB disk space (for file-based
         * stores). Unless auto-commit is disabled, changes are automatically
         * saved if there are more than this amount of changes.
         * <p>
         * The default is 1024 KB.
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
         * Set the auto-compact target fill rate. If the average fill rate (the
         * percentage of the storage space that contains active data) of the
         * chunks is lower, then the chunks with a low fill rate are re-written.
         * Also, if the percentage of empty space between chunks is higher than
         * this value, then chunks at the end of the file are moved. Compaction
         * stops if the target fill rate is reached.
         * <p>
         * The default value is 40 (40%). The value 0 disables auto-compacting.
         * <p>
         *
         * @param percent the target fill rate
         * @return this
         */
        public Builder autoCompactFillRate(int percent) {
            return set("autoCompactFillRate", percent);
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
         * Open the file in recovery mode, where some errors may be ignored.
         *
         * @return this
         */
        public Builder recoveryMode() {
            return set("recoveryMode", 1);
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
         * Set the read cache concurrency. The default is 16, meaning 16
         * segments are used.
         *
         * @param concurrency the cache concurrency
         * @return this
         */
        public Builder cacheConcurrency(int concurrency) {
            return set("cacheConcurrency", concurrency);
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
        public Builder compress() {
            return set("compress", 1);
        }

        /**
         * Compress data before writing using the Deflate algorithm. This will
         * save more disk space, but will slow down read and write operations
         * quite a bit.
         * <p>
         * This setting only affects writes; it is not necessary to enable
         * compression when reading, even if compression was enabled when
         * writing.
         *
         * @return this
         */
        public Builder compressHigh() {
            return set("compress", 2);
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
         * <p>
         * File stores passed in this way need to be open. They are not closed
         * when closing the store.
         * <p>
         * Please note that any kind of store (including an off-heap store) is
         * considered a "persistence", while an "in-memory store" means objects
         * are not persisted and fully kept in the JVM heap.
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
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public static Builder fromString(String s) {
            // Cast from HashMap<String, String> to HashMap<String, Object> is safe
            return new Builder((HashMap) DataUtils.parseMap(s));
        }
    }
}
