/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import org.h2.compress.CompressDeflate;
import org.h2.compress.CompressLZF;
import org.h2.compress.Compressor;
import org.h2.mvstore.type.StringDataType;
import org.h2.store.fs.FileUtils;
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
public final class MVStore implements AutoCloseable {

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
     * Store is closed.
     */
    private static final int STATE_CLOSED = 2;

    /**
     * This designates the "last stored" version for a store which was
     * just open for the first time.
     */
    static final long INITIAL_VERSION = -1;


    /**
     * Lock which governs access to major store operations: store(), close(), ...
     * It serves as a replacement for synchronized(this), except it allows for
     * non-blocking lock attempts.
     */
    private final ReentrantLock storeLock = new ReentrantLock(true);

    /**
     * Flag to refine the state under storeLock.
     * It indicates that store() operation is running, and we have to prevent possible re-entrance.
     */
    private final AtomicBoolean storeOperationInProgress = new AtomicBoolean();

    private volatile int state;

    private final FileStore<?> fileStore;

    private final boolean fileStoreShallBeClosed;

    private final int keysPerPage;

    private long updateCounter = 0;
    private long updateAttemptCounter = 0;

    /**
     * The metadata map. Holds name -> id and id -> name and id -> metadata
     * mapping for all maps. This is relatively slow changing part of metadata
     */
    private final MVMap<String, String> meta;

    private final ConcurrentHashMap<Integer, MVMap<?, ?>> maps = new ConcurrentHashMap<>();

    private final AtomicInteger lastMapId = new AtomicInteger();

    private int versionsToKeep = 5;

    /**
     * The compression level for new pages (0 for disabled, 1 for fast, 2 for
     * high). Even if disabled, the store may contain (old) compressed pages.
     */
    private final int compressionLevel;

    private Compressor compressorFast;

    private Compressor compressorHigh;

    public final UncaughtExceptionHandler backgroundExceptionHandler;

    private volatile long currentVersion;

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

    private volatile boolean metaChanged;


    private volatile MVStoreException panicException;

    private long lastTimeAbsolute;

    private long leafCount;
    private long nonLeafCount;

    /**
     * Callback for maintenance after some unused store versions were dropped
     */
    private volatile LongConsumer oldestVersionTracker;


    /**
     * Create and open the store.
     *
     * @param config the configuration to use
     * @throws MVStoreException if the file is corrupt, or an exception
     *             occurred while opening
     * @throws IllegalArgumentException if the directory does not exist
     */
    MVStore(Map<String, Object> config) {
        compressionLevel = DataUtils.getConfigParam(config, "compress", 0);
        String fileName = (String) config.get("fileName");
        FileStore<?> fileStore = (FileStore<?>) config.get("fileStore");
        boolean fileStoreShallBeOpen = false;
        if (fileStore == null) {
            if (fileName != null) {
                fileStore = new SingleFileStore(config);
                fileStoreShallBeOpen = true;
            }
            fileStoreShallBeClosed = true;
        } else {
            if (fileName != null) {
                throw new IllegalArgumentException("fileName && fileStore");
            }
            Boolean fileStoreIsAdopted = (Boolean) config.get("fileStoreIsAdopted");
            fileStoreShallBeClosed = fileStoreIsAdopted != null && fileStoreIsAdopted;
        }
        this.fileStore = fileStore;
        keysPerPage = DataUtils.getConfigParam(config, "keysPerPage", 48);
        backgroundExceptionHandler =
                (UncaughtExceptionHandler)config.get("backgroundExceptionHandler");
        if (fileStore != null) {
            // 19 KB memory is about 1 KB storage
            int kb = Math.max(1, Math.min(19, Utils.scaleForAvailableMemory(64))) * 1024;
            kb = DataUtils.getConfigParam(config, "autoCommitBufferSize", kb);
            autoCommitMemory = kb * 1024;
            char[] encryptionKey = (char[]) config.remove("encryptionKey");
            MVMap<String, String> metaMap = null;
            // there is no need to lock store here, since it is not opened (or even created) yet,
            // just to make some assertions happy, when they ensure single-threaded access
            storeLock.lock();
            try {
                if (fileStoreShallBeOpen) {
                    boolean readOnly = config.containsKey("readOnly");
                    fileStore.open(fileName, readOnly, encryptionKey);
                }
                fileStore.bind(this);
                metaMap = fileStore.start();
            } catch (MVStoreException e) {
                panic(e);
            } finally {
                if (encryptionKey != null) {
                    Arrays.fill(encryptionKey, (char) 0);
                }
                unlockAndCheckPanicCondition();
            }

            meta = metaMap;
            scrubMetaMap();

            // setAutoCommitDelay starts the thread, but only if
            // the parameter is different from the old value
            int delay = DataUtils.getConfigParam(config, "autoCommitDelay", 1000);
            setAutoCommitDelay(delay);
        } else {
            autoCommitMemory = 0;
            meta = openMetaMap();
        }
        onVersionChange(currentVersion);
    }

    public MVMap<String,String> openMetaMap() {
        int metaId = fileStore != null ? fileStore.getMetaMapId(this::getNextMapId) : 1;
        MVMap<String,String> map = new MVMap<>(this, metaId, StringDataType.INSTANCE, StringDataType.INSTANCE);
        map.setRootPos(getRootPos(map.getId()), currentVersion);
        return map;
    }

    private void scrubMetaMap() {
        Set<String> keysToRemove = new HashSet<>();

        // ensure that there is only one name mapped to each id
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
            adjustLastMapId(DataUtils.parseHexInt(mapIdStr));
            // each map should have a proper name
            if(!mapIdStr.equals(meta.get(DataUtils.META_NAME + mapName))) {
                meta.put(DataUtils.META_NAME + mapName, mapIdStr);
                markMetaChanged();
            }
        }
    }

    private void unlockAndCheckPanicCondition() {
        storeLock.unlock();
        MVStoreException exception = getPanicException();
        if (exception != null) {
            closeImmediately();
            throw exception;
        }
    }

    public void panic(MVStoreException e) {
        if (isOpen()) {
            handleException(e);
            panicException = e;
        }
        throw e;
    }

    public MVStoreException getPanicException() {
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
        return openMap(name, new MVMap.Builder<>());
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
        if (id >= 0) {
            @SuppressWarnings("unchecked")
            M map = (M) getMap(id);
            if(map == null) {
                map = openMap(id, builder);
            }
            assert builder.getKeyType() == null || map.getKeyType().getClass().equals(builder.getKeyType().getClass());
            assert builder.getValueType() == null
                    || map.getValueType().getClass().equals(builder.getValueType().getClass());
            return map;
        } else {
            HashMap<String, Object> c = new HashMap<>();
            id = getNextMapId();
            assert getMap(id) == null;
            c.put("id", id);
            long curVersion = currentVersion;
            c.put("createVersion", curVersion);
            M map = builder.create(this, c);
            String x = Integer.toHexString(id);
            meta.put(MVMap.getMapKey(id), map.asString(name));
            String existing = meta.putIfAbsent(DataUtils.META_NAME + name, x);
            if (existing != null) {
                // looks like map was created concurrently, cleanup and re-start
                meta.remove(MVMap.getMapKey(id));
                return openMap(name, builder);
            }
            map.setRootPos(0, curVersion);
            markMetaChanged();
            @SuppressWarnings("unchecked")
            M existingMap = (M) maps.putIfAbsent(id, map);
            if (existingMap != null) {
                map = existingMap;
            }
            return map;
        }
    }

    /**
     * Open an existing map with the given builder.
     *
     * @param <M> the map type
     * @param <K> the key type
     * @param <V> the value type
     * @param id the map id
     * @param builder the map builder
     * @return the map
     */
    @SuppressWarnings("unchecked")
    public <M extends MVMap<K, V>, K, V> M openMap(int id, MVMap.MapBuilder<M, K, V> builder) {
        M map;
        while ((map = (M)getMap(id)) == null) {
            String configAsString = meta.get(MVMap.getMapKey(id));
            DataUtils.checkArgument(configAsString != null, "Missing map with id {0}", id);
            HashMap<String, Object> config = new HashMap<>(DataUtils.parseMap(configAsString));
            config.put("id", id);
            map = builder.create(this, config);
            long root = getRootPos(id);
            map.setRootPos(root, currentVersion);
            if (maps.putIfAbsent(id, map) == null) {
                break;
            }
            // looks like map has been concurrently created already, re-start
        }
        return map;
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
        checkNotClosed();
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
        checkNotClosed();
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
     * Get this store's layout map. This data is for informational purposes only. The
     * data is subject to change in future versions.
     * <p>
     * The data in this map should not be modified (changing system data may corrupt the store).
     * <p>
     * The layout map contains the following entries:
     * <pre>
     * chunk.{chunkId} = {chunk metadata}
     * root.{mapId} = {root position}
     * </pre>
     *
     * @return the metadata map
     */
    public Map<String, String> getLayoutMap() {
        return fileStore == null ? null : fileStore.getLayoutMap();
    }

    @SuppressWarnings("ReferenceEquality")
    private boolean isRegularMap(MVMap<?,?> map) {
        return map != meta && (fileStore == null || fileStore.isRegularMap(map));
    }

    /**
     * Get the metadata map. This data is for informational purposes only. The
     * data is subject to change in future versions.
     * <p>
     * The data in this map should not be modified (changing system data may corrupt the store).
     * <p>
     * The metadata map contains the following entries:
     * <pre>
     * name.{name} = {mapId}
     * map.{mapId} = {map metadata}
     * setting.storeVersion = {version}
     * </pre>
     *
     * @return the metadata map
     */
    public MVMap<String, String> getMetaMap() {
        checkNotClosed();
        return meta;
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
        return hasMap(name) && getRootPos(getMapId(name)) != 0;
    }

    void markMetaChanged() {
        // changes in the metadata alone are usually not detected, as the meta
        // map is changed after storing
        metaChanged = true;
    }

    int getLastMapId() {
        return lastMapId.get();
    }

    private int getNextMapId() {
        return lastMapId.incrementAndGet();
    }

    void adjustLastMapId(int mapId) {
        if (mapId > lastMapId.get()) {
            lastMapId.set(mapId);
        }
    }

    void resetLastMapId(int mapId) {
        lastMapId.set(mapId);
    }

    /**
     * Close the file and the store. Unsaved changes are written to disk first.
     */
    @Override
    public void close() {
        closeStore(true, 0);
    }


    /**
     * Close the store. Pending changes are persisted.
     * If time is allocated for housekeeping, chunks with a low
     * fill rate are compacted, and some chunks are put next to each other.
     * If time is unlimited then full compaction is performed, which uses
     * different algorithm - opens alternative temp store and writes all live
     * data there, then replaces this store with a new one.
     *
     * @param allowedCompactionTime time (in milliseconds) allotted for file
     *                              compaction activity, 0 means no compaction,
     *                              -1 means unlimited time (full compaction)
     */
    public void close(int allowedCompactionTime) {
        if (!isClosed()) {
            if (fileStore != null) {
                boolean compactFully = allowedCompactionTime == -1;
                if (fileStore.isReadOnly()) {
                    compactFully = false;
                } else {
                    commit();
                }
                if (compactFully) {
                    allowedCompactionTime = 0;
                }

                closeStore(true, allowedCompactionTime);

                String fileName = fileStore.getFileName();
                if (compactFully && FileUtils.exists(fileName)) {
                    // the file could have been deleted concurrently,
                    // so only compact if the file still exists
                    MVStoreTool.compact(fileName, true);
                }
            } else {
                close();
            }
        }
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

    private void closeStore(boolean normalShutdown, int allowedCompactionTime) {
        // If any other thead have already initiated closure procedure,
        // isClosed() would wait until closure is done and then  we jump out of the loop.
        // This is a subtle difference between !isClosed() and isOpen().
        while (!isClosed()) {
            setAutoCommitDelay(-1);
            setOldestVersionTracker(null);
            storeLock.lock();
            try {
                if (state == STATE_OPEN) {
                    state = STATE_STOPPING;
                    try {
                        try {
                            if (normalShutdown && fileStore != null && !fileStore.isReadOnly()) {
                                for (MVMap<?, ?> map : maps.values()) {
                                    if (map.isClosed()) {
                                        fileStore.deregisterMapRoot(map.getId());
                                    }
                                }
                                setRetentionTime(0);
                                commit();
                                assert oldestVersionToKeep.get() == currentVersion : oldestVersionToKeep.get() + " != "
                                        + currentVersion;
                                fileStore.stop(allowedCompactionTime);
                            }

                            if (meta != null) {
                                meta.close();
                            }
                            for (MVMap<?, ?> m : new ArrayList<>(maps.values())) {
                                m.close();
                            }
                            maps.clear();
                        } finally {
                            if (fileStore != null && fileStoreShallBeClosed) {
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
     * Indicates whether this MVStore is backed by FileStore,
     * and therefore it's data will survive this store closure
     * (but not necessary process termination in case of in-memory store).
     * @return true if persistent
     */
    public boolean isPersistent() {
        return fileStore != null;
    }

    /**
     * Unlike regular commit this method returns immediately if there is commit
     * in progress on another thread, otherwise it acts as regular commit.
     *
     * This method may return BEFORE this thread changes are actually persisted!
     *
     * @return the new version (incremented if there were changes) or -1 if there were no commit
     */
    public long tryCommit() {
        return tryCommit(x -> true);
    }

    private long tryCommit(Predicate<MVStore> check) {
        if (canStartStoreOperation() && storeLock.tryLock()) {
            try {
                if (check.test(this)) {
                    return store(false);
                }
            } finally {
                unlockAndCheckPanicCondition();
            }
        }
        return INITIAL_VERSION;
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
     * @return the new version (incremented if there were changes) or -1 if there were no commit
     */
    public long commit() {
        return commit(x -> true);
    }

    private long commit(Predicate<MVStore> check) {
        if(canStartStoreOperation()) {
            storeLock.lock();
            try {
                if (check.test(this)) {
                    return store(true);
                }
            } finally {
                unlockAndCheckPanicCondition();
            }
        }
        return INITIAL_VERSION;
    }

    private boolean canStartStoreOperation() {
        // we need to prevent re-entrance, which may be possible,
        // because meta map is modified within storeNow() and that
        // causes beforeWrite() call with possibility of going back here
        return !storeLock.isHeldByCurrentThread() || !storeOperationInProgress.get();
    }

    private long store(boolean syncWrite) {
        assert storeLock.isHeldByCurrentThread();
        if (isOpenOrStopping() && hasUnsavedChanges() && storeOperationInProgress.compareAndSet(false, true)) {
            try {
                @SuppressWarnings({"NonAtomicVolatileUpdate", "NonAtomicOperationOnVolatileField"})
                long result = ++currentVersion;
                if (fileStore == null) {
                    setWriteVersion(currentVersion);
                } else {
                    if (fileStore.isReadOnly()) {
                        throw DataUtils.newMVStoreException(
                                DataUtils.ERROR_WRITING_FAILED, "This store is read-only");
                    }
                    fileStore.dropUnusedChunks();
                    storeNow(syncWrite);
                }
                return result;
            } finally {
                storeOperationInProgress.set(false);
            }
        }
        return INITIAL_VERSION;
    }

    private void setWriteVersion(long version) {
        for (Iterator<MVMap<?, ?>> iter = maps.values().iterator(); iter.hasNext(); ) {
            MVMap<?, ?> map = iter.next();
            assert isRegularMap(map);
            if (map.setWriteVersion(version) == null) {
                iter.remove();
            }
        }
        meta.setWriteVersion(version);
        onVersionChange(version);
    }

    @SuppressWarnings({"NonAtomicVolatileUpdate", "NonAtomicOperationOnVolatileField"})
    void storeNow() {
        // it is ok, since that path suppose to be single-threaded under storeLock
        ++currentVersion;
        storeNow(true);
    }

    private void storeNow(boolean syncWrite) {
        try {
            int currentUnsavedMemory = unsavedMemory;
            long version = currentVersion;

            assert storeLock.isHeldByCurrentThread();
            fileStore.storeIt(collectChangedMapRoots(version), version, syncWrite);

            // some pages might have been changed in the meantime (in the newest
            // version)
            saveNeeded = false;
            unsavedMemory = Math.max(0, unsavedMemory - currentUnsavedMemory);
        } catch (MVStoreException e) {
            panic(e);
        } catch (Throwable e) {
            panic(DataUtils.newMVStoreException(DataUtils.ERROR_INTERNAL, "{0}", e.toString(),
                    e));
        }
    }

    private ArrayList<Page<?,?>> collectChangedMapRoots(long version) {
        long lastStoredVersion = version - 2;
        ArrayList<Page<?,?>> changed = new ArrayList<>();
        for (Iterator<MVMap<?, ?>> iter = maps.values().iterator(); iter.hasNext(); ) {
            MVMap<?, ?> map = iter.next();
            RootReference<?,?> rootReference = map.setWriteVersion(version);
            if (rootReference == null) {
                iter.remove();
            } else if (map.getCreateVersion() < version && // if map was created after storing started, skip it
                    !map.isVolatile() &&
                    map.hasChangesSince(lastStoredVersion)) {
                assert rootReference.version <= version : rootReference.version + " > " + version;
                // simply checking rootPage.isSaved() won't work here because
                // after deletion previously saved page
                // may pop up as a root, but we still need
                // to save new root pos in meta
                changed.add(rootReference.root);
            }
        }
        RootReference<?,?> rootReference = meta.setWriteVersion(version);
        if (meta.hasChangesSince(lastStoredVersion) || metaChanged) {
            assert rootReference != null && rootReference.version <= version
                    : rootReference == null ? "null" : rootReference.version + " > " + version;
            changed.add(rootReference.root);
        }
        return changed;
    }

    public long getTimeAbsolute() {
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
     * Check whether there are any unsaved changes.
     *
     * @return if there are any changes
     */
    public boolean hasUnsavedChanges() {
        if (metaChanged) {
            return true;
        }
        long lastStoredVersion = currentVersion - 1;
        for (MVMap<?, ?> m : maps.values()) {
            if (!m.isClosed()) {
                if(m.hasChangesSince(lastStoredVersion)) {
                    return true;
                }
            }
        }
        return fileStore != null && fileStore.hasChangesSince(lastStoredVersion);
    }

    public void executeFilestoreOperation(Runnable operation) {
        storeLock.lock();
        try {
            checkNotClosed();
            fileStore.executeFileStoreOperation(operation);
        } catch (MVStoreException e) {
            panic(e);
        } catch (Throwable e) {
            panic(DataUtils.newMVStoreException(
                    DataUtils.ERROR_INTERNAL, "{0}", e.toString(), e));
        } finally {
            unlockAndCheckPanicCondition();
        }
    }

    <R> R tryExecuteUnderStoreLock(Callable<R> operation) throws InterruptedException {
        R result = null;
        if (storeLock.tryLock(10, TimeUnit.MILLISECONDS)) {
            try {
                result = operation.call();
            } catch (MVStoreException e) {
                panic(e);
            } catch (Throwable e) {
                panic(DataUtils.newMVStoreException(
                        DataUtils.ERROR_INTERNAL, "{0}", e.toString(), e));
            } finally {
                unlockAndCheckPanicCondition();
            }
        }
        return result;
    }

    /**
     * Force all stored changes to be written to the storage. The default
     * implementation calls FileChannel.force(true).
     */
    public void sync() {
        checkOpen();
        FileStore<?> f = fileStore;
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
    public void compactFile(int maxCompactTime) {
        if (fileStore != null) {
            setRetentionTime(0);
            storeLock.lock();
            try {
                fileStore.compactStore(maxCompactTime);
            } finally {
                unlockAndCheckPanicCondition();
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
     * @return if any chunk was re-written
     */
    public boolean compact(int targetFillRate, int write) {
        checkOpen();
        return fileStore != null && fileStore.compact(targetFillRate, write);
    }

    public int getFillRate() {
        return fileStore.getFillRate();
    }

    /**
     * Read a page.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param map the map
     * @param pos the page position
     * @return the page
     */
    <K,V> Page<K,V> readPage(MVMap<K,V> map, long pos) {
        checkNotClosed();
        return fileStore.readPage(map, pos);
    }

    /**
     * Remove a page.
     *  @param pos the position of the page
     * @param version at which page was removed
     * @param pinned whether page is considered pinned
     * @param pageNo sequential page number within chunk
     */
    void accountForRemovedPage(long pos, long version, boolean pinned, int pageNo) {
        fileStore.accountForRemovedPage(pos, version, pinned, pageNo);
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

    public int getKeysPerPage() {
        return keysPerPage;
    }

    public long getMaxPageSize() {
        return fileStore == null ? Long.MAX_VALUE : fileStore.getMaxPageSize();
    }

    /**
     * Get the maximum cache size, in MB.
     * Note that this does not include the page chunk references cache, which is
     * 25% of the size of the page cache.
     *
     * @return the cache size
     */
    public int getCacheSize() {
        return fileStore == null ? 0 : fileStore.getCacheSize();
    }

    /**
     * Get the amount of memory used for caching, in MB.
     * Note that this does not include the page chunk references cache, which is
     * 25% of the size of the page cache.
     *
     * @return the amount of memory used for caching
     */
    public int getCacheSizeUsed() {
        return fileStore == null ? 0 : fileStore.getCacheSizeUsed();
    }

    /**
     * Set the maximum memory to be used by the cache.
     *
     * @param kb the maximum size in KB
     */
    public void setCacheSize(int kb) {
        if (fileStore != null) {
            fileStore.setCacheSize(Math.max(1, kb / 1024));
        }
    }

    public boolean isSpaceReused() {
        return fileStore.isSpaceReused();
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
        fileStore.setReuseSpace(reuseSpace);
    }

    public int getRetentionTime() {
        return fileStore == null ? 0 : fileStore.getRetentionTime();
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
        if (fileStore != null) {
            fileStore.setRetentionTime(ms);
        }
    }

    /**
     * Indicates whether store versions are rolling.
     * @return true if versions are rolling, false otherwise
     */
    public boolean isVersioningRequired() {
        return fileStore != null && !fileStore.isReadOnly() || versionsToKeep > 0;
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
     * and will not release the version, while version is still in use.
     *
     * @return the version
     */
    long getOldestVersionToKeep() {
        return Math.min(oldestVersionToKeep.get(),
                        Math.max(currentVersion - versionsToKeep, INITIAL_VERSION));
    }

    private void setOldestVersionToKeep(long version) {
        boolean success;
        do {
            long current = oldestVersionToKeep.get();
            // Oldest version may only advance, never goes back
            success = version <= current ||
                        oldestVersionToKeep.compareAndSet(current, version);
        } while (!success);
        assert version <= currentVersion : version + " <= " + currentVersion;

        if (oldestVersionTracker != null) {
            oldestVersionTracker.accept(version);
        }
    }

    public void setOldestVersionTracker(LongConsumer callback) {
        oldestVersionTracker = callback;
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
        long curVersion = getCurrentVersion();
        if (version > curVersion || version < 0) {
            return false;
        }
        if (version == curVersion) {
            // no stored data
            return true;
        }
        return fileStore == null || fileStore.isKnownVersion(version);
    }

    /**
     * Adjust amount of "unsaved memory" meaning amount of RAM occupied by pages
     * not saved yet to the file. This is the amount which triggers auto-commit.
     *
     * @param memory adjustment
     */
    public void registerUnsavedMemory(int memory) {
        assert fileStore != null;
        // this counter was intentionally left unprotected against race
        // condition for performance reasons
        // TODO: evaluate performance impact of atomic implementation,
        //       since updates to unsavedMemory are largely aggregated now
        unsavedMemory += memory;
        if (needStore()) {
            saveNeeded = true;
        }
    }

    void registerUnsavedMemoryAndCommitIfNeeded(int memory) {
        registerUnsavedMemory(memory);
        if (saveNeeded) {
            commit();
        }
    }

    /**
     * This method is called before writing to a map.
     *
     * @param map the map
     */
    void beforeWrite(MVMap<?, ?> map) {
        if (saveNeeded && isOpenOrStopping() &&
                // condition below is to prevent potential deadlock,
                // because we should never seek storeLock while holding
                // map root lock
                (storeLock.isHeldByCurrentThread() || !map.getRoot().isLockedByCurrentThread()) &&
                // to avoid infinite recursion via store() -> dropUnusedChunks() -> layout.remove()
                fileStore.isRegularMap(map)) {
            saveNeeded = false;
            // check again, because it could have been written by now
            if (needStore()) {
                // if unsaved memory creation rate is too high,
                // some back pressure need to be applied
                // to slow things down and avoid OOME
                if (requireStore() && !map.isSingleWriter()) {
                    commit(MVStore::requireStore);
                } else {
                    tryCommit(MVStore::needStore);
                }
            }
        }
    }

    private boolean requireStore() {
        return 3 * unsavedMemory > 4 * autoCommitMemory;
    }

    private boolean needStore() {
        return autoCommitMemory > 0 && fileStore.shouldSaveNow(unsavedMemory, autoCommitMemory);
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
            currentVersion = version;
            checkOpen();
            DataUtils.checkArgument(isKnownVersion(version), "Unknown version {0}", version);

            TxCounter txCounter;
            while ((txCounter = versions.peekLast()) != null && txCounter.version >= version) {
                versions.removeLast();
            }
            currentTxCounter = new TxCounter(version);
            if (oldestVersionToKeep.get() > version) {
                oldestVersionToKeep.set(version);
            }

            if (fileStore != null) {
                fileStore.rollbackTo(version);
            }
            if (!meta.rollbackRoot(version)) {
                meta.setRootPos(getRootPos(meta.getId()), version - 1);
            }
            metaChanged = false;

            for (MVMap<?, ?> m : new ArrayList<>(maps.values())) {
                int id = m.getId();
                if (m.getCreateVersion() >= version) {
                    m.close();
                    maps.remove(id);
                } else {
                    if (!m.rollbackRoot(version)) {
                        m.setRootPos(getRootPos(id), version);
                    }
                }
            }
            onVersionChange(currentVersion);
            assert !hasUnsavedChanges();
        } finally {
            unlockAndCheckPanicCondition();
        }
    }

    private long getRootPos(int mapId) {
        return fileStore == null ? 0 : fileStore.getRootPos(mapId);
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

    void setCurrentVersion(long curVersion) {
        currentVersion = curVersion;
    }

    /**
     * Get the file store.
     *
     * @return the file store
     */
    public FileStore<?> getFileStore() {
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
        return fileStore.getStoreHeader();
    }

    private void checkOpen() {
        if (!isOpen()) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_CLOSED,
                    "This store is closed", panicException);
        }
    }

    private void checkNotClosed() {
        if (!isOpenOrStopping()) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_CLOSED,
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
        DataUtils.checkArgument(isRegularMap(map), "Renaming the meta map is not allowed");
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
    public void removeMap(MVMap<?,?> map) {
        storeLock.lock();
        try {
            checkOpen();
            DataUtils.checkArgument(isRegularMap(map), "Removing the meta map is not allowed");
            RootReference<?,?> rootReference = map.clearIt();
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
            // normally actual map removal is delayed, up until this current version go out os scope,
            // but for in-memory case, when versions rolling is turned off, do it now
            if (!isVersioningRequired()) {
                maps.remove(id);
            }
        } finally {
            storeLock.unlock();
        }
    }

    /**
     * Performs final stage of map removal - delete root location info from the layout table.
     * Map is supposedly closed and anonymous and has no outstanding usage by now.
     *
     * @param mapId to deregister
     */
    void deregisterMapRoot(int mapId) {
        if (fileStore != null && fileStore.deregisterMapRoot(mapId)) {
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
                map = openMap(name, MVStoreTool.getGenericMapBuilder());
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
        String m = meta.get(MVMap.getMapKey(id));
        return m == null ? null : DataUtils.getMapName(m);
    }

    private int getMapId(String name) {
        String m = meta.get(DataUtils.META_NAME + name);
        return m == null ? -1 : DataUtils.parseHexInt(m);
    }

    public void populateInfo(BiConsumer<String, String> consumer) {
        consumer.accept("info.UPDATE_FAILURE_PERCENT",
            String.format(Locale.ENGLISH, "%.2f%%", 100 * getUpdateFailureRatio()));
        consumer.accept("info.LEAF_RATIO", Integer.toString(getLeafRatio()));

        if (fileStore != null) {
            fileStore.populateInfo(consumer);
        }
    }

    boolean handleException(Throwable ex) {
        if (backgroundExceptionHandler != null) {
            try {
                backgroundExceptionHandler.uncaughtException(Thread.currentThread(), ex);
            } catch(Throwable e) {
                if (ex != e) { // OOME may be the same
                    ex.addSuppressed(e);
                }
            }
            return true;
        }
        return false;
    }

    boolean isOpen() {
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
            return state == STATE_CLOSED;
        } finally {
            storeLock.unlock();
        }
    }

    private boolean isOpenOrStopping() {
        return state <= STATE_STOPPING;
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
        if (fileStore != null) {
            fileStore.setAutoCommitDelay(millis);
        }
    }

    /**
     * Get the auto-commit delay.
     *
     * @return the delay in milliseconds, or 0 if auto-commit is disabled.
     */
    public int getAutoCommitDelay() {
        return fileStore == null ? 0 : fileStore.getAutoCommitDelay();
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
     * Whether the store is read-only.
     *
     * @return true if it is
     */
    public boolean isReadOnly() {
        return fileStore != null && fileStore.isReadOnly();
    }

    private int getLeafRatio() {
        return (int)(leafCount * 100 / Math.max(1, leafCount + nonLeafCount));
    }

    private double getUpdateFailureRatio() {
        long updateCounter = this.updateCounter;
        long updateAttemptCounter = this.updateAttemptCounter;
        RootReference<?,?> rootReference = meta.getRoot();
        updateCounter += rootReference.updateCounter;
        updateAttemptCounter += rootReference.updateAttemptCounter;
        for (MVMap<?, ?> map : maps.values()) {
            RootReference<?,?> root = map.getRoot();
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
        if(decrementVersionUsageCounter(txCounter)) {
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

    /**
     * De-register (close) completed operation (transaction).
     * This will decrement usage counter for the corresponding version.
     *
     * @param txCounter to be decremented, obtained from registerVersionUsage()
     * @return true if counter reaches zero, which indicates that version is no longer in use, false otherwise.
     */
    public boolean decrementVersionUsageCounter(TxCounter txCounter) {
        return txCounter != null &&  txCounter.decrementAndGet() <= 0;
    }

    void onVersionChange(long version) {
        metaChanged = false;
        TxCounter txCounter = currentTxCounter;
        assert txCounter.get() >= 0;
        versions.add(txCounter);
        currentTxCounter = new TxCounter(version);
        txCounter.decrementAndGet();
        dropUnusedVersions();
    }

    private void dropUnusedVersions() {
        TxCounter txCounter;
        while ((txCounter = versions.peek()) != null
                && txCounter.get() < 0) {
            versions.poll();
        }
        long oldestVersionToKeep = (txCounter != null ? txCounter : currentTxCounter).version;
        setOldestVersionToKeep(oldestVersionToKeep);
    }

    public void countNewPage(boolean leaf) {
        if (leaf) {
            ++leafCount;
        } else {
            ++nonLeafCount;
        }
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
         * The default value is 90 (90%). The value 0 disables auto-compacting.
         * </p>
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
         * Set the number of keys per page.
         *
         * @param keyCount the number of keys
         * @return this
         */
        public Builder keysPerPage(int keyCount) {
            return set("keysPerPage", keyCount);
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
        public Builder fileStore(FileStore<?> store) {
            return set("fileStore", store);
        }

        public Builder adoptFileStore(FileStore store) {
            set("fileStoreIsAdopted", true);
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
        @SuppressWarnings({"unchecked", "rawtypes", "unused"})
        public static Builder fromString(String s) {
            // Cast from HashMap<String, String> to HashMap<String, Object> is safe
            return new Builder((HashMap) DataUtils.parseMap(s));
        }
    }
}
