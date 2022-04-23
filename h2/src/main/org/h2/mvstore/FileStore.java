/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import static org.h2.mvstore.MVStore.INITIAL_VERSION;
import org.h2.mvstore.cache.CacheLongKeyLIRS;
import org.h2.mvstore.type.StringDataType;
import org.h2.util.MathUtils;
import org.h2.util.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;
import java.util.zip.ZipOutputStream;

/**
 * Class FileStore is a base class to allow for different store implementations.
 * FileStore concept revolves around notion of a "chunk", which is a piece of data
 * written into the store at once.
 *
 * @author <a href="mailto:andrei.tokar@gmail.com">Andrei Tokar</a>
 */
public abstract class FileStore
{
    // The following are attribute names (keys) in store header map
    static final String HDR_H = "H";
    static final String HDR_BLOCK_SIZE = "blockSize";
    static final String HDR_FORMAT = "format";
    static final String HDR_CREATED = "created";
    static final String HDR_FORMAT_READ = "formatRead";
    static final String HDR_CHUNK = "chunk";
    static final String HDR_BLOCK = "block";
    static final String HDR_VERSION = "version";
    static final String HDR_CLEAN = "clean";
    private static final String HDR_FLETCHER = "fletcher";

    /**
     * The key for the entry within "layout" map, which contains id of "meta" map.
     * Entry value (hex encoded) is usually equal to 1, unless it's a legacy
     * (upgraded) database and id 1 has been taken already by another map.
     */
    public static final String META_ID_KEY = "meta.id";

    /**
     * The block size (physical sector size) of the disk. The store header is
     * written twice, one copy in each block, to ensure it survives a crash.
     */
    static final int BLOCK_SIZE = 4 * 1024;

    private static final int FORMAT_WRITE_MIN = 2;
    private static final int FORMAT_WRITE_MAX = 2;
    private static final int FORMAT_READ_MIN = 2;
    private static final int FORMAT_READ_MAX = 2;

    private MVStore mvStore;
    private boolean closed;

    /**
     * The number of read operations.
     */
    protected final AtomicLong readCount = new AtomicLong();

    /**
     * The number of read bytes.
     */
    protected final AtomicLong readBytes = new AtomicLong();

    /**
     * The number of write operations.
     */
    protected final AtomicLong writeCount = new AtomicLong();

    /**
     * The number of written bytes.
     */
    protected final AtomicLong writeBytes = new AtomicLong();

    /**
     * The file name.
     */
    private String fileName;

    /**
     * For how long (in milliseconds) to retain a persisted chunk after it becomes irrelevant
     * (not in use, because it only contains data from some old versions).
     * Non-positive value allows chunk to be discarded immediately, once it goes out of use.
     */
    private int retentionTime = getDefaultRetentionTime();

    private final int maxPageSize;

    /**
     * The file size (cached).
     */
    private long size;

    /**
     * Whether this store is read-only.
     */
    private boolean readOnly;

    /**
     * Lock guarding submission to serializationExecutor
     */
    private final ReentrantLock serializationLock = new ReentrantLock(true);

    /**
     * Single-threaded executor for serialization of the store snapshot into ByteBuffer
     */
    private ThreadPoolExecutor serializationExecutor;

    /**
     * Single-threaded executor for saving ByteBuffer as a new Chunk
     */
    private ThreadPoolExecutor bufferSaveExecutor;


    /**
     * The page cache. The default size is 16 MB, and the average size is 2 KB.
     * It is split in 16 segments. The stack move distance is 2% of the expected
     * number of entries.
     */
    private final CacheLongKeyLIRS<Page<?,?>> cache;

    /**
     * Cache for chunks "Table of Content" used to translate page's
     * sequential number within containing chunk into byte position
     * within chunk's image. Cache keyed by chunk id.
     */
    private final CacheLongKeyLIRS<long[]> chunksToC;

    private final Queue<RemovedPageInfo> removedPages = new PriorityBlockingQueue<>();

    /**
     * The newest chunk. If nothing was stored yet, this field is not set.
     */
    protected volatile Chunk lastChunk;

    private int lastChunkId;   // protected by serializationLock

    protected final ReentrantLock saveChunkLock = new ReentrantLock(true);

    /**
     * The map of chunks.
     */
    private final ConcurrentHashMap<Integer, Chunk> chunks = new ConcurrentHashMap<>();

    private final HashMap<String, Object> storeHeader = new HashMap<>();

    /**
     * The time the store was created, in milliseconds since 1970.
     */
    private long creationTime;


    private final Queue<WriteBuffer> writeBufferPool = new ArrayBlockingQueue<>(PIPE_LENGTH + 1);

    /**
     * The layout map. Contains chunks metadata and root locations for all maps.
     * This is relatively fast changing part of metadata
     */
    private MVMap<String, String> layout;

    private final Deque<Chunk> deadChunks = new ArrayDeque<>();

    /**
     * Reference to a background thread, which is expected to be running, if any.
     */
    private final AtomicReference<BackgroundWriterThread> backgroundWriterThread = new AtomicReference<>();

    private final int autoCompactFillRate;

    /**
     * The delay in milliseconds to automatically commit and write changes.
     */
    private int autoCommitDelay;

    private long autoCompactLastFileOpCount;

    private long lastCommitTime;

    private final boolean recoveryMode;

    public static final int PIPE_LENGTH = 3;




    protected FileStore(Map<String, Object> config) {
        recoveryMode = config.containsKey("recoveryMode");
        autoCompactFillRate = DataUtils.getConfigParam(config, "autoCompactFillRate", 90);
        CacheLongKeyLIRS.Config cc = null;
        int mb = DataUtils.getConfigParam(config, "cacheSize", 16);
        if (mb > 0) {
            cc = new CacheLongKeyLIRS.Config();
            cc.maxMemory = mb * 1024L * 1024L;
            Object o = config.get("cacheConcurrency");
            if (o != null) {
                cc.segmentCount = (Integer)o;
            }
        }
        cache = cc == null ? null : new CacheLongKeyLIRS<>(cc);

        CacheLongKeyLIRS.Config cc2 = new CacheLongKeyLIRS.Config();
        cc2.maxMemory = 1024L * 1024L;
        chunksToC = new CacheLongKeyLIRS<>(cc2);

        int maxPageSize = Integer.MAX_VALUE;
        // Make sure pages will fit into cache
        if (cache != null) {
            maxPageSize = 16 * 1024;
            int maxCachableSize = (int) (cache.getMaxItemSize() >> 4);
            if (maxPageSize > maxCachableSize) {
                maxPageSize = maxCachableSize;
            }
        }
        this.maxPageSize = maxPageSize;
    }

    public void open(String fileName, boolean readOnly, char[] encryptionKey,
                     MVStore mvStore) {
        open(fileName, readOnly,
                encryptionKey == null ? null
                        : fileChannel -> new FileEncrypt(fileName, FilePathEncrypt.getPasswordBytes(encryptionKey),
                                fileChannel));
    }

    public FileStore open(String fileName, boolean readOnly) {

        FileStore result = new FileStore();
        result.open(fileName, readOnly, encryptedFile == null ? null :
                fileChannel -> new FileEncrypt(fileName, (FileEncrypt)file, fileChannel));
        return result;
    }

    private void open(String fileName, boolean readOnly, Function<FileChannel,FileChannel> encryptionTransformer) {
        if (file != null) {
            return;
        }
        // ensure the Cache file system is registered
        FilePathCache.INSTANCE.getScheme();
        this.fileName = fileName;
        this.readOnly = readOnly;
        bind(mvStore);
        scrubLayoutMap(mvStore);
    }

    public final void bind(MVStore mvStore) {
        if(this.mvStore != mvStore) {
            layout = new MVMap<>(mvStore, 0, StringDataType.INSTANCE, StringDataType.INSTANCE);
            this.mvStore = mvStore;
            mvStore.resetLastMapId(lastChunk == null ? 0 : lastChunk.mapId);
            mvStore.setCurrentVersion(lastChunkVersion());
        }
    }

    public final void stop(long allowedCompactionTime) {
        if (allowedCompactionTime > 0) {
            compactStore(allowedCompactionTime);
        }
        writeCleanShutdown();
        clearCaches();
    }

    public void close() {
        layout.close();
        closed = true;
        chunks.clear();
    }

    public final int getMetaMapId(IntSupplier nextIdSupplier) {
        String metaIdStr = layout.get(META_ID_KEY);
        int metaId;
        if (metaIdStr == null) {
            metaId = nextIdSupplier.getAsInt();
            layout.put(META_ID_KEY, Integer.toHexString(metaId));
        } else {
            metaId = DataUtils.parseHexInt(metaIdStr);
        }
        return metaId;
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
    public final Map<String, String> getLayoutMap() {
        return new TreeMap<>(layout);
    }

    public final boolean isRegularMap(MVMap<?,?> map) {
        return map != layout;
    }

    /**
     * Get "position" of the root page for the specified map
     * @param mapId to get root position for
     * @return opaque "position" value, that should be used to read the page
     */
    public final long getRootPos(int mapId) {
        String root = layout.get(MVMap.getMapRootKey(mapId));
        return root == null ? 0 : DataUtils.parseHexLong(root);
    }

    /**
     * Performs final stage of map removal - delete root location info from the layout map.
     * Specified map is supposedly closed, is anonymous and has no outstanding usage by now.
     *
     * @param mapId to deregister
     */
    public final boolean deregisterMapRoot(int mapId) {
        return layout.remove(MVMap.getMapRootKey(mapId)) != null;
    }

    /**
     * Check whether there are any unsaved changes since specified version.
     *
     * @return if there are any changes
     */
    public final boolean hasChangesSince(long lastStoredVersion) {
        return layout.hasChangesSince(lastStoredVersion) && lastStoredVersion > INITIAL_VERSION;
    }

    public final long lastChunkVersion() {
        Chunk chunk = lastChunk;
        return chunk == null ? INITIAL_VERSION + 1 : chunk.version;
    }

    public final long getMaxPageSize() {
        return maxPageSize;
    }

    public final int getRetentionTime() {
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
    public final void setRetentionTime(int ms) {
        retentionTime = ms;
    }

    /**
     * Decision about autocommit is delegated to store
     * @param unsavedMemory amount of unsaved memory, so far
     * @param autoCommitMemory configured limit on amount of unsaved memory
     * @return true if commit should happen now
     */
    public abstract boolean shouldSaveNow(int unsavedMemory, int autoCommitMemory);

    /**
     * Get the auto-commit delay.
     *
     * @return the delay in milliseconds, or 0 if auto-commit is disabled.
     */
    public final int getAutoCommitDelay() {
        return autoCommitDelay;
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
    public final void setAutoCommitDelay(int millis) {
        if (autoCommitDelay != millis) {
            autoCommitDelay = millis;
            if (!isReadOnly()) {
                stopBackgroundThread(true);
                // start the background thread if needed
                if (millis > 0 && mvStore.isOpen()) {
                    int sleep = Math.max(1, millis / 10);
                    BackgroundWriterThread t = new BackgroundWriterThread(this, sleep, toString());
                    if (backgroundWriterThread.compareAndSet(null, t)) {
                        t.start();
                        serializationExecutor = Utils.createSingleThreadExecutor("H2-serialization");
                        bufferSaveExecutor = Utils.createSingleThreadExecutor("H2-save");
                    }
                }
            }
        }
    }

    /**
     * Check whether all data can be read from this version. This requires that
     * all chunks referenced by this version are still available (not
     * overwritten).
     *
     * @param version the version
     * @return true if all data can be read
     */
    public final boolean isKnownVersion(long version) {
        if (chunks.isEmpty()) {
            // no stored data
            return true;
        }
        // need to check if a chunk for this version exists
        Chunk c = getChunkForVersion(version);
        if (c == null) {
            return false;
        }
        try {
            // also, all chunks referenced by this version
            // need to be available in the file
            MVMap<String, String> oldLayoutMap = getLayoutMap(version);
            for (Chunk chunk : getChunksFromLayoutMap(oldLayoutMap)) {
                String chunkKey = Chunk.getMetaKey(chunk.id);
                // if current layout map does not have it - verify it's existence
                if (!layout.containsKey(chunkKey) && !isValidChunk(chunk)) {
                    return false;
                }
            }
        } catch (MVStoreException e) {
            // the chunk missing where the metadata is stored
            return false;
        }
        return true;
    }

    public final void rollbackTo(long version) {
        if (version == 0) {
            // special case: remove all data
            String metaId = layout.get(META_ID_KEY);
            layout.setInitialRoot(layout.createEmptyLeaf(), INITIAL_VERSION);
            layout.put(META_ID_KEY, metaId);
        } else {
            if (!layout.rollbackRoot(version)) {
                MVMap<String, String> layoutMap = getLayoutMap(version);
                layout.setInitialRoot(layoutMap.getRootPage(), version);
            }
        }
        serializationLock.lock();
        try {
            Chunk keep = getChunkForVersion(version);
            if (keep != null) {
                saveChunkLock.lock();
                try {
                    deadChunks.clear();
                    setLastChunk(keep);
                    storeHeader.put(HDR_CLEAN, 1);
                    writeStoreHeader();
                    readStoreHeader(false);
                } finally {
                    saveChunkLock.unlock();
                }
            }
        } finally {
            serializationLock.unlock();
        }
        removedPages.clear();
        clearCaches();
    }


    private long getTimeSinceCreation() {
        return Math.max(0, mvStore.getTimeAbsolute() - getCreationTime());
    }

    private MVMap<String, String> getLayoutMap(long version) {
        Chunk chunk = getChunkForVersion(version);
        DataUtils.checkArgument(chunk != null, "Unknown version {0}", version);
        return layout.openReadOnly(chunk.layoutRootPos, version);
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

    private void scrubLayoutMap(MVStore mvStore) {
        MVMap<String, String> meta = mvStore.getMetaMap();
        Set<String> keysToRemove = new HashSet<>();

        // split meta map off layout map
        for (String prefix : new String[]{ DataUtils.META_NAME, DataUtils.META_MAP }) {
            for (Iterator<String> it = layout.keyIterator(prefix); it.hasNext(); ) {
                String key = it.next();
                if (!key.startsWith(prefix)) {
                    break;
                }
                meta.putIfAbsent(key, layout.get(key));
                mvStore.markMetaChanged();
                keysToRemove.add(key);
            }
        }

        // remove roots of non-existent maps (leftover after unfinished map removal)
        for (Iterator<String> it = layout.keyIterator(DataUtils.META_ROOT); it.hasNext();) {
            String key = it.next();
            if (!key.startsWith(DataUtils.META_ROOT)) {
                break;
            }
            String mapIdStr = key.substring(key.lastIndexOf('.') + 1);
            if(!meta.containsKey(DataUtils.META_MAP + mapIdStr) && DataUtils.parseHexInt(mapIdStr) != meta.getId()) {
                keysToRemove.add(key);
            }
        }

        for (String key : keysToRemove) {
            layout.remove(key);
        }
    }

    protected final boolean hasPersitentData() {
        return lastChunk != null;
    }

    protected final boolean isIdle() {
        return autoCompactLastFileOpCount == getWriteCount() + getReadCount();
    }

    private void setLastChunk(Chunk last) {
        lastChunk = last;
        chunks.clear();
        lastChunkId = 0;
        long layoutRootPos = 0;
        if (last != null) { // there is a valid chunk
            lastChunkId = last.id;
            layoutRootPos = last.layoutRootPos;
            chunks.put(last.id, last);
        }
        layout.setRootPos(layoutRootPos, lastChunkVersion());
    }

    private void registerDeadChunk(Chunk chunk) {
        deadChunks.offer(chunk);
    }

    public final void dropUnusedChunks() {
        if (!deadChunks.isEmpty()) {
            long oldestVersionToKeep = mvStore.getOldestVersionToKeep();
            long time = getTimeSinceCreation();
            List<Chunk> toBeFreed = new ArrayList<>();
            Chunk chunk;
            while ((chunk = deadChunks.poll()) != null &&
                    (isSeasonedChunk(chunk, time) && canOverwriteChunk(chunk, oldestVersionToKeep) ||
                            // if chunk is not ready yet, put it back and exit
                            // since this deque is unbounded, offerFirst() always return true
                            !deadChunks.offerFirst(chunk))) {

                if (chunks.remove(chunk.id) != null) {
                    // purge dead pages from cache
                    long[] toc = cleanToCCache(chunk);
                    if (toc != null && cache != null) {
                        for (long tocElement : toc) {
                            long pagePos = DataUtils.composePagePos(chunk.id, tocElement);
                            cache.remove(pagePos);
                        }
                    }

                    if (layout.remove(Chunk.getMetaKey(chunk.id)) != null) {
                        mvStore.markMetaChanged();
                    }
                    if (chunk.isAllocated()) {
                        toBeFreed.add(chunk);
                    }
                }
            }
            if (!toBeFreed.isEmpty()) {
                saveChunkLock.lock();
                try {
                    freeChunkSpace(toBeFreed);
                } finally {
                    saveChunkLock.unlock();
                }
            }
        }
    }

    private static boolean canOverwriteChunk(Chunk c, long oldestVersionToKeep) {
        return !c.isLive() && c.unusedAtVersion < oldestVersionToKeep;
    }

    private boolean isSeasonedChunk(Chunk chunk, long time) {
        int retentionTime = getRetentionTime();
        return retentionTime < 0 || chunk.time + retentionTime <= time;
    }

    private boolean isRewritable(Chunk chunk, long time) {
        return chunk.isRewritable() && isSeasonedChunk(chunk, time);
    }

    /**
     * Write data to the store.
     * @param volumeId 0-based index
     * @param pos the write "position"
     * @param src the source buffer
     */
    protected abstract void writeFully(Chunk chunk, long pos, ByteBuffer src);

    /**
     * Read data from the store.
     *
     *
     * @param volumeId 0-based index
     * @param pos the read "position"
     * @param len the number of bytes to read
     * @return the byte buffer with data requested
     */
    public abstract ByteBuffer readFully(Chunk chunk, long pos, int len);

    protected final ByteBuffer readFully(FileChannel file, long pos, int len) {
        ByteBuffer dst = ByteBuffer.allocate(len);
        DataUtils.readFully(file, pos, dst);
        readCount.incrementAndGet();
        readBytes.addAndGet(len);
        return dst;
    }


    /**
     * Allocate logical space and assign position of the buffer within the store.
     *
     * @param chunk to allocate space for
     * @param buff to allocate space for
     */
    protected abstract void allocateChunkSpace(Chunk chunk, WriteBuffer buff);

    private boolean isWriteStoreHeader(Chunk c, boolean storeAtEndOfFile) {
        // whether we need to write the store header
        boolean writeStoreHeader = false;
        if (!storeAtEndOfFile) {
            Chunk chunk = lastChunk;
            if (chunk == null) {
                writeStoreHeader = true;
            } else if (chunk.next != c.block) {
                // the last prediction did not matched
                writeStoreHeader = true;
            } else {
                long headerVersion = DataUtils.readHexLong(storeHeader, HDR_VERSION, 0);
                if (chunk.version - headerVersion > 20) {
                    // we write after at least every 20 versions
                    writeStoreHeader = true;
                } else {
                    for (int chunkId = DataUtils.readHexInt(storeHeader, HDR_CHUNK, 0);
                         !writeStoreHeader && chunkId <= chunk.id; ++chunkId) {
                        // one of the chunks in between
                        // was removed
                        writeStoreHeader = !getChunks().containsKey(chunkId);
                    }
                }
            }
        }

        if (storeHeader.remove(HDR_CLEAN) != null) {
            writeStoreHeader = true;
        }
        return writeStoreHeader;
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

    private void initializeStoreHeader(long time) {
        setLastChunk(null);
        creationTime = time;
        storeHeader.put(FileStore.HDR_H, 2);
        storeHeader.put(FileStore.HDR_BLOCK_SIZE, FileStore.BLOCK_SIZE);
        storeHeader.put(FileStore.HDR_FORMAT, FileStore.FORMAT_WRITE_MAX);
        storeHeader.put(FileStore.HDR_CREATED, creationTime);
        writeStoreHeader();
    }

    private Chunk createChunk(long time, long version) {
        int newChunkId = findNewChunkId();
        Chunk c = createChunk(newChunkId);
        c.time = time;
        c.version = version;
        c.occupancy = new BitSet();
        return c;
    }

    protected abstract Chunk createChunk(int id);

    protected abstract Chunk createChunk(String s);

    protected abstract Chunk createChunk(Map<String, String> map, boolean full);


    private int findNewChunkId() {
        int newChunkId;
        while (true) {
            newChunkId = ++lastChunkId & Chunk.MAX_ID;
            if (newChunkId == lastChunkId) {
                break;
            }
            Chunk old = chunks.get(newChunkId);
            if (old == null) {
                break;
            }
            if (!old.isSaved()) {
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_INTERNAL,
                        "Last block {0} not stored, possibly due to out-of-memory", old);
            }
        }
        return newChunkId;
    }

    protected void writeStoreHeader() {
        StringBuilder buff = new StringBuilder(112);
        if (hasPersitentData()) {
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
        writeFully(null, 0, header);
    }

    protected void writeCleanShutdown() {
        if (!isReadOnly()) {
            saveChunkLock.lock();
            try {
                shrinkStoreIfPossible(0);
                storeHeader.put(HDR_CLEAN, 1);
                writeStoreHeader();
                sync();
                assert validateFileLength("on close");
            } finally {
                saveChunkLock.unlock();
            }
        }
    }

    /**
     * Store chunk's serialized metadata as an entry in a layout map.
     * Key for this entry would be "chunk.<id>"
     *
     * @param chunk to save
     */
    public void saveChunkMetadataChanges(Chunk chunk) {
        assert serializationLock.isHeldByCurrentThread();
        // chunk's location has to be determined before
        // it's metadata can be is serialized
        while (!chunk.isAllocated()) {
            saveChunkLock.lock();
            try {
                if (chunk.isAllocated()) {
                    break;
                }
            } finally {
                saveChunkLock.unlock();
            }
            // just let chunks saving thread to deal with it
            Thread.yield();
        }
        layout.put(Chunk.getMetaKey(chunk.id), chunk.asString());
    }

    /**
     * Mark the space occupied by specified chunks as free.
     *
     * @param chunks chunks to be processed
     */
    protected abstract void freeChunkSpace(Iterable<Chunk> chunks);

    protected abstract boolean validateFileLength(String msg);

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
        if (hasPersitentData()) {
            if (targetFillRate > 0 && getChunksFillRate() < targetFillRate) {
                // We can't wait forever for the lock here,
                // because if called from the background thread,
                // it might go into deadlock with concurrent database closure
                // and attempt to stop this thread.
                try {
                    Boolean result = mvStore.tryExecuteUnderStoreLock(() -> rewriteChunks(write, 100));
                    return result != null && result;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return false;
    }

    public void compactStore(long maxCompactTime) {
        compactStore(autoCompactFillRate, maxCompactTime, 16 * 1024 * 1024, mvStore);
    }

    /**
     * Compact store file, that is, compact blocks that have a low
     * fill rate, and move chunks next to each other. This will typically
     * shrink the file. Changes are flushed to the file, and old
     * chunks are overwritten.
     *
     * @param thresholdFildRate do not compact if store fill rate above this value (0-100)
     * @param maxCompactTime the maximum time in milliseconds to compact
     * @param maxWriteSize the maximum amount of data to be written as part of this call
     */
    protected abstract void compactStore(int thresholdFildRate, long maxCompactTime, int maxWriteSize, MVStore mvStore);

    protected abstract void doHousekeeping(MVStore mvStore) throws InterruptedException;



    public void start() {
        if (size() == 0) {
            initializeStoreHeader(mvStore.getTimeAbsolute());
        } else {
            saveChunkLock.lock();
            try {
                readStoreHeader(recoveryMode);
            } finally {
                saveChunkLock.unlock();
            }
        }
        lastCommitTime = getTimeSinceCreation();
        mvStore.resetLastMapId(lastMapId());
        mvStore.setCurrentVersion(lastChunkVersion());
    }

    private int lastMapId() {
        Chunk chunk = lastChunk;
        return chunk == null ? 0 : chunk.mapId;
    }

    protected void readStoreHeader(boolean recoveryMode) {
        long now = System.currentTimeMillis();
        Chunk newest = null;
        boolean assumeCleanShutdown = true;
        boolean validStoreHeader = false;
        // find out which chunk and version are the newest
        // read the first two blocks
        ByteBuffer fileHeaderBlocks = readFully((Chunk)null, 0, 2 * FileStore.BLOCK_SIZE);
        byte[] buff = new byte[FileStore.BLOCK_SIZE];
        for (int i = 0; i <= FileStore.BLOCK_SIZE; i += FileStore.BLOCK_SIZE) {
            fileHeaderBlocks.get(buff);
            // the following can fail for various reasons
            try {
                HashMap<String, String> m = DataUtils.parseChecksummedMap(buff);
                if (m == null) {
                    assumeCleanShutdown = false;
                    continue;
                }
                long version = DataUtils.readHexLong(m, FileStore.HDR_VERSION, 0);
                // if both header blocks do agree on version
                // we'll continue on happy path - assume that previous shutdown was clean
                assumeCleanShutdown = assumeCleanShutdown && (newest == null || version == newest.version);
                if (newest == null || version > newest.version) {
                    validStoreHeader = true;
                    storeHeader.putAll(m);
                    creationTime = DataUtils.readHexLong(m, FileStore.HDR_CREATED, 0);
                    int chunkId = DataUtils.readHexInt(m, FileStore.HDR_CHUNK, 0);
                    long block = DataUtils.readHexLong(m, FileStore.HDR_BLOCK, 2);
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
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Store header is corrupt: {0}", this);
        }
        int blockSize = DataUtils.readHexInt(storeHeader, FileStore.HDR_BLOCK_SIZE, FileStore.BLOCK_SIZE);
        if (blockSize != FileStore.BLOCK_SIZE) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "Block size {0} is currently not supported",
                    blockSize);
        }
        long format = DataUtils.readHexLong(storeHeader, HDR_FORMAT, 1);
        if (!isReadOnly()) {
            if (format > FORMAT_WRITE_MAX) {
                throw getUnsupportedWriteFormatException(format, FORMAT_WRITE_MAX,
                        "The write format {0} is larger than the supported format {1}");
            } else if (format < FORMAT_WRITE_MIN) {
                throw getUnsupportedWriteFormatException(format, FORMAT_WRITE_MIN,
                        "The write format {0} is smaller than the supported format {1}");
            }
        }
        format = DataUtils.readHexLong(storeHeader, HDR_FORMAT_READ, format);
        if (format > FORMAT_READ_MAX) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The read format {0} is larger than the supported format {1}",
                    format, FORMAT_READ_MAX);
        } else if (format < FORMAT_READ_MIN) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The read format {0} is smaller than the supported format {1}",
                    format, FORMAT_READ_MIN);
        }

        assumeCleanShutdown = assumeCleanShutdown && newest != null && !recoveryMode;
        if (assumeCleanShutdown) {
            assumeCleanShutdown = DataUtils.readHexInt(storeHeader, FileStore.HDR_CLEAN, 0) != 0;
        }
        getChunks().clear();
        // calculate the year (doesn't have to be exact;
        // we assume 365.25 days per year, * 4 = 1461)
        int year =  1970 + (int) (now / (1000L * 60 * 60 * 6 * 1461));
        if (year < 2014) {
            // if the year is before 2014,
            // we assume the system doesn't have a real-time clock,
            // and we set the creationTime to the past, so that
            // existing chunks are overwritten
            creationTime = now - getRetentionTime();
        } else if (now < creationTime) {
            // the system time was set to the past:
            // we change the creation time
            creationTime = now;
            storeHeader.put(FileStore.HDR_CREATED, creationTime);
        }

        long fileSize = size();
        long blocksInStore = fileSize / FileStore.BLOCK_SIZE;

        Comparator<Chunk> chunkComparator = (one, two) -> {
            int result = Long.compare(two.version, one.version);
            if (result == 0) {
                // out of two copies of the same chunk we prefer the one
                // close to the beginning of file (presumably later version)
                result = Long.compare(one.block, two.block);
            }
            return result;
        };

        Map<Long, Chunk> validChunksByLocation = new HashMap<>();
        if (!assumeCleanShutdown) {
            Chunk tailChunk = discoverChunk(blocksInStore);
            if (tailChunk != null) {
                blocksInStore = tailChunk.block; // for a possible full scan later on
                validChunksByLocation.put(blocksInStore, tailChunk);
                if (newest == null || tailChunk.version > newest.version) {
                    newest = tailChunk;
                }
            }
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
                    newest = test;
                }
            }
        }

        if (assumeCleanShutdown) {
            // quickly check latest 20 chunks referenced in meta table
            Queue<Chunk> chunksToVerify = new PriorityQueue<>(20, Collections.reverseOrder(chunkComparator));
            try {
                setLastChunk(newest);
                // load the chunk metadata: although meta's root page resides in the lastChunk,
                // traversing meta map might recursively load another chunk(s)
                for (Chunk c : getChunksFromLayoutMap()) {
                    // might be there already, due to meta traversal
                    // see readPage() ... getChunkIfFound()
                    chunksToVerify.offer(c);
                    if (chunksToVerify.size() == 20) {
                        chunksToVerify.poll();
                    }
                }
                Chunk c;
                while (assumeCleanShutdown && (c = chunksToVerify.poll()) != null) {
                    Chunk test = readChunkHeaderAndFooter(c.block, c.id);
                    assumeCleanShutdown = test != null;
                    if (assumeCleanShutdown) {
                        validChunksByLocation.put(test.block, test);
                    }
                }
            } catch(IllegalStateException ignored) {
                assumeCleanShutdown = false;
            }
        }

        if (!assumeCleanShutdown) {
            // now we know, that previous shutdown did not go well and file
            // is possibly corrupted but there is still hope for a quick
            // recovery
            boolean quickRecovery = !recoveryMode &&
                    findLastChunkWithCompleteValidChunkSet(chunkComparator, validChunksByLocation, false);
            if (!quickRecovery) {
                // scan whole file and try to fetch chunk header and/or footer out of every block
                // matching pairs with nothing in-between are considered as valid chunk
                long block = blocksInStore;
                Chunk tailChunk;
                while ((tailChunk = discoverChunk(block)) != null) {
                    block = tailChunk.block;
                    validChunksByLocation.put(block, tailChunk);
                }

                if (!findLastChunkWithCompleteValidChunkSet(chunkComparator, validChunksByLocation, true)
                        && hasPersitentData()) {
                    throw DataUtils.newMVStoreException(
                            DataUtils.ERROR_FILE_CORRUPT,
                            "File is corrupted - unable to recover a valid set of chunks");
                }
            }
        }

        clear();
        // build the free space list
        for (Chunk c : getChunks().values()) {
            if (c.isAllocated()) {
                long start = c.block * FileStore.BLOCK_SIZE;
                int length = c.len * FileStore.BLOCK_SIZE;
                markUsed(start, length);
            }
            if (!c.isLive()) {
                registerDeadChunk(c);
            }
        }
        assert validateFileLength("on open");
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

    private MVStoreException getUnsupportedWriteFormatException(long format, int expectedFormat, String s) {
        format = DataUtils.readHexLong(storeHeader, HDR_FORMAT_READ, format);
        if (format >= FORMAT_READ_MIN && format <= FORMAT_READ_MAX) {
            s += ", and the file was not opened in read-only mode";
        }
        return DataUtils.newMVStoreException(DataUtils.ERROR_UNSUPPORTED_FORMAT, s, format, expectedFormat);
    }

    private boolean findLastChunkWithCompleteValidChunkSet(Comparator<Chunk> chunkComparator,
                                                           Map<Long, Chunk> validChunksByLocation,
                                                           boolean afterFullScan) {
        // this collection will hold potential candidates for lastChunk to fall back to,
        // in order from the most to least likely
        Chunk[] lastChunkCandidates = validChunksByLocation.values().toArray(new Chunk[0]);
        Arrays.sort(lastChunkCandidates, chunkComparator);
        Map<Integer, Chunk> validChunksById = new HashMap<>();
        for (Chunk chunk : lastChunkCandidates) {
            validChunksById.put(chunk.id, chunk);
        }
        // Try candidates for "last chunk" in order from newest to oldest
        // until suitable is found. Suitable one should have meta map
        // where all chunk references point to valid locations.
        for (Chunk chunk : lastChunkCandidates) {
            boolean verified = true;
            try {
                setLastChunk(chunk);
                // load the chunk metadata: although meta's root page resides in the lastChunk,
                // traversing meta map might recursively load another chunk(s)
                for (Chunk c : getChunksFromLayoutMap()) {
                    Chunk test;
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
                        } else if (c.isLive() && (afterFullScan || readChunkHeaderAndFooter(c.block, c.id) == null)) {
                            // chunk reference is invalid
                            // this "last chunk" candidate is not suitable
                            verified = false;
                            break;
                        }
                    }
                    if (!c.isLive()) {
                        // we can just remove entry from meta, referencing to this chunk,
                        // but store maybe R/O, and it's not properly started yet,
                        // so lets make this chunk "dead" and taking no space,
                        // and it will be automatically removed later.
                        c.block = 0;
                        c.len = 0;
                        if (c.unused == 0) {
                            c.unused = creationTime;
                        }
                        if (c.unusedAtVersion == 0) {
                            c.unusedAtVersion = INITIAL_VERSION;
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

    private Chunk readChunkHeader(long block) {
        long p = block * FileStore.BLOCK_SIZE;
        ByteBuffer buff = readFully((Chunk)null, p, Chunk.MAX_HEADER_LENGTH);
        Throwable exception = null;
        try {
            Chunk chunk = createChunk(Chunk.readChunkHeader(buff, p));
            if (chunk.block == 0) {
                chunk.block = block;
            }
            if (chunk.block == block) {
                return chunk;
            }
        } catch (MVStoreException e) {
            exception = e.getCause();
        } catch (Throwable e) {
            // there could be various reasons
            exception = e;
        }
        throw DataUtils.newMVStoreException(
                DataUtils.ERROR_FILE_CORRUPT,
                "File corrupt reading chunk at position {0}", p, exception);
    }

    private Iterable<Chunk> getChunksFromLayoutMap() {
        return getChunksFromLayoutMap(layout);
    }

    private Iterable<Chunk> getChunksFromLayoutMap(MVMap<String, String> layoutMap) {
        return () -> new Iterator<Chunk>() {
            private final Cursor<String, String> cursor = layoutMap.cursor(DataUtils.META_CHUNK);
            private Chunk nextChunk;

            @Override
            public boolean hasNext() {
                if(nextChunk == null && cursor.hasNext()) {
                    if (cursor.next().startsWith(DataUtils.META_CHUNK)) {
                        nextChunk = Chunk.fromString(cursor.getValue(), FileStore.this);
                        // might be there already, due to layout traversal
                        // see readPage() ... getChunkIfFound(),
                        // then take existing one instead
                        Chunk existingChunk = chunks.putIfAbsent(nextChunk.id, nextChunk);
                        if (existingChunk != null) {
                            nextChunk = existingChunk;
                        }
                    }
                }
                return nextChunk != null;
            }

            @Override
            public Chunk next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Chunk chunk = nextChunk;
                nextChunk = null;
                return chunk;
            }
        };
    }


    /**
     * Read a chunk header and footer, and verify the stored data is consistent.
     *
     * @param chunk to verify existence
     * @return true if Chunk exists in the file and is valid, false otherwise
     */
    private boolean isValidChunk(Chunk chunk) {
        return readChunkHeaderAndFooter(chunk.block, chunk.id) != null;
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

    private Chunk readChunkHeaderOptionally(long block, int expectedId) {
        Chunk chunk = readChunkHeaderOptionally(block);
        return chunk == null || chunk.id != expectedId ? null : chunk;
    }

    private Chunk readChunkHeaderOptionally(long block) {
        try {
            Chunk chunk = readChunkHeader(block);
            return chunk.block != block ? null : chunk;
        } catch (Exception ignore) {
            return null;
        }
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
            long pos = block * FileStore.BLOCK_SIZE - Chunk.FOOTER_LENGTH;
            if(pos < 0) {
                return null;
            }
            ByteBuffer lastBlock = readFully((Chunk)null, pos, Chunk.FOOTER_LENGTH);
            byte[] buff = new byte[Chunk.FOOTER_LENGTH];
            lastBlock.get(buff);
            HashMap<String, String> m = DataUtils.parseChecksummedMap(buff);
            if (m != null) {
                Chunk chunk = createChunk(m, false);
                if (chunk.block == 0) {
                    chunk.block = block - chunk.len;
                }
                return chunk;
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    /**
     * Get a buffer for writing. This caller must synchronize on the store
     * before calling the method and until after using the buffer.
     *
     * @return the buffer
     */
    public WriteBuffer getWriteBuffer() {
        WriteBuffer buff = writeBufferPool.poll();
        if (buff != null) {
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
    public void releaseWriteBuffer(WriteBuffer buff) {
        if (buff.capacity() <= 4 * 1024 * 1024) {
            writeBufferPool.offer(buff);
        }
    }

    public long getCreationTime() {
        return creationTime;
    }

    protected final int getAutoCompactFillRate() {
        return autoCompactFillRate;
    }


    public void sync() {}

    public abstract int getFillRate();

    /**
     * Shrink store if possible, and if at least a given percentage can be
     * saved.
     *
     * @param minPercent the minimum percentage to save
     */
    protected abstract void shrinkStoreIfPossible(int minPercent);


    /**
     * Get the file size.
     *
     * @return the file size
     */
    public long size() {
        return size;
    }

    protected final void setSize(long size) {
        this.size = size;
    }

    /**
     * Get the number of write operations since this store was opened.
     * For file based stores, this is the number of file write operations.
     *
     * @return the number of write operations
     */
    public long getWriteCount() {
        return writeCount.get();
    }

    /**
     * Get the number of written bytes since this store was opened.
     *
     * @return the number of write operations
     */
    public long getWriteBytes() {
        return writeBytes.get();
    }

    /**
     * Get the number of read operations since this store was opened.
     * For file based stores, this is the number of file read operations.
     *
     * @return the number of read operations
     */
    public long getReadCount() {
        return readCount.get();
    }

    /**
     * Get the number of read bytes since this store was opened.
     *
     * @return the number of write operations
     */
    public long getReadBytes() {
        return readBytes.get();
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Get the default retention time for this store in milliseconds.
     *
     * @return the retention time
     */
    public int getDefaultRetentionTime() {
        return 45_000;
    }

    public void clear() {
        saveChunkLock.lock();
        try {
            deadChunks.clear();
            lastChunk = null;
            readCount.set(0);
            readBytes.set(0);
            writeCount.set(0);
            writeBytes.set(0);
        } finally {
            saveChunkLock.unlock();
        }
    }

    /**
     * Get the file name.
     *
     * @return the file name
     */
    public String getFileName() {
        return fileName;
    }

    protected final MVStore getMvStore() {
        return mvStore;
    }

    /**
     * Mark the space as in use.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    protected abstract void markUsed(long pos, int length);

    public abstract void backup(ZipOutputStream out) throws IOException;

    protected final ConcurrentMap<Integer, Chunk> getChunks() {
        return chunks;
    }

    protected Collection<Chunk> getRewriteCandidates() {
        return null;
    }

    public boolean isSpaceReused() {
        return true;
    }

    public void setReuseSpace(boolean reuseSpace) {
    }

    protected final void store() {
        serializationLock.unlock();
        try {
            mvStore.storeNow();
        } finally {
            serializationLock.lock();
        }
    }

    private int serializationExecutorHWM;


    final void storeIt(ArrayList<Page<?,?>> changed, long version, boolean syncWrite) throws ExecutionException {
        lastCommitTime = getTimeSinceCreation();
        serializationExecutorHWM = submitOrRun(serializationExecutor,
                () -> serializeAndStore(syncWrite, changed, lastCommitTime, version),
                syncWrite, PIPE_LENGTH, serializationExecutorHWM);
    }

    private static int submitOrRun(ThreadPoolExecutor executor, Runnable action,
                                    boolean syncRun, int threshold, int hwm) throws ExecutionException {
        if (executor != null) {
            try {
                Future<?> future = executor.submit(action);
                int size = executor.getQueue().size();
                if (size > hwm) {
                    hwm = size;
//                    System.err.println(executor + " HWM: " + hwm);
                }
                if (syncRun || size > threshold) {
                    try {
                        future.get();
                    } catch (InterruptedException ignore) {/**/}
                }
                return hwm;
            } catch (RejectedExecutionException ex) {
                assert executor.isShutdown();
                Utils.shutdownExecutor(executor);
            }
        }
        action.run();
        return hwm;
    }


    private int bufferSaveExecutorHWM;

    private void serializeAndStore(boolean syncRun, ArrayList<Page<?,?>> changed, long time, long version) {
        serializationLock.lock();
        try {
            Chunk lastChunk = null;
            int chunkId = lastChunkId;
            if (chunkId != 0) {
                chunkId &= Chunk.MAX_ID;
                lastChunk = chunks.get(chunkId);
                assert lastChunk != null : lastChunkId + " ("+chunkId+") " + chunks;
                // never go backward in time
                time = Math.max(lastChunk.time, time);
            }
            Chunk c = createChunk(time, version);
            WriteBuffer buff = getWriteBuffer();
            serializeToBuffer(buff, changed, c, lastChunk);
            chunks.put(c.id, c);

            bufferSaveExecutorHWM = submitOrRun(bufferSaveExecutor, () -> storeBuffer(c, buff),
                    syncRun, 5, bufferSaveExecutorHWM);

            for (Page<?, ?> p : changed) {
                p.releaseSavedPages();
            }
        } catch (MVStoreException e) {
            mvStore.panic(e);
        } catch (Throwable e) {
            mvStore.panic(DataUtils.newMVStoreException(DataUtils.ERROR_INTERNAL, "{0}", e.toString(), e));
        } finally {
            serializationLock.unlock();
        }
    }

    private void serializeToBuffer(WriteBuffer buff, ArrayList<Page<?, ?>> changed, Chunk c, Chunk previousChunk) {
        // need to patch the header later
        c.writeChunkHeader(buff, 0);
        int headerLength = buff.position() + 66; // len:0[fffffff]map:0[fffffff],toc:0[fffffffffffffff],root:0[fffffffffffffff,next:ffffffffffffffff]
        buff.position(headerLength);
        c.next = headerLength;

        long version = c.version;
        PageSerializationManager pageSerializationManager = new PageSerializationManager(c, buff);
        for (Page<?,?> p : changed) {
            String key = MVMap.getMapRootKey(p.getMapId());
            if (p.getTotalCount() == 0) {
                layout.remove(key);
            } else {
                p.writeUnsavedRecursive(pageSerializationManager);
                long root = p.getPos();
                layout.put(key, Long.toHexString(root));
            }
        }

        acceptChunkOccupancyChanges(c.time, version);

        if (previousChunk != null) {
            // the metadata of the last chunk was not stored in the layout map yet,
            // just was embeded into the chunk itself, and this need to be done now
            // (it's better not to update right after storing, because that
            // would modify the meta map again)
            if (!layout.containsKey(Chunk.getMetaKey(previousChunk.id))) {
                saveChunkMetadataChanges(previousChunk);
            }
        }

        RootReference<String,String> layoutRootReference = layout.setWriteVersion(version);
        assert layoutRootReference != null;
        assert layoutRootReference.version == version : layoutRootReference.version + " != " + version;

        acceptChunkOccupancyChanges(c.time, version);

        mvStore.onVersionChange(version);

        Page<String,String> layoutRoot = layoutRootReference.root;
        layoutRoot.writeUnsavedRecursive(pageSerializationManager);
        c.layoutRootPos = layoutRoot.getPos();
        changed.add(layoutRoot);

        // last allocated map id should be captured after the meta map was saved, because
        // this will ensure that concurrently created map, which made it into meta before save,
        // will have it's id reflected in mapid header field of the currently written chunk
        c.mapId = mvStore.getLastMapId();

        c.tocPos = buff.position();
        pageSerializationManager.serializeToC();
        int chunkLength = buff.position();

        // add the store header and round to the next block
        int length = MathUtils.roundUpInt(chunkLength + Chunk.FOOTER_LENGTH, FileStore.BLOCK_SIZE);
        buff.limit(length);
        c.len = buff.limit() / FileStore.BLOCK_SIZE;
        c.buffer = buff.getBuffer();
    }

    private void storeBuffer(Chunk c, WriteBuffer buff) {
        saveChunkLock.lock();
        try {
            if (closed) {
                throw DataUtils.newMVStoreException(DataUtils.ERROR_WRITING_FAILED, "This fileStore is closed");
            }
            allocateChunkSpace(c, buff);
            buff.position(0);
            long filePos = c.block * BLOCK_SIZE;
            writeFully(c, filePos, buff.getBuffer());

            // end of the used space is not necessarily the end of the file
            boolean storeAtEndOfFile = filePos + buff.limit() >= size();
            boolean writeStoreHeader = isWriteStoreHeader(c, storeAtEndOfFile);
            lastChunk = c;
            if (writeStoreHeader) {
                writeStoreHeader();
            }
            if (!storeAtEndOfFile) {
                // may only shrink after the store header was written
                shrinkStoreIfPossible(1);
            }
        } catch (MVStoreException e) {
            mvStore.panic(e);
        } catch (Throwable e) {
            mvStore.panic(DataUtils.newMVStoreException(DataUtils.ERROR_INTERNAL, "{0}", e.toString(), e));
        } finally {
            saveChunkLock.unlock();
            c.buffer = null;
            releaseWriteBuffer(buff);
        }
    }

    /**
     * Apply the freed space to the chunk metadata. The metadata is updated, but
     * completely free chunks are not removed from the set of chunks, and the
     * disk space is not yet marked as free. They are queued instead and wait until
     * their usage is over.
     */
    private void acceptChunkOccupancyChanges(long time, long version) {
        assert serializationLock.isHeldByCurrentThread();
        if (hasPersitentData()) {
            Set<Chunk> modifiedChunks = new HashSet<>();
            while (true) {
                RemovedPageInfo rpi;
                while ((rpi = removedPages.peek()) != null && rpi.version < version) {
                    rpi = removedPages.poll();  // could be different from the peeked one
                    assert rpi != null;         // since nobody else retrieves from queue
                    assert rpi.version < version : rpi + " < " + version;
                    int chunkId = rpi.getPageChunkId();
                    Chunk chunk = chunks.get(chunkId);
                    assert !mvStore.isOpen() || chunk != null : chunkId;
                    if (chunk != null) {
                        modifiedChunks.add(chunk);
                        if (chunk.accountForRemovedPage(rpi.getPageNo(), rpi.getPageLength(),
                                rpi.isPinned(), time, rpi.version)) {
                            registerDeadChunk(chunk);
                        }
                    }
                }
                if (modifiedChunks.isEmpty()) {
                    return;
                }
                for (Chunk chunk : modifiedChunks) {
                    saveChunkMetadataChanges(chunk);
                }
                modifiedChunks.clear();
            }
        }
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
        return getChunksFillRate(true);
    }

    public int getRewritableChunksFillRate() {
        return getChunksFillRate(false);
    }

    private int getChunksFillRate(boolean all) {
        long maxLengthSum = 1;
        long maxLengthLiveSum = 1;
        long time = getTimeSinceCreation();
        for (Chunk c : chunks.values()) {
            if (all || isRewritable(c, time)) {
                assert c.maxLen >= 0;
                maxLengthSum += c.maxLen;
                maxLengthLiveSum += c.maxLenLive;
            }
        }
        // the fill rate of all chunks combined
        int fillRate = (int) (100 * maxLengthLiveSum / maxLengthSum);
        return fillRate;
    }

    /**
     * Get data chunks count.
     *
     * @return number of existing chunks in store.
     */
    public int getChunkCount() {
        return chunks.size();
    }

    /**
     * Get data pages count.
     *
     * @return number of existing pages in store.
     */
    public int getPageCount() {
        int count = 0;
        for (Chunk chunk : chunks.values()) {
            count += chunk.pageCount;
        }
        return count;
    }

    /**
     * Get live data pages count.
     *
     * @return number of existing live pages in store.
     */
    public int getLivePageCount() {
        int count = 0;
        for (Chunk chunk : chunks.values()) {
            count += chunk.pageCountLive;
        }
        return count;
    }

    /**
     * Put the page in the cache.
     * @param page the page
     */
    private void cachePage(Page<?,?> page) {
        if (cache != null) {
            cache.put(page.getPos(), page, page.getMemory());
        }
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

    private void cacheToC(Chunk chunk, long[] toc) {
        chunksToC.put(chunk.version, toc, toc.length * 8);
    }

    private long[] cleanToCCache(Chunk chunk) {
        return chunksToC.remove(chunk.version);
    }

    public int getCacheHitRatio() {
        return getCacheHitRatio(cache);
    }

    public int getTocCacheHitRatio() {
        return getCacheHitRatio(chunksToC);
    }

    private static int getCacheHitRatio(CacheLongKeyLIRS<?> cache) {
        if (cache == null) {
            return 0;
        }
        long hits = cache.getHits();
        return (int) (100 * hits / (hits + cache.getMisses() + 1));
    }

    public boolean isBackgroundThread() {
        return Thread.currentThread() == backgroundWriterThread.get();
    }

    void stopBackgroundThread(boolean waitForIt) {
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
                shutdownExecutors();
                break;
            }
        }
    }

    private void shutdownExecutors() {
        Utils.shutdownExecutor(serializationExecutor);
        serializationExecutor = null;
        Utils.shutdownExecutor(bufferSaveExecutor);
        bufferSaveExecutor = null;
    }

    private Iterable<Chunk> findOldChunks(int writeLimit, int targetFillRate) {
        assert hasPersitentData();
        long time = getTimeSinceCreation();

        // the queue will contain chunks we want to free up
        // the smaller the collectionPriority, the more desirable this chunk's re-write is
        // queue will be ordered in descending order of collectionPriority values,
        // so most desirable chunks will stay at the tail
        PriorityQueue<Chunk> queue = new PriorityQueue<>(this.chunks.size() / 4 + 1,
                (o1, o2) -> {
                    int comp = Integer.compare(o2.collectPriority, o1.collectPriority);
                    if (comp == 0) {
                        comp = Long.compare(o2.maxLenLive, o1.maxLenLive);
                    }
                    return comp;
                });

        long totalSize = 0;
        long latestVersion = lastChunkVersion() + 1;

        Collection<Chunk> candidates = getRewriteCandidates();
        if (candidates == null) {
            candidates = chunks.values();
        }
        for (Chunk chunk : candidates) {
            // only look at chunk older than the retention time
            // (it's possible to compact chunks earlier, but right
            // now we don't do that)
            int fillRate = chunk.getFillRate();
            if (isRewritable(chunk, time) && fillRate <= targetFillRate) {
                long age = Math.max(1, latestVersion - chunk.version);
                chunk.collectPriority = (int) (fillRate * 1000 / age);
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


    /**
     * Commit and save all changes, if there are any, and compact the store if
     * needed.
     */
    void writeInBackground() {
        try {
            if (!mvStore.isOpenOrStopping() || isReadOnly()) {
                return;
            }

            // could also commit when there are many unsaved pages,
            // but according to a test it doesn't really help
            long time = getTimeSinceCreation();
            if (time > lastCommitTime + autoCommitDelay) {
                mvStore.tryCommit();
            }
            doHousekeeping(mvStore);
            autoCompactLastFileOpCount = getWriteCount() + getReadCount();
        } catch (InterruptedException ignore) {
        } catch (Throwable e) {
            mvStore.handleException(e);
            if (mvStore.backgroundExceptionHandler == null) {
                throw e;
            }
        }
    }

    protected boolean rewriteChunks(int writeLimit, int targetFillRate) {
        serializationLock.lock();
        try {
            MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
            try {
                acceptChunkOccupancyChanges(getTimeSinceCreation(), mvStore.getCurrentVersion());
                Iterable<Chunk> old = findOldChunks(writeLimit, targetFillRate);
                if (old != null) {
                    HashSet<Integer> idSet = createIdSet(old);
                    return !idSet.isEmpty() && compactRewrite(idSet) > 0;
                }
            } finally {
                mvStore.deregisterVersionUsage(txCounter);
            }
            return false;
        } finally {
            serializationLock.unlock();
        }
    }

    private static HashSet<Integer> createIdSet(Iterable<Chunk> toCompact) {
        HashSet<Integer> set = new HashSet<>();
        for (Chunk c : toCompact) {
            set.add(c.id);
        }
        return set;
    }

    public void executeFilestoreOperation(Runnable operation) {
        // because serializationExecutor is a single-threaded one and
        // all task submissions to it are done under storeLock,
        // it is guaranteed, that upon this dummy task completion
        // there are no pending / in-progress task here
        Utils.flushExecutor(serializationExecutor);
        serializationLock.lock();
        try {
            // similarly, all task submissions to bufferSaveExecutor
            // are done under serializationLock, and upon this dummy task completion
            // it will be no pending / in-progress task here
            Utils.flushExecutor(bufferSaveExecutor);
            operation.run();
        } finally {
            serializationLock.unlock();
        }
    }

    private int compactRewrite(Set<Integer> set) {
        acceptChunkOccupancyChanges(getTimeSinceCreation(), mvStore.getCurrentVersion());
        int rewrittenPageCount = rewriteChunks(set, false);
        acceptChunkOccupancyChanges(getTimeSinceCreation(), mvStore.getCurrentVersion());
        rewrittenPageCount += rewriteChunks(set, true);
        return rewrittenPageCount;
    }

    private int rewriteChunks(Set<Integer> set, boolean secondPass) {
        int rewrittenPageCount = 0;
        for (int chunkId : set) {
            Chunk chunk = chunks.get(chunkId);
            long[] toc = getToC(chunk);
            if (toc != null) {
                for (int pageNo = 0; (pageNo = chunk.occupancy.nextClearBit(pageNo)) < chunk.pageCount; ++pageNo) {
                    long tocElement = toc[pageNo];
                    int mapId = DataUtils.getPageMapId(tocElement);
                    MVMap<String, String> metaMap = mvStore.getMetaMap();
                    MVMap<?, ?> map = mapId == layout.getId() ? layout : mapId == metaMap.getId() ? metaMap : mvStore.getMap(mapId);
                    if (map != null && !map.isClosed()) {
                        assert !map.isSingleWriter();
                        if (secondPass || DataUtils.isLeafPosition(tocElement)) {
                            long pagePos = DataUtils.composePagePos(chunkId, tocElement);
                            serializationLock.unlock();
                            try {
                                if (map.rewritePage(pagePos)) {
                                    ++rewrittenPageCount;
                                    if (map == metaMap) {
                                        mvStore.markMetaChanged();
                                    }
                                }
                            } finally {
                                serializationLock.lock();
                            }
                        }
                    }
                }
            }
        }
        return rewrittenPageCount;
    }


    /**
     * Read a page.
     *
     * @param map the map
     * @param pos the page position
     * @return the page
     */
    <K,V> Page<K,V> readPage(MVMap<K,V> map, long pos) {
        try {
            if (!DataUtils.isPageSaved(pos)) {
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_FILE_CORRUPT, "Position 0");
            }
            Page<K,V> page = readPageFromCache(pos);
            if (page == null) {
                Chunk chunk = getChunk(pos);
                int pageOffset = DataUtils.getPageOffset(pos);
                while(true) {
                    MVStoreException exception = null;
                    ByteBuffer buff = chunk.buffer;
                    boolean alreadySaved = buff == null;
                    if (alreadySaved) {
                        buff = chunk.readBufferForPage(this, pageOffset, pos);
                    } else {
//                        System.err.println("Using unsaved buffer " + chunk.id + "/" + pageOffset);
                        buff = buff.duplicate();
                        buff.position(pageOffset);
                        buff = buff.slice();
                    }
                    try {
                        page = Page.read(buff, pos, map);
                    } catch (MVStoreException e) {
                        exception = e;
                    } catch (Exception e) {
                        exception = DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT,
                                "Unable to read the page at position 0x{0}, chunk {1}, offset 0x{3}",
                                Long.toHexString(pos), chunk, Long.toHexString(pageOffset), e);
                    }
                    if (alreadySaved || buff == chunk.buffer) {
                        if (exception == null) {
                            break;
                        }
                        throw exception;
                    }
                }
                cachePage(page);
            }
            return page;
        } catch (MVStoreException e) {
            if (recoveryMode) {
                return map.createEmptyLeaf();
            }
            throw e;
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
            String s = layout.get(Chunk.getMetaKey(chunkId));
            if (s == null) {
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_CHUNK_NOT_FOUND,
                        "Chunk {0} not found", chunkId);
            }
            c = Chunk.fromString(s, this);
            if (!c.isSaved()) {
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Chunk {0} is invalid", chunkId);
            }
            chunks.put(c.id, c);
        }
        return c;
    }

    private int calculatePageNo(long pos) {
        int pageNo = -1;
        Chunk chunk = getChunk(pos);
        long[] toC = getToC(chunk);
        if (toC != null) {
            int offset = DataUtils.getPageOffset(pos);
            int low = 0;
            int high = toC.length - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midVal = DataUtils.getPageOffset(toC[mid]);
                if (midVal < offset) {
                    low = mid + 1;
                } else if (midVal > offset) {
                    high = mid - 1;
                } else {
                    pageNo = mid;
                    break;
                }
            }
        }
        return pageNo;
    }

    private void clearCaches() {
        if (cache != null) {
            cache.clear();
        }
        if (chunksToC != null) {
            chunksToC.clear();
        }
        removedPages.clear();
    }

    private long[] getToC(Chunk chunk) {
        if (chunk.tocPos == 0) {
            // legacy chunk without table of content
            return null;
        }
        long[] toc = chunksToC.get(chunk.id);
        if (toc == null) {
            toc = chunk.readToC(this);
            cacheToC(chunk, toc);
        }
        assert toc.length == chunk.pageCount : toc.length + " != " + chunk.pageCount;
        return toc;
    }

    @SuppressWarnings("unchecked")
    private <K, V> Page<K, V> readPageFromCache(long pos) {
        return cache == null ? null : (Page<K,V>)cache.get(pos);
    }

    /**
     * Remove a page.
     * @param pos the position of the page
     * @param version at which page was removed
     * @param pinned whether page is considered pinned
     * @param pageNo sequential page number within chunk
     */
    public void accountForRemovedPage(long pos, long version, boolean pinned, int pageNo) {
        assert DataUtils.isPageSaved(pos);
        if (pageNo < 0) {
            pageNo = calculatePageNo(pos);
        }
        RemovedPageInfo rpi = new RemovedPageInfo(pos, pinned, version, pageNo);
        removedPages.add(rpi);
    }



    public final class PageSerializationManager
    {
        private final Chunk chunk;
        private final WriteBuffer buff;
        private final List<Long> toc = new ArrayList<>();

        PageSerializationManager(Chunk chunk, WriteBuffer buff) {
            this.chunk = chunk;
            this.buff = buff;
        }

        public WriteBuffer getBuffer() {
            return buff;
        }

        private int getChunkId() {
            return chunk.id;
        }

        public int getPageNo() {
            return toc.size();
        }

        public long getPagePosition(int mapId, int offset, int pageLength, int type) {
            long tocElement = DataUtils.getTocElement(mapId, offset, pageLength, type);
            toc.add(tocElement);
            long pagePos = DataUtils.composePagePos(chunk.id, tocElement);
            int chunkId = getChunkId();
            int check = DataUtils.getCheckValue(chunkId)
                    ^ DataUtils.getCheckValue(offset)
                    ^ DataUtils.getCheckValue(pageLength);
            buff.putInt(offset, pageLength).
                putShort(offset + 4, (short) check);
            return pagePos;
        }

        public void onPageSerialized(Page<?,?> page, boolean isDeleted, int diskSpaceUsed, boolean isPinned) {
            cachePage(page);
            if (!page.isLeaf()) {
                // cache again - this will make sure nodes stays in the cache
                // for a longer time
                cachePage(page);
            }
            chunk.accountForWrittenPage(diskSpaceUsed, isPinned);
            if (isDeleted) {
                accountForRemovedPage(page.getPos(), chunk.version + 1, isPinned, page.pageNo);
            }
        }

        public void serializeToC() {
            long[] tocArray = new long[toc.size()];
            int index = 0;
            for (long tocElement : toc) {
                tocArray[index++] = tocElement;
                buff.putLong(tocElement);
                mvStore.countNewPage(DataUtils.isLeafPosition(tocElement));
            }
            cacheToC(chunk, tocArray);
        }
    }


    private static final class RemovedPageInfo implements Comparable<RemovedPageInfo> {
        final long version;
        final long removedPageInfo;

        RemovedPageInfo(long pagePos, boolean pinned, long version, int pageNo) {
            this.removedPageInfo = createRemovedPageInfo(pagePos, pinned, pageNo);
            this.version = version;
        }

        @Override
        public int compareTo(RemovedPageInfo other) {
            return Long.compare(version, other.version);
        }

        int getPageChunkId() {
            return DataUtils.getPageChunkId(removedPageInfo);
        }

        int getPageNo() {
            return DataUtils.getPageOffset(removedPageInfo);
        }

        int getPageLength() {
            return DataUtils.getPageMaxLength(removedPageInfo);
        }

        /**
         * Find out if removed page was pinned (can not be evacuated to a new chunk).
         * @return true if page has been pinned
         */
        boolean isPinned() {
            return (removedPageInfo & 1) == 1;
        }

        /**
         * Transforms saved page position into removed page info by
         * replacing "page offset" with "page sequential number" and
         * "page type" bit with "pinned page" flag.
         * @param pagePos of the saved page
         * @param isPinned whether page belong to a "single writer" map
         * @param pageNo 0-based sequential page number within containing chunk
         * @return removed page info that contains chunk id, page number, page length and pinned flag
         */
        private static long createRemovedPageInfo(long pagePos, boolean isPinned, int pageNo) {
            assert pageNo >= 0;
            assert pageNo >> 26 == 0;

            long result = (pagePos & ~((0xFFFFFFFFL << 6) | 1)) | ((long)pageNo << 6);
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
                    ", pageNo=" + getPageNo() +
                    ", len=" + getPageLength() +
                    (isPinned() ? ", pinned" : "") +
                    '}';
        }
    }

    /**
     * A background writer thread to automatically store changes from time to
     * time.
     */
    private static final class BackgroundWriterThread extends Thread {

        public final Object sync = new Object();
        private final FileStore store;
        private final int sleep;

        BackgroundWriterThread(FileStore store, int sleep, String fileStoreName) {
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
}
