/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import static org.h2.mvstore.MVMap.INITIAL_VERSION;
import org.h2.mvstore.cache.CacheLongKeyLIRS;
import org.h2.mvstore.type.StringDataType;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipOutputStream;

/**
 * Class FileStore.
 * <UL>
 * <LI> 4/5/20 2:03 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
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
    static final int FORMAT_WRITE = 1;
    static final int FORMAT_READ = 1;

    private MVStore mvStore;

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
     * The file size (cached).
     */
    private long size;

    /**
     * Whether this store is read-only.
     */
    private boolean readOnly;

    /**
     * The newest chunk. If nothing was stored yet, this field is not set.
     */
    volatile Chunk lastChunk;

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


    private final Queue<WriteBuffer> writeBufferPool = new ArrayBlockingQueue<>(MVStore.PIPE_LENGTH + 1);

    /**
     * The layout map. Contains chunks metadata and root locations for all maps.
     * This is relatively fast changing part of metadata
     */
    private MVMap<String, String> layout;

    private final Deque<Chunk> deadChunks = new ArrayDeque<>();



    public FileStore() {
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
        scrubLayoutMap();
    }

    public int getMetaMapId() {
        String metaIdStr = layout.get(META_ID_KEY);
        int metaId;
        if (metaIdStr == null) {
            metaId = mvStore.getNextMapId();
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
    public MVMap<String, String> getLayoutMap() {
        return layout;
    }

    public long getRootPos(int mapId) {
        String root = layout.get(MVMap.getMapRootKey(mapId));
        return root == null ? 0 : DataUtils.parseHexLong(root);
    }

    /**
     * Performs final stage of map removal - delete root location info from the layout table.
     * Map is supposedly closed and anonymous and has no outstanding usage by now.
     *
     * @param mapId to deregister
     */
    public boolean deregisterMapRoot(int mapId) {
        return layout.remove(MVMap.getMapRootKey(mapId)) != null;
    }

    /**
     * Check whether all data can be read from this version. This requires that
     * all chunks referenced by this version are still available (not
     * overwritten).
     *
     * @param version the version
     * @return true if all data can be read
     */
    public boolean isKnownVersion(long version) {
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

    public void setWriteVersion(long version) {
        layout.setWriteVersion(version);
    }

    public void rollbackTo(long version) {
        if (version == 0) {
            // special case: remove all data
            layout.setInitialRoot(layout.createEmptyLeaf(), INITIAL_VERSION);
        } else {
            layout.rollbackTo(version);
        }
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

    /**
     * Check whether there are any unsaved changes since specified version.
     *
     * @return if there are any changes
     */
    public boolean hasChangesSince(long lastStoredVersion) {
        return layout.hasChangesSince(lastStoredVersion) && lastStoredVersion > INITIAL_VERSION;
    }

    private void scrubLayoutMap() {
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

    public void bind(MVStore mvStore) {
        if(this.mvStore != mvStore) {
            layout = new MVMap<>(mvStore, 0, StringDataType.INSTANCE, StringDataType.INSTANCE);
            this.mvStore = mvStore;
            mvStore.resetLastMapId(lastChunk == null ? 0 : lastChunk.mapId);
            mvStore.setCurrentVersion(lastChunkVersion());
        }
    }

    public void close() {
        chunks.clear();
        mvStore = null;
    }

    public boolean hasPersitentData() {
        return lastChunk != null;
    }

    public long lastChunkVersion() {
        Chunk chunk = lastChunk;
        return chunk == null ? INITIAL_VERSION + 1 : chunk.version;
    }

    public void setLastChunk(Chunk last) {
        lastChunk = last;
        long curVersion = lastChunkVersion();
        chunks.clear();
        lastChunkId = 0;
        long layoutRootPos = 0;
        int mapId = 0;
        if (last != null) { // there is a valid chunk
            lastChunkId = last.id;
            curVersion = last.version;
            layoutRootPos = last.layoutRootPos;
            mapId = last.mapId;
            chunks.put(last.id, last);
        }
        mvStore.resetLastMapId(mapId);
        mvStore.setCurrentVersion(curVersion);
        layout.setRootPos(layoutRootPos, curVersion - 1);
    }

    public void registerDeadChunk(Chunk chunk) {
//        if (!chunk.isLive()) {
            deadChunks.offer(chunk);
//        }
    }

    public int dropUnusedChunks() {
        int count = 0;
        if (!deadChunks.isEmpty()) {
            long oldestVersionToKeep = mvStore.getOldestVersionToKeep();
            long time = mvStore.getTimeSinceCreation();
            List<Chunk> toBeFreed = new ArrayList<>();
            Chunk chunk;
            while ((chunk = deadChunks.poll()) != null &&
                    (isSeasonedChunk(chunk, time) && canOverwriteChunk(chunk, oldestVersionToKeep) ||
                            // if chunk is not ready yet, put it back and exit
                            // since this deque is unbounded, offerFirst() always return true
                            !deadChunks.offerFirst(chunk))) {

                if (chunks.remove(chunk.id) != null) {
                    // purge dead pages from cache
                    CacheLongKeyLIRS<long[]> toCCache = mvStore.getToCCache();
                    long[] toc = toCCache.remove(chunk.id);
                    CacheLongKeyLIRS<Page<?, ?>> cache = mvStore.getCache();
                    if (toc != null && cache != null) {
                        for (long tocElement : toc) {
                            long pagePos = DataUtils.getPagePos(chunk.id, tocElement);
                            cache.remove(pagePos);
                        }
                    }

                    if (getLayoutMap().remove(Chunk.getMetaKey(chunk.id)) != null) {
                        mvStore.markMetaChanged();
                    }
                    if (chunk.isSaved()) {
                        toBeFreed.add(chunk);
                    }
                    ++count;
                }
            }
            if (!toBeFreed.isEmpty()) {
                freeChunkSpace(toBeFreed);
            }
        }
        return count;
    }

    private static boolean canOverwriteChunk(Chunk c, long oldestVersionToKeep) {
        return !c.isLive() && c.unusedAtVersion < oldestVersionToKeep;
    }

    private boolean isSeasonedChunk(Chunk chunk, long time) {
        int retentionTime = mvStore.getRetentionTime();
        return retentionTime < 0 || chunk.time + retentionTime <= time;
    }

    public boolean isRewritable(Chunk chunk, long time) {
        return chunk.isRewritable() && isSeasonedChunk(chunk, time);
    }


    /**
     * Read data from the store.
     *
     * @param pos the read "position"
     * @param len the number of bytes to read
     * @return the byte buffer with data requested
     */
    public abstract ByteBuffer readFully(long pos, int len);

    protected abstract void allocateChunkSpace(Chunk c, WriteBuffer buff);

    public void storeBuffer(Chunk c, WriteBuffer buff) {
//        allocateChunkSpace(c, buff);
        saveChunkLock.lock();
        try {
            buff.position(0);
            long filePos = c.block * BLOCK_SIZE;
            writeFully(filePos, buff.getBuffer());

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
        } finally {
            saveChunkLock.unlock();
        }
    }

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

    public void initializeStoreHeader(long time) {
        setLastChunk(null);
        creationTime = time;
        storeHeader.put(FileStore.HDR_H, 2);
        storeHeader.put(FileStore.HDR_BLOCK_SIZE, FileStore.BLOCK_SIZE);
        storeHeader.put(FileStore.HDR_FORMAT, FileStore.FORMAT_WRITE);
        storeHeader.put(FileStore.HDR_CREATED, creationTime);
        writeStoreHeader();
    }

    public Chunk createChunk(long time, long version) {
        int chunkId = lastChunkId;
        if (chunkId != 0) {
            chunkId &= Chunk.MAX_ID;
            Chunk lastChunk = chunks.get(chunkId);
            assert lastChunk != null;
//            assert lastChunk.isSaved();
//            assert lastChunk.version + 1 == version : lastChunk.version + " " +  version;
            // the metadata of the last chunk was not stored so far, and needs to be
            // set now (it's better not to update right after storing, because that
            // would modify the meta map again)
            acceptChunkChanges(lastChunk);
            // never go backward in time
            time = Math.max(lastChunk.time, time);
        }
        int newChunkId;
        while (true) {
            newChunkId = ++lastChunkId & Chunk.MAX_ID;
            Chunk old = chunks.get(newChunkId);
            if (old == null) {
                break;
            }
            if (!old.isSaved()) {
                MVStoreException e = DataUtils.newMVStoreException(
                        DataUtils.ERROR_INTERNAL,
                        "Last block {0} not stored, possibly due to out-of-memory", old);
                mvStore.panic(e);
            }
        }
        Chunk c = new Chunk(newChunkId);
        c.time = time;
        c.version = version;
        c.occupancy = new BitSet();
        return c;
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
        writeFully(0, header);
    }

    // TODO: merge into close
    public void writeCleanShutdown() {
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

    public void acceptChunkChanges(Chunk chunk) {
        layout.put(Chunk.getMetaKey(chunk.id), chunk.asString());
    }

    private void freeChunkSpace(Iterable<Chunk> chunks) {
        saveChunkLock.lock();
        try {
            for (Chunk chunk : chunks) {
                freeChunkSpace(chunk);
            }
            assert validateFileLength(String.valueOf(chunks));
        } finally {
            saveChunkLock.unlock();
        }
    }

    private void freeChunkSpace(Chunk chunk) {
        long start = chunk.block * BLOCK_SIZE;
        int length = chunk.len * BLOCK_SIZE;
        free(start, length);
    }

    protected boolean validateFileLength(String msg) {
        assert saveChunkLock.isHeldByCurrentThread();
        assert getFileLengthInUse() == measureFileLengthInUse() :
                getFileLengthInUse() + " != " + measureFileLengthInUse() + " " + msg;
        return true;
    }

    private long measureFileLengthInUse() {
        assert saveChunkLock.isHeldByCurrentThread();
        long size = 2;
        for (Chunk c : getChunks().values()) {
            if (c.isSaved()) {
                size = Math.max(size, c.block + c.len);
            }
        }
        return size * BLOCK_SIZE;
    }

    /**
     * Shrink the store if possible, and if at least a given percentage can be
     * saved.
     *
     * @param minPercent the minimum percentage to save
     */
    protected void shrinkStoreIfPossible(int minPercent) {
        assert saveChunkLock.isHeldByCurrentThread();
        long result = getFileLengthInUse();
        assert result == measureFileLengthInUse() : result + " != " + measureFileLengthInUse();
        shrinkIfPossible(minPercent);
    }

    /**
     * Compact store file, that is, compact blocks that have a low
     * fill rate, and move chunks next to each other. This will typically
     * shrink the file. Changes are flushed to the file, and old
     * chunks are overwritten.
     *
     * @param tresholdFildRate do not compact if store fill rate above this value (0-100)
     * @param maxCompactTime the maximum time in milliseconds to compact
     * @param maxWriteSize the maximum amount of data to be written as part of this call
     */
//    public abstract void compactFile(int tresholdFildRate, long maxCompactTime, long maxWriteSize);

    public boolean compactChunks(int targetFillRate, long moveSize, MVStore mvStore) {
        saveChunkLock.lock();
        try {
            if (hasPersitentData() && getFillRate() <= targetFillRate) {
                return compactMoveChunks(moveSize, mvStore);
            }
        } finally {
            saveChunkLock.unlock();
        }
        return false;
    }

    protected abstract boolean compactMoveChunks(long moveSize, MVStore mvStore);


    public void readStoreHeader(boolean recoveryMode) {
        saveChunkLock.lock();
        try {
            _readStoreHeader(recoveryMode);
        } finally {
            saveChunkLock.unlock();
        }
    }

    private void _readStoreHeader(boolean recoveryMode) {
        Chunk newest = null;
        boolean assumeCleanShutdown = true;
        boolean validStoreHeader = false;
        // find out which chunk and version are the newest
        // read the first two blocks
        ByteBuffer fileHeaderBlocks = readFully(0, 2 * FileStore.BLOCK_SIZE);
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
        long format = DataUtils.readHexLong(storeHeader, FileStore.HDR_FORMAT, 1);
        if (format > FileStore.FORMAT_WRITE && !isReadOnly()) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The write format {0} is larger " +
                    "than the supported format {1}, " +
                    "and the file was not opened in read-only mode",
                    format, FileStore.FORMAT_WRITE);
        }
        format = DataUtils.readHexLong(storeHeader, FileStore.HDR_FORMAT_READ, format);
        if (format > FileStore.FORMAT_READ) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The read format {0} is larger " +
                    "than the supported format {1}",
                    format, FileStore.FORMAT_READ);
        }

        assumeCleanShutdown = assumeCleanShutdown && newest != null && !recoveryMode;
        if (assumeCleanShutdown) {
            assumeCleanShutdown = DataUtils.readHexInt(storeHeader, FileStore.HDR_CLEAN, 0) != 0;
        }
        getChunks().clear();
        long now = System.currentTimeMillis();
        // calculate the year (doesn't have to be exact;
        // we assume 365.25 days per year, * 4 = 1461)
        int year =  1970 + (int) (now / (1000L * 60 * 60 * 6 * 1461));
        if (year < 2014) {
            // if the year is before 2014,
            // we assume the system doesn't have a real-time clock,
            // and we set the creationTime to the past, so that
            // existing chunks are overwritten
            creationTime = now - mvStore.getRetentionTime();
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
                        validChunksById, false, mvStore);
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
                if (!findLastChunkWithCompleteValidChunkSet(lastChunkCandidates, validChunksByLocation,
                        validChunksById, true, mvStore) && hasPersitentData()) {
                    throw DataUtils.newMVStoreException(
                            DataUtils.ERROR_FILE_CORRUPT,
                            "File is corrupted - unable to recover a valid set of chunks");

                }
            }
        }

        clear();
        // build the free space list
        for (Chunk c : getChunks().values()) {
            if (c.isSaved()) {
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

    private boolean findLastChunkWithCompleteValidChunkSet(Chunk[] lastChunkCandidates,
            Map<Long, Chunk> validChunksByLocation,
            Map<Integer, Chunk> validChunksById,
            boolean afterFullScan, MVStore mvStore) {
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
        ByteBuffer buff = readFully(p, Chunk.MAX_HEADER_LENGTH);
        Chunk chunk = Chunk.readChunkHeader(buff, p);
        if (chunk.block == 0) {
            chunk.block = block;
        } else if (chunk.block != block) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupt reading chunk at position {0}", p);
        }
        return chunk;
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
                        nextChunk = Chunk.fromString(cursor.getValue());
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
    public boolean isValidChunk(Chunk chunk) {
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
            ByteBuffer lastBlock = readFully(pos, Chunk.FOOTER_LENGTH);
            byte[] buff = new byte[Chunk.FOOTER_LENGTH];
            lastBlock.get(buff);
            HashMap<String, String> m = DataUtils.parseChecksummedMap(buff);
            if (m != null) {
                Chunk chunk = new Chunk(m);
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


    /**
     * Write data to the store.
     *
     * @param pos the write "position"
     * @param src the source buffer
     */
    protected abstract void writeFully(long pos, ByteBuffer src);

    public void sync() {}

    public abstract int getFillRate();

    public abstract int getProjectedFillRate(int vacatedBlocks);

    abstract long getFirstFree();

    abstract long getFileLengthInUse();

    /**
     * Shrink store if possible, and if at least a given percentage can be
     * saved.
     *
     * @param minPercent the minimum percentage to save
     */
    protected abstract void shrinkIfPossible(int minPercent);


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

    /**
     * Calculates relative "priority" for chunk to be moved.
     *
     * @param block where chunk starts
     * @return priority, bigger number indicate that chunk need to be moved sooner
     */
    public abstract int getMovePriority(int block);

    public long getAfterLastBlock() {
        assert saveChunkLock.isHeldByCurrentThread();
        return getAfterLastBlock_();
    }

    protected abstract long getAfterLastBlock_();

    protected final MVStore getMvStore() {
        return mvStore;
    }

    /**
     * Mark the space as in use.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public abstract void markUsed(long pos, int length);

    /**
     * Mark the space as free.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public abstract void free(long pos, int length);

    abstract boolean isFragmented();

    public abstract void backup(ZipOutputStream out) throws IOException;

    public void rollback(Chunk keep, ArrayList<Chunk> remove) {
        // remove the youngest first, so we don't create gaps
        // (in case we remove many chunks)
        remove.sort(Comparator.<Chunk>comparingLong(o -> o.version).reversed());

        saveChunkLock.lock();
        try {
            freeChunkSpace(remove);
            for (Chunk c : remove) {
                if (c != null) {
                    long start = c.block * FileStore.BLOCK_SIZE;
                    int length = c.len * FileStore.BLOCK_SIZE;
//                    freeChunkSpace(c);
                    // overwrite the chunk,
                    // so it is not be used later on
                    WriteBuffer buff = getWriteBuffer();
                    try {
                        buff.limit(length);
                        // buff.clear() does not set the data
                        Arrays.fill(buff.getBuffer().array(), (byte) 0);
                        writeFully(start, buff.getBuffer());
                    } finally {
                        releaseWriteBuffer(buff);
                    }
                    // only really needed if we remove many chunks, when writes are
                    // re-ordered - but we do it always, because rollback is not
                    // performance critical
                    sync();
                }
            }
            deadChunks.clear();
            lastChunk = keep;
            writeStoreHeader();
            readStoreHeader(false);
        } finally {
            saveChunkLock.unlock();
        }
    }

    public ConcurrentMap<Integer, Chunk> getChunks() {
        return chunks;
    }

    public Collection<Chunk> getRewriteCandidates() {
        return null;
    }

    public boolean isSpaceReused() {
        return true;
    }

    public void setReuseSpace(boolean reuseSpace) {
    }

//    public void registerDeadChunk(Chunk chunk) {
//        deadChunks.offer(chunk);
//    }
}
