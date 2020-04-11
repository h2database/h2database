/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import static org.h2.mvstore.Chunk.MAX_HEADER_LENGTH;
import static org.h2.mvstore.MVMap.INITIAL_VERSION;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
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
     * The block size (physical sector size) of the disk. The store header is
     * written twice, one copy in each block, to ensure it survives a crash.
     */
    static final int BLOCK_SIZE = 4 * 1024;
    static final int FORMAT_WRITE = 1;
    static final int FORMAT_READ = 1;
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

    private final ReentrantLock saveChunkLock = new ReentrantLock(true);

    /**
     * The map of chunks.
     */
    private ConcurrentHashMap<Integer, Chunk> chunks = new ConcurrentHashMap<>();

    private final HashMap<String, Object> storeHeader = new HashMap<>();

    /**
     * The time the store was created, in milliseconds since 1970.
     */
    private long creationTime;

    private final Queue<WriteBuffer> writeBufferPool = new ArrayBlockingQueue<>(MVStore.PIPE_LENGTH + 1);




    public FileStore() {
    }

    public void open(String fileName, boolean readOnly, char[] encryptionKey, ConcurrentHashMap<Integer, Chunk> chunks) {
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
        setChunks(chunks);
    }

    public void setChunks(ConcurrentHashMap<Integer, Chunk> chunks) {
        this.chunks = chunks;
    }

    public abstract void close();

    public boolean hasPersitentData() {
        return lastChunk != null;
    }

    public long lastChunkVersion() {
        Chunk chunk = lastChunk;
        return chunk == null ? INITIAL_VERSION + 1 : chunk.version;
    }

    public void setLastChunk(Chunk last) {
        lastChunk = last;
    }

    /**
     * Read data from the store.
     *
     * @param pos the read "position"
     * @param len the number of bytes to read
     * @return the byte buffer with data requested
     */
    public abstract ByteBuffer readFully(long pos, int len);

    public void allocateChunkSpace(Chunk c, WriteBuffer buff, long reservedLow, Supplier<Long> reservedHighSupplier,
                            int headerLength) {
        saveChunkLock.lock();
        try {
            Long reservedHigh = reservedHighSupplier.get();
            long filePos = allocate(buff.limit(), reservedLow, reservedHigh);
            // calculate and set the likely next position
            if (reservedLow > 0 || reservedHigh == reservedLow) {
                c.next = predictAllocation(c.len, 0, 0);
            } else {
                // just after this chunk
                c.next = 0;
            }
            assert c.pageCountLive == c.pageCount : c;
            assert c.occupancy.cardinality() == 0 : c;

            buff.position(0);
            assert c.pageCountLive == c.pageCount : c;
            assert c.occupancy.cardinality() == 0 : c;
            c.writeChunkHeader(buff, headerLength);

            buff.position(buff.limit() - Chunk.FOOTER_LENGTH);
            buff.put(c.getFooterBytes());

            c.block = filePos / BLOCK_SIZE;
            assert validateFileLength(c.asString());
        } finally {
            saveChunkLock.unlock();
        }
    }

    public void storeBuffer(Chunk c, WriteBuffer buff) {
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
                shrinkFileIfPossible(1);
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
        creationTime = time;
        storeHeader.put(FileStore.HDR_H, 2);
        storeHeader.put(FileStore.HDR_BLOCK_SIZE, FileStore.BLOCK_SIZE);
        storeHeader.put(FileStore.HDR_FORMAT, FileStore.FORMAT_WRITE);
        storeHeader.put(FileStore.HDR_CREATED, creationTime);
    }

    public void writeStoreHeader() {
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
                shrinkFileIfPossible(0);
                storeHeader.put(HDR_CLEAN, 1);
                writeStoreHeader();
                sync();
                assert validateFileLength("on close");
            } finally {
                saveChunkLock.unlock();
            }
        }
    }

    public void freeChunkSpace(Iterable<Chunk> chunks) {
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

    public boolean validateFileLength(String msg) {
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
    public void shrinkIfPossible(int minPercent) {
        assert saveChunkLock.isHeldByCurrentThread();
        long result = getFileLengthInUse();
        assert result == measureFileLengthInUse() : result + " != " + measureFileLengthInUse();
        shrinkFileIfPossible(minPercent);
    }

    public boolean compactChunks(int targetFillRate, long moveSize, MVStore mvStore) {
        saveChunkLock.lock();
        try {
            if (hasPersitentData() && getFillRate() <= targetFillRate) {
                long start = getFirstFree() / FileStore.BLOCK_SIZE;
                Iterable<Chunk> chunksToMove = findChunksToMove(start, moveSize);
                if (chunksToMove != null) {
                    compactMoveChunks(chunksToMove, mvStore);
                    return true;
                }
            }
        } finally {
            saveChunkLock.unlock();
        }
        return false;
    }

    private Iterable<Chunk> findChunksToMove(long startBlock, long moveSize) {
        long maxBlocksToMove = moveSize / FileStore.BLOCK_SIZE;
        Iterable<Chunk> result = null;
        if (maxBlocksToMove > 0) {
            PriorityQueue<Chunk> queue = new PriorityQueue<>(getChunks().size() / 2 + 1,
                    (o1, o2) -> {
                        // instead of selection just closest to beginning of the file,
                        // pick smaller chunk(s) which sit in between bigger holes
                        int res = Integer.compare(o2.collectPriority, o1.collectPriority);
                        if (res != 0) {
                            return res;
                        }
                        return Long.signum(o2.block - o1.block);
                    });
            long size = 0;
            for (Chunk chunk : getChunks().values()) {
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
                list.sort(Chunk.PositionComparator.INSTANCE);
                result = list;
            }
        }
        return result;
    }

    private int getMovePriority(Chunk chunk) {
        return getMovePriority((int)chunk.block);
    }

    private void compactMoveChunks(Iterable<Chunk> move, MVStore mvStore) {
        assert saveChunkLock.isHeldByCurrentThread();
        if (move != null) {
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
                moveChunk(chunk, leftmostBlock, originalBlockCount, mvStore);
            }
            // update the metadata (hopefully within the file)
            store(leftmostBlock, originalBlockCount, mvStore);
            sync();

            Chunk chunkToMove = lastChunk;
            assert chunkToMove != null;
            long postEvacuationBlockCount = getAfterLastBlock();

            boolean chunkToMoveIsAlreadyInside = chunkToMove.block < leftmostBlock;
            boolean movedToEOF = !chunkToMoveIsAlreadyInside;
            // move all chunks, which previously did not fit before reserved area
            // now we can re-use previously reserved area [leftmostBlock, originalBlockCount),
            // but need to reserve [originalBlockCount, postEvacuationBlockCount)
            for (Chunk c : move) {
                if (c.block >= originalBlockCount &&
                        moveChunk(c, originalBlockCount, postEvacuationBlockCount, mvStore)) {
                    assert c.block < originalBlockCount;
                    movedToEOF = true;
                }
            }
            assert postEvacuationBlockCount >= getAfterLastBlock();

            if (movedToEOF) {
                boolean moved = moveChunkInside(chunkToMove, originalBlockCount, mvStore);

                // store a new chunk with updated metadata (hopefully within a file)
                store(originalBlockCount, postEvacuationBlockCount, mvStore);
                sync();
                // if chunkToMove did not fit within originalBlockCount (move is
                // false), and since now previously reserved area
                // [originalBlockCount, postEvacuationBlockCount) also can be
                // used, lets try to move that chunk into this area, closer to
                // the beginning of the file
                long lastBoundary = moved || chunkToMoveIsAlreadyInside ?
                                        postEvacuationBlockCount : chunkToMove.block;
                moved = !moved && moveChunkInside(chunkToMove, lastBoundary, mvStore);
                if (moveChunkInside(lastChunk, lastBoundary, mvStore) || moved) {
                    store(lastBoundary, -1, mvStore);
                }
            }

            shrinkFileIfPossible(0);
            sync();
        }
    }

    private void store(long reservedLow, long reservedHigh, MVStore mvStore) {
        saveChunkLock.unlock();
        try {
            mvStore.store(reservedLow, reservedHigh);
        } finally {
            saveChunkLock.lock();
        }
    }

    private boolean moveChunkInside(Chunk chunkToMove, long boundary, MVStore mvStore) {
        boolean res = chunkToMove.block >= boundary &&
                predictAllocation(chunkToMove.len, boundary, -1) < boundary &&
                moveChunk(chunkToMove, boundary, -1, mvStore);
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
    private boolean moveChunk(Chunk chunk, long reservedAreaLow, long reservedAreaHigh, MVStore mvStore) {
        // ignore if already removed during the previous store operations
        // those are possible either as explicit commit calls
        // or from meta map updates at the end of this method
        if (!getChunks().containsKey(chunk.id)) {
            return false;
        }
        long start = chunk.block * FileStore.BLOCK_SIZE;
        int length = chunk.len * FileStore.BLOCK_SIZE;
        long block;
        WriteBuffer buff = getWriteBuffer();
        try {
            buff.limit(length);
            ByteBuffer readBuff = readFully(start, length);
            Chunk chunkFromFile = Chunk.readChunkHeader(readBuff, start);
            int chunkHeaderLen = readBuff.position();
            buff.position(chunkHeaderLen);
            buff.put(readBuff);
            long pos = allocate(length, reservedAreaLow, reservedAreaHigh);
            block = pos / FileStore.BLOCK_SIZE;
            // in the absence of a reserved area,
            // block should always move closer to the beginning of the file
            assert reservedAreaHigh > 0 || block <= chunk.block : block + " " + chunk;
            buff.position(0);
            // also occupancy accounting fields should not leak into header
            chunkFromFile.block = block;
            chunkFromFile.next = 0;
            chunkFromFile.writeChunkHeader(buff, chunkHeaderLen);
            buff.position(length - Chunk.FOOTER_LENGTH);
            buff.put(chunkFromFile.getFooterBytes());
            buff.position(0);
            writeFully(pos, buff.getBuffer());
        } finally {
            releaseWriteBuffer(buff);
        }
        free(start, length);
        // can not set chunk's new block/len until it's fully written at new location,
        // because concurrent reader can pick it up prematurely,
        chunk.block = block;
        chunk.next = 0;
        mvStore.registerChunk(chunk);
        return true;
    }

    public void readStoreHeader(MVStore mvStore, boolean recoveryMode) {
        saveChunkLock.lock();
        try {
            _readStoreHeader(mvStore, recoveryMode);
        } finally {
            saveChunkLock.unlock();
        }
    }

    public void _readStoreHeader(MVStore mvStore, boolean recoveryMode) {
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
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Store header is corrupt: {0}", this);
        }
        int blockSize = DataUtils.readHexInt(storeHeader, FileStore.HDR_BLOCK_SIZE, FileStore.BLOCK_SIZE);
        if (blockSize != FileStore.BLOCK_SIZE) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "Block size {0} is currently not supported",
                    blockSize);
        }
        long format = DataUtils.readHexLong(storeHeader, FileStore.HDR_FORMAT, 1);
        if (format > FileStore.FORMAT_WRITE && !isReadOnly()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The write format {0} is larger " +
                    "than the supported format {1}, " +
                    "and the file was not opened in read-only mode",
                    format, FileStore.FORMAT_WRITE);
        }
        format = DataUtils.readHexLong(storeHeader, FileStore.HDR_FORMAT_READ, format);
        if (format > FileStore.FORMAT_READ) {
            throw DataUtils.newIllegalStateException(
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
            creationTime = now - getDefaultRetentionTime();
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
                mvStore.setLastChunk(newest);
                // load the chunk metadata: although meta's root page resides in the lastChunk,
                // traversing meta map might recursively load another chunk(s)
                for (Chunk c : mvStore.getChunksFromLayoutMap()) {
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
                    throw DataUtils.newIllegalStateException(
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
                mvStore.registerDeadChunk(c);
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
//                if (test.len > 0) {
//                    test.block = block - test.len;
//                }
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
                mvStore.setLastChunk(chunk);
                // load the chunk metadata: although meta's root page resides in the lastChunk,
                // traversing meta map might recursively load another chunk(s)
                for (Chunk c : mvStore.getChunksFromLayoutMap()) {
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

    public Chunk readChunkHeader(long block) {
        long p = block * FileStore.BLOCK_SIZE;
        ByteBuffer buff = readFully(p, MAX_HEADER_LENGTH);
        Chunk chunk = Chunk.readChunkHeader(buff, p);
        if (chunk.block == 0) {
            chunk.block = block;
        } else if (chunk.block != block) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupt reading chunk at position {0}", p);
        }
        return chunk;
    }

    /**
     * Read a chunk header and footer, and verify the stored data is consistent.
     *
     * @param block the block
     * @param expectedId of the chunk
     * @return the chunk, or null if the header or footer don't match or are not
     *         consistent
     */
    public Chunk readChunkHeaderAndFooter(long block, int expectedId) {
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
    public Chunk readChunkFooter(long block) {
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
    public abstract void writeFully(long pos, ByteBuffer src);

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
    protected abstract void shrinkFileIfPossible(int minPercent);


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

    /**
     * Mark the space as in use.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public abstract void markUsed(long pos, int length);

    /**
     * Allocate a number of blocks and mark them as used.
     *
     * @param length the number of bytes to allocate
     * @param reservedLow start block index of the reserved area (inclusive)
     * @param reservedHigh end block index of the reserved area (exclusive),
     *                     special value -1 means beginning of the infinite free area
     * @return the start position in bytes
     */
    abstract long allocate(int length, long reservedLow, long reservedHigh);

    /**
     * Calculate starting position of the prospective allocation.
     *
     * @param blocks the number of blocks to allocate
     * @param reservedLow start block index of the reserved area (inclusive)
     * @param reservedHigh end block index of the reserved area (exclusive),
     *                     special value -1 means beginning of the infinite free area
     * @return the starting block index
     */
    abstract long predictAllocation(int blocks, long reservedLow, long reservedHigh);

    /**
     * Mark the space as free.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public abstract void free(long pos, int length);

    abstract boolean isFragmented();

    public abstract void backup(ZipOutputStream out) throws IOException;

    public void rollback(Chunk keep, ArrayList<Chunk> remove, MVStore mvStore) {
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
            lastChunk = keep;
            writeStoreHeader();
            readStoreHeader(mvStore, false);
        } finally {
            saveChunkLock.unlock();
        }
    }

    public ConcurrentMap<Integer, Chunk> getChunks() {
        return chunks;
    }

//    public void registerDeadChunk(Chunk chunk) {
//        deadChunks.offer(chunk);
//    }
}
