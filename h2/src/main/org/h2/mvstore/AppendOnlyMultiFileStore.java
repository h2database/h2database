/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import org.h2.mvstore.cache.FilePathCache;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.encrypt.FileEncrypt;
import org.h2.store.fs.encrypt.FilePathEncrypt;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.zip.ZipOutputStream;

/**
 * Class AppendOnlyMultiFileStore.
 *
 * @author <a href="mailto:andrei.tokar@gmail.com">Andrei Tokar</a>
 */
@SuppressWarnings("unused")
public final class AppendOnlyMultiFileStore extends FileStore<MFChunk>
{
    /**
     * Limit for the number of files used by this store
     */
    @SuppressWarnings("FieldCanBeLocal")
    private final int maxFileCount;

    /**
     * The time the store was created, in milliseconds since 1970.
     */
    private long creationTime;

    private int volumeId;

    /**
     * Current number of files in use
     */
    private int fileCount;

    /**
     * The current file. This is writable channel in append mode
     */
    private FileChannel fileChannel;

    /**
     * The encrypted file (if encryption is used).
     */
    private FileChannel originalFileChannel;

    /**
     * All files currently used by this store. This includes current one at first position.
     * Previous files are opened in read-only mode.
     * Logical length of this array is defined by fileCount.
     */
    @SuppressWarnings("MismatchedReadAndWriteOfArray")
    private final FileChannel[] fileChannels;

    /**
     * The file lock.
     */
    private FileLock fileLock;

    private final Map<String, Object> config;


    public AppendOnlyMultiFileStore(Map<String, Object> config) {
        super(config);
        this.config = config;
        maxFileCount = DataUtils.getConfigParam(config, "maxFileCount", 16);
        fileChannels = new FileChannel[maxFileCount];
    }

    @Override
    protected final MFChunk createChunk(int newChunkId) {
        return new MFChunk(newChunkId);
    }

    @Override
    public MFChunk createChunk(String s) {
        return new MFChunk(s);
    }

    @Override
    protected MFChunk createChunk(Map<String, String> map) {
        return new MFChunk(map);
    }

    @Override
    public boolean shouldSaveNow(int unsavedMemory, int autoCommitMemory) {
        return unsavedMemory > autoCommitMemory;
    }

    @Override
    public void open(String fileName, boolean readOnly, char[] encryptionKey) {
        open(fileName, readOnly,
                encryptionKey == null ? null
                        : fileChannel -> new FileEncrypt(fileName, FilePathEncrypt.getPasswordBytes(encryptionKey),
                                fileChannel));
    }

    @Override
    public AppendOnlyMultiFileStore open(String fileName, boolean readOnly) {
        AppendOnlyMultiFileStore result = new AppendOnlyMultiFileStore(config);
        result.open(fileName, readOnly, originalFileChannel == null ? null :
                fileChannel -> new FileEncrypt(fileName, (FileEncrypt)this.fileChannel, fileChannel));
        return result;
    }

    private void open(String fileName, boolean readOnly, Function<FileChannel,FileChannel> encryptionTransformer) {
        if (fileChannel != null && fileChannel.isOpen()) {
            return;
        }
        // ensure the Cache file system is registered
        FilePathCache.INSTANCE.getScheme();
        FilePath f = FilePath.get(fileName);
        FilePath parent = f.getParent();
        if (parent != null && !parent.exists()) {
            throw DataUtils.newIllegalArgumentException(
                    "Directory does not exist: {0}", parent);
        }
        if (f.exists() && !f.canWrite()) {
            readOnly = true;
        }
        init(fileName, readOnly);
        try {
            fileChannel = f.open(readOnly ? "r" : "rw");
            if (encryptionTransformer != null) {
                originalFileChannel = fileChannel;
                fileChannel = encryptionTransformer.apply(fileChannel);
            }
            try {
                fileLock = fileChannel.tryLock(0L, Long.MAX_VALUE, readOnly);
            } catch (OverlappingFileLockException e) {
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_FILE_LOCKED,
                        "The file is locked: {0}", fileName, e);
            }
            if (fileLock == null) {
                try { close(); } catch (Exception ignore) {}
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_FILE_LOCKED,
                        "The file is locked: {0}", fileName);
            }
            saveChunkLock.lock();
            try {
                setSize(fileChannel.size());
            } finally {
                saveChunkLock.unlock();
            }
        } catch (IOException e) {
            try { close(); } catch (Exception ignore) {}
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_READING_FAILED,
                    "Could not open file {0}", fileName, e);
        }
    }

    /**
     * Close this store.
     */
    @Override
    public void close() {
        try {
            if(fileChannel.isOpen()) {
                if (fileLock != null) {
                    fileLock.release();
                }
                fileChannel.close();
            }
        } catch (Exception e) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_WRITING_FAILED,
                    "Closing failed for file {0}", getFileName(), e);
        } finally {
            fileLock = null;
            super.close();
        }
    }

    @Override
    protected void writeFully(MFChunk chunk, long pos, ByteBuffer src) {
        assert chunk.volumeId == volumeId;
        int len = src.remaining();
        setSize(Math.max(super.size(), pos + len));
        DataUtils.writeFully(fileChannels[volumeId], pos, src);
        writeCount.incrementAndGet();
        writeBytes.addAndGet(len);
    }

    @Override
    public ByteBuffer readFully(MFChunk chunk, long pos, int len) {
        int volumeId = chunk.volumeId;
        return readFully(fileChannels[volumeId], pos, len);
    }

    @Override
    protected void initializeStoreHeader(long time) {
    }

    @Override
    protected void readStoreHeader(boolean recoveryMode) {
        ByteBuffer fileHeaderBlocks = readFully(new MFChunk(""), 0, FileStore.BLOCK_SIZE);
        byte[] buff = new byte[FileStore.BLOCK_SIZE];
        fileHeaderBlocks.get(buff);
        // the following can fail for various reasons
        try {
            HashMap<String, String> m = DataUtils.parseChecksummedMap(buff);
            if (m == null) {
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Store header is corrupt: {0}", this);
            }
            storeHeader.putAll(m);
        } catch (Exception ignore) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Store header is corrupt: {0}", this);
        }

        processCommonHeaderAttributes();

        long fileSize = size();
        long blocksInVolume = fileSize / FileStore.BLOCK_SIZE;

        MFChunk chunk = discoverChunk(blocksInVolume);
        setLastChunk(chunk);
        // load the chunk metadata: although meta's root page resides in the lastChunk,
        // traversing meta map might recursively load another chunk(s)
        for (MFChunk c : getChunksFromLayoutMap()) {
            // might be there already, due to meta traversal
            // see readPage() ... getChunkIfFound()
            if (!c.isLive()) {
                registerDeadChunk(c);
            }
        }
    }

    @Override
    protected void allocateChunkSpace(MFChunk chunk, WriteBuffer buff) {
        chunk.block = size() / BLOCK_SIZE;
        setSize((chunk.block + chunk.len) * BLOCK_SIZE);
    }

    @Override
    protected void writeChunk(MFChunk chunk, WriteBuffer buff) {
        long filePos = chunk.block * BLOCK_SIZE;
        writeFully(chunk, filePos, buff.getBuffer());
    }

    @Override
    protected void writeCleanShutdownMark() {

    }

    @Override
    protected void adjustStoreToLastChunk() {

    }

    @Override
    protected void compactStore(int thresholdFillRate, long maxCompactTime, int maxWriteSize, MVStore mvStore) {

    }

    @Override
    protected void doHousekeeping(MVStore mvStore) throws InterruptedException {}

    @Override
    public int getFillRate() {
        return 0;
    }

    @Override
    protected void shrinkStoreIfPossible(int minPercent) {}

    @Override
    public void markUsed(long pos, int length) {}

    @Override
    protected void freeChunkSpace(Iterable<MFChunk> chunks) {}

    @Override
    protected boolean validateFileLength(String msg) {
        return true;
    }

    @Override
    public void backup(ZipOutputStream out) throws IOException {

    }
}
