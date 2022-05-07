/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Map;
import java.util.zip.ZipOutputStream;

/**
 * Class AppendOnlyMultiFileStore.
 *
 * @author <a href="mailto:andrei.tokar@gmail.com">Andrei Tokar</a>
 */
public class AppendOnlyMultiFileStore extends FileStore<MFChunk>
{
    /**
     * Limit for the number of files used by this store
     */
    private final int maxFileCount;

    /**
     * Current number of files in use
     */
    private int fileCount;

    /**
     * The current file. This is writable channel in append mode
     */
    private FileChannel fileChannel;

    /**
     * All files currently used by this store. This includes current one at first position.
     * Previous files are opened in read-only mode.
     * Locical length of this array is determined by fileCount.
     */
    private final FileChannel[] fileChannels;

    /**
     * The file lock.
     */
    private FileLock fileLock;


    public AppendOnlyMultiFileStore(Map<String, Object> config) {
        super(config);
        maxFileCount = DataUtils.getConfigParam(config, "maxFileCount", 16);
        fileChannels = new FileChannel[maxFileCount];
    }

    protected final MFChunk createChunk(int newChunkId) {
        return new MFChunk(newChunkId);
    }

    public MFChunk createChunk(String s) {
        return new MFChunk(s);
    }

    protected MFChunk createChunk(Map<String, String> map) {
        return new MFChunk(map);
    }


    @Override
    public boolean shouldSaveNow(int unsavedMemory, int autoCommitMemory) {
        return unsavedMemory > autoCommitMemory;
    }

    @Override
    protected void writeFully(MFChunk chunk, long pos, ByteBuffer src) {
        int volumeId = chunk.volumeId;
        int len = src.remaining();
        setSize(Math.max(super.size(), pos + len));
        DataUtils.writeFully(fileChannels[volumeId], pos, src);
        writeCount.incrementAndGet();
        writeBytes.addAndGet(len);
    }

    public ByteBuffer readFully(MFChunk chunk, long pos, int len) {
        int volumeId = chunk.volumeId;
        return readFully(fileChannels[volumeId], pos, len);
    }

    @Override
    protected void allocateChunkSpace(MFChunk chunk, WriteBuffer buff) {
        saveChunkLock.lock();
        try {
            int headerLength = (int) chunk.next;

            buff.position(0);
            chunk.writeChunkHeader(buff, headerLength);

            buff.position(buff.limit() - Chunk.FOOTER_LENGTH);
            buff.put(chunk.getFooterBytes());

            chunk.block = size() / BLOCK_SIZE;
            setSize((chunk.block + chunk.len) * BLOCK_SIZE);
        } finally {
            saveChunkLock.unlock();
        }
    }

    @Override
    protected void compactStore(int thresholdFildRate, long maxCompactTime, int maxWriteSize, MVStore mvStore) {

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

    protected boolean validateFileLength(String msg) {
        return true;
    }

    @Override
    public void backup(ZipOutputStream out) throws IOException {

    }
}
