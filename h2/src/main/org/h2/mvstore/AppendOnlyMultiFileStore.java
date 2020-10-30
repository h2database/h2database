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
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public class AppendOnlyMultiFileStore extends FileStore
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
    private FileChannel file;

    /**
     * All files currently used by this store. This includes current one at first position.
     * Previous files are opened in read-only mode.
     * Locical length of this array is determined by fileCount.
     */
    private final FileChannel[] files;

    /**
     * The file lock.
     */
    private FileLock fileLock;


    public AppendOnlyMultiFileStore(Map<String, Object> config) {
        super(config);
        maxFileCount = DataUtils.getConfigParam(config, "maxFileCount", 16);
        files = new FileChannel[maxFileCount];
    }

    @Override
    public boolean shouldSaveNow(int unsavedMemory, int autoCommitMemory) {
        return false;
    }

    /**
     * Read from the file.
     *
     * @param pos the write position
     * @param len the number of bytes to read
     * @return the byte buffer
     */
    public ByteBuffer readFully(long pos, int len) {

        return readFully(file, pos, len);
    }

    @Override
    protected void allocateChunkSpace(Chunk c, WriteBuffer buff) {
        saveChunkLock.lock();
        try {
            int headerLength = (int)c.next;

            buff.position(0);
            c.writeChunkHeader(buff, headerLength);

            buff.position(buff.limit() - Chunk.FOOTER_LENGTH);
            buff.put(c.getFooterBytes());

            c.block = size() / BLOCK_SIZE;
            setSize((c.block + c.len) * BLOCK_SIZE);
        } finally {
            saveChunkLock.unlock();
        }
    }

    @Override
    protected void compactStore(int thresholdFildRate, long maxCompactTime, int maxWriteSize, MVStore mvStore) {

    }

    @Override
    protected void doHousekeeping(MVStore mvStore) throws InterruptedException {

    }

    @Override
    protected void writeFully(long pos, ByteBuffer src) {

    }

    @Override
    public int getFillRate() {
        return 0;
    }

    @Override
    protected int getProjectedFillRate(int vacatedBlocks) {
        return 0;
    }

    @Override
    protected void shrinkStoreIfPossible(int minPercent) {}

    @Override
    public void markUsed(long pos, int length) {}

    @Override
    protected void freeChunkSpace(Iterable<Chunk> chunks) {}

    protected boolean validateFileLength(String msg) {
        return true;
    }

    @Override
    public void backup(ZipOutputStream out) throws IOException {

    }


}
