/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipOutputStream;

/**
 * Class FileStore.
 * <UL>
 * <LI> 4/5/20 2:03 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public abstract class FileStore {
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


    public FileStore() {
    }

    public void open(String fileName, boolean readOnly, char[] encryptionKey) {
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
    }

    public abstract void close();

    /**
     * Read data from the store.
     *
     * @param pos the read "position"
     * @param len the number of bytes to read
     * @return the byte buffer with data requested
     */
    public abstract ByteBuffer readFully(long pos, int len);

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
    public abstract void shrinkFileIfPossible(int minPercent);


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

    public abstract void clear();

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

    public abstract long getAfterLastBlock();

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

}
