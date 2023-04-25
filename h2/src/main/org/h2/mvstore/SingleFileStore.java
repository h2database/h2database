/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Map;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.h2.mvstore.cache.FilePathCache;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.encrypt.FileEncrypt;
import org.h2.store.fs.encrypt.FilePathEncrypt;
import org.h2.util.IOUtils;

/**
 * The default storage mechanism of the MVStore. This implementation persists
 * data to a file. The file store is responsible to persist data and for free
 * space management.
 */
public class SingleFileStore extends RandomAccessStore {

    /**
     * The file.
     */
    private FileChannel fileChannel;

    /**
     * The encrypted file (if encryption is used).
     */
    private FileChannel originalFileChannel;

    /**
     * The file lock.
     */
    private FileLock fileLock;

    private final Map<String, Object> config;


    public SingleFileStore(Map<String, Object> config) {
        super(config);
        this.config = config;
    }

    @Override
    public String toString() {
        return getFileName();
    }

    @Override
    public ByteBuffer readFully(SFChunk chunk, long pos, int len) {
        return readFully(fileChannel, pos, len);
    }

    @Override
    protected void writeFully(SFChunk chunk, long pos, ByteBuffer src) {
        int len = src.remaining();
        setSize(Math.max(super.size(), pos + len));
        DataUtils.writeFully(fileChannel, pos, src);
        writeCount.incrementAndGet();
        writeBytes.addAndGet(len);
    }

    /**
     * Try to open the file.
     *  @param fileName the file name
     * @param readOnly whether the file should only be opened in read-only mode,
     *            even if the file is writable
     * @param encryptionKey the encryption key, or null if encryption is not
     */
    @Override
    public void open(String fileName, boolean readOnly, char[] encryptionKey) {
        open(fileName, readOnly,
                encryptionKey == null ? null
                        : fileChannel -> new FileEncrypt(fileName, FilePathEncrypt.getPasswordBytes(encryptionKey),
                                fileChannel));
    }

    @Override
    public SingleFileStore open(String fileName, boolean readOnly) {
        SingleFileStore result = new SingleFileStore(config);
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
            fileLock = lockFileChannel(fileChannel, readOnly, fileName);
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

    private FileLock lockFileChannel(FileChannel fileChannel, boolean readOnly, String fileName) throws IOException {
        FileLock fileLock;
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
        return fileLock;
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

    /**
     * Flush all changes.
     */
    @Override
    public void sync() {
        if (fileChannel.isOpen()) {
            try {
                fileChannel.force(true);
            } catch (IOException e) {
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_WRITING_FAILED,
                        "Could not sync file {0}", getFileName(), e);
            }
        }
    }

    /**
     * Truncate the file.
     *
     * @param size the new file size
     */
    @Override
    @SuppressWarnings("ThreadPriorityCheck")
    public void truncate(long size) {
        int attemptCount = 0;
        while (true) {
            try {
                writeCount.incrementAndGet();
                fileChannel.truncate(size);
                setSize(Math.min(super.size(), size));
                return;
            } catch (IOException e) {
                if (++attemptCount == 10) {
                    throw DataUtils.newMVStoreException(
                            DataUtils.ERROR_WRITING_FAILED,
                            "Could not truncate file {0} to size {1}",
                            getFileName(), size, e);
                }
                System.gc();
                Thread.yield();
            }
        }
    }

    /**
     * Calculates relative "priority" for chunk to be moved.
     *
     * @param block where chunk starts
     * @return priority, bigger number indicate that chunk need to be moved sooner
     */
    @Override
    public int getMovePriority(int block) {
        return freeSpace.getMovePriority(block);
    }

    @Override
    protected long getAfterLastBlock_() {
        return freeSpace.getAfterLastBlock();
    }

    @Override
    public void backup(ZipOutputStream out) throws IOException {
        boolean before = isSpaceReused();
        setReuseSpace(false);
        try {
            backupFile(out, getFileName(), originalFileChannel != null ? originalFileChannel : fileChannel);
        } finally {
            setReuseSpace(before);
        }
    }

    private static void backupFile(ZipOutputStream out, String fileName, FileChannel in) throws IOException {
        String f = FilePath.get(fileName).toRealPath().getName();
        f = correctFileName(f);
        out.putNextEntry(new ZipEntry(f));
        IOUtils.copy(in, out);
        out.closeEntry();
    }

    /**
     * Fix the file name, replacing backslash with slash.
     *
     * @param f the file name
     * @return the corrected file name
     */
    public static String correctFileName(String f) {
        f = f.replace('\\', '/');
        if (f.startsWith("/")) {
            f = f.substring(1);
        }
        return f;
    }
}
