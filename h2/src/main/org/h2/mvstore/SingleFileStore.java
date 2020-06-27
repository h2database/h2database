/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.h2.mvstore.cache.FilePathCache;
import org.h2.store.fs.FileChannelInputStream;
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
    private FileChannel file;

    /**
     * The encrypted file (if encryption is used).
     */
    private FileChannel encryptedFile;

    /**
     * The file lock.
     */
    private FileLock fileLock;


    public SingleFileStore() {
        super();
    }

    @Override
    public String toString() {
        return getFileName();
    }

    /**
     * Read from the file.
     *
     * @param pos the write position
     * @param len the number of bytes to read
     * @return the byte buffer
     */
    public ByteBuffer readFully(long pos, int len) {
        ByteBuffer dst = ByteBuffer.allocate(len);
        DataUtils.readFully(file, pos, dst);
        readCount.incrementAndGet();
        readBytes.addAndGet(len);
        return dst;
    }

    /**
     * Write to the file.
     *
     * @param pos the write position
     * @param src the source buffer
     */
    public void writeFully(long pos, ByteBuffer src) {
        int len = src.remaining();
        setSize(Math.max(super.size(), pos + len));
        DataUtils.writeFully(file, pos, src);
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
    public void open(String fileName, boolean readOnly, char[] encryptionKey,
                     MVStore mvStore) {
        if (file != null && file.isOpen()) {
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
        super.open(fileName, readOnly, encryptionKey, mvStore);
        try {
            file = f.open(readOnly ? "r" : "rw");
            if (encryptionKey != null) {
                byte[] key = FilePathEncrypt.getPasswordBytes(encryptionKey);
                encryptedFile = file;
                file = new FileEncrypt(fileName, key, file);
            }
            try {
                if (readOnly) {
                    fileLock = file.tryLock(0, Long.MAX_VALUE, true);
                } else {
                    fileLock = file.tryLock();
                }
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
            setSize(file.size());
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
            if(file.isOpen()) {
                if (fileLock != null) {
                    fileLock.release();
                }
                file.close();
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
     * Get the file size.
     *
     * @return the file size
     */
    public long size() {
        long size = super.size();
        assert validateFileSize(size);
        return size;
    }

    /**
     * Flush all changes.
     */
    @Override
    public void sync() {
        if (file.isOpen()) {
            try {
                file.force(true);
            } catch (IOException e) {
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_WRITING_FAILED,
                        "Could not sync file {0}", getFileName(), e);
            }
        }
    }

    private boolean validateFileSize(long size) {
        try {
            if (file.isOpen()) {
                long fileSize = file.size();
                assert fileSize == size : fileSize + " != " + size;
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
        return true;
    }

    /**
     * Truncate the file.
     *
     * @param size the new file size
     */
    public void truncate(long size) {
        int attemptCount = 0;
        while (true) {
            try {
                writeCount.incrementAndGet();
                file.truncate(size);
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
     * Get the file instance in use.
     * <p>
     * The application may read from the file (for example for online backup),
     * but not write to it or truncate it.
     *
     * @return the file
     */
    public FileChannel getFile() {
        return file;
    }

    /**
     * Get the encrypted file instance, if encryption is used.
     * <p>
     * The application may read from the file (for example for online backup),
     * but not write to it or truncate it.
     *
     * @return the encrypted file, or null if encryption is not used
     */
    public FileChannel getEncryptedFile() {
        return encryptedFile;
    }

    /**
     * Calculates relative "priority" for chunk to be moved.
     *
     * @param block where chunk starts
     * @return priority, bigger number indicate that chunk need to be moved sooner
     */
    public int getMovePriority(int block) {
        return freeSpace.getMovePriority(block);
    }

    protected long getAfterLastBlock_() {
        return freeSpace.getAfterLastBlock();
    }

    public InputStream getInputStream() {
        FileChannel fc = getEncryptedFile();
        if (fc == null) {
            fc = getFile();
        }
        return new FileChannelInputStream(fc, false);
    }

    public void backup(ZipOutputStream out) throws IOException {
        InputStream in = getInputStream();
        backupFile(out, getFileName(), in);
    }

    private static void backupFile(ZipOutputStream out, String fileName, InputStream in) throws IOException {
        String f = FilePath.get(fileName).toRealPath().getName();
        f = correctFileName(f);
        out.putNextEntry(new ZipEntry(f));
        IOUtils.copyAndCloseInput(in, out);
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
