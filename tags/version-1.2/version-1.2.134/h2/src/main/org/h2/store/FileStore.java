/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.lang.ref.Reference;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.security.SecureFileStore;
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileSystem;
import org.h2.util.TempFileDeleter;
import org.h2.util.Utils;

/**
 * This class is an abstraction of a random access file.
 * Each file contains a magic header, and reading / writing is done in blocks.
 * See also {@link SecureFileStore}
 */
public class FileStore {

    /**
     * The size of the file header in bytes.
     */
    public static final int HEADER_LENGTH = 3 * Constants.FILE_BLOCK_SIZE;

    /**
     * An empty buffer to speed up extending the file (it seems that writing 0
     * bytes is faster then calling setLength).
     */
    protected static final byte[] EMPTY = new byte[16 * 1024];

    /**
     * The magic file header.
     */
    private static final String HEADER = "-- H2 0.5/B --      ".substring(0, Constants.FILE_BLOCK_SIZE - 1) + "\n";

    /**
     * The file name.
     */
    protected String name;

    /**
     * The callback object is responsible to check access rights, and free up
     * disk space if required.
     */
    protected DataHandler handler;

    private FileObject file;
    private long filePos;
    private long fileLength;
    private Reference< ? > autoDeleteReference;
    private boolean checkedWriting = true;
    private boolean synchronousMode;
    private String mode;
    private TempFileDeleter tempFileDeleter;
    private boolean textMode;

    /**
     * Create a new file using the given settings.
     *
     * @param handler the callback object
     * @param name the file name
     * @param mode the access mode ("r", "rw", "rws", "rwd")
     */
    protected FileStore(DataHandler handler, String name, String mode) {
        FileSystem fs = FileSystem.getInstance(name);
        this.handler = handler;
        this.name = name;
        this.mode = mode;
        if (handler != null) {
            tempFileDeleter = handler.getTempFileDeleter();
        }
        try {
            fs.createDirs(name);
            if (fs.exists(name) && !fs.canWrite(name)) {
                mode = "r";
                this.mode = mode;
            }
            file = fs.openFileObject(name, mode);
            if (mode.length() > 2) {
                synchronousMode = true;
            }
            fileLength = file.length();
        } catch (IOException e) {
            throw DbException.convertIOException(e, "name: " + name + " mode: " + mode);
        }
    }

    /**
     * Open a non encrypted file store with the given settings.
     *
     * @param handler the data handler
     * @param name the file name
     * @param mode the access mode (r, rw, rws, rwd)
     * @return the created object
     */
    public static FileStore open(DataHandler handler, String name, String mode) {
        return open(handler, name, mode, null, null, 0);
    }

    /**
     * Open an encrypted file store with the given settings.
     *
     * @param handler the data handler
     * @param name the file name
     * @param mode the access mode (r, rw, rws, rwd)
     * @param cipher the name of the cipher algorithm
     * @param key the encryption key
     * @return the created object
     */
    public static FileStore open(DataHandler handler, String name, String mode, String cipher, byte[] key) {
        return open(handler, name, mode, cipher, key, Constants.ENCRYPTION_KEY_HASH_ITERATIONS);
    }

    /**
     * Open an encrypted file store with the given settings.
     *
     * @param handler the data handler
     * @param name the file name
     * @param mode the access mode (r, rw, rws, rwd)
     * @param cipher the name of the cipher algorithm
     * @param key the encryption key
     * @param keyIterations the number of iterations the key should be hashed
     * @return the created object
     */
    public static FileStore open(DataHandler handler, String name, String mode, String cipher,
            byte[] key, int keyIterations) {
        FileStore store;
        if (cipher == null) {
            store = new FileStore(handler, name, mode);
        } else {
            store = new SecureFileStore(handler, name, mode, cipher, key, keyIterations);
        }
        return store;
    }

    /**
     * Generate the random salt bytes if required.
     *
     * @return the random salt or the magic
     */
    protected byte[] generateSalt() {
        return HEADER.getBytes();
    }

    /**
     * Initialize the key using the given salt.
     *
     * @param salt the salt
     */
    protected void initKey(byte[] salt) {
        // do nothing
    }

    public void setCheckedWriting(boolean value) {
        this.checkedWriting = value;
    }

    private void checkWritingAllowed() {
        if (handler != null && checkedWriting) {
            handler.checkWritingAllowed();
        }
    }

    private void checkPowerOff() {
        if (handler != null) {
            handler.checkPowerOff();
        }
    }

    /**
     * Initialize the file. This method will write or check the file header if
     * required.
     */
    public void init() {
        int len = Constants.FILE_BLOCK_SIZE;
        byte[] salt;
        byte[] magic = HEADER.getBytes();
        if (length() < HEADER_LENGTH) {
            // write unencrypted
            checkedWriting = false;
            writeDirect(magic, 0, len);
            salt = generateSalt();
            writeDirect(salt, 0, len);
            initKey(salt);
            // write (maybe) encrypted
            write(magic, 0, len);
            checkedWriting = true;
        } else {
            // read unencrypted
            seek(0);
            byte[] buff = new byte[len];
            readFullyDirect(buff, 0, len);
            if (Utils.compareNotNull(buff, magic) != 0) {
                throw DbException.get(ErrorCode.FILE_VERSION_ERROR_1, name);
            }
            salt = new byte[len];
            readFullyDirect(salt, 0, len);
            initKey(salt);
            // read (maybe) encrypted
            readFully(buff, 0, Constants.FILE_BLOCK_SIZE);
            if (textMode) {
                buff[10] = 'B';
            }
            if (Utils.compareNotNull(buff, magic) != 0) {
                throw DbException.get(ErrorCode.FILE_ENCRYPTION_ERROR_1, name);
            }
        }
    }

    /**
     * Close the file.
     */
    public void close() {
        if (file != null) {
            try {
                trace("close", name, file);
                file.close();
            } catch (IOException e) {
                throw DbException.convertIOException(e, name);
            } finally {
                file = null;
            }
        }
    }

    /**
     * Close the file without throwing any exceptions. Exceptions are simply
     * ignored.
     */
    public void closeSilently() {
        try {
            close();
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Close the file (ignoring exceptions) and delete the file.
     */
    public void closeAndDeleteSilently() {
        if (file != null) {
            closeSilently();
            tempFileDeleter.deleteFile(autoDeleteReference, name);
            name = null;
        }
    }

    /**
     * Read a number of bytes without decrypting.
     *
     * @param b the target buffer
     * @param off the offset
     * @param len the number of bytes to read
     */
    protected void readFullyDirect(byte[] b, int off, int len) {
        readFully(b, off, len);
    }

    /**
     * Read a number of bytes.
     *
     * @param b the target buffer
     * @param off the offset
     * @param len the number of bytes to read
     */
    public void readFully(byte[] b, int off, int len) {
        if (SysProperties.CHECK && (len < 0 || len % Constants.FILE_BLOCK_SIZE != 0)) {
            DbException.throwInternalError("unaligned read " + name + " len " + len);
        }
        checkPowerOff();
        try {
            file.readFully(b, off, len);
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
        filePos += len;
    }

    /**
     * Go to the specified file location.
     *
     * @param pos the location
     */
    public void seek(long pos) {
        if (SysProperties.CHECK && pos % Constants.FILE_BLOCK_SIZE != 0) {
            DbException.throwInternalError("unaligned seek " + name + " pos " + pos);
        }
        try {
            if (pos != filePos) {
                file.seek(pos);
                filePos = pos;
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    /**
     * Write a number of bytes without encrypting.
     *
     * @param b the source buffer
     * @param off the offset
     * @param len the number of bytes to write
     */
    protected void writeDirect(byte[] b, int off, int len) {
        write(b, off, len);
    }

    /**
     * Write a number of bytes.
     *
     * @param b the source buffer
     * @param off the offset
     * @param len the number of bytes to write
     */
    public void write(byte[] b, int off, int len) {
        if (SysProperties.CHECK && (len < 0 || len % Constants.FILE_BLOCK_SIZE != 0)) {
            DbException.throwInternalError("unaligned write " + name + " len " + len);
        }
        checkWritingAllowed();
        checkPowerOff();
        try {
            file.write(b, off, len);
        } catch (IOException e) {
            if (freeUpDiskSpace()) {
                try {
                    file.write(b, off, len);
                } catch (IOException e2) {
                    throw DbException.convertIOException(e2, name);
                }
            } else {
                throw DbException.convertIOException(e, name);
            }
        }
        filePos += len;
        fileLength = Math.max(filePos, fileLength);
    }

    private boolean freeUpDiskSpace() {
        if (handler == null) {
            return false;
        }
        handler.freeUpDiskSpace();
        return true;
    }

    private void extendByWriting(long newLength) throws IOException {
        long pos = filePos;
        file.seek(fileLength);
        byte[] empty = EMPTY;
        while (true) {
            int p = (int) Math.min(newLength - fileLength, EMPTY.length);
            if (p <= 0) {
                break;
            }
            file.write(empty, 0, p);
            fileLength += p;
        }
        file.seek(pos);
    }

    /**
     * Set the length of the file. This will expand or shrink the file.
     *
     * @param newLength the new file size
     */
    public void setLength(long newLength) {
        if (SysProperties.CHECK && newLength % Constants.FILE_BLOCK_SIZE != 0) {
            DbException.throwInternalError("unaligned setLength " + name + " pos " + newLength);
        }
        checkPowerOff();
        checkWritingAllowed();
        try {
            if (synchronousMode && newLength > fileLength) {
                extendByWriting(newLength);
            } else {
                file.setFileLength(newLength);
            }
            fileLength = newLength;
        } catch (IOException e) {
            if (newLength > fileLength && freeUpDiskSpace()) {
                try {
                    file.setFileLength(newLength);
                } catch (IOException e2) {
                    throw DbException.convertIOException(e2, name);
                }
            } else {
                throw DbException.convertIOException(e, name);
            }
        }
    }

    /**
     * Get the file size in bytes.
     *
     * @return the file size
     */
    public long length() {
        try {
            long len = fileLength;
            if (SysProperties.CHECK2) {
                len = file.length();
                if (len != fileLength) {
                    DbException.throwInternalError("file " + name + " length " + len + " expected " + fileLength);
                }
            }
            if (SysProperties.CHECK2 && len % Constants.FILE_BLOCK_SIZE != 0) {
                long newLength = len + Constants.FILE_BLOCK_SIZE - (len % Constants.FILE_BLOCK_SIZE);
                file.setFileLength(newLength);
                fileLength = newLength;
                DbException.throwInternalError("unaligned file length " + name + " len " + len);
            }
            return len;
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    /**
     * Get the current location of the file pointer.
     *
     * @return the location
     */
    public long getFilePointer() {
        if (SysProperties.CHECK2) {
            try {
                if (file.getFilePointer() != filePos) {
                    DbException.throwInternalError();
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, name);
            }
        }
        return filePos;
    }

    /**
     * Call fsync. Depending on the operating system and hardware, this may or
     * may not in fact write the changes.
     */
    public void sync() {
        try {
            file.sync();
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    /**
     * Automatically delete the file once it is no longer in use.
     */
    public void autoDelete() {
        if (autoDeleteReference == null) {
            autoDeleteReference = tempFileDeleter.addFile(name, this);
        }
    }

    /**
     * No longer automatically delete the file once it is no longer in use.
     */
    public void stopAutoDelete() {
        tempFileDeleter.stopAutoDelete(autoDeleteReference, name);
        autoDeleteReference = null;
    }

    /**
     * Close the file. The file may later be re-opened using openFile.
     */
    public void closeFile() throws IOException {
        file.close();
        file = null;
    }

    /**
     * Re-open the file. The file pointer will be reset to the previous
     * location.
     */
    public void openFile() throws IOException {
        if (file == null) {
            file = FileSystem.getInstance(name).openFileObject(name, mode);
            file.seek(filePos);
        }
    }

    private static void trace(String method, String fileName, Object o) {
        if (SysProperties.TRACE_IO) {
            System.out.println("FileStore." + method + " " + fileName + " " + o);
        }
    }

    /**
     * Check if the file store is in text mode.
     *
     * @return true if it is
     */
    public boolean isTextMode() {
        return textMode;
    }

}
