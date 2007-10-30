/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.lang.ref.Reference;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.security.SecureFileStore;
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileSystem;
import org.h2.util.ByteUtils;
import org.h2.util.TempFileDeleter;

/**
 * This class is an abstraction of a random access file.
 * Each file contains a magic header, and reading / writing is done in blocks.
 * See also {@link MemoryFileStore} and {@link SecureFileStore}
 */
public class FileStore {

    public static final int HEADER_LENGTH = 3 * Constants.FILE_BLOCK_SIZE;
    private static final byte[] EMPTY = new byte[16 * 1024];

    protected String name;
    protected DataHandler handler;
    private byte[] magic;
    private FileObject file;
    private long filePos;
    private long fileLength;
    private Reference autoDeleteReference;
    private boolean checkedWriting = true;
    private boolean synchronousMode;
    private String mode;

    public static FileStore open(DataHandler handler, String name, String mode, byte[] magic) throws SQLException {
        return open(handler, name, mode, magic, null, null, 0);
    }

    public static FileStore open(DataHandler handler, String name, String mode, byte[] magic, String cipher, byte[] key) throws SQLException {
        return open(handler, name, mode, magic, cipher, key, Constants.ENCRYPTION_KEY_HASH_ITERATIONS);
    }
    
    public static FileStore open(DataHandler handler, String name, String mode, byte[] magic, String cipher,
            byte[] key, int keyIterations) throws SQLException {
        FileStore store;
        if (cipher == null) {
            store = new FileStore(handler, name, mode, magic);
        } else {
            store = new SecureFileStore(handler, name, mode, magic, cipher, key, keyIterations);
        }
        return store;
    }

    protected FileStore(DataHandler handler, String name, String mode, byte[] magic) throws SQLException {
        FileSystem fs = FileSystem.getInstance(name);
        this.handler = handler;
        this.name = name;
        this.magic = magic;
        this.mode = mode;
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
            throw Message.convertIOException(e, "name: " + name + " mode: " + mode);
        }
    }

    protected FileStore(DataHandler handler, byte[] magic) {
        this.handler = handler;
        this.magic = magic;
    }

    protected byte[] generateSalt() {
        return magic;
    }

    protected void initKey(byte[] salt) {
        // do nothing
    }
    
    public void setCheckedWriting(boolean value) {
        this.checkedWriting = value;
    }

    protected void checkWritingAllowed() throws SQLException {
        if (handler != null && checkedWriting) {
            handler.checkWritingAllowed();
        }
    }

    protected void checkPowerOff() throws SQLException {
        if (handler != null) {
            handler.checkPowerOff();
        }
    }

    public void init() throws SQLException {
        int len = Constants.FILE_BLOCK_SIZE;
        byte[] salt;
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
            if (ByteUtils.compareNotNull(buff, magic) != 0) {
                throw Message.getSQLException(ErrorCode.FILE_VERSION_ERROR_1, name);
            }
            salt = new byte[len];
            readFullyDirect(salt, 0, len);
            initKey(salt);
            // read (maybe) encrypted
            readFully(buff, 0, Constants.FILE_BLOCK_SIZE);
            if (ByteUtils.compareNotNull(buff, magic) != 0) {
                throw Message.getSQLException(ErrorCode.FILE_ENCRYPTION_ERROR_1, name);
            }
        }
    }

    public void close() throws IOException {
        if (file != null) {
            try {
                trace("close", name, file);
                file.close();
            } finally {
                file = null;
            }
        }
    }

    public void closeSilently() {
        try {
            close();
        } catch (IOException e) {
            // ignore
        }
    }

    public void closeAndDeleteSilently() {
        if (file != null) {
            closeSilently();
            TempFileDeleter.deleteFile(autoDeleteReference, name);
            name = null;
        }
    }

    protected void readFullyDirect(byte[] b, int off, int len) throws SQLException {
        readFully(b, off, len);
    }

    public void readFully(byte[] b, int off, int len) throws SQLException {
        if (SysProperties.CHECK && len < 0) {
            throw Message.getInternalError("read len " + len);
        }
        if (SysProperties.CHECK && len % Constants.FILE_BLOCK_SIZE != 0) {
            throw Message.getInternalError("unaligned read " + name + " len " + len);
        }
        checkPowerOff();
        try {
            file.readFully(b, off, len);
        } catch (IOException e) {
            throw Message.convertIOException(e, name);
        }
        filePos += len;
    }

    public void seek(long pos) throws SQLException {
        if (SysProperties.CHECK && pos % Constants.FILE_BLOCK_SIZE != 0) {
            throw Message.getInternalError("unaligned seek " + name + " pos " + pos);
        }
        try {
            if (pos != filePos) {
                file.seek(pos);
                filePos = pos;
            }
        } catch (IOException e) {
            throw Message.convertIOException(e, name);
        }
    }

    protected void writeDirect(byte[] b, int off, int len) throws SQLException {
        write(b, off, len);
    }

    public void write(byte[] b, int off, int len) throws SQLException {
        if (SysProperties.CHECK && len < 0) {
            throw Message.getInternalError("read len " + len);
        }
        if (SysProperties.CHECK && len % Constants.FILE_BLOCK_SIZE != 0) {
            throw Message.getInternalError("unaligned write " + name + " len " + len);
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
                    throw Message.convertIOException(e2, name);
                }
            } else {
                throw Message.convertIOException(e, name);
            }
        }
        filePos += len;
        fileLength = Math.max(filePos, fileLength);
    }

    private boolean freeUpDiskSpace() throws SQLException {
        if (handler == null) {
            return false;
        }
        handler.freeUpDiskSpace();
        return true;
    }

    public void setLength(long newLength) throws SQLException {
        if (SysProperties.CHECK && newLength % Constants.FILE_BLOCK_SIZE != 0) {
            throw Message.getInternalError("unaligned setLength " + name + " pos " + newLength);
        }
        checkPowerOff();
        checkWritingAllowed();
        try {
            if (synchronousMode && newLength > fileLength) {
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
            } else {
                file.setLength(newLength);
            }
            fileLength = newLength;
        } catch (IOException e) {
            if (freeUpDiskSpace()) {
                try {
                    file.setLength(newLength);
                } catch (IOException e2) {
                    throw Message.convertIOException(e2, name);
                }
            } else {
                throw Message.convertIOException(e, name);
            }
        }
    }

    public long length() throws SQLException {
        try {
            long len = fileLength;
            if (SysProperties.CHECK2) {
                len = file.length();
                if (len != fileLength) {
                    throw Message.getInternalError("file " + name + " length " + len + " expected " + fileLength);
                }
            }
            if (SysProperties.CHECK2 && len % Constants.FILE_BLOCK_SIZE != 0) {
                long newLength = len + Constants.FILE_BLOCK_SIZE - (len % Constants.FILE_BLOCK_SIZE);
                file.setLength(newLength);
                fileLength = newLength;
                throw Message.getInternalError("unaligned file length " + name + " len " + len);
            }
            return len;
        } catch (IOException e) {
            throw Message.convertIOException(e, name);
        }
    }

    public long getFilePointer() throws SQLException {
        if (SysProperties.CHECK2) {
            try {
                if (file.getFilePointer() != filePos) {
                    throw Message.getInternalError();
                }
            } catch (IOException e) {
                throw Message.convertIOException(e, name);
            }
        }
        return filePos;
    }

    public void sync() {
        try {
            file.sync();
        } catch (IOException e) {
            // TODO log exception
        }
    }

    public void autoDelete() {
        autoDeleteReference = TempFileDeleter.addFile(name, this);
    }

    public void stopAutoDelete() {
        TempFileDeleter.stopAutoDelete(autoDeleteReference, name);
        autoDeleteReference = null;
    }

    public boolean isEncrypted() {
        return false;
    }

    public void closeFile() throws IOException {
        file.close();
        file = null;
    }

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

}
