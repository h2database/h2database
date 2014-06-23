/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.security.BlockCipher;
import org.h2.security.CipherFactory;
import org.h2.security.SHA256;
import org.h2.store.fs.FileBase;
import org.h2.store.fs.FileChannelInputStream;
import org.h2.store.fs.FileChannelOutputStream;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathWrapper;
import org.h2.store.fs.FileUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * A file system that encrypts the contents of the files.
 */
public class FilePathCrypt extends FilePathWrapper {

    /**
     * Register this file system.
     */
    public static void register() {
        FilePath.register(new FilePathCrypt());
    }

    protected String getPrefix() {
        String[] parsed = parse(name);
        return getScheme() + ":" + parsed[0] + ":" + parsed[1] + ":";
    }

    public FilePath unwrap(String fileName) {
        return FilePath.get(parse(fileName)[2]);
    }

    public long size() {
        long len = getBase().size();
        return Math.max(0, len - FileCrypt.HEADER_LENGTH - FileCrypt.BLOCK_SIZE);
    }

    public FileChannel open(String mode) throws IOException {
        String[] parsed = parse(name);
        FileChannel file = FileUtils.open(parsed[2], mode);
        return new FileCrypt(name, parsed[0], parsed[1], file);
    }

    public OutputStream newOutputStream(boolean append) {
        try {
            return new FileChannelOutputStream(open("rw"), append);
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    public InputStream newInputStream() {
        try {
            return new FileChannelInputStream(open("r"));
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    /**
     * Split the file name into algorithm, password, and base file name.
     *
     * @param fileName the file name
     * @return an array with algorithm, password, and base file name
     */
    private String[] parse(String fileName) {
        if (!fileName.startsWith(getScheme())) {
            DbException.throwInternalError(fileName + " doesn't start with " + getScheme());
        }
        fileName = fileName.substring(getScheme().length() + 1);
        int idx = fileName.indexOf(':');
        String algorithm, password;
        if (idx < 0) {
            DbException.throwInternalError(fileName + " doesn't contain encryption algorithm and password");
        }
        algorithm = fileName.substring(0, idx);
        fileName = fileName.substring(idx + 1);
        idx = fileName.indexOf(':');
        if (idx < 0) {
            DbException.throwInternalError(fileName + " doesn't contain encryption password");
        }
        password = fileName.substring(0, idx);
        fileName = fileName.substring(idx + 1);
        return new String[] { algorithm, password, fileName };
    }

    public String getScheme() {
        return "crypt";
    }

}

/**
 * An encrypted file.
 */
class FileCrypt extends FileBase {

    /**
     * The length of the file header. Using a smaller header is possible, but
     * might result in un-aligned reads and writes.
     */
    static final int HEADER_LENGTH = 4096;

    /**
     * The block size.
     */
    static final int BLOCK_SIZE = Constants.FILE_BLOCK_SIZE;

    // TODO improve the header
    // TODO store the number of empty blocks in the last block
    private static final byte[] HEADER = "-- H2 crypt --\n\0".getBytes();
    private static final int SALT_POS = HEADER.length;
    private static final int SALT_LENGTH = 16;
    private static final int HASH_ITERATIONS = Constants.ENCRYPTION_KEY_HASH_ITERATIONS;

    private final String name;
    private final FileChannel file;
    private final BlockCipher cipher, cipherForInitVector;

    private byte[] bufferForInitVector;

    public FileCrypt(String name, String algorithm, String password, FileChannel file) throws IOException {
        this.name = name;
        this.file = file;
        boolean newFile = file.size() < HEADER_LENGTH + BLOCK_SIZE;
        byte[] filePasswordHash;
        if (algorithm.endsWith("-hash")) {
            filePasswordHash = StringUtils.convertHexToBytes(password);
            algorithm = algorithm.substring(0, algorithm.length() - "-hash".length());
        } else {
            filePasswordHash = SHA256.getKeyPasswordHash("file", password.toCharArray());
        }
        cipher = CipherFactory.getBlockCipher(algorithm);
        cipherForInitVector = CipherFactory.getBlockCipher(algorithm);
        int keyIterations = HASH_ITERATIONS;
        byte[] salt;
        if (newFile) {
            salt = MathUtils.secureRandomBytes(SALT_LENGTH);
            FileUtils.writeFully(file, ByteBuffer.wrap(HEADER));
            file.position(SALT_POS);
            FileUtils.writeFully(file, ByteBuffer.wrap(salt));
        } else {
            salt = new byte[SALT_LENGTH];
            file.position(SALT_POS);
            FileUtils.readFully(file, ByteBuffer.wrap(salt));
        }
        byte[] key = SHA256.getHashWithSalt(filePasswordHash, salt);
        for (int i = 0; i < keyIterations; i++) {
            key = SHA256.getHash(key, true);
        }
        cipher.setKey(key);
        bufferForInitVector = new byte[BLOCK_SIZE];
        position(0);
    }

    public long position() throws IOException {
        return Math.max(0, file.position() - HEADER_LENGTH);
    }

    public long size() throws IOException {
        return Math.max(0, file.size() - HEADER_LENGTH - BLOCK_SIZE);
    }

    public FileChannel position(long pos) throws IOException {
        file.position(pos + HEADER_LENGTH);
        return this;
    }

    public void force(boolean metaData) throws IOException {
        file.force(metaData);
    }

    public synchronized FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return file.tryLock();
    }

    public void implCloseChannel() throws IOException {
        file.close();
    }

    public FileChannel truncate(long newLength) throws IOException {
        if (newLength >= size()) {
            return this;
        }
        int mod = (int) (newLength % BLOCK_SIZE);
        if (mod == 0) {
            file.truncate(HEADER_LENGTH + newLength);
        } else {
            file.truncate(HEADER_LENGTH + newLength + BLOCK_SIZE - mod);
            byte[] buff = new byte[BLOCK_SIZE - mod];
            long pos = position();
            position(newLength);
            write(buff, 0, buff.length);
            position(pos);
        }
        file.truncate(HEADER_LENGTH + newLength + BLOCK_SIZE);
        if (newLength < position()) {
            position(newLength);
        }
        return this;
    }

    public int read(ByteBuffer dst) throws IOException {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        long pos = position();
        len = (int) Math.min(len, size() - pos);
        if (len <= 0) {
            return -1;
        }
        int posMod = (int) (pos % BLOCK_SIZE);
        if (posMod == 0 && len % BLOCK_SIZE == 0) {
            readAligned(pos, dst.array(), dst.position(), len);
        } else {
            long p = pos - posMod;
            int l = len;
            if (posMod != 0) {
                l += posMod;
            }
            l = MathUtils.roundUpInt(l, BLOCK_SIZE);
            position(p);
            byte[] temp = new byte[l];
            try {
                readAligned(p, temp, 0, l);
                System.arraycopy(temp, posMod, dst.array(), dst.position(), len);
            } finally {
                position(pos + len);
            }
        }
        dst.position(dst.position() + len);
        return len;
    }

    public int write(ByteBuffer src) throws IOException {
        int len = src.remaining();
        if (len == 0) {
            return 0;
        }
        write(src.array(), src.position(), len);
        src.position(src.position() + len);
        return len;
    }

    private void write(byte[] b, int off, int len) throws IOException {
        long pos = position();
        int posMod = (int) (pos % BLOCK_SIZE);
        if (posMod == 0 && len % BLOCK_SIZE == 0) {
            byte[] temp = new byte[len];
            System.arraycopy(b, off, temp, 0, len);
            writeAligned(pos, temp, 0, len);
        } else {
            long p = pos - posMod;
            int l = len;
            if (posMod != 0) {
                l += posMod;
            }
            l = MathUtils.roundUpInt(l, BLOCK_SIZE);
            position(p);
            byte[] temp = new byte[l];
            if (file.size() < HEADER_LENGTH + p + l) {
                file.position(HEADER_LENGTH + p + l - 1);
                FileUtils.writeFully(file, ByteBuffer.wrap(new byte[1]));
                position(p);
            }
            readAligned(p, temp, 0, l);
            System.arraycopy(b, off, temp, posMod, len);
            position(p);
            try {
                writeAligned(p, temp, 0, l);
            } finally {
                position(pos + len);
            }
        }
        pos = file.position();
        if (file.size() < pos + BLOCK_SIZE) {
            file.position(pos + BLOCK_SIZE - 1);
            FileUtils.writeFully(file, ByteBuffer.wrap(new byte[1]));
            file.position(pos);
        }
    }

    private void readAligned(long pos, byte[] b, int off, int len) throws IOException {
        FileUtils.readFully(file, ByteBuffer.wrap(b, off, len));
        for (int p = 0; p < len; p += BLOCK_SIZE) {
            for (int i = 0; i < BLOCK_SIZE; i++) {
                // empty blocks are not decrypted
                if (b[p + off + i] != 0) {
                    cipher.decrypt(b, p + off, BLOCK_SIZE);
                    xorInitVector(b, p + off, BLOCK_SIZE, p + pos);
                    break;
                }
            }
        }
    }

    private void writeAligned(long pos, byte[] b, int off, int len) throws IOException {
        for (int p = 0; p < len; p += BLOCK_SIZE) {
            for (int i = 0; i < BLOCK_SIZE; i++) {
                // empty blocks are not decrypted
                if (b[p + off + i] != 0) {
                    xorInitVector(b, p + off, BLOCK_SIZE, p + pos);
                    cipher.encrypt(b, p + off, BLOCK_SIZE);
                    break;
                }
            }
        }
        FileUtils.writeFully(file, ByteBuffer.wrap(b, off, len));
    }

    private void xorInitVector(byte[] b, int off, int len, long p) {
        byte[] iv = bufferForInitVector;
        while (len > 0) {
            for (int i = 0; i < BLOCK_SIZE; i += 8) {
                long block = (p + i) >>> 3;
                iv[i] = (byte) (block >> 56);
                iv[i + 1] = (byte) (block >> 48);
                iv[i + 2] = (byte) (block >> 40);
                iv[i + 3] = (byte) (block >> 32);
                iv[i + 4] = (byte) (block >> 24);
                iv[i + 5] = (byte) (block >> 16);
                iv[i + 6] = (byte) (block >> 8);
                iv[i + 7] = (byte) block;
            }
            cipherForInitVector.encrypt(iv, 0, BLOCK_SIZE);
            for (int i = 0; i < BLOCK_SIZE; i++) {
                b[off + i] ^= iv[i];
            }
            p += BLOCK_SIZE;
            off += BLOCK_SIZE;
            len -= BLOCK_SIZE;
        }
    }

    public String toString() {
        return name;
    }

}
