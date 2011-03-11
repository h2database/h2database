/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.fs;

import java.io.EOFException;
import java.io.IOException;
import org.h2.engine.Constants;
import org.h2.security.BlockCipher;
import org.h2.security.CipherFactory;
import org.h2.security.SHA256;
import org.h2.store.fs.FileObject;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * An encrypted file.
 */
public class FileObjectCrypt implements FileObject {

    static final int HEADER_LENGTH = 4096;

    // TODO header
    private static final byte[] HEADER = "-- H2 crypt --\n\0".getBytes();
    private static final int SALT_POS = HEADER.length;
    private static final int SALT_LENGTH = 16;
    private static final int HASH_ITERATIONS = Constants.ENCRYPTION_KEY_HASH_ITERATIONS;
    private static final int BLOCK_SIZE = Constants.FILE_BLOCK_SIZE;

    private final String name;
    private final FileObject file;
    private final BlockCipher cipher, cipherForInitVector;

    private byte[] bufferForInitVector;

    public FileObjectCrypt(String name, String algorithm, String password, FileObject file) throws IOException {
        this.name = name;
        this.file = file;
        boolean newFile = file.length() < 2 * HEADER_LENGTH;
        byte[] filePasswordHash;
        if (algorithm.endsWith("-hash")) {
            filePasswordHash = StringUtils.convertStringToBytes(password);
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
            file.write(HEADER, 0, HEADER.length);
            file.seek(SALT_POS);
            file.write(salt, 0, salt.length);
        } else {
            salt = new byte[SALT_LENGTH];
            file.seek(SALT_POS);
            file.readFully(salt, 0, SALT_LENGTH);
        }
        byte[] key = SHA256.getHashWithSalt(filePasswordHash, salt);
        for (int i = 0; i < keyIterations; i++) {
            key = SHA256.getHash(key, true);
        }
        cipher.setKey(key);
        bufferForInitVector = new byte[BLOCK_SIZE];
        seek(0);
    }

    public long getFilePointer() throws IOException {
        return Math.max(0, file.getFilePointer() - HEADER_LENGTH);
    }

    public String getName() {
        return name;
    }

    public long length() throws IOException {
        return Math.max(0, file.length() - 2 * HEADER_LENGTH);
    }

    public void releaseLock() {
        file.releaseLock();
    }

    public void seek(long pos) throws IOException {
        file.seek(pos + HEADER_LENGTH);
    }

    public void sync() throws IOException {
        file.sync();
    }

    public boolean tryLock() {
        return file.tryLock();
    }

    public void close() throws IOException {
        file.close();
    }

    public void setFileLength(long newLength) throws IOException {
        if (newLength < length()) {
            int mod = (int) (newLength % BLOCK_SIZE);
            if (mod == 0) {
                file.setFileLength(newLength + HEADER_LENGTH);
            } else {
                file.setFileLength(newLength + HEADER_LENGTH + BLOCK_SIZE - mod);
                byte[] buff = new byte[BLOCK_SIZE - mod];
                long pos = getFilePointer();
                seek(newLength);
                write(buff, 0, buff.length);
                seek(pos);
            }
        }
        file.setFileLength(newLength + 2 * HEADER_LENGTH);
        if (newLength < getFilePointer()) {
            seek(newLength);
        }
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        long pos = getFilePointer();
        long length = length();
        if (pos + len > length) {
            throw new EOFException("pos: " + pos + " len: " + len + " length: " + length);
        }
        int posMod = (int) (pos % BLOCK_SIZE);
        int lenMod = len % BLOCK_SIZE;
        if (posMod == 0 && lenMod == 0) {
            readAligned(pos, b, off, len);
        } else {
            long p = pos - posMod;
            int l = len + 2 * BLOCK_SIZE - lenMod;
            seek(p);
            byte[] temp = new byte[l];
            try {
                readAligned(p, temp, 0, l);
                System.arraycopy(temp, posMod, b, off, len);
            } finally {
                seek(pos + len);
            }
        }
    }

    public void write(byte[] b, int off, int len) throws IOException {
        long pos = getFilePointer();
        int posMod = (int) (pos % BLOCK_SIZE);
        int lenMod = len % BLOCK_SIZE;
        if (posMod == 0 && lenMod == 0) {
            byte[] temp = new byte[len];
            System.arraycopy(b, off, temp, 0, len);
            writeAligned(pos, temp, 0, len);
        } else {
            long p = pos - posMod;
            int l = len + 2 * BLOCK_SIZE - lenMod;
            seek(p);
            byte[] temp = new byte[l];
            if (file.length() < pos + l + HEADER_LENGTH) {
                file.setFileLength(pos + l + HEADER_LENGTH);
            }
            readAligned(p, temp, 0, l);
            System.arraycopy(b, off, temp, posMod, len);
            seek(p);
            try {
                writeAligned(p, temp, 0, l);
            } finally {
                seek(pos + len);
            }
        }
        pos = file.getFilePointer();
        if (file.length() < pos + HEADER_LENGTH) {
            file.setFileLength(pos + HEADER_LENGTH);
        }
    }

    private void readAligned(long pos, byte[] b, int off, int len) throws IOException {
        file.readFully(b, off, len);
        for (int p = 0; p < len; p += BLOCK_SIZE) {
            for (int i = 0; i < BLOCK_SIZE; i++) {
                // currently, empty blocks are not decrypted
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
                // currently, empty blocks are not decrypted
                if (b[p + off + i] != 0) {
                    xorInitVector(b, p + off, BLOCK_SIZE, p + pos);
                    cipher.encrypt(b, p + off, BLOCK_SIZE);
                    break;
                }
            }
        }
        file.write(b, off, len);
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

}
