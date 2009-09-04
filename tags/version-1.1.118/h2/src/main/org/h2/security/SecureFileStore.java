/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.util.RandomUtils;

/**
 * A file store that encrypts all data before writing,
 * and decrypts all data after reading.
 */
public class SecureFileStore extends FileStore {

    private byte[] key;
    private BlockCipher cipher;
    private BlockCipher cipherForInitVector;
    private byte[] buffer = new byte[4];
    private long pos;
    private byte[] bufferForInitVector;
    private int keyIterations;

    public SecureFileStore(DataHandler handler, String name, String mode, String cipher, byte[] key, int keyIterations) throws SQLException {
        super(handler, name, mode);
        this.key = key;
        this.cipher = CipherFactory.getBlockCipher(cipher);
        this.cipherForInitVector = CipherFactory.getBlockCipher(cipher);
        this.keyIterations = keyIterations;
        bufferForInitVector = new byte[Constants.FILE_BLOCK_SIZE];
    }

    protected byte[] generateSalt() {
        return RandomUtils.getSecureBytes(Constants.FILE_BLOCK_SIZE);
    }

    protected void initKey(byte[] salt) {
        SHA256 sha = new SHA256();
        key = sha.getHashWithSalt(key, salt);
        for (int i = 0; i < keyIterations; i++) {
            key = sha.getHash(key, true);
        }
        cipher.setKey(key);
        key = sha.getHash(key, true);
        cipherForInitVector.setKey(key);
    }

    protected void writeDirect(byte[] b, int off, int len) throws SQLException {
        super.write(b, off, len);
        pos += len;
    }

    public void write(byte[] b, int off, int len) throws SQLException {
        if (buffer.length < b.length) {
            buffer = new byte[len];
        }
        System.arraycopy(b, off, buffer, 0, len);
        xorInitVector(buffer, 0, len, pos);
        cipher.encrypt(buffer, 0, len);
        super.write(buffer, 0, len);
        pos += len;
    }

    protected void readFullyDirect(byte[] b, int off, int len) throws SQLException {
        super.readFully(b, off, len);
        pos += len;
    }

    public void readFully(byte[] b, int off, int len) throws SQLException {
        super.readFully(b, off, len);
        cipher.decrypt(b, off, len);
        xorInitVector(b, off, len, pos);
        pos += len;
    }

    public void seek(long x) throws SQLException {
        this.pos = x;
        super.seek(x);
    }

    public void setLength(long newLength) throws SQLException {
        long oldPos = pos;
        long length = length();
        if (newLength > length) {
            seek(length);
            byte[] empty = EMPTY;
            while (true) {
                int p = (int) Math.min(newLength - length, EMPTY.length);
                if (p <= 0) {
                    break;
                }
                write(empty, 0, p);
                length += p;
            }
            seek(oldPos);
        } else {
            super.setLength(newLength);
        }
    }

    private void xorInitVector(byte[] b, int off, int len, long pos) {
        byte[] iv = bufferForInitVector;
        while (len > 0) {
            for (int i = 0; i < Constants.FILE_BLOCK_SIZE; i += 8) {
                long block = (pos + i) >>> 3;
                iv[i] = (byte) (block >> 56);
                iv[i + 1] = (byte) (block >> 48);
                iv[i + 2] = (byte) (block >> 40);
                iv[i + 3] = (byte) (block >> 32);
                iv[i + 4] = (byte) (block >> 24);
                iv[i + 5] = (byte) (block >> 16);
                iv[i + 6] = (byte) (block >> 8);
                iv[i + 7] = (byte) block;
            }
            cipherForInitVector.encrypt(iv, 0, Constants.FILE_BLOCK_SIZE);
            for (int i = 0; i < Constants.FILE_BLOCK_SIZE; i++) {
                b[off + i] ^= iv[i];
            }
            pos += Constants.FILE_BLOCK_SIZE;
            off += Constants.FILE_BLOCK_SIZE;
            len -= Constants.FILE_BLOCK_SIZE;
        }
    }

    public boolean isEncrypted() {
        return true;
    }

}
