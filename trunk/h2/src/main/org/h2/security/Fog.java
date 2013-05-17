/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

import org.h2.util.Utils;

/**
 * A pseudo-encryption algorithm that makes the data appear to be
 * encrypted. This algorithm is cryptographically extremely weak, and should
 * only be used to hide data from reading the plain text using a text editor.
 */
public class Fog implements BlockCipher {

    private int key;

    @Override
    public void encrypt(byte[] bytes, int off, int len) {
        for (int i = off; i < off + len; i += 16) {
            encryptBlock(bytes, bytes, i);
        }
    }

    @Override
    public void decrypt(byte[] bytes, int off, int len) {
        for (int i = off; i < off + len; i += 16) {
            decryptBlock(bytes, bytes, i);
        }
    }

    private void encryptBlock(byte[] in, byte[] out, int off) {
        int x0 = (in[off] << 24) | ((in[off+1] & 255) << 16) | ((in[off+2] & 255) << 8) | (in[off+3] & 255);
        int x1 = (in[off+4] << 24) | ((in[off+5] & 255) << 16) | ((in[off+6] & 255) << 8) | (in[off+7] & 255);
        int x2 = (in[off+8] << 24) | ((in[off+9] & 255) << 16) | ((in[off+10] & 255) << 8) | (in[off+11] & 255);
        int x3 = (in[off+12] << 24) | ((in[off+13] & 255) << 16) | ((in[off+14] & 255) << 8) | (in[off+15] & 255);
        int k = key;
        int s = x1 & 31;
        x0 ^= k;
        x0 = (x0 << s) | (x0 >>> (32 - s));
        x2 ^= k;
        x2 = (x2 << s) | (x2 >>> (32 - s));
        s = x0 & 31;
        x1 ^= k;
        x1 = (x1 << s) | (x1 >>> (32 - s));
        x3 ^= k;
        x3 = (x3 << s) | (x3 >>> (32 - s));
        out[off] = (byte) (x0 >> 24); out[off+1] = (byte) (x0 >> 16);
        out[off+2] = (byte) (x0 >> 8); out[off+3] = (byte) x0;
        out[off+4] = (byte) (x1 >> 24); out[off+5] = (byte) (x1 >> 16);
        out[off+6] = (byte) (x1 >> 8); out[off+7] = (byte) x1;
        out[off+8] = (byte) (x2 >> 24); out[off+9] = (byte) (x2 >> 16);
        out[off+10] = (byte) (x2 >> 8); out[off+11] = (byte) x2;
        out[off+12] = (byte) (x3 >> 24); out[off+13] = (byte) (x3 >> 16);
        out[off+14] = (byte) (x3 >> 8); out[off+15] = (byte) x3;
    }

    private void decryptBlock(byte[] in, byte[] out, int off) {
        int x0 = (in[off] << 24) | ((in[off+1] & 255) << 16) | ((in[off+2] & 255) << 8) | (in[off+3] & 255);
        int x1 = (in[off+4] << 24) | ((in[off+5] & 255) << 16) | ((in[off+6] & 255) << 8) | (in[off+7] & 255);
        int x2 = (in[off+8] << 24) | ((in[off+9] & 255) << 16) | ((in[off+10] & 255) << 8) | (in[off+11] & 255);
        int x3 = (in[off+12] << 24) | ((in[off+13] & 255) << 16) | ((in[off+14] & 255) << 8) | (in[off+15] & 255);
        int k = key;
        int s = 32 - (x0 & 31);
        x1 = (x1 << s) | (x1 >>> (32 - s));
        x1 ^= k;
        x3 = (x3 << s) | (x3 >>> (32 - s));
        x3 ^= k;
        s = 32 - (x1 & 31);
        x0 = (x0 << s) | (x0 >>> (32 - s));
        x0 ^= k;
        x2 = (x2 << s) | (x2 >>> (32 - s));
        x2 ^= k;
        out[off] = (byte) (x0 >> 24); out[off+1] = (byte) (x0 >> 16);
        out[off+2] = (byte) (x0 >> 8); out[off+3] = (byte) x0;
        out[off+4] = (byte) (x1 >> 24); out[off+5] = (byte) (x1 >> 16);
        out[off+6] = (byte) (x1 >> 8); out[off+7] = (byte) x1;
        out[off+8] = (byte) (x2 >> 24); out[off+9] = (byte) (x2 >> 16);
        out[off+10] = (byte) (x2 >> 8); out[off+11] = (byte) x2;
        out[off+12] = (byte) (x3 >> 24); out[off+13] = (byte) (x3 >> 16);
        out[off+14] = (byte) (x3 >> 8); out[off+15] = (byte) x3;
    }

    @Override
    public int getKeyLength() {
        return 16;
    }

    @Override
    public void setKey(byte[] key) {
        this.key = (int) Utils.readLong(key, 0);
    }

}
