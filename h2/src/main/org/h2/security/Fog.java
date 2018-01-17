/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

import org.h2.util.Bits;

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
        int x0 = Bits.readInt(in, off);
        int x1 = Bits.readInt(in, off + 4);
        int x2 = Bits.readInt(in, off + 8);
        int x3 = Bits.readInt(in, off + 12);
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
        Bits.writeInt(out, off, x0);
        Bits.writeInt(out, off + 4, x1);
        Bits.writeInt(out, off + 8, x2);
        Bits.writeInt(out, off + 12, x3);
    }

    private void decryptBlock(byte[] in, byte[] out, int off) {
        int x0 = Bits.readInt(in, off);
        int x1 = Bits.readInt(in, off + 4);
        int x2 = Bits.readInt(in, off + 8);
        int x3 = Bits.readInt(in, off + 12);
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
        Bits.writeInt(out, off, x0);
        Bits.writeInt(out, off + 4, x1);
        Bits.writeInt(out, off + 8, x2);
        Bits.writeInt(out, off + 12, x3);
    }

    @Override
    public int getKeyLength() {
        return 16;
    }

    @Override
    public void setKey(byte[] key) {
        this.key = (int) Bits.readLong(key, 0);
    }

}
