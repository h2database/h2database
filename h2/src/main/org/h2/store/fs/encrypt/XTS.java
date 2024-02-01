/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.encrypt;

import org.h2.security.BlockCipher;

/**
 * An XTS implementation as described in
 * IEEE P1619 (Standard Architecture for Encrypted Shared Storage Media).
 * See also
 * http://axelkenzo.ru/downloads/1619-2007-NIST-Submission.pdf
 */
class XTS {

    /**
     * Galois field feedback.
     */
    private static final int GF_128_FEEDBACK = 0x87;

    /**
     * The AES encryption block size.
     */
    private static final int CIPHER_BLOCK_SIZE = 16;

    private final BlockCipher cipher;

    XTS(BlockCipher cipher) {
        this.cipher = cipher;
    }

    /**
     * Encrypt the data.
     *
     * @param id the (sector) id
     * @param len the number of bytes
     * @param data the data
     * @param offset the offset within the data
     */
    void encrypt(long id, int len, byte[] data, int offset) {
        byte[] tweak = initTweak(id);
        int i = 0;
        for (; i + CIPHER_BLOCK_SIZE <= len; i += CIPHER_BLOCK_SIZE) {
            if (i > 0) {
                updateTweak(tweak);
            }
            xorTweak(data, i + offset, tweak);
            cipher.encrypt(data, i + offset, CIPHER_BLOCK_SIZE);
            xorTweak(data, i + offset, tweak);
        }
        if (i < len) {
            updateTweak(tweak);
            swap(data, i + offset, i - CIPHER_BLOCK_SIZE + offset, len - i);
            xorTweak(data, i - CIPHER_BLOCK_SIZE + offset, tweak);
            cipher.encrypt(data, i - CIPHER_BLOCK_SIZE + offset, CIPHER_BLOCK_SIZE);
            xorTweak(data, i - CIPHER_BLOCK_SIZE + offset, tweak);
        }
    }

    /**
     * Decrypt the data.
     *
     * @param id the (sector) id
     * @param len the number of bytes
     * @param data the data
     * @param offset the offset within the data
     */
    void decrypt(long id, int len, byte[] data, int offset) {
        byte[] tweak = initTweak(id), tweakEnd = tweak;
        int i = 0;
        for (; i + CIPHER_BLOCK_SIZE <= len; i += CIPHER_BLOCK_SIZE) {
            if (i > 0) {
                updateTweak(tweak);
                if (i + CIPHER_BLOCK_SIZE + CIPHER_BLOCK_SIZE > len &&
                        i + CIPHER_BLOCK_SIZE < len) {
                    tweakEnd = tweak.clone();
                    updateTweak(tweak);
                }
            }
            xorTweak(data, i + offset, tweak);
            cipher.decrypt(data, i + offset, CIPHER_BLOCK_SIZE);
            xorTweak(data, i + offset, tweak);
        }
        if (i < len) {
            swap(data, i, i - CIPHER_BLOCK_SIZE + offset, len - i + offset);
            xorTweak(data, i - CIPHER_BLOCK_SIZE  + offset, tweakEnd);
            cipher.decrypt(data, i - CIPHER_BLOCK_SIZE + offset, CIPHER_BLOCK_SIZE);
            xorTweak(data, i - CIPHER_BLOCK_SIZE + offset, tweakEnd);
        }
    }

    private byte[] initTweak(long id) {
        byte[] tweak = new byte[CIPHER_BLOCK_SIZE];
        for (int j = 0; j < CIPHER_BLOCK_SIZE; j++, id >>>= 8) {
            tweak[j] = (byte) (id & 0xff);
        }
        cipher.encrypt(tweak, 0, CIPHER_BLOCK_SIZE);
        return tweak;
    }

    private static void xorTweak(byte[] data, int pos, byte[] tweak) {
        for (int i = 0; i < CIPHER_BLOCK_SIZE; i++) {
            data[pos + i] ^= tweak[i];
        }
    }

    private static void updateTweak(byte[] tweak) {
        byte ci = 0, co = 0;
        for (int i = 0; i < CIPHER_BLOCK_SIZE; i++) {
            co = (byte) ((tweak[i] >> 7) & 1);
            tweak[i] = (byte) (((tweak[i] << 1) + ci) & 255);
            ci = co;
        }
        if (co != 0) {
            tweak[0] ^= GF_128_FEEDBACK;
        }
    }

    private static void swap(byte[] data, int source, int target, int len) {
        for (int i = 0; i < len; i++) {
            byte temp = data[source + i];
            data[source + i] = data[target + i];
            data[target + i] = temp;
        }
    }

}