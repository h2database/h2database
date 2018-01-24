/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.h2.util.Bits;

/**
 * This class implements the cryptographic hash function SHA-256.
 */
public class SHA256 {

    private final MessageDigest md;

    private final byte[] result = new byte[32];

    private SHA256() {
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Calculate the hash code by using the given salt. The salt is appended
     * after the data before the hash code is calculated. After generating the
     * hash code, the data and all internal buffers are filled with zeros to
     * avoid keeping insecure data in memory longer than required (and possibly
     * swapped to disk).
     *
     * @param data the data to hash
     * @param salt the salt to use
     * @return the hash code
     */
    public static byte[] getHashWithSalt(byte[] data, byte[] salt) {
        byte[] buff = new byte[data.length + salt.length];
        System.arraycopy(data, 0, buff, 0, data.length);
        System.arraycopy(salt, 0, buff, data.length, salt.length);
        return getHash(buff, true);
    }

    /**
     * Calculate the hash of a password by prepending the user name and a '@'
     * character. Both the user name and the password are encoded to a byte
     * array using UTF-16. After generating the hash code, the password array
     * and all internal buffers are filled with zeros to avoid keeping the plain
     * text password in memory longer than required (and possibly swapped to
     * disk).
     *
     * @param userName the user name
     * @param password the password
     * @return the hash code
     */
    public static byte[] getKeyPasswordHash(String userName, char[] password) {
        String user = userName + "@";
        byte[] buff = new byte[2 * (user.length() + password.length)];
        int n = 0;
        for (int i = 0, length = user.length(); i < length; i++) {
            char c = user.charAt(i);
            buff[n++] = (byte) (c >> 8);
            buff[n++] = (byte) c;
        }
        for (char c : password) {
            buff[n++] = (byte) (c >> 8);
            buff[n++] = (byte) c;
        }
        Arrays.fill(password, (char) 0);
        return getHash(buff, true);
    }

    /**
     * Calculate the hash-based message authentication code.
     *
     * @param key the key
     * @param message the message
     * @return the hash
     */
    public static byte[] getHMAC(byte[] key, byte[] message) {
        key = normalizeKeyForHMAC(key);
        int len = message.length;
        SHA256 sha = new SHA256();
        byte[] iKey = new byte[64 + len];
        byte[] oKey = new byte[64 + 32];
        sha.calculateHMAC(key, message, len, iKey, oKey);
        return sha.result;
    }

    private void calculateHMAC(byte[] key, byte[] message, int len,
            byte[] iKey, byte[] oKey) {
        Arrays.fill(iKey, 0, 64, (byte) 0x36);
        xor(iKey, key, 64);
        System.arraycopy(message, 0, iKey, 64, len);
        calculateHash(iKey, 64 + len);
        Arrays.fill(oKey, 0, 64, (byte) 0x5c);
        xor(oKey, key, 64);
        System.arraycopy(result, 0, oKey, 64, 32);
        calculateHash(oKey, 64 + 32);
    }

    private static byte[] normalizeKeyForHMAC(byte[] key) {
        if (key.length > 64) {
            key = getHash(key, false);
        }
        if (key.length < 64) {
            key = Arrays.copyOf(key, 64);
        }
        return key;
    }

    private static void xor(byte[] target, byte[] data, int len) {
        for (int i = 0; i < len; i++) {
            target[i] ^= data[i];
        }
    }

    /**
     * Calculate the hash using the password-based key derivation function 2.
     *
     * @param password the password
     * @param salt the salt
     * @param iterations the number of iterations
     * @param resultLen the number of bytes in the result
     * @return the result
     */
    public static byte[] getPBKDF2(byte[] password, byte[] salt,
            int iterations, int resultLen) {
        byte[] result = new byte[resultLen];
        byte[] key = normalizeKeyForHMAC(password);
        SHA256 sha = new SHA256();
        int len = 64 + Math.max(32, salt.length + 4);
        byte[] message = new byte[len];
        byte[] iKey = new byte[64 + len];
        byte[] oKey = new byte[64 + 32];
        for (int k = 1, offset = 0; offset < resultLen; k++, offset += 32) {
            for (int i = 0; i < iterations; i++) {
                if (i == 0) {
                    System.arraycopy(salt, 0, message, 0, salt.length);
                    Bits.writeInt(message, salt.length, k);
                    len = salt.length + 4;
                } else {
                    System.arraycopy(sha.result, 0, message, 0, 32);
                    len = 32;
                }
                sha.calculateHMAC(key, message, len, iKey, oKey);
                for (int j = 0; j < 32 && j + offset < resultLen; j++) {
                    result[j + offset] ^= sha.result[j];
                }
            }
        }
        Arrays.fill(password, (byte) 0);
        Arrays.fill(key, (byte) 0);
        return result;
    }

    /**
     * Calculate the hash code for the given data.
     *
     * @param data the data to hash
     * @param nullData if the data should be filled with zeros after calculating
     *            the hash code
     * @return the hash code
     */
    public static byte[] getHash(byte[] data, boolean nullData) {
        int len = data.length;
        SHA256 sha = new SHA256();
        sha.calculateHash(data, len);
        if (nullData) {
            Arrays.fill(data, (byte) 0);
        }
        return sha.result;
    }

    private void calculateHash(byte[] data, int len) {
        try {
            md.update(data, 0, len);
            md.digest(result, 0, 32);
        } catch (DigestException e) {
            throw new RuntimeException(e);
        }
    }

}
