/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

import org.h2.constant.ErrorCode;
import org.h2.message.DbException;

/**
 * A factory to create new block cipher objects.
 */
public class CipherFactory {

    private CipherFactory() {
        // utility class
    }

    /**
     * Get a new block cipher object for the given algorithm.
     *
     * @param algorithm the algorithm
     * @return a new cipher object
     */
    public static BlockCipher getBlockCipher(String algorithm) {
        if ("XTEA".equalsIgnoreCase(algorithm)) {
            return new XTEA();
        } else if ("AES".equalsIgnoreCase(algorithm)) {
            return new AES();
        }
        throw DbException.get(ErrorCode.UNSUPPORTED_CIPHER, algorithm);
    }

    /**
     * Get a new cryptographic hash object for the given algorithm.
     *
     * @param algorithm the algorithm
     * @return a new hash object
     */
    public static SHA256 getHash(String algorithm) {
        if ("SHA256".equalsIgnoreCase(algorithm)) {
            return new SHA256();
        }
        throw DbException.getInvalidValueException(algorithm, "algorithm");
    }

}
