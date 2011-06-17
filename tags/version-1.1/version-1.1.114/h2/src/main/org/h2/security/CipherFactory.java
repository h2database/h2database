/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;

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
    public static BlockCipher getBlockCipher(String algorithm) throws SQLException {
        if ("XTEA".equalsIgnoreCase(algorithm)) {
            return new XTEA();
        } else if ("AES".equalsIgnoreCase(algorithm)) {
            return new AES();
        }
        throw Message.getSQLException(ErrorCode.UNSUPPORTED_CIPHER, algorithm);
    }

    /**
     * Get a new cryptographic hash object for the given algorithm.
     *
     * @param algorithm the algorithm
     * @return a new hash object
     */
    public static SHA256 getHash(String algorithm) throws SQLException {
        if ("SHA256".equalsIgnoreCase(algorithm)) {
            return new SHA256();
        }
        throw Message.getInvalidValueException(algorithm, "algorithm");
    }

}
