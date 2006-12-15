/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

import java.sql.SQLException;

import org.h2.message.Message;

public class CipherFactory {
    
    public static BlockCipher getBlockCipher(String algorithm) throws SQLException {
        if ("XTEA".equalsIgnoreCase(algorithm)) {
            return new XTEA();
        } else if ("AES".equalsIgnoreCase(algorithm)) {
            return new AES();
        } else {
            throw Message.getSQLException(Message.UNSUPPORTED_CIPHER, algorithm);
        }
    }

    public static SHA256 getHash(String algorithm) throws SQLException {
        if("SHA256".equalsIgnoreCase(algorithm)) {
            return new SHA256();
        } else {
            throw Message.getInvalidValueException(algorithm, "algorithm");
        }
    }
    
}
