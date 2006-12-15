/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

public interface BlockCipher {

    int ALIGN = 16;

    void setKey(byte[] key);
    void encrypt(byte[] bytes, int off, int len);
    void decrypt(byte[] bytes, int off, int len);
    int getKeyLength();
}
