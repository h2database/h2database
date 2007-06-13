/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

public class RandomUtils {
    private static SecureRandom secureRandom;
    private static Random random;

    static {
        try {
            secureRandom = SecureRandom.getInstance("SHA1PRNG");
            byte[] seed = secureRandom.generateSeed(64);
            secureRandom.setSeed(seed);
            random = new Random(secureRandom.nextLong());
        } catch (NoSuchAlgorithmException e) {
            // random is null if the algorithm is not found
            // TODO log exception
            random = new Random();
        }
    }
    
    public static long getSecureLong() {
        if(secureRandom == null) {
            byte[] buff = SecureRandom.getSeed(8);
            return ByteUtils.readLong(buff, 0);
        }
        return secureRandom.nextLong();
    }
    
    

    public static byte[] getSecureBytes(int len) {
        if(secureRandom == null) {
            return SecureRandom.getSeed(len);
        }
        if(len <= 0) {
            len = 1;
        }
        byte[] buff = new byte[len];
        secureRandom.nextBytes(buff);
        return buff;
    }

    public static int nextInt(int max) {
        return random.nextInt(max);
    }

}
