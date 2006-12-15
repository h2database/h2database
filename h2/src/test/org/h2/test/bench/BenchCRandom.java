/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;

public class BenchCRandom {

    private Random random = new Random(10);
    
    int getNonUniform(int a, int min, int max) {
        int c = 0;
        return (((getInt(0, a) | getInt(min, max)) + c) % (max - min + 1))
                + min;
    }

    int getInt(int min, int max) {
        return max <= min ? min : (random.nextInt(max - min) + min);
    }

    boolean[] getBoolean(int length, int trueCount) {
        boolean[] data = new boolean[length];
        for (int i = 0, pos; i < trueCount; i++) {
            do {
                pos = getInt(0, length);
            } while (data[pos]);
            data[pos] = true;
        }
        return data;
    }

    String replace(String text, String replacement) {
        int pos = getInt(0, text.length() - replacement.length());
        StringBuffer buffer = new StringBuffer(text);
        buffer.replace(pos, pos + 7, replacement);
        return buffer.toString();
    }

    String getNumberString(int min, int max) {
        int len = getInt(min, max);
        char[] buff = new char[len];
        for (int i = 0; i < len; i++) {
            buff[i] = (char) getInt('0', '9');
        }
        return new String(buff);
    }

    String[] getAddress() {
        String str1 = getString(10, 20);
        String str2 = getString(10, 20);
        String city = getString(10, 20);
        String state = getString(2);
        String zip = getNumberString(9, 9);
        return new String[] { str1, str2, city, state, zip };
    }

    String getString(int min, int max) {
        return getString(getInt(min, max));
    }

    String getString(int len) {
        char[] buff = new char[len];
        for (int i = 0; i < len; i++) {
            buff[i] = (char) getInt('A', 'Z');
        }
        return new String(buff);
    }

    int[] getPermutation(int length) {
        int[] data = new int[length];
        for (int i = 0; i < length; i++) {
            data[i] = i;
        }
        for (int i = 0; i < length; i++) {
            int j = getInt(0, length);
            int temp = data[i];
            data[i] = data[j];
            data[j] = temp;
        }
        return data;
    }


    BigDecimal getBigDecimal(int value, int scale) {
        return new BigDecimal(new BigInteger(String.valueOf(value)), scale);
    }

    String getLastname(int i) {
        String[] n = { "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI",
                "CALLY", "ATION", "EING" };
        StringBuffer buff = new StringBuffer();
        buff.append(n[i / 100]);
        buff.append(n[(i / 10) % 10]);
        buff.append(n[i % 10]);
        return buff.toString();
    }

}
