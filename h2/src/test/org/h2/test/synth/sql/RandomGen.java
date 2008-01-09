/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Random;

/**
 * A random data generator class.
 */
public class RandomGen {
    private Random random = new Random();

    // private TestSynth config;

    public RandomGen(TestSynth config) {
        // this.config = config;
        random.setSeed(12);
    }

    public int getInt(int max) {
        return max == 0 ? 0 : random.nextInt(max);
    }

    public double nextGaussian() {
        return random.nextGaussian();
    }

    public int getLog(int max) {
        if (max == 0) {
            return 0;
        }
        while (true) {
            int d = Math.abs((int) (random.nextGaussian() / 2. * max));
            if (d < max) {
                return d;
            }
        }
    }

    public void getBytes(byte[] data) {
        random.nextBytes(data);
    }

    public boolean getBoolean(int percent) {
        return random.nextInt(100) <= percent;
    }

    public String randomString(int len) {
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < len; i++) {
            String from = (i % 2 == 0) ? "bdfghklmnpqrst" : "aeiou";
            buff.append(from.charAt(getInt(from.length())));
        }
        return buff.toString();
    }

    public int getRandomInt() {
        switch (random.nextInt(10)) {
        case 0:
            return Integer.MAX_VALUE;
        case 1:
            return Integer.MIN_VALUE;
        case 2:
            return random.nextInt();
        case 3:
        case 4:
            return 0;
        case 5:
            return (int) (random.nextGaussian() * 2000) - 200;
        default:
            return (int) (random.nextGaussian() * 20) - 5;
        }
    }

    public long getRandomLong() {
        switch (random.nextInt(10)) {
        case 0:
            return Long.MAX_VALUE;
        case 1:
            return Long.MIN_VALUE;
        case 2:
            return random.nextLong();
        case 3:
        case 4:
            return 0;
        case 5:
            return (int) (random.nextGaussian() * 20000) - 2000;
        default:
            return (int) (random.nextGaussian() * 200) - 50;
        }
    }

    public double getRandomDouble() {
        switch (random.nextInt(10)) {
        case 0:
            return Double.MIN_VALUE;
        case 1:
            return Double.MAX_VALUE;
        case 2:
            return Float.MIN_VALUE;
        case 3:
            return Float.MAX_VALUE;
        case 4:
            return random.nextDouble();
        case 5:
        case 6:
            return 0;
        case 7:
            return random.nextGaussian() * 20000. - 2000.;
        default:
            return random.nextGaussian() * 200. - 50.;
        }
    }

    public boolean nextBoolean() {
        return random.nextBoolean();
    }

    public int[] getIntArray() {
        switch (random.nextInt(10)) {
        case 0:
            return null;
        default:
            int len = getInt(100);
            int[] list = new int[len];
            for (int i = 0; i < len; i++) {
                list[i] = getRandomInt();
            }
            return list;
        }
    }

    public Object getByteArray() {
        switch (random.nextInt(10)) {
        case 0:
            return null;
        default:
            int len = getInt(100);
            byte[] list = new byte[len];
            random.nextBytes(list);
            return list;
        }
    }

    public Time randomTime() {
        if (random.nextInt(10) == 0) {
            return null;
        }
        StringBuffer buff = new StringBuffer();
        buff.append(getInt(24));
        buff.append(':');
        buff.append(getInt(24));
        buff.append(':');
        buff.append(getInt(24));
        return Time.valueOf(buff.toString());

    }

    public Timestamp randomTimestamp() {
        if (random.nextInt(10) == 0) {
            return null;
        }
        StringBuffer buff = new StringBuffer();
        buff.append(getInt(10) + 2000);
        buff.append('-');
        buff.append(getInt(12) + 1);
        buff.append('-');
        buff.append(getInt(28) + 1);
        buff.append(' ');
        buff.append(getInt(24));
        buff.append(':');
        buff.append(getInt(60));
        buff.append(':');
        buff.append(getInt(60));
        // TODO test timestamp nanos
        return Timestamp.valueOf(buff.toString());
    }

    public Date randomDate() {
        if (random.nextInt(10) == 0) {
            return null;
        }
        StringBuffer buff = new StringBuffer();
        buff.append(getInt(10) + 2000);
        buff.append('-');
        buff.append(getInt(11) + 1);
        buff.append('-');
        buff.append(getInt(29) + 1);
        return Date.valueOf(buff.toString());
    }

    public String modify(String sql) {
        int len = getLog(10);
        for (int i = 0; i < len; i++) {
            int pos = getInt(sql.length());
            if (getBoolean(50)) {
                String badChars = "abcABCDEF\u00ef\u00f6\u00fcC1230=<>+\"\\*%&/()=?$_-.:,;{}[]"; // auml
                                                                                                    // ouml
                                                                                                    // uuml
                char bad = badChars.charAt(getInt(badChars.length()));
                sql = sql.substring(0, pos) + bad + sql.substring(pos);
            } else {
                if (pos >= sql.length()) {
                    sql = sql.substring(0, pos);
                } else {
                    sql = sql.substring(0, pos) + sql.substring(pos + 1);
                }
            }
        }
        return sql;
    }

    public void setSeed(int seed) {
        random.setSeed(seed);
    }

}
