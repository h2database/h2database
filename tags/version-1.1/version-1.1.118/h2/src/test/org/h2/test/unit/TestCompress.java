/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.SQLException;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.tools.CompressTool;

/**
 * Data compression tests.
 */
public class TestCompress extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        if (config.big) {
            for (int i = 0; i < 100; i++) {
                test(i);
            }
            for (int i = 100; i < 10000; i += i + i + 1) {
                test(i);
            }
        } else {
            test(0);
            test(1);
            test(7);
            test(50);
            test(200);
        }
    }

    private void test(int len) throws SQLException {
        Random r = new Random(len);
        for (int pattern = 0; pattern < 4; pattern++) {
            byte[] buff = new byte[len];
            switch (pattern) {
            case 0:
                // leave empty
                break;
            case 1: {
                for (int x = 0; x < len; x++) {
                    buff[x] = (byte) (x & 10);
                }
                break;
            }
            case 2: {
                r.nextBytes(buff);
                break;
            }
            case 3: {
                for (int x = 0; x < len; x++) {
                    buff[x] = (byte) (x / 10);
                }
                break;
            }
            default:
            }
            if (r.nextInt(2) < 1) {
                for (int x = 0; x < len; x++) {
                    if (r.nextInt(20) < 1) {
                        buff[x] = (byte) (r.nextInt(255));
                    }
                }
            }
            CompressTool utils = CompressTool.getInstance();
            for (String a : new String[] { "LZF", "Deflate", "No" }) {
                byte[] out = utils.compress(buff, a);
                byte[] test = utils.expand(out);
                assertEquals(buff.length, test.length);
                assertEquals(buff, test);
            }
        }
    }

}
