/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.test.TestBase;
import org.h2.util.MathUtils;

/**
 * Tests math utility methods.
 */
public class TestMathUtils extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() {
        testRandom();
    }

    private void testRandom() {
        int bits = 0;
        for (int i = 0; i < 1000; i++) {
            bits |= 1 << MathUtils.randomInt(8);
        }
        assertEquals(255, bits);
        bits = 0;
        for (int i = 0; i < 1000; i++) {
            bits |= 1 << MathUtils.secureRandomInt(8);
        }
        assertEquals(255, bits);
        bits = 0;
        for (int i = 0; i < 1000; i++) {
            bits |= 1 << (MathUtils.secureRandomLong() & 7);
        }
        assertEquals(255, bits);
        // just verify the method doesn't throw an exception
        byte[] data = MathUtils.generateAlternativeSeed();
        assertTrue(data.length > 10);
    }

}
