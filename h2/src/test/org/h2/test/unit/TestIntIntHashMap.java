/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.util.Random;

import org.h2.test.TestBase;
import org.h2.util.IntIntHashMap;

/**
 * Tests the IntHashMap class.
 */
public class TestIntIntHashMap extends TestBase {

    private final Random rand = new Random();

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() {
        IntIntHashMap map = new IntIntHashMap();
        map.put(1, 1);
        map.put(1, 2);
        assertEquals(1, map.size());
        map.put(0, 1);
        map.put(0, 2);
        assertEquals(2, map.size());
        rand.setSeed(10);
        test(true, true);
        test(false, true);
        test(true, false);
        test(false, false);
    }

    private void test(boolean random, boolean autoShrink) {
        int len = 2000;
        int[] x = new int[len];
        for (int i = 0; i < len; i++) {
            x[i] = random ? rand.nextInt() : i;
        }
        IntIntHashMap map = new IntIntHashMap(autoShrink);
        for (int i = 0; i < len; i++) {
            map.put(x[i], i);
        }
        testAll(map, x, len);
        for (int i = 1; i < len; i += 2) {
            map.remove(x[i]);
        }
        for (int i = 1; i < len; i += 2) {
            if (map.get(x[i]) != -1) {
                throw new AssertionError("get " + x[i] + " = " + map.get(i) + " should be -1");
            }
        }
        for (int i = 1; i < len; i += 2) {
            map.put(x[i], i);
        }
        testAll(map, x, len);
        map.clear();
        assertEquals(0, map.size());
        for (int i = 0; i < len; i++) {
            assertEquals(-1, map.get(x[i]));
        }
        for (int i = 0; i < len; i++) {
            map.put(x[i], i);
        }
        testAll(map, x, len);
    }

    private static void testAll(IntIntHashMap map, int[] x, int len) throws AssertionError {
        for (int i = 0; i < len; i++) {
            if (map.get(x[i]) != i) {
                throw new AssertionError("get " + x[i] + " = " + map.get(i) + " should be " + i);
            }
        }
    }

}
