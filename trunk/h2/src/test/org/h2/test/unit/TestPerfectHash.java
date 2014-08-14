/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.h2.dev.hash.PerfectHash;
import org.h2.test.TestBase;

/**
 * Tests the perfect hash tool.
 */
public class TestPerfectHash extends TestBase {

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
        for (int i = 0; i < 1000; i++) {
            test(i, true);
            test(i, false);
        }
        for (int i = 1000; i <= 100000; i *= 10) {
            test(i, true);
            test(i, false);
        }
    }

    void test(int size, boolean minimal) {
        Random r = new Random(size);
        HashSet<Integer> set = new HashSet<Integer>();
        while (set.size() < size) {
            set.add(r.nextInt());
        }
        byte[] desc = PerfectHash.generate(set, minimal);
        int max = test(desc, set);
        if (minimal) {
            assertEquals(size - 1, max);
        } else {
            if (size > 10) {
                assertTrue(max < 1.5 * size);
            }
        }
    }

    int test(byte[] desc, Set<Integer> set) {
        int max = -1;
        HashSet<Integer> test = new HashSet<Integer>();
        PerfectHash hash = new PerfectHash(desc);
        for (int x : set) {
            int h = hash.get(x);
            assertTrue(h >= 0);
            assertTrue(h <= set.size() * 3);
            max = Math.max(max, h);
            assertFalse(test.contains(h));
            test.add(h);
        }
        return max;
    }
}
