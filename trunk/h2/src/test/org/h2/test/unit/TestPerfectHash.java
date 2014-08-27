/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.h2.dev.hash.MinimalPerfectHash;
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
        TestPerfectHash test = (TestPerfectHash) TestBase.createCaller().init();
        test.test();
        test.measure();
    }

    /**
     * Measure the hash functions.
     */
    public void measure() {
        int size = 1000000;

        int s = testMinimal(size);
        System.out.println((double) s / size + " bits/key (minimal)");

        s = test(size, true);
        System.out.println((double) s / size + " bits/key (minimal old)");
        s = test(size, false);
        System.out.println((double) s / size + " bits/key (not minimal)");
    }

    @Override
    public void test() {
        for (int i = 0; i < 100; i++) {
            testMinimal(i);
        }
        for (int i = 100; i <= 100000; i *= 10) {
            testMinimal(i);
        }
        for (int i = 0; i < 100; i++) {
            test(i, true);
            test(i, false);
        }
        for (int i = 100; i <= 100000; i *= 10) {
            test(i, true);
            test(i, false);
        }
    }

    private int test(int size, boolean minimal) {
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
        return desc.length * 8;
    }

    private int test(byte[] desc, Set<Integer> set) {
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

    private int testMinimal(int size) {
        Random r = new Random(size);
        HashSet<Integer> set = new HashSet<Integer>();
        while (set.size() < size) {
            set.add(r.nextInt());
        }
        byte[] desc = MinimalPerfectHash.generate(set);
        int max = testMinimal(desc, set);
        assertEquals(size - 1, max);
        return desc.length * 8;
    }

    private int testMinimal(byte[] desc, Set<Integer> set) {
        int max = -1;
        HashSet<Integer> test = new HashSet<Integer>();
        MinimalPerfectHash hash = new MinimalPerfectHash(desc);
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
