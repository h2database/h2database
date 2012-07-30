/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.HashMap;
import java.util.Random;
import org.h2.dev.store.btree.CacheLirs;
import org.h2.test.TestBase;
import org.h2.util.New;

/**
 * Tests the cache algorithm.
 */
public class TestCache extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testEdgeCases();
        testGetPutPeekRemove();
        testLimitHot();
        testLimitNonResident();
        testBadHashMethod();
        testScanResistance();
        testRandomOperations();
    }

    private void testEdgeCases() {
        CacheLirs<Integer, Integer> test = CacheLirs.newInstance(0);
        test.put(1, 10);
        assertEquals(10, test.get(1).intValue());
    }

    private void testGetPutPeekRemove() {
        CacheLirs<Integer, Integer> test = CacheLirs.newInstance(4);
        test.put(1,  10);
        test.put(2,  20);
        test.put(3,  30);
        assertNull(test.peek(4));
        assertNull(test.get(4));
        test.put(4,  40);
        assertEquals("mem: 4 stack: 4 3 2 1 cold: non-resident:", toString(test));
        // move middle to front
        assertEquals(30, test.get(3).intValue());
        assertEquals(20, test.get(2).intValue());
        assertEquals(20, test.peek(2).intValue());
        // already on (an optimization)
        assertEquals(20, test.get(2).intValue());
        assertEquals(10, test.peek(1).intValue());
        assertEquals(10, test.get(1).intValue());
        assertEquals("mem: 4 stack: 1 2 3 4 cold: non-resident:", toString(test));
        test.put(3,  30);
        assertEquals("mem: 4 stack: 3 1 2 4 cold: non-resident:", toString(test));
        // 5 is cold; will make 4 non-resident
        test.put(5,  50);
        assertEquals("mem: 4 stack: 5 3 1 2 cold: 5 non-resident: 4", toString(test));
        assertNull(test.peek(4));
        assertNull(test.get(4));
        assertEquals(10, test.get(1).intValue());
        assertEquals(20, test.get(2).intValue());
        assertEquals(30, test.get(3).intValue());
        assertEquals("mem: 4 stack: 3 2 1 cold: 5 non-resident: 4", toString(test));
        assertEquals(50, test.get(5).intValue());
        assertEquals("mem: 4 stack: 5 3 2 1 cold: 5 non-resident: 4", toString(test));
        assertEquals(50, test.get(5).intValue());
        assertEquals("mem: 4 stack: 5 3 2 cold: 1 non-resident: 4", toString(test));

        // remove
        assertTrue(test.remove(5));
        assertFalse(test.remove(5));
        assertEquals("mem: 3 stack: 3 2 1 cold: non-resident: 4", toString(test));
        assertTrue(test.remove(4));
        assertFalse(test.remove(4));
        assertEquals("mem: 3 stack: 3 2 1 cold: non-resident:", toString(test));
        test.put(4,  40);
        test.put(5,  50);
        assertEquals("mem: 4 stack: 5 4 3 2 cold: 5 non-resident: 1", toString(test));
        test.get(5);
        test.get(2);
        test.get(3);
        test.get(4);
        assertEquals("mem: 4 stack: 4 3 2 5 cold: 2 non-resident: 1", toString(test));
        assertTrue(test.remove(5));
        assertEquals("mem: 3 stack: 4 3 2 cold: non-resident: 1", toString(test));
        assertTrue(test.remove(2));
        assertTrue(test.remove(1));
        assertEquals("mem: 2 stack: 4 3 cold: non-resident:", toString(test));
        test.put(1,  10);
        test.put(2,  20);
        assertEquals("mem: 4 stack: 2 1 4 3 cold: non-resident:", toString(test));
        test.get(1);
        test.get(3);
        test.get(4);
        assertEquals("mem: 4 stack: 4 3 1 2 cold: non-resident:", toString(test));
        assertTrue(test.remove(1));
        assertEquals("mem: 3 stack: 4 3 2 cold: non-resident:", toString(test));
        test.remove(2);
        test.remove(3);
        test.remove(4);

        // test clear
        test.clear();
        assertEquals("mem: 0 stack: cold: non-resident:", toString(test));
    }

    private void testLimitHot() {
        CacheLirs<Integer, Integer> test = CacheLirs.newInstance(100);
        for (int i = 0; i < 300; i++) {
            test.put(i, 10 * i);
        }
        assertEquals(199, test.getSize());
        assertEquals(93, test.getHotSize());
        assertEquals(99, test.getNonResidentSize());
    }

    private void testLimitNonResident() {
        CacheLirs<Integer, Integer> test = CacheLirs.newInstance(4);
        for (int i = 0; i < 20; i++) {
            test.put(i, 10 * i);
        }
        assertEquals("mem: 4 stack: 19 18 17 16 3 2 1 cold: 19 non-resident: 18 17 16", toString(test));
    }

    private void testBadHashMethod() {
        // ensure an 2^n cache size
        final int size = 4;

        /**
         * A class with a bad hashCode implementation.
         */
        class BadHash {
            int x;

            BadHash(int x) {
                this.x = x;
            }

            public int hashCode() {
                return (x & 1) * size * 2;
            }

            public boolean equals(Object o) {
                return ((BadHash) o).x == x;
            }

            public String toString() {
                return "" + x;
            }

        }

        CacheLirs<BadHash, Integer> test = CacheLirs.newInstance(size * 2);
        for (int i = 0; i < size; i++) {
            test.put(new BadHash(i), i);
        }
        for (int i = 0; i < size; i++) {
            if (i % 3 == 0) {
                assertTrue(test.remove(new BadHash(i)));
                assertFalse(test.remove(new BadHash(i)));
            }
        }
        for (int i = 0; i < size; i++) {
            if (i % 3 == 0) {
                assertNull(test.get(new BadHash(i)));
            } else {
                assertEquals(i, test.get(new BadHash(i)).intValue());
            }
        }
        for (int i = 0; i < size; i++) {
            test.put(new BadHash(i), i);
        }
        for (int i = 0; i < size; i++) {
            if (i % 3 == 0) {
                assertTrue(test.remove(new BadHash(i)));
                assertFalse(test.remove(new BadHash(i)));
            }
        }
        for (int i = 0; i < size; i++) {
            if (i % 3 == 0) {
                assertNull(test.get(new BadHash(i)));
            } else {
                assertEquals(i, test.get(new BadHash(i)).intValue());
            }
        }
    }

    private void testScanResistance() {
        boolean log = false;
        int size = 20;
        // cache size 11 (10 hot, 1 cold)
        CacheLirs<Integer, Integer> test = CacheLirs.newInstance(size / 2 + 1);
        // init the cache with some dummy entries
        for (int i = 0; i < size; i++) {
            test.put(-i, -i * 10);
        }
        // init with 0..9, ensure those are hot entries
        for (int i = 0; i < size / 2; i++) {
            test.put(i, i * 10);
            test.get(i);
            if (log) {
                System.out.println("get " + i + " -> " + test);
            }
        }
        // read 0..9, add 10..19 (cold)
        for (int i = 0; i < size; i++) {
            Integer x = test.get(i);
            Integer y = test.peek(i);
            if (i < size / 2) {
                assertTrue("i: " + i, x != null);
                assertTrue("i: " + i, y != null);
                assertEquals(i * 10, x.intValue());
                assertEquals(i * 10, y.intValue());
            } else {
                assertNull(x);
                assertNull(y);
                test.put(i, i * 10);
                // peek should have no effect
                assertEquals(i * 10, test.peek(i).intValue());
            }
            if (log) {
                System.out.println("get " + i + " -> " + test);
            }
        }
        // ensure 0..9 are hot, 10..18 are not resident, 19 is cold
        for (int i = 0; i < size; i++) {
            Integer x = test.get(i);
            if (i < size / 2 || i == size - 1) {
                assertTrue("i: " + i, x != null);
                assertEquals(i * 10, x.intValue());
            } else {
                assertNull(x);
            }
        }
    }

    private void testRandomOperations() {
        boolean log = false;
        int size = 10;
        Random r = new Random(1);
        for (int j = 0; j < 100; j++) {
            CacheLirs<Integer, Integer> test = CacheLirs.newInstance(size / 2);
            HashMap<Integer, Integer> good = New.hashMap();
            for (int i = 0; i < 10000; i++) {
                int key = r.nextInt(size);
                int value = r.nextInt();
                switch (r.nextInt(3)) {
                case 0:
                    if (log) {
                        System.out.println(i + " put " + key + " " + value);
                    }
                    good.put(key, value);
                    test.put(key, value);
                    break;
                case 1:
                    if (log) {
                        System.out.println(i + " get " + key);
                    }
                    Integer a = good.get(key);
                    Integer b = test.get(key);
                    if (a == null) {
                        assertNull(b);
                    } else if (b != null) {
                        assertEquals(a, b);
                    }
                    break;
                case 2:
                    if (log) {
                        System.out.println(i + " remove " + key);
                    }
                    good.remove(key);
                    test.remove(key);
                    break;
                }
                if (log) {
                    System.out.println(" -> " + toString(test));
                }
            }
        }
    }

    private static <K, V> String toString(CacheLirs<K, V> cache) {
        StringBuilder buff = new StringBuilder();
        buff.append("mem: " + cache.getUsedMemory());
        buff.append(" stack:");
        for (K k : cache.keys(false,  false)) {
            buff.append(' ').append(k);
        }
        buff.append(" cold:");
        for (K k : cache.keys(true,  false)) {
            buff.append(' ').append(k);
        }
        buff.append(" non-resident:");
        for (K k : cache.keys(true,  true)) {
            buff.append(' ').append(k);
        }
        return buff.toString();
    }

}
