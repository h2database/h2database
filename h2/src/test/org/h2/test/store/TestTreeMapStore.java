/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;
import org.h2.dev.store.btree.BtreeMap;
import org.h2.dev.store.btree.BtreeMapStore;
import org.h2.jaqu.bytecode.Null;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Tests the tree map store.
 */
public class TestTreeMapStore extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testBtreeStore();
        testDefragment();
        testReuseSpace();
        testRandom();
        testKeyValueClasses();
        testIterate();
        testSimple();
    }

    private void testBtreeStore() {
        String fileName = getBaseDir() + "/testBtreeStore.h3";
        FileUtils.delete(fileName);
        BtreeMapStore s = BtreeMapStore.open(fileName);
        BtreeMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
        int count = 2000;
        // Profiler p = new Profiler();
        // p.startCollecting();
        // long t = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            m.put(i, "hello " + i);
            assertEquals("hello " + i, m.get(i));
        }
        // System.out.println("put: " + (System.currentTimeMillis() - t));
        // System.out.println(p.getTop(5));
        // p = new Profiler();
        //p.startCollecting();
        // t = System.currentTimeMillis();
        s.store();
        // System.out.println("store: " + (System.currentTimeMillis() - t));
        // System.out.println(p.getTop(5));
        m.remove(0);
        assertNull(m.get(0));
        for (int i = 1; i < count; i++) {
            assertEquals("hello " + i, m.get(i));
        }
        s.close();
        s = BtreeMapStore.open(fileName);
        m = s.openMap("data", Integer.class, String.class);
        assertNull(m.get(0));
        for (int i = 1; i < count; i++) {
            assertEquals("hello " + i, m.get(i));
        }
        for (int i = 1; i < count; i++) {
            m.remove(i);
        }
        s.store();
        assertNull(m.get(0));
        for (int i = 0; i < count; i++) {
            assertNull(m.get(i));
        }
        s.close();
    }

    private void testDefragment() {
        String fileName = getBaseDir() + "/testDefragment.h3";
        FileUtils.delete(fileName);
        long initialLength = 0;
        for (int j = 0; j < 20; j++) {
            BtreeMapStore s = BtreeMapStore.open(fileName);
            BtreeMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
            for (int i = 0; i < 10; i++) {
                m.put(j + i, "Hello " + j);
            }
            s.store();
            s.compact();
            s.close();
            long len = FileUtils.size(fileName);
            // System.out.println("   len:" + len);
            if (initialLength == 0) {
                initialLength = len;
            } else {
                assertTrue("initial: " + initialLength + " len: " + len, len <= initialLength * 3);
            }
        }
        long len = FileUtils.size(fileName);
        // System.out.println("len0: " + len);
        BtreeMapStore s = BtreeMapStore.open(fileName);
        BtreeMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
        for (int i = 0; i < 100; i++) {
            m.remove(i);
        }
        s.store();
        s.compact();
        s.close();
        len = FileUtils.size(fileName);
        // System.out.println("len1: " + len);
        s = BtreeMapStore.open(fileName);
        m = s.openMap("data", Integer.class, String.class);
        s.compact();
        s.close();
        len = FileUtils.size(fileName);
        // System.out.println("len2: " + len);
    }

    private void testReuseSpace() {
        String fileName = getBaseDir() + "/testReuseSpace.h3";
        FileUtils.delete(fileName);
        long initialLength = 0;
        for (int j = 0; j < 10; j++) {
            BtreeMapStore s = BtreeMapStore.open(fileName);
            BtreeMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
            for (int i = 0; i < 10; i++) {
                m.put(i, "Hello");
            }
            s.store();
            for (int i = 0; i < 10; i++) {
                m.remove(i);
            }
            s.store();
            s.close();
            long len = FileUtils.size(fileName);
            if (initialLength == 0) {
                initialLength = len;
            } else {
                assertTrue(len <= initialLength * 2);
            }
        }
    }

    private void testRandom() {
        String fileName = getBaseDir() + "/testRandom.h3";
        FileUtils.delete(fileName);
        BtreeMapStore s = BtreeMapStore.open(fileName);
        BtreeMap<Integer, Integer> m = s.openMap("data", Integer.class, Integer.class);
        TreeMap<Integer, Integer> map = new TreeMap<Integer, Integer>();
        Random r = new Random(1);
        int operationCount = 1000;
        int maxValue = 20;
        for (int i = 0; i < operationCount; i++) {
            int k = r.nextInt(maxValue);
            int v = r.nextInt();
            boolean compareAll;
            switch (r.nextInt(3)) {
            case 0:
                m.put(k, v);
                map.put(k, v);
                compareAll = true;
                break;
            case 1:
                m.remove(k);
                map.remove(k);
                compareAll = true;
                break;
            default:
                Integer a = map.get(k);
                Integer b = m.get(k);
                if (a == null || b == null) {
                    assertTrue(a == b);
                } else {
                    assertEquals(a.intValue(), b.intValue());
                }
                compareAll = false;
                break;
            }
            if (compareAll) {
                Iterator<Integer> it = m.keyIterator(null);
                Iterator<Integer> it2 = map.keySet().iterator();
                while (it2.hasNext()) {
                    assertTrue(it.hasNext());
                    assertEquals(it2.next(), it.next());
                }
                assertFalse(it.hasNext());
            }
        }
        s.close();
    }

    private void testKeyValueClasses() {
        String fileName = getBaseDir() + "/testKeyValueClasses.h3";
        FileUtils.delete(fileName);
        BtreeMapStore s = BtreeMapStore.open(fileName);
        BtreeMap<Integer, String> is = s.openMap("intString", Integer.class, String.class);
        is.put(1, "Hello");
        BtreeMap<Integer, Integer> ii = s.openMap("intInt", Integer.class, Integer.class);
        ii.put(1, 10);
        BtreeMap<String, Integer> si = s.openMap("stringInt", String.class, Integer.class);
        si.put("Test", 10);
        BtreeMap<String, String> ss = s.openMap("stringString", String.class, String.class);
        ss.put("Hello", "World");
        try {
            s.openMap("invalid", Null.class, Integer.class);
            fail();
        } catch (RuntimeException e) {
            // expected
        }
        try {
            s.openMap("invalid", Integer.class, Null.class);
            fail();
        } catch (RuntimeException e) {
            // expected
        }
        s.close();
        s = BtreeMapStore.open(fileName);
        is = s.openMap("intString", Integer.class, String.class);
        assertEquals("Hello", is.get(1));
        ii = s.openMap("intInt", Integer.class, Integer.class);
        assertEquals(10, ii.get(1).intValue());
        si = s.openMap("stringInt", String.class, Integer.class);
        assertEquals(10, si.get("Test").intValue());
        ss = s.openMap("stringString", String.class, String.class);
        assertEquals("World", ss.get("Hello"));
        s.close();
    }

    private void testIterate() {
        String fileName = getBaseDir() + "/testIterate.h3";
        FileUtils.delete(fileName);
        BtreeMapStore s = BtreeMapStore.open(fileName);
        BtreeMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
        Iterator<Integer> it = m.keyIterator(null);
        assertFalse(it.hasNext());
        for (int i = 0; i < 10; i++) {
            m.put(i, "hello " + i);
        }
        s.store();
        it = m.keyIterator(null);
        it.next();
        assertThrows(UnsupportedOperationException.class, it).remove();

        it = m.keyIterator(null);
        for (int i = 0; i < 10; i++) {
            assertTrue(it.hasNext());
            assertEquals(i, it.next().intValue());
        }
        assertFalse(it.hasNext());
        assertNull(it.next());
        for (int j = 0; j < 10; j++) {
            it = m.keyIterator(j);
            for (int i = j; i < 10; i++) {
                assertTrue(it.hasNext());
                assertEquals(i, it.next().intValue());
            }
            assertFalse(it.hasNext());
        }
        s.close();
    }

    private void testSimple() {
        String fileName = getBaseDir() + "/testSimple.h3";
        FileUtils.delete(fileName);
        BtreeMapStore s = BtreeMapStore.open(fileName);
        BtreeMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
        for (int i = 0; i < 3; i++) {
            m.put(i, "hello " + i);
        }
        s.store();
        m.remove(0);

        assertNull(m.get(0));
        for (int i = 1; i < 3; i++) {
            assertEquals("hello " + i, m.get(i));
        }

        s.close();

        s = BtreeMapStore.open(fileName);
        m = s.openMap("data", Integer.class, String.class);
        assertNull(m.get(0));
        for (int i = 1; i < 3; i++) {
            assertEquals("hello " + i, m.get(i));
        }
        s.close();
    }

}
