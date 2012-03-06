/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;
import org.h2.dev.store.StoredMap;
import org.h2.dev.store.TreeMapStore;
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
        testRandom();
        testKeyValueClasses();
        testIterate();
        testSimple();
    }

    private void testRandom() {
        String fileName = getBaseDir() + "/data.h3";
        FileUtils.delete(fileName);
        TreeMapStore s = TreeMapStore.open(fileName);
        StoredMap<Integer, Integer> m = s.openMap("intString", Integer.class, Integer.class);
        TreeMap<Integer, Integer> map = new TreeMap<Integer, Integer>();
        Random r = new Random(1);
        for (int i = 0; i < 100; i++) {
            int k = r.nextInt(20);
            int v = r.nextInt();
            if (r.nextBoolean()) {
                m.put(k, v);
                map.put(k, v);
            } else {
                m.remove(k);
                map.remove(k);
            }
            Iterator<Integer> it = m.keyIterator(null);
            Iterator<Integer> itm = map.keySet().iterator();
            while (itm.hasNext()) {
                assertTrue(it.hasNext());
                assertEquals(itm.next(), it.next());
            }
            assertFalse(it.hasNext());
        }
        s.close();
    }

    private void testKeyValueClasses() {
        String fileName = getBaseDir() + "/data.h3";
        FileUtils.delete(fileName);
        TreeMapStore s = TreeMapStore.open(fileName);
        StoredMap<Integer, String> is = s.openMap("intString", Integer.class, String.class);
        is.put(1, "Hello");
        StoredMap<Integer, Integer> ii = s.openMap("intInt", Integer.class, Integer.class);
        ii.put(1, 10);
        StoredMap<String, Integer> si = s.openMap("stringInt", String.class, Integer.class);
        si.put("Test", 10);
        StoredMap<String, String> ss = s.openMap("stringString", String.class, String.class);
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
        s = TreeMapStore.open(fileName);
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
        String fileName = getBaseDir() + "/data.h3";
        FileUtils.delete(fileName);
        TreeMapStore s = TreeMapStore.open(fileName);
        StoredMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
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
    }

    private void testSimple() {
        String fileName = getBaseDir() + "/data.h3";
        FileUtils.delete(fileName);
        TreeMapStore s = TreeMapStore.open(fileName);
        StoredMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
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

        s = TreeMapStore.open(fileName);
        m = s.openMap("data", Integer.class, String.class);
        assertNull(m.get(0));
        for (int i = 1; i < 3; i++) {
            assertEquals("hello " + i, m.get(i));
        }
        s.close();
    }

}
