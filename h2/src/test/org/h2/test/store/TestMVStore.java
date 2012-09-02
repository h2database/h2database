/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;
import org.h2.dev.store.btree.MVMap;
import org.h2.dev.store.btree.MVStore;
import org.h2.jaqu.bytecode.Null;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.New;

/**
 * Tests the tree map store.
 */
public class TestMVStore extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() {
        testIterateOverChanges();
        testVersion();
        testTruncateFile();
        testFastDelete();
        testRollbackInMemory();
        testRollbackStored();
        testMeta();
        testInMemory();
        testLargeImport();
        testBtreeStore();
        testDefragment();
        testReuseSpace();
        testRandom();
        testKeyValueClasses();
        testIterate();
        testSimple();
    }

    private void testIterateOverChanges() {
        String fileName = getBaseDir() + "/testVersion.h3";
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        s.setMaxPageSize(6);
        MVMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
        for (int i = 0; i < 60; i++) {
            m.put(i, "Hello");
        }
        s.commit();
        s.store();
        for (int i = 20; i < 40; i++) {
            m.put(i, "Hello");
        }
        s.commit();
        for (int i = 10; i < 15; i++) {
            m.put(i, "Hallo");
        }
        m.put(50, "Hallo");
        Iterator<Integer> it = m.changeIterator(s.getCurrentVersion());
        ArrayList<Integer> list = New.arrayList();
        while (it.hasNext()) {
            list.add(it.next());
        }
        assertEquals("[9, 10, 11, 12, 13, 14, 48, 49, 50]", list.toString());
    }

    private void testVersion() {
        String fileName = getBaseDir() + "/testVersion.h3";
        FileUtils.delete(fileName);
        MVStore s;
        s = openStore(fileName);
        MVMap<String, String> m;
        s = openStore(fileName);
        m = s.openMap("data", String.class, String.class);
        long first = s.getCurrentVersion();
        m.put("1", "Hello");
        m.put("2", "World");
        for (int i = 10; i < 20; i++) {
            m.put("" + i, "data");
        }
        s.commit();
        long old = s.getCurrentVersion();
        m.put("1", "Hallo");
        m.put("2", "Welt");
        MVMap<String, String> mFirst;
        mFirst = m.openVersion(first);
        assertEquals(0, mFirst.size());
        MVMap<String, String> mOld;
        assertEquals("Hallo", m.get("1"));
        assertEquals("Welt", m.get("2"));
        mOld = m.openVersion(old);
        assertEquals("Hello", mOld.get("1"));
        assertEquals("World", mOld.get("2"));
        assertTrue(mOld.isReadOnly());
        s.getCurrentVersion();
        s.setRetainChunk(0);
        long old2 = s.store();

        // the old version is still available
        assertEquals("Hello", mOld.get("1"));
        assertEquals("World", mOld.get("2"));

        m.put("1",  "Hi");
        assertEquals("Welt", m.remove("2"));
        s.store();
        s.close();

        s = openStore(fileName);
        m = s.openMap("data", String.class, String.class);
        assertEquals("Hi", m.get("1"));
        assertEquals(null, m.get("2"));
        mOld = m.openVersion(old2);
        assertEquals("Hallo", mOld.get("1"));
        assertEquals("Welt", mOld.get("2"));
        s.close();
    }

    private void testTruncateFile() {
        String fileName = getBaseDir() + "/testTruncate.h3";
        FileUtils.delete(fileName);
        MVStore s;
        MVMap<Integer, String> m;
        s = openStore(fileName);
        m = s.openMap("data", Integer.class, String.class);
        for (int i = 0; i < 1000; i++) {
            m.put(i, "Hello World");
        }
        s.store();
        s.close();
        long len = FileUtils.size(fileName);
        s = openStore(fileName);
        m = s.openMap("data", Integer.class, String.class);
        m.clear();
        s.store();
        s.compact(100);
        s.close();
        long len2 = FileUtils.size(fileName);
        assertTrue(len2 < len);
    }

    private void testFastDelete() {
        String fileName = getBaseDir() + "/testFastDelete.h3";
        FileUtils.delete(fileName);
        MVStore s;
        MVMap<Integer, String> m;
        s = openStore(fileName);
        s.setMaxPageSize(100);
        m = s.openMap("data", Integer.class, String.class);
        for (int i = 0; i < 1000; i++) {
            m.put(i, "Hello World");
            assertEquals(i + 1, m.size());
        }
        assertEquals(1000, m.size());
        s.store();
        assertEquals(3, s.getWriteCount());
        s.close();

        s = openStore(fileName);
        m = s.openMap("data", Integer.class, String.class);
        m.clear();
        assertEquals(0, m.size());
        s.store();
        // ensure only nodes are read, but not leaves
        assertEquals(4, s.getReadCount());
        assertEquals(2, s.getWriteCount());
        s.close();
    }

    private void testRollbackStored() {
        String fileName = getBaseDir() + "/testRollback.h3";
        FileUtils.delete(fileName);
        MVMap<String, String> meta;
        MVStore s = openStore(fileName);
        assertEquals(-1, s.getRetainChunk());
        s.setRetainChunk(0);
        assertEquals(0, s.getRetainChunk());
        assertEquals(1, s.getCurrentVersion());
        assertFalse(s.hasUnsavedChanges());
        MVMap<String, String> m = s.openMap("data", String.class, String.class);
        assertTrue(s.hasUnsavedChanges());
        MVMap<String, String> m0 = s.openMap("data0", String.class, String.class);
        m.put("1", "Hello");
        assertEquals(1, s.commit());
        s.rollbackTo(1);
        assertEquals("Hello", m.get("1"));
        long v2 = s.store();
        assertEquals(2, v2);
        assertEquals(3, s.getCurrentVersion());
        assertFalse(s.hasUnsavedChanges());
        s.close();

        s = openStore(fileName);
        assertEquals(3, s.getCurrentVersion());
        s.setRetainChunk(0);
        meta = s.getMetaMap();
        m = s.openMap("data", String.class, String.class);
        m0 = s.openMap("data0", String.class, String.class);
        MVMap<String, String> m1 = s.openMap("data1", String.class, String.class);
        m.put("1", "Hallo");
        m0.put("1", "Hallo");
        m1.put("1", "Hallo");
        assertEquals("Hallo", m.get("1"));
        assertEquals("Hallo", m1.get("1"));
        assertTrue(s.hasUnsavedChanges());
        s.rollbackTo(v2);
        assertFalse(s.hasUnsavedChanges());
        assertNull(meta.get("map.data1"));
        assertNull(m0.get("1"));
        assertEquals("Hello", m.get("1"));
        s.store();
        s.close();

        s = openStore(fileName);
        s.setRetainChunk(0);
        assertEquals(3, s.getCurrentVersion());
        meta = s.getMetaMap();
        assertTrue(meta.get("map.data") != null);
        assertTrue(meta.get("map.data0") != null);
        assertNull(meta.get("map.data1"));
        m = s.openMap("data", String.class, String.class);
        m0 = s.openMap("data0", String.class, String.class);
        assertNull(m0.get("1"));
        assertEquals("Hello", m.get("1"));
        assertFalse(m0.isReadOnly());
        m.put("1",  "Hallo");
        s.commit();
        assertEquals(4, s.getCurrentVersion());
        long v4 = s.store();
        assertEquals(4, v4);
        assertEquals(5, s.getCurrentVersion());
        s.close();

        s = openStore(fileName);
        s.setRetainChunk(0);
        m = s.openMap("data", String.class, String.class);
        m.put("1",  "Hello");
        s.store();
        s.close();

        s = openStore(fileName);
        s.setRetainChunk(0);
        m = s.openMap("data", String.class, String.class);
        assertEquals("Hello", m.get("1"));
        s.rollbackTo(v4);
        assertEquals("Hallo", m.get("1"));
        s.close();

        s = openStore(fileName);
        s.setRetainChunk(0);
        m = s.openMap("data", String.class, String.class);
        assertEquals("Hallo", m.get("1"));
        s.close();
    }

    private void testRollbackInMemory() {
        String fileName = getBaseDir() + "/testRollback.h3";
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        assertEquals(1, s.getCurrentVersion());
        s.setMaxPageSize(5);
        MVMap<String, String> m = s.openMap("data", String.class, String.class);
        s.rollbackTo(0);
        assertTrue(m.isClosed());
        assertEquals(1, s.getCurrentVersion());
        m = s.openMap("data", String.class, String.class);

        MVMap<String, String> m0 = s.openMap("data0", String.class, String.class);
        MVMap<String, String> m2 = s.openMap("data2", String.class, String.class);
        m.put("1", "Hello");
        for (int i = 0; i < 10; i++) {
            m2.put("" + i, "Test");
        }
        long v1 = s.commit();
        assertEquals(1, v1);
        assertEquals(2, s.getCurrentVersion());
        MVMap<String, String> m1 = s.openMap("data1", String.class, String.class);
        assertEquals("Test", m2.get("1"));
        m.put("1", "Hallo");
        m0.put("1", "Hallo");
        m1.put("1", "Hallo");
        m2.clear();
        assertEquals("Hallo", m.get("1"));
        assertEquals("Hallo", m1.get("1"));
        s.rollbackTo(v1);
        assertEquals(2, s.getCurrentVersion());
        for (int i = 0; i < 10; i++) {
            assertEquals("Test", m2.get("" + i));
        }
        assertEquals("Hello", m.get("1"));
        assertNull(m0.get("1"));
        assertTrue(m1.isClosed());
        assertFalse(m0.isReadOnly());
        assertTrue(m1.isReadOnly());
        s.close();
    }

    private void testMeta() {
        String fileName = getBaseDir() + "/testMeta.h3";
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        MVMap<String, String> m = s.getMetaMap();
        s.setRetainChunk(0);
        MVMap<String, String> data = s.openMap("data", String.class, String.class);
        data.put("1", "Hello");
        data.put("2", "World");
        s.store();
        assertEquals("1/1///", m.get("map.data"));
        assertTrue(m.containsKey("chunk.1"));
        data.put("1", "Hallo");
        s.store();
        assertEquals("1/1///", m.get("map.data"));
        assertTrue(m.get("root.1").length() > 0);
        assertTrue(m.containsKey("chunk.1"));
        assertTrue(m.containsKey("chunk.2"));
        s.rollbackTo(1);
        assertTrue(m.containsKey("chunk.1"));
        assertFalse(m.containsKey("chunk.2"));
        s.close();
    }

    private void testInMemory() {
        for (int j = 0; j < 1; j++) {
            MVStore s = openStore(null);
            // s.setMaxPageSize(10);
            // long t;
            int len = 100;
            // TreeMap<Integer, String> m = new TreeMap<Integer, String>();
            // HashMap<Integer, String> m = New.hashMap();
            MVMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
            // t = System.currentTimeMillis();
            for (int i = 0; i < len; i++) {
                m.put(i, "Hello World");
            }
            // System.out.println("put: " + (System.currentTimeMillis() - t));
            // t = System.currentTimeMillis();
            for (int i = 0; i < len; i++) {
                assertEquals("Hello World", m.get(i));
            }
            // System.out.println("get: " + (System.currentTimeMillis() - t));
            // t = System.currentTimeMillis();
            for (int i = 0; i < len; i++) {
                assertEquals("Hello World", m.remove(i));
            }
            // System.out.println("remove: " + (System.currentTimeMillis() - t));
            // System.out.println();
            assertEquals(null, m.get(0));
            assertEquals(0, m.size());
            s.close();
        }
    }

    private void testLargeImport() {
        String fileName = getBaseDir() + "/testImport.h3";
        int len = 1000;
        for (int j = 0; j < 5; j++) {
            FileUtils.delete(fileName);
            MVStore s = openStore(fileName);
            // s.setCompressor(null);
            s.setMaxPageSize(40);
            MVMap<Integer, Object[]> m = s.openMap("data", "", "i", "r(i,,)");
            // Profiler prof = new Profiler();
            // prof.startCollecting();
            // long t = System.currentTimeMillis();
            for (int i = 0; i < len;) {
                Object[] o = new Object[3];
                o[0] = i;
                o[1] = "Hello World";
                o[2] = "World";
                m.put(i, o);
                i++;
                if (i % 10000 == 0) {
                    s.store();
                }
            }
            s.store();
            s.close();
            // System.out.println(prof.getTop(5));
            // System.out.println("store time " + (System.currentTimeMillis() - t));
            // System.out.println("store size " + FileUtils.size(fileName));
        }
    }

    private void testBtreeStore() {
        String fileName = getBaseDir() + "/testBtreeStore.h3";
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        s.close();
        s = openStore(fileName);
        MVMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
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
        assertEquals("hello 0", m.remove(0));
        assertNull(m.get(0));
        for (int i = 1; i < count; i++) {
            assertEquals("hello " + i, m.get(i));
        }
        s.store();
        s.close();
        s = openStore(fileName);
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
            MVStore s = openStore(fileName);
            MVMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
            for (int i = 0; i < 10; i++) {
                m.put(j + i, "Hello " + j);
            }
            s.store();
            s.compact(80);
            s.close();
            long len = FileUtils.size(fileName);
            // System.out.println("   len:" + len);
            if (initialLength == 0) {
                initialLength = len;
            } else {
                assertTrue("initial: " + initialLength + " len: " + len, len <= initialLength * 3);
            }
        }
        // long len = FileUtils.size(fileName);
        // System.out.println("len0: " + len);
        MVStore s = openStore(fileName);
        MVMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
        for (int i = 0; i < 100; i++) {
            m.remove(i);
        }
        s.store();
        s.compact(80);
        s.close();
        // len = FileUtils.size(fileName);
        // System.out.println("len1: " + len);
        s = openStore(fileName);
        m = s.openMap("data", Integer.class, String.class);
        s.compact(80);
        s.close();
        // len = FileUtils.size(fileName);
        // System.out.println("len2: " + len);
    }

    private void testReuseSpace() {
        String fileName = getBaseDir() + "/testReuseSpace.h3";
        FileUtils.delete(fileName);
        long initialLength = 0;
        for (int j = 0; j < 20; j++) {
            MVStore s = openStore(fileName);
            MVMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
            for (int i = 0; i < 10; i++) {
                m.put(i, "Hello");
            }
            s.store();
            for (int i = 0; i < 10; i++) {
                assertEquals("Hello", m.get(i));
                assertEquals("Hello", m.remove(i));
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
        MVStore s = openStore(fileName);
        MVMap<Integer, Integer> m = s.openMap("data", Integer.class, Integer.class);
        TreeMap<Integer, Integer> map = new TreeMap<Integer, Integer>();
        Random r = new Random(1);
        int operationCount = 1000;
        int maxValue = 30;
        for (int i = 0; i < operationCount; i++) {
            int k = r.nextInt(maxValue);
            int v = r.nextInt();
            boolean compareAll;
            switch (r.nextInt(3)) {
            case 0:
                log(i + ": put " + k + " = " + v);
                m.put(k, v);
                map.put(k, v);
                compareAll = true;
                break;
            case 1:
                log(i + ": remove " + k);
                Integer expected = map.remove(k);
                Integer got = m.remove(k);
                if (expected == null) {
                    assertNull(got);
                } else {
                    assertEquals(expected, got);
                }
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
                Iterator<Integer> itExpected = map.keySet().iterator();
                while (itExpected.hasNext()) {
                    assertTrue(it.hasNext());
                    Integer expected = itExpected.next();
                    Integer got = it.next();
                    assertEquals(expected, got);
                }
                assertFalse(it.hasNext());
            }
        }
        s.close();
    }

    private void testKeyValueClasses() {
        String fileName = getBaseDir() + "/testKeyValueClasses.h3";
        FileUtils.delete(fileName);
        MVStore s = openStore(fileName);
        MVMap<Integer, String> is = s.openMap("intString", Integer.class, String.class);
        is.put(1, "Hello");
        MVMap<Integer, Integer> ii = s.openMap("intInt", Integer.class, Integer.class);
        ii.put(1, 10);
        MVMap<String, Integer> si = s.openMap("stringInt", String.class, Integer.class);
        si.put("Test", 10);
        MVMap<String, String> ss = s.openMap("stringString", String.class, String.class);
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
        s.store();
        s.close();
        s = openStore(fileName);
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
        MVStore s = openStore(fileName);
        MVMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
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
        MVStore s = openStore(fileName);
        MVMap<Integer, String> m = s.openMap("data", Integer.class, String.class);
        for (int i = 0; i < 3; i++) {
            m.put(i, "hello " + i);
        }
        s.store();
        assertEquals("hello 0", m.remove(0));

        assertNull(m.get(0));
        for (int i = 1; i < 3; i++) {
            assertEquals("hello " + i, m.get(i));
        }
        s.store();
        s.close();

        s = openStore(fileName);
        m = s.openMap("data", Integer.class, String.class);
        assertNull(m.get(0));
        for (int i = 1; i < 3; i++) {
            assertEquals("hello " + i, m.get(i));
        }
        s.close();
    }

    protected static MVStore openStore(String fileName) {
        MVStore store = MVStore.open(fileName, new TestMapFactory());
        store.setMaxPageSize(10);
        return store;
    }

    /**
     * Log the message.
     *
     * @param msg the message
     */
    protected static void log(String msg) {
        // System.out.println(msg);
    }

}
