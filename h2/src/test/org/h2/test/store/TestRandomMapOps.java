/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestAll;
import org.h2.test.TestBase;

/**
 * Tests the MVStore.
 */
public class TestRandomMapOps extends TestBase {

    private static final boolean LOG = false;
    private final Random r = new Random();
    private int op;


    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        TestAll config = test.config;
        config.big = true;
//        config.memory = true;

        test.println(config.toString());
        for (int i = 0; i < 10; i++) {
            test.testFromMain();
            test.println("Done pass #" + i);
        }
    }

    @Override
    public void test() throws Exception {
        if (config.memory) {
            testMap(null);
        } else {
            String fileName = "memFS:" + getTestName();
            testMap(fileName);
        }
    }

    private void testMap(String fileName) {
        int size = getSize(500, 3000);
        long seed = 0;
//        seed = System.currentTimeMillis();
//        seed = -3407210256209708616L;
        for (int cnt = 0; cnt < 100; cnt++) {
            try {
                testOps(fileName, size, seed);
            } catch (Exception | AssertionError ex) {
                println("seed:" + seed + " op:" + op + " " + ex);
                throw ex;
            } finally {
                if (fileName != null) {
                    FileUtils.delete(fileName);
                }
            }
            seed = r.nextLong();
        }
    }

    private void testOps(String fileName, int loopCount, long seed) {
        r.setSeed(seed);
        op = 0;
        MVStore s = openStore(fileName);
        int keysPerPage = s.getKeysPerPage();
        int keyRange = 2000;
        MVMap<Integer, String> m = s.openMap("data");
        TreeMap<Integer, String> map = new TreeMap<>();
        int[] recentKeys = new int[2 * keysPerPage];
        for (; op < loopCount; op++) {
            int k = r.nextInt(3 * keyRange / 2);
            if (k >= keyRange) {
                k = recentKeys[k  % recentKeys.length];
            } else {
                recentKeys[op % recentKeys.length] = k;
            }
            String v = k + "_Value_" + op;
            int type = r.nextInt(15);
            switch (type) {
            case 0:
            case 1:
            case 2:
            case 3:
                log(op, k, v, "m.put({0}, {1})");
                m.put(k, v);
                map.put(k, v);
                break;
            case 4:
            case 5:
                log(op, k, v, "m.remove({0})");
                m.remove(k);
                map.remove(k);
                break;
            case 6:
                log(op, k, v, "s.compact(90, 1024)");
                s.compact(90, 1024);
                break;
            case 7:
                if (op % 64 == 0) {
                    log(op, k, v, "m.clear()");
                    m.clear();
                    map.clear();
                }
                break;
            case 8:
                log(op, k, v, "s.commit()");
                s.commit();
                break;
            case 9:
                if (fileName != null) {
                    log(op, k, v, "s.commit()");
                    s.commit();
                    log(op, k, v, "s.close()");
                    s.close();
                    log(op, k, v, "s = openStore(fileName)");
                    s = openStore(fileName);
                    log(op, k, v, "m = s.openMap(\"data\")");
                    m = s.openMap("data");
                }
                break;
            case 10:
                log(op, k, v, "s.commit()");
                s.commit();
                log(op, k, v, "s.compactFile(0)");
                s.compactFile(0);
                break;
            case 11: {
                int rangeSize = r.nextInt(2 * keysPerPage);
                int step = r.nextBoolean() ? 1 : -1;
                for (int i = 0; i < rangeSize; i++) {
                    log(op, k, v, "m.put({0}, {1})");
                    m.put(k, v);
                    map.put(k, v);
                    k += step;
                    v = k + "_Value_" + op;
                }
                break;
            }
            case 12: {
                int rangeSize = r.nextInt(2 * keysPerPage);
                int step = r.nextBoolean() ? 1 : -1;
                for (int i = 0; i < rangeSize; i++) {
                    log(op, k, v, "m.remove({0})");
                    m.remove(k);
                    map.remove(k);
                    k += step;
                }
                break;
            }
            default:
                log(op, k, v, "m.getKeyIndex({0})");
                ArrayList<Integer> keyList = new ArrayList<>(map.keySet());
                int index = Collections.binarySearch(keyList, k, null);
                int index2 = (int) m.getKeyIndex(k);
                assertEquals(index, index2);
                if (index >= 0) {
                    int k2 = m.getKey(index);
                    assertEquals(k2, k);
                }
                break;
            }
            assertEquals(map.get(k), m.get(k));
            assertEquals(map.ceilingKey(k), m.ceilingKey(k));
            assertEquals(map.floorKey(k), m.floorKey(k));
            assertEquals(map.higherKey(k), m.higherKey(k));
            assertEquals(map.lowerKey(k), m.lowerKey(k));
            assertEquals(map.isEmpty(), m.isEmpty());
            assertEquals(map.size(), m.size());
            if (!map.isEmpty()) {
                assertEquals(map.firstKey(), m.firstKey());
                assertEquals(map.lastKey(), m.lastKey());
            }

            int from = r.nextBoolean() ? r.nextInt(keyRange) : k + r.nextInt(2 * keysPerPage) - keysPerPage;
            int to = r.nextBoolean() ? r.nextInt(keyRange) : from + r.nextInt(2 * keysPerPage) - keysPerPage;

            Cursor<Integer, String> cursor;
            Collection<Map.Entry<Integer, String>> entrySet;
            String msg;
            if (from <= to) {
                msg = "(" + from + ", null)";
                cursor = m.cursor(from, null, false);
                entrySet = map.tailMap(from).entrySet();
                assertEquals(msg, entrySet, cursor);

                msg = "(null, " + from + ")";
                cursor = m.cursor(null, from, false);
                entrySet = map.headMap(from + 1).entrySet();
                assertEquals(msg, entrySet, cursor);

                msg = "(" + from + ", " + to + ")";
                cursor = m.cursor(from, to, false);
                entrySet = map.subMap(from, to + 1).entrySet();
                assertEquals(msg, entrySet, cursor);
            }

            if (from >= to) {
                msg = "rev (" + from + ", null)";
                cursor = m.cursor(from, null, true);
                entrySet = reverse(map.headMap(from + 1).entrySet());
                assertEquals(msg, entrySet, cursor);

                msg = "rev (null, "+from+")";
                cursor = m.cursor(null, from, true);
                entrySet = reverse(map.tailMap(from).entrySet());
                assertEquals(msg, entrySet, cursor);

                msg = "rev (" + from + ", " + to + ")";
                cursor = m.cursor(from, to, true);
                entrySet = reverse(map.subMap(to, from + 1).entrySet());
                assertEquals(msg, entrySet, cursor);
            }
        }
        s.close();
    }

    private static <K,V> Collection<Map.Entry<K,V>> reverse(Collection<Map.Entry<K,V>> entrySet) {
        ArrayList<Map.Entry<K,V>> list = new ArrayList<>(entrySet);
        Collections.reverse(list);
        entrySet = list;
        return entrySet;
    }

    private <K,V> void assertEquals(String msg, Iterable<Map.Entry<K, V>> entrySet, Cursor<K, V> cursor) {
        int cnt = 0;
        for (Map.Entry<K,V> entry : entrySet) {
            String message = msg + " " + cnt;
            assertTrue(message, cursor.hasNext());
            assertEquals(message, entry.getKey(), cursor.next());
            assertEquals(message, entry.getKey(), cursor.getKey());
            assertEquals(message, entry.getValue(), cursor.getValue());
            ++cnt;
        }
        assertFalse(msg, cursor.hasNext());
    }

    public void assertEquals(String message, Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            fail(message + " expected: " + expected + " actual: " + actual);
        }
    }

    private static MVStore openStore(String fileName) {
        MVStore s = new MVStore.Builder().fileName(fileName)
                .keysPerPage(7).autoCommitDisabled().open();
        s.setRetentionTime(1000);
        return s;
    }

    /**
     * Log the operation
     *
     * @param op the operation id
     * @param k the key
     * @param v the value
     * @param msg the message
     */
    private static void log(int op, int k, String v, String msg) {
        if (LOG) {
            msg = MessageFormat.format(msg, k, v);
            System.out.println(msg + "; // op " + op);
        }
    }

}
