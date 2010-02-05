/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Random;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.test.TestBase;
import org.h2.util.New;
import org.h2.util.SmallLRUCache;
import org.h2.util.TempFileDeleter;
import org.h2.util.ValueHashMap;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInt;

/**
 * Tests the value hash map.
 */
public class TestValueHashMap extends TestBase implements DataHandler {

    CompareMode compareMode = CompareMode.getInstance(null, 0);

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        testNotANumber();
        testRandomized();
    }

    private void testNotANumber() throws SQLException {
        ValueHashMap<Integer> map = ValueHashMap.newInstance(this);
        for (int i = 1; i < 100; i++) {
            double d = Double.longBitsToDouble(0x7ff0000000000000L | i);
            ValueDouble v = ValueDouble.get(d);
            map.put(v, null);
            assertEquals(1, map.size());
        }
    }

    private void testRandomized() throws SQLException {
        ValueHashMap<Value> map = ValueHashMap.newInstance(this);
        HashMap<Value, Value> hash = New.hashMap();
        Random random = new Random(1);
        Comparator<Value> vc = new Comparator<Value>() {
            public int compare(Value v1, Value v2) {
                try {
                    return v1.compareTo(v2, compareMode);
                } catch (SQLException e) {
                    throw new AssertionError(e);
                }
            }
        };
        for (int i = 0; i < 10000; i++) {
            int op = random.nextInt(10);
            Value key = ValueInt.get(random.nextInt(100));
            Value value = ValueInt.get(random.nextInt(100));
            switch (op) {
            case 0:
                map.put(key, value);
                hash.put(key, value);
                break;
            case 1:
                map.remove(key);
                hash.remove(key);
                break;
            case 2:
                Value v1 = map.get(key);
                Value v2 = hash.get(key);
                assertTrue(v1 == null ? v2 == null : v1.compareEqual(v2));
                break;
            case 3: {
                ArrayList<Value> a1 = map.keys();
                ArrayList<Value> a2 = New.arrayList(hash.keySet());
                assertEquals(a1.size(), a2.size());
                Collections.sort(a1, vc);
                Collections.sort(a2, vc);
                for (int j = 0; j < a1.size(); j++) {
                    assertTrue(a1.get(j).compareEqual(a2.get(j)));
                }
                break;
            }
            case 4:
                ArrayList<Value> a1 = map.values();
                ArrayList<Value> a2 = New.arrayList(hash.values());
                assertEquals(a1.size(), a2.size());
                Collections.sort(a1, vc);
                Collections.sort(a2, vc);
                for (int j = 0; j < a1.size(); j++) {
                    assertTrue(a1.get(j).compareEqual(a2.get(j)));
                }
                break;
            default:
            }
        }
    }

    public String getDatabasePath() {
        return null;
    }

    public FileStore openFile(String name, String mode, boolean mustExist) {
        return null;
    }

    public int getChecksum(byte[] data, int s, int e) {
        return 0;
    }

    public void checkPowerOff() {
        // nothing to do
    }

    public void checkWritingAllowed() {
        // nothing to do
    }

    public void freeUpDiskSpace() {
        // nothing to do
    }

    public int compareTypeSave(Value a, Value b) throws SQLException {
        return a.compareTo(b, compareMode);
    }

    public int getMaxLengthInplaceLob() {
        return 0;
    }

    public String getLobCompressionAlgorithm(int type) {
        return null;
    }

    public Object getLobSyncObject() {
        return this;
    }

    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return null;
    }

    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

}
