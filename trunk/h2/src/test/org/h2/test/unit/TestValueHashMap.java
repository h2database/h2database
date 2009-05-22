/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Random;

import org.h2.constant.SysProperties;
import org.h2.message.Trace;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.test.TestBase;
import org.h2.util.New;
import org.h2.util.ObjectArray;
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

    CompareMode compareMode = new CompareMode(null, 0);

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        testNotANumber();
        testRandomized();
    }

    private void testNotANumber() throws SQLException {
        ValueHashMap map = new ValueHashMap(this);
        for (int i = 1; i < 100; i++) {
            double d = Double.longBitsToDouble(0x7ff0000000000000L | i);
            ValueDouble v = ValueDouble.get(d);
            map.put(v, null);
            assertEquals(1, map.size());
        }
    }

    private void testRandomized() throws SQLException {
        ValueHashMap map = new ValueHashMap(this);
        HashMap<Value, Value> hash = New.hashMap();
        Random random = new Random(1);
        Comparator<Value> vc = new Comparator<Value>() {
            public int compare(Value v1, Value v2) {
                try {
                    return v1.compareTo(v2, compareMode);
                } catch (SQLException e) {
                    throw new Error(e);
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
                Value v1 = (Value) map.get(key);
                Value v2 = hash.get(key);
                assertTrue((v1 == null && v2 == null) || v1.compareEqual(v2));
                break;
            case 3: {
                ObjectArray a1 = map.keys();
                ObjectArray a2 = new ObjectArray(hash.keySet());
                assertEquals(a1.size(), a2.size());
                a1.sort(vc);
                a2.sort(vc);
                for (int j = 0; j < a1.size(); j++) {
                    assertTrue(((Value) a1.get(j)).compareEqual((Value) a2.get(j)));
                }
                break;
            }
            case 4:
                ObjectArray a1 = map.values();
                ObjectArray a2 = new ObjectArray(hash.values());
                assertEquals(a1.size(), a2.size());
                a1.sort(vc);
                a2.sort(vc);
                for (int j = 0; j < a1.size(); j++) {
                    assertTrue(((Value) a1.get(j)).compareEqual((Value) a2.get(j)));
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

    public int getChecksum(byte[] data, int start, int end) {
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

    public void handleInvalidChecksum() {
        // nothing to do
    }

    public int compareTypeSave(Value a, Value b) throws SQLException {
        return a.compareTo(b, compareMode);
    }

    public int getMaxLengthInplaceLob() {
        return 0;
    }

    public int allocateObjectId(boolean b, boolean c) {
        return 0;
    }

    public String createTempFile() {
        return null;
    }

    public String getLobCompressionAlgorithm(int type) {
        return null;
    }

    public Object getLobSyncObject() {
        return this;
    }

    public boolean getLobFilesInDirectories() {
        return SysProperties.LOB_FILES_IN_DIRECTORIES;
    }

    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return null;
    }

    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

    public Trace getTrace() {
        return null;
    }

}
