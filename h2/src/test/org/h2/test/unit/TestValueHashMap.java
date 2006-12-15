/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Random;

import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.test.TestBase;
import org.h2.util.ObjectArray;
import org.h2.util.ValueHashMap;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueInt;

public class TestValueHashMap extends TestBase implements DataHandler {
    
    CompareMode compareMode = new CompareMode(null, null);

    public void test() throws Exception {
        ValueHashMap map = new ValueHashMap(this);
        HashMap hash = new HashMap();
        Random random = new Random(1);           
        Comparator vc = new Comparator() {
            public int compare(Object o1, Object o2) {
                Value v1 = (Value)o1;
                Value v2 = (Value)o2;
                try {
                    return v1.compareTo(v2, compareMode);
                } catch(SQLException e) {
                    throw new Error(e);
                }
            }
        };
        for(int i=0; i<10000; i++) {
            int op = random.nextInt(10);
            Value key = ValueInt.get(random.nextInt(100));
            Value value = ValueInt.get(random.nextInt(100));
            switch(op) {
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
                Value v2 = (Value) hash.get(key);
                check((v1==null && v2==null) || v1.compareEqual(v2));
                break;
            case 3: {
                ObjectArray a1 = map.keys();
                ObjectArray a2 = new ObjectArray(hash.keySet());
                check(a1.size(), a2.size());
                a1.sort(vc);
                a2.sort(vc);
                for(int j=0; j<a1.size(); j++) {
                    check(((Value)a1.get(j)).compareEqual((Value)a2.get(j)));
                }
                break;
            }
            case 4:
                ObjectArray a1 = map.values();
                ObjectArray a2 = new ObjectArray(hash.values());
                check(a1.size(), a2.size());
                a1.sort(vc);
                a2.sort(vc);
                for(int j=0; j<a1.size(); j++) {
                    check(((Value)a1.get(j)).compareEqual((Value)a2.get(j)));
                }
                break;
            }
        }
    }

    public boolean getTextStorage() {
        return false;
    }

    public String getDatabasePath() {
        return null;
    }

    public FileStore openFile(String name, boolean mustExist) throws SQLException {
        return null;
    }

    public int getChecksum(byte[] data, int start, int end) {
        return 0;
    }

    public void checkPowerOff() throws SQLException {
    }

    public void checkWritingAllowed() throws SQLException {
    }

    public void freeUpDiskSpace() throws SQLException {
    }

    public void handleInvalidChecksum() throws SQLException {
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

    public String createTempFile() throws SQLException {
        return null;
    }

    public String getLobCompressionAlgorithm(int type) {
        return null;
    }

}
