/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.util.concurrent.TimeUnit;

import org.h2.pagestore.db.LobStorageBackend;
import org.h2.store.Data;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.test.TestBase;
import org.h2.util.LegacyDateTimeUtils;
import org.h2.util.SmallLRUCache;
import org.h2.util.TempFileDeleter;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueChar;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInteger;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueReal;
import org.h2.value.ValueSmallint;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueTinyint;
import org.h2.value.ValueUuid;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;
import org.h2.value.ValueVarcharIgnoreCase;

/**
 * Data page tests.
 */
public class TestDataPage extends TestBase implements DataHandler {

    private boolean testPerformance;
    private final CompareMode compareMode = CompareMode.getInstance(null, 0);

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() {
        if (testPerformance) {
            testPerformance();
            System.exit(0);
            return;
        }
        testValues();
        testAll();
    }

    private static void testPerformance() {
        Data data = Data.create(null, 1024);
        for (int j = 0; j < 4; j++) {
            long time = System.nanoTime();
            for (int i = 0; i < 100000; i++) {
                data.reset();
                for (int k = 0; k < 30; k++) {
                    data.writeString("Hello World");
                }
            }
            //            for (int i = 0; i < 5000000; i++) {
            //                data.reset();
            //                for (int k = 0; k < 100; k++) {
            //                    data.writeInt(k * k);
            //                }
            //            }
            //            for (int i = 0; i < 200000; i++) {
            //                data.reset();
            //                for (int k = 0; k < 100; k++) {
            //                    data.writeVarInt(k * k);
            //                }
            //            }
            System.out.println("write: " +
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time) +
                    " ms");
        }
        for (int j = 0; j < 4; j++) {
            long time = System.nanoTime();
            for (int i = 0; i < 1000000; i++) {
                data.reset();
                for (int k = 0; k < 30; k++) {
                    data.readString();
                }
            }
            //            for (int i = 0; i < 3000000; i++) {
            //                data.reset();
            //                for (int k = 0; k < 100; k++) {
            //                    data.readVarInt();
            //                }
            //            }
            //            for (int i = 0; i < 50000000; i++) {
            //                data.reset();
            //                for (int k = 0; k < 100; k++) {
            //                    data.readInt();
            //                }
            //            }
            System.out.println("read: " +
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time) +
                    " ms");
        }
    }

    private void testValues() {
        testValue(ValueNull.INSTANCE);
        testValue(ValueBoolean.FALSE);
        testValue(ValueBoolean.TRUE);
        for (int i = 0; i < 256; i++) {
            testValue(ValueTinyint.get((byte) i));
        }
        for (int i = 0; i < 256 * 256; i += 10) {
            testValue(ValueSmallint.get((short) i));
        }
        for (int i = 0; i < 256 * 256; i += 10) {
            testValue(ValueInteger.get(i));
            testValue(ValueInteger.get(-i));
            testValue(ValueBigint.get(i));
            testValue(ValueBigint.get(-i));
        }
        testValue(ValueInteger.get(Integer.MAX_VALUE));
        testValue(ValueInteger.get(Integer.MIN_VALUE));
        for (long i = 0; i < Integer.MAX_VALUE; i += 10 + i / 4) {
            testValue(ValueInteger.get((int) i));
            testValue(ValueInteger.get((int) -i));
        }
        testValue(ValueBigint.get(Long.MAX_VALUE));
        testValue(ValueBigint.get(Long.MIN_VALUE));
        for (long i = 0; i >= 0; i += 10 + i / 4) {
            testValue(ValueBigint.get(i));
            testValue(ValueBigint.get(-i));
        }
        testValue(ValueNumeric.get(BigDecimal.ZERO));
        testValue(ValueNumeric.get(BigDecimal.ONE));
        testValue(ValueNumeric.get(BigDecimal.TEN));
        testValue(ValueNumeric.get(BigDecimal.ONE.negate()));
        testValue(ValueNumeric.get(BigDecimal.TEN.negate()));
        for (long i = 0; i >= 0; i += 10 + i / 4) {
            testValue(ValueNumeric.get(new BigDecimal(i)));
            testValue(ValueNumeric.get(new BigDecimal(-i)));
            for (int j = 0; j < 200; j += 50) {
                testValue(ValueNumeric.get(new BigDecimal(i).setScale(j)));
                testValue(ValueNumeric.get(new BigDecimal(i * i).setScale(j)));
            }
            testValue(ValueNumeric.get(new BigDecimal(i * i)));
        }
        testValue(LegacyDateTimeUtils.fromDate(null, null, new Date(System.currentTimeMillis())));
        testValue(LegacyDateTimeUtils.fromDate(null, null, new Date(0)));
        testValue(LegacyDateTimeUtils.fromTime(null, null, new Time(System.currentTimeMillis())));
        testValue(LegacyDateTimeUtils.fromTime(null, null, new Time(0)));
        testValue(LegacyDateTimeUtils.fromTimestamp(null, System.currentTimeMillis(), 0));
        testValue(LegacyDateTimeUtils.fromTimestamp(null, 0L, 0));
        testValue(ValueTimestampTimeZone.parse("2000-01-01 10:00:00+00", null));
        testValue(ValueJavaObject.getNoCopy(new byte[0]));
        testValue(ValueJavaObject.getNoCopy(new byte[100]));
        for (int i = 0; i < 300; i++) {
            testValue(ValueVarbinary.getNoCopy(new byte[i]));
        }
        for (int i = 0; i < 65000; i += 10 + i) {
            testValue(ValueVarbinary.getNoCopy(new byte[i]));
        }
        testValue(ValueUuid.getNewRandom());
        for (int i = 0; i < 100; i++) {
            testValue(ValueVarchar.get(new String(new char[i])));
        }
        for (int i = 0; i < 65000; i += 10 + i) {
            testValue(ValueVarchar.get(new String(new char[i])));
            testValue(ValueChar.get(new String(new char[i])));
            testValue(ValueVarcharIgnoreCase.get(new String(new char[i])));
        }
        testValue(ValueReal.get(0f));
        testValue(ValueReal.get(1f));
        testValue(ValueReal.get(-1f));
        testValue(ValueDouble.get(0));
        testValue(ValueDouble.get(1));
        testValue(ValueDouble.get(-1));
        for (int i = 0; i < 65000; i += 10 + i) {
            for (double j = 0.1; j < 65000; j += 10 + j) {
                testValue(ValueReal.get((float) (i / j)));
                testValue(ValueDouble.get(i / j));
                testValue(ValueReal.get((float) -(i / j)));
                testValue(ValueDouble.get(-(i / j)));
            }
        }
        testValue(ValueArray.get(new Value[0], null));
        testValue(ValueArray.get(new Value[] { ValueInteger.get(-20), ValueInteger.get(10) }, null));
    }

    private void testValue(Value v) {
        Data data = Data.create(null, 1024);
        data.checkCapacity((int) v.getType().getPrecision());
        data.writeValue(v);
        data.writeInt(123);
        data.reset();
        Value v2 = data.readValue(v.getType());
        assertEquals(v.getValueType(), v2.getValueType());
        assertEquals(0, v.compareTo(v2, null, compareMode));
        assertEquals(123, data.readInt());
    }

    private void testAll() {
        Data page = Data.create(this, 128);

        char[] data = new char[0x10000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (char) i;
        }
        String s = new String(data);
        page.checkCapacity(s.length() * 4);
        page.writeString(s);
        int len = page.length();
        assertEquals(len, Data.getStringLen(s));
        page.reset();
        assertEquals(s, page.readString());
        page.reset();

        page.writeString("H\u1111!");
        page.writeString("John\tBrack's \"how are you\" M\u1111ller");
        page.writeValue(ValueInteger.get(10));
        page.writeValue(ValueVarchar.get("test"));
        page.writeValue(ValueReal.get(-2.25f));
        page.writeValue(ValueDouble.get(10.40));
        page.writeValue(ValueNull.INSTANCE);
        trace(new String(page.getBytes()));
        page.reset();

        trace(page.readString());
        trace(page.readString());
        trace(page.readValue(TypeInfo.TYPE_INTEGER).getInt());
        trace(page.readValue(TypeInfo.TYPE_VARCHAR).getString());
        trace("" + page.readValue(TypeInfo.TYPE_REAL).getFloat());
        trace("" + page.readValue(TypeInfo.TYPE_DOUBLE).getDouble());
        trace(page.readValue(TypeInfo.TYPE_VARCHAR).toString());
        page.reset();

        page.writeInt(0);
        page.writeInt(Integer.MAX_VALUE);
        page.writeInt(Integer.MIN_VALUE);
        page.writeInt(1);
        page.writeInt(-1);
        page.writeInt(1234567890);
        page.writeInt(54321);
        trace(new String(page.getBytes()));
        page.reset();
        trace(page.readInt());
        trace(page.readInt());
        trace(page.readInt());
        trace(page.readInt());
        trace(page.readInt());
        trace(page.readInt());
        trace(page.readInt());

        page = null;
    }

    @Override
    public String getDatabasePath() {
        return null;
    }

    @Override
    public FileStore openFile(String name, String mode, boolean mustExist) {
        return null;
    }

    @Override
    public void checkPowerOff() {
        // nothing to do
    }

    @Override
    public void checkWritingAllowed() {
        // ok
    }

    @Override
    public int getMaxLengthInplaceLob() {
        throw new AssertionError();
    }

    @Override
    public String getLobCompressionAlgorithm(int type) {
        throw new AssertionError();
    }

    @Override
    public Object getLobSyncObject() {
        return this;
    }

    @Override
    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return null;
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

    @Override
    public LobStorageBackend getLobStorage() {
        return null;
    }

    @Override
    public int readLob(long lobId, byte[] hmac, long offset, byte[] buff,
            int off, int length) {
        return -1;
    }

    @Override
    public CompareMode getCompareMode() {
        return compareMode;
    }
}
