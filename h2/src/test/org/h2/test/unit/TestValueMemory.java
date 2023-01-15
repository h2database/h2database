/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Random;
import org.h2.api.IntervalQualifier;
import org.h2.engine.Constants;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.LobStorageInterface;
import org.h2.test.TestBase;
import org.h2.test.utils.MemoryFootprint;
import org.h2.util.DateTimeUtils;
import org.h2.util.SmallLRUCache;
import org.h2.util.TempFileDeleter;
import org.h2.util.Utils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBinary;
import org.h2.value.ValueBlob;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueChar;
import org.h2.value.ValueClob;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecfloat;
import org.h2.value.ValueDouble;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueInteger;
import org.h2.value.ValueInterval;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueJson;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueReal;
import org.h2.value.ValueRow;
import org.h2.value.ValueSmallint;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueTinyint;
import org.h2.value.ValueUuid;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;
import org.h2.value.ValueVarcharIgnoreCase;

/**
 * Tests the memory consumption of values. Values can estimate how much memory
 * they occupy, and this tests if this estimation is correct.
 */
public class TestValueMemory extends TestBase implements DataHandler {

    private static final long MIN_ABSOLUTE_DAY = DateTimeUtils.absoluteDayFromDateValue(DateTimeUtils.MIN_DATE_VALUE);

    private static final long MAX_ABSOLUTE_DAY = DateTimeUtils.absoluteDayFromDateValue(DateTimeUtils.MAX_DATE_VALUE);

    private final Random random = new Random(1);
    private final SmallLRUCache<String, String[]> lobFileListCache = SmallLRUCache
            .newInstance(128);
    private LobStorageTest lobStorage;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        // run using -javaagent:ext/h2-1.2.139.jar
        TestBase test = TestBase.createCaller().init();
        test.config.traceTest = true;
        test.testFromMain();
    }

    @Override
    public void test() throws SQLException {
        testCompare();
        for (int i = 0; i < Value.TYPE_COUNT; i++) {
            if (i == 23) {
                // this used to be "TIMESTAMP UTC", which was a short-lived
                // experiment
                continue;
            }
            if (i == Value.ENUM) {
                // TODO ENUM
                continue;
            }
            Value v = create(i);
            String s = "type: " + v.getValueType() +
                    " calculated: " + v.getMemory() +
                    " real: " + MemoryFootprint.getObjectSize(v) + " " +
                    v.getClass().getName() + ": " + v.toString();
            trace(s);
        }
        for (int i = 0; i < Value.TYPE_COUNT; i++) {
            if (i == 23) {
                // this used to be "TIMESTAMP UTC", which was a short-lived
                // experiment
                continue;
            }
            if (i == Value.ENUM) {
                // TODO ENUM
                continue;
            }
            Value v = create(i);
            if (v == ValueNull.INSTANCE && i == Value.GEOMETRY) {
                // jts not in the classpath, OK
                continue;
            }
            assertEquals(i, v.getValueType());
            testType(i);
        }
    }

    private void testCompare() {
        ValueNumeric a = ValueNumeric.get(new BigDecimal("0.0"));
        ValueNumeric b = ValueNumeric.get(new BigDecimal("-0.00"));
        assertTrue(a.hashCode() != b.hashCode());
        assertFalse(a.equals(b));
    }

    private void testType(int type) throws SQLException {
        System.gc();
        System.gc();
        long first = Utils.getMemoryUsed();
        ArrayList<Value> list = new ArrayList<>();
        long memory = 0;
        while (memory < 1000000) {
            Value v = create(type);
            memory += v.getMemory() + Constants.MEMORY_POINTER;
            list.add(v);
        }
        Object[] array = list.toArray();
        IdentityHashMap<Object, Object> map = new IdentityHashMap<>();
        for (Object a : array) {
            map.put(a, a);
        }
        int size = map.size();
        map.clear();
        map = null;
        list = null;
        System.gc();
        System.gc();
        long used = Utils.getMemoryUsed() - first;
        memory /= 1024;
        if (config.traceTest || used > memory * 3) {
            String msg = "Type: " + type + " Used memory: " + used +
                    " calculated: " + memory + " length: " + array.length + " size: " + size;
            if (config.traceTest) {
                trace(msg);
            }
            if (used > memory * 3) {
                fail(msg);
            }
        }
    }
    private Value create(int type) throws SQLException {
        switch (type) {
        case Value.NULL:
            return ValueNull.INSTANCE;
        case Value.BOOLEAN:
            return ValueBoolean.FALSE;
        case Value.TINYINT:
            return ValueTinyint.get((byte) random.nextInt());
        case Value.SMALLINT:
            return ValueSmallint.get((short) random.nextInt());
        case Value.INTEGER:
            return ValueInteger.get(random.nextInt());
        case Value.BIGINT:
            return ValueBigint.get(random.nextLong());
        case Value.NUMERIC:
            return ValueNumeric.get(new BigDecimal(random.nextInt()));
            // + "12123344563456345634565234523451312312"
        case Value.DOUBLE:
            return ValueDouble.get(random.nextDouble());
        case Value.REAL:
            return ValueReal.get(random.nextFloat());
        case Value.DECFLOAT:
            return ValueDecfloat.get(new BigDecimal(random.nextInt()));
        case Value.TIME:
            return ValueTime.fromNanos(randomTimeNanos());
        case Value.TIME_TZ:
            return ValueTimeTimeZone.fromNanos(randomTimeNanos(), randomZoneOffset());
        case Value.DATE:
            return ValueDate.fromDateValue(randomDateValue());
        case Value.TIMESTAMP:
            return ValueTimestamp.fromDateValueAndNanos(randomDateValue(), randomTimeNanos());
        case Value.TIMESTAMP_TZ:
            return ValueTimestampTimeZone.fromDateValueAndNanos(
                    randomDateValue(), randomTimeNanos(), randomZoneOffset());
        case Value.VARBINARY:
            return ValueVarbinary.get(randomBytes(random.nextInt(1000)));
        case Value.VARCHAR:
            return ValueVarchar.get(randomString(random.nextInt(100)));
        case Value.VARCHAR_IGNORECASE:
            return ValueVarcharIgnoreCase.get(randomString(random.nextInt(100)));
        case Value.BLOB: {
            int len = (int) Math.abs(random.nextGaussian() * 10);
            byte[] data = randomBytes(len);
            return getLobStorage().createBlob(new ByteArrayInputStream(data), len);
        }
        case Value.CLOB: {
            int len = (int) Math.abs(random.nextGaussian() * 10);
            String s = randomString(len);
            return getLobStorage().createClob(new StringReader(s), len);
        }
        case Value.ARRAY:
            return ValueArray.get(createArray(), null);
        case Value.ROW:
            return ValueRow.get(createArray());
        case Value.JAVA_OBJECT:
            return ValueJavaObject.getNoCopy(randomBytes(random.nextInt(100)));
        case Value.UUID:
            return ValueUuid.get(random.nextLong(), random.nextLong());
        case Value.CHAR:
            return ValueChar.get(randomString(random.nextInt(100)));
        case Value.GEOMETRY:
            return ValueGeometry.get("POINT (" + random.nextInt(100) + ' ' + random.nextInt(100) + ')');
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
            return ValueInterval.from(IntervalQualifier.valueOf(type - Value.INTERVAL_YEAR),
                    random.nextBoolean(), random.nextInt(Integer.MAX_VALUE), 0);
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            return ValueInterval.from(IntervalQualifier.valueOf(type - Value.INTERVAL_YEAR),
                    random.nextBoolean(), random.nextInt(Integer.MAX_VALUE), random.nextInt(1_000_000_000));
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_MINUTE:
            return ValueInterval.from(IntervalQualifier.valueOf(type - Value.INTERVAL_YEAR),
                    random.nextBoolean(), random.nextInt(Integer.MAX_VALUE), random.nextInt(12));
        case Value.JSON:
            return ValueJson.fromJson("{\"key\":\"value\"}");
        case Value.BINARY:
            return ValueBinary.get(randomBytes(random.nextInt(1000)));
        default:
            throw new AssertionError("type=" + type);
        }
    }

    private long randomDateValue() {
        return DateTimeUtils.dateValueFromAbsoluteDay(
                (random.nextLong() & Long.MAX_VALUE) % (MAX_ABSOLUTE_DAY - MIN_ABSOLUTE_DAY + 1) + MIN_ABSOLUTE_DAY);
    }

    private long randomTimeNanos() {
        return (random.nextLong() & Long.MAX_VALUE) % DateTimeUtils.NANOS_PER_DAY;
    }

    private short randomZoneOffset() {
        return (short) (random.nextInt() % (18 * 60));
    }

    private Value[] createArray() throws SQLException {
        int len = random.nextInt(20);
        Value[] list = new Value[len];
        for (int i = 0; i < list.length; i++) {
            list[i] = create(Value.VARCHAR);
        }
        return list;
    }

    private byte[] randomBytes(int len) {
        byte[] data = new byte[len];
        if (random.nextBoolean()) {
            // don't initialize always (compression)
            random.nextBytes(data);
        }
        return data;
    }

    private String randomString(int len) {
        char[] chars = new char[len];
        if (random.nextBoolean()) {
            // don't initialize always (compression)
            for (int i = 0; i < chars.length; i++) {
                chars[i] = (char) (random.nextGaussian() * 100);
            }
        }
        return new String(chars);
    }

    @Override
    public void checkPowerOff() {
        // nothing to do
    }

    @Override
    public void checkWritingAllowed() {
        // nothing to do
    }

    @Override
    public String getDatabasePath() {
        return getBaseDir() + "/valueMemory";
    }

    @Override
    public Object getLobSyncObject() {
        return this;
    }

    @Override
    public int getMaxLengthInplaceLob() {
        return 100;
    }

    @Override
    public FileStore openFile(String name, String mode, boolean mustExist) {
        return FileStore.open(this, name, mode);
    }

    @Override
    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return lobFileListCache;
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

    @Override
    public LobStorageInterface getLobStorage() {
        if (lobStorage == null) {
            lobStorage = new LobStorageTest();
        }
        return lobStorage;
    }

    @Override
    public int readLob(long lobId, byte[] hmac, long offset, byte[] buff,
            int off, int length) {
        return -1;
    }

    @Override
    public CompareMode getCompareMode() {
        return CompareMode.getInstance(null, 0);
    }


    private class LobStorageTest implements LobStorageInterface {

        LobStorageTest() {
        }

        @Override
        public void removeLob(ValueLob lob) {
            // not stored in the database
        }

        @Override
        public InputStream getInputStream(long lobId,
                long byteCount) throws IOException {
            // this method is only implemented on the server side of a TCP connection
            throw new IllegalStateException();
        }

        @Override
        public InputStream getInputStream(long lobId, int tableId,
                long byteCount) throws IOException {
            // this method is only implemented on the server side of a TCP connection
            throw new IllegalStateException();
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public ValueLob copyLob(ValueLob old, int tableId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeAllForTable(int tableId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ValueBlob createBlob(InputStream in, long maxLength) {
            // need to use a temp file, because the input stream could come from
            // the same database, which would create a weird situation (trying
            // to read a block while writing something)
            return ValueBlob.createTempBlob(in, maxLength, TestValueMemory.this);
        }

        /**
         * Create a CLOB object.
         *
         * @param reader the reader
         * @param maxLength the maximum length (-1 if not known)
         * @return the LOB
         */
        @Override
        public ValueClob createClob(Reader reader, long maxLength) {
            // need to use a temp file, because the input stream could come from
            // the same database, which would create a weird situation (trying
            // to read a block while writing something)
            return ValueClob.createTempClob(reader, maxLength, TestValueMemory.this);
        }
    }
}
