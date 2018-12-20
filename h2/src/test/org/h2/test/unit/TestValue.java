/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.store.DataHandler;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.test.utils.AssertThrows;
import org.h2.tools.SimpleResultSet;
import org.h2.util.Bits;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueLobDb;
import org.h2.value.ValueNull;
import org.h2.value.ValueResultSet;
import org.h2.value.ValueString;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;

/**
 * Tests features of values.
 */
public class TestValue extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        testResultSetOperations();
        testBinaryAndUuid();
        testCastTrim();
        testValueResultSet();
        testDataType();
        testArray();
        testUUID();
        testDouble(false);
        testDouble(true);
        testTimestamp();
        testModulusDouble();
        testModulusDecimal();
        testModulusOperator();
        testLobComparison();
    }

    private void testResultSetOperations() throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.setAutoClose(false);
        rs.addColumn("X", Types.INTEGER, 10, 0);
        rs.addRow(new Object[]{null});
        rs.next();
        for (int type = Value.NULL; type < Value.TYPE_COUNT; type++) {
            if (type == 23) {
                // a defunct experimental type
            } else {
                Value v = DataType.readValue(null, rs, 1, type);
                assertTrue(v == ValueNull.INSTANCE);
            }
        }
        testResultSetOperation(new byte[0]);
        testResultSetOperation(1);
        testResultSetOperation(Boolean.TRUE);
        testResultSetOperation((byte) 1);
        testResultSetOperation((short) 2);
        testResultSetOperation((long) 3);
        testResultSetOperation(4.0f);
        testResultSetOperation(5.0d);
        testResultSetOperation(new Date(6));
        testResultSetOperation(new Time(7));
        testResultSetOperation(new Timestamp(8));
        testResultSetOperation(new BigDecimal("9"));
        testResultSetOperation(UUID.randomUUID());

        SimpleResultSet rs2 = new SimpleResultSet();
        rs2.setAutoClose(false);
        rs2.addColumn("X", Types.INTEGER, 10, 0);
        rs2.addRow(new Object[]{1});
        rs2.next();
        testResultSetOperation(rs2);

    }

    private void testResultSetOperation(Object obj) throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.setAutoClose(false);
        int valueType = DataType.getTypeFromClass(obj.getClass());
        int sqlType = DataType.convertTypeToSQLType(valueType);
        rs.addColumn("X", sqlType, 10, 0);
        rs.addRow(new Object[]{obj});
        rs.next();
        Value v = DataType.readValue(null, rs, 1, valueType);
        Value v2 = DataType.convertToValue(null, obj, valueType);
        if (v.getType() == Value.RESULT_SET) {
            assertEquals(v.toString(), v2.toString());
        } else {
            assertTrue(v.equals(v2));
        }
    }

    private void testBinaryAndUuid() throws SQLException {
        try (Connection conn = getConnection("binaryAndUuid")) {
            UUID uuid = UUID.randomUUID();
            PreparedStatement prep;
            ResultSet rs;
            // Check conversion to byte[]
            prep = conn.prepareStatement("SELECT * FROM TABLE(X BINARY=?)");
            prep.setObject(1, new Object[] { uuid });
            rs = prep.executeQuery();
            rs.next();
            assertTrue(Arrays.equals(Bits.uuidToBytes(uuid), (byte[]) rs.getObject(1)));
            // Check that type is not changed
            prep = conn.prepareStatement("SELECT * FROM TABLE(X UUID=?)");
            prep.setObject(1, new Object[] { uuid });
            rs = prep.executeQuery();
            rs.next();
            assertEquals(uuid, rs.getObject(1));
        } finally {
            deleteDb("binaryAndUuid");
        }
    }

    private void testCastTrim() {
        Value v;
        String spaces = new String(new char[100]).replace((char) 0, ' ');

        v = ValueArray.get(new Value[] { ValueString.get("hello"),
                ValueString.get("world") });
        assertEquals(10, v.getPrecision());
        assertEquals(5, v.convertPrecision(5, true).getPrecision());
        v = ValueArray.get(new Value[]{ValueString.get(""), ValueString.get("")});
        assertEquals(0, v.getPrecision());
        assertEquals("['']", v.convertPrecision(1, true).toString());

        v = ValueBytes.get(spaces.getBytes());
        assertEquals(100, v.getPrecision());
        assertEquals(10, v.convertPrecision(10, false).getPrecision());
        assertEquals(10, v.convertPrecision(10, false).getBytes().length);
        assertEquals(32, v.convertPrecision(10, false).getBytes()[9]);
        assertEquals(10, v.convertPrecision(10, true).getPrecision());

        final Value vd = ValueDecimal.get(new BigDecimal("1234567890.123456789"));
        assertEquals(19, vd.getPrecision());
        assertEquals("1234567890.1234567", vd.convertPrecision(10, true).getString());
        new AssertThrows(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1) {
            @Override
            public void test() {
                vd.convertPrecision(10, false);
            }
        };

        v = ValueLobDb.createSmallLob(Value.CLOB, spaces.getBytes(), 100);
        assertEquals(100, v.getPrecision());
        assertEquals(10, v.convertPrecision(10, false).getPrecision());
        assertEquals(10, v.convertPrecision(10, false).getString().length());
        assertEquals("          ", v.convertPrecision(10, false).getString());
        assertEquals(10, v.convertPrecision(10, true).getPrecision());

        v = ValueLobDb.createSmallLob(Value.BLOB, spaces.getBytes(), 100);
        assertEquals(100, v.getPrecision());
        assertEquals(10, v.convertPrecision(10, false).getPrecision());
        assertEquals(10, v.convertPrecision(10, false).getBytes().length);
        assertEquals(32, v.convertPrecision(10, false).getBytes()[9]);
        assertEquals(10, v.convertPrecision(10, true).getPrecision());

        SimpleResult rs = new SimpleResult();
        rs.addColumn("X", "X", Value.INT, 0, 0, ValueInt.DISPLAY_SIZE);
        rs.addRow(ValueInt.get(1));
        v = ValueResultSet.get(rs);
        assertEquals(Integer.MAX_VALUE, v.getPrecision());
        assertEquals(Integer.MAX_VALUE, v.convertPrecision(10, false).getPrecision());
        assertEquals(1, v.convertPrecision(10, false).getResult().getRowCount());
        assertEquals(0, v.convertPrecision(10, true).getResult().getRowCount());
        assertEquals(Integer.MAX_VALUE, v.convertPrecision(10, true).getPrecision());

        v = ValueString.get(spaces);
        assertEquals(100, v.getPrecision());
        assertEquals(10, v.convertPrecision(10, false).getPrecision());
        assertEquals("          ", v.convertPrecision(10, false).getString());
        assertEquals("          ", v.convertPrecision(10, true).getString());

    }

    private void testValueResultSet() throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.setAutoClose(false);
        rs.addColumn("ID", Types.INTEGER, 0, 0);
        rs.addColumn("NAME", Types.VARCHAR, 255, 0);
        rs.addRow(1, "Hello");
        rs.addRow(2, "World");
        rs.addRow(3, "Peace");

        testValueResultSetTest(ValueResultSet.get(null, rs, Integer.MAX_VALUE), Integer.MAX_VALUE, true);
        rs.beforeFirst();
        testValueResultSetTest(ValueResultSet.get(null, rs, 2), 2, true);

        SimpleResult result = new SimpleResult();
        result.addColumn("ID", "ID", Value.INT, 0, 0, ValueInt.DISPLAY_SIZE);
        result.addColumn("NAME", "NAME", Value.STRING, 255, 0, 255);
        result.addRow(ValueInt.get(1), ValueString.get("Hello"));
        result.addRow(ValueInt.get(2), ValueString.get("World"));
        result.addRow(ValueInt.get(3), ValueString.get("Peace"));

        ValueResultSet v = ValueResultSet.get(result);
        testValueResultSetTest(v, Integer.MAX_VALUE, false);

        testValueResultSetTest(ValueResultSet.get(v.getResult(), Integer.MAX_VALUE), Integer.MAX_VALUE, false);
        testValueResultSetTest(ValueResultSet.get(v.getResult(), 2), 2, false);
    }

    private void testValueResultSetTest(ValueResultSet v, int count, boolean fromSimple) {
        ResultInterface res = v.getResult();
        assertEquals(2, res.getVisibleColumnCount());
        assertEquals("ID", res.getAlias(0));
        assertEquals("ID", res.getColumnName(0));
        assertEquals(Value.INT, res.getColumnType(0));
        assertEquals(0, res.getColumnPrecision(0));
        assertEquals(0, res.getColumnScale(0));
        assertEquals(fromSimple ? 15 : ValueInt.DISPLAY_SIZE, res.getDisplaySize(0));
        assertEquals("NAME", res.getAlias(1));
        assertEquals("NAME", res.getColumnName(1));
        assertEquals(Value.STRING, res.getColumnType(1));
        assertEquals(255, res.getColumnPrecision(1));
        assertEquals(0, res.getColumnScale(1));
        assertEquals(fromSimple ? 15 : 255, res.getDisplaySize(1));
        if (count >= 1) {
            assertTrue(res.next());
            assertEquals(new Value[] {ValueInt.get(1), ValueString.get("Hello")}, res.currentRow());
            if (count >= 2) {
                assertTrue(res.next());
                assertEquals(new Value[] {ValueInt.get(2), ValueString.get("World")}, res.currentRow());
                if (count >= 3) {
                    assertTrue(res.next());
                    assertEquals(new Value[] {ValueInt.get(3), ValueString.get("Peace")}, res.currentRow());
                }
            }
        }
        assertFalse(res.next());
    }

    private void testDataType() {
        testDataType(Value.NULL, null);
        testDataType(Value.NULL, Void.class);
        testDataType(Value.NULL, void.class);
        testDataType(Value.ARRAY, String[].class);
        testDataType(Value.STRING, String.class);
        testDataType(Value.INT, Integer.class);
        testDataType(Value.LONG, Long.class);
        testDataType(Value.BOOLEAN, Boolean.class);
        testDataType(Value.DOUBLE, Double.class);
        testDataType(Value.BYTE, Byte.class);
        testDataType(Value.SHORT, Short.class);
        testDataType(Value.FLOAT, Float.class);
        testDataType(Value.BYTES, byte[].class);
        testDataType(Value.UUID, UUID.class);
        testDataType(Value.NULL, Void.class);
        testDataType(Value.DECIMAL, BigDecimal.class);
        testDataType(Value.RESULT_SET, ResultSet.class);
        testDataType(Value.BLOB, ValueLobDb.class);
        // see FIXME in DataType.getTypeFromClass
        //testDataType(Value.CLOB, Value.ValueClob.class);
        testDataType(Value.DATE, Date.class);
        testDataType(Value.TIME, Time.class);
        testDataType(Value.TIMESTAMP, Timestamp.class);
        testDataType(Value.TIMESTAMP, java.util.Date.class);
        testDataType(Value.CLOB, java.io.Reader.class);
        testDataType(Value.CLOB, java.sql.Clob.class);
        testDataType(Value.BLOB, java.io.InputStream.class);
        testDataType(Value.BLOB, java.sql.Blob.class);
        testDataType(Value.ARRAY, Object[].class);
        testDataType(Value.JAVA_OBJECT, StringBuffer.class);
    }

    private void testDataType(int type, Class<?> clazz) {
        assertEquals(type, DataType.getTypeFromClass(clazz));
    }

    private void testDouble(boolean useFloat) {
        double[] d = {
                Double.NEGATIVE_INFINITY,
                -1,
                0,
                1,
                Double.POSITIVE_INFINITY,
                Double.NaN
        };
        Value[] values = new Value[d.length];
        for (int i = 0; i < d.length; i++) {
            Value v = useFloat ? (Value) ValueFloat.get((float) d[i])
                    : (Value) ValueDouble.get(d[i]);
            values[i] = v;
            assertTrue(values[i].compareTypeSafe(values[i], null) == 0);
            assertTrue(v.equals(v));
            assertEquals(Integer.compare(i, 2), v.getSignum());
        }
        for (int i = 0; i < d.length - 1; i++) {
            assertTrue(values[i].compareTypeSafe(values[i+1], null) < 0);
            assertTrue(values[i + 1].compareTypeSafe(values[i], null) > 0);
            assertFalse(values[i].equals(values[i+1]));
        }
    }

    private void testTimestamp() {
        ValueTimestamp valueTs = ValueTimestamp.parse("2000-01-15 10:20:30.333222111");
        Timestamp ts = Timestamp.valueOf("2000-01-15 10:20:30.333222111");
        assertEquals(ts.toString(), valueTs.getString());
        assertEquals(ts, valueTs.getTimestamp());
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
        c.set(2018, 02, 25, 1, 59, 00);
        c.set(Calendar.MILLISECOND, 123);
        long expected = c.getTimeInMillis();
        ts = ValueTimestamp.parse("2018-03-25 01:59:00.123123123 Europe/Berlin").getTimestamp();
        assertEquals(expected, ts.getTime());
        assertEquals(123123123, ts.getNanos());
        ts = ValueTimestamp.parse("2018-03-25 01:59:00.123123123+01").getTimestamp();
        assertEquals(expected, ts.getTime());
        assertEquals(123123123, ts.getNanos());
        expected += 60000; // 1 minute
        ts = ValueTimestamp.parse("2018-03-25 03:00:00.123123123 Europe/Berlin").getTimestamp();
        assertEquals(expected, ts.getTime());
        assertEquals(123123123, ts.getNanos());
        ts = ValueTimestamp.parse("2018-03-25 03:00:00.123123123+02").getTimestamp();
        assertEquals(expected, ts.getTime());
        assertEquals(123123123, ts.getNanos());
    }

    private void testArray() {
        ValueArray src = ValueArray.get(String.class,
                new Value[] {ValueString.get("1"), ValueString.get("22"), ValueString.get("333")});
        assertEquals(6, src.getPrecision());
        assertSame(src, src.convertPrecision(5, false));
        assertSame(src, src.convertPrecision(6, true));
        ValueArray exp = ValueArray.get(String.class,
                new Value[] {ValueString.get("1"), ValueString.get("22"), ValueString.get("33")});
        Value got = src.convertPrecision(5, true);
        assertEquals(exp, got);
        assertEquals(String.class, ((ValueArray) got).getComponentType());
        exp = ValueArray.get(String.class, new Value[] {ValueString.get("1"), ValueString.get("22")});
        got = src.convertPrecision(3, true);
        assertEquals(exp, got);
        assertEquals(String.class, ((ValueArray) got).getComponentType());
        exp = ValueArray.get(String.class, new Value[0]);
        got = src.convertPrecision(0, true);
        assertEquals(exp, got);
        assertEquals(String.class, ((ValueArray) got).getComponentType());
    }

    private void testUUID() {
        long maxHigh = 0, maxLow = 0, minHigh = -1L, minLow = -1L;
        for (int i = 0; i < 100; i++) {
            ValueUuid uuid = ValueUuid.getNewRandom();
            maxHigh |= uuid.getHigh();
            maxLow |= uuid.getLow();
            minHigh &= uuid.getHigh();
            minLow &= uuid.getLow();
        }
        ValueUuid max = ValueUuid.get(maxHigh, maxLow);
        assertEquals("ffffffff-ffff-4fff-bfff-ffffffffffff", max.getString());
        ValueUuid min = ValueUuid.get(minHigh, minLow);
        assertEquals("00000000-0000-4000-8000-000000000000", min.getString());

        // Test conversion from ValueJavaObject to ValueUuid
        String uuidStr = "12345678-1234-4321-8765-123456789012";

        UUID origUUID = UUID.fromString(uuidStr);
        ValueJavaObject valObj = ValueJavaObject.getNoCopy(origUUID, null, null);
        Value valUUID = valObj.convertTo(Value.UUID);
        assertTrue(valUUID instanceof ValueUuid);
        assertTrue(valUUID.getString().equals(uuidStr));
        assertTrue(valUUID.getObject().equals(origUUID));

        ValueJavaObject voString = ValueJavaObject.getNoCopy(
                new String("This is not a ValueUuid object"), null, null);
        assertThrows(DbException.class, voString).convertTo(Value.UUID);
    }

    private void testModulusDouble() {
        final ValueDouble vd1 = ValueDouble.get(12);
        new AssertThrows(ErrorCode.DIVISION_BY_ZERO_1) { @Override
        public void test() {
            vd1.modulus(ValueDouble.get(0));
        }};
        ValueDouble vd2 = ValueDouble.get(10);
        ValueDouble vd3 = vd1.modulus(vd2);
        assertEquals(2, vd3.getDouble());
    }

    private void testModulusDecimal() {
        final ValueDecimal vd1 = ValueDecimal.get(new BigDecimal(12));
        new AssertThrows(ErrorCode.DIVISION_BY_ZERO_1) { @Override
        public void test() {
            vd1.modulus(ValueDecimal.get(new BigDecimal(0)));
        }};
        ValueDecimal vd2 = ValueDecimal.get(new BigDecimal(10));
        ValueDecimal vd3 = vd1.modulus(vd2);
        assertEquals(2, vd3.getDouble());
    }

    private void testModulusOperator() throws SQLException {
        try (Connection conn = getConnection("modulus")) {
            ResultSet rs = conn.createStatement().executeQuery("CALL 12 % 10");
            rs.next();
            assertEquals(2, rs.getInt(1));
        } finally {
            deleteDb("modulus");
        }
    }

    private void testLobComparison() throws SQLException {
        assertEquals(0, testLobComparisonImpl(null, Value.BLOB, 0, 0, 0, 0));
        assertEquals(0, testLobComparisonImpl(null, Value.CLOB, 0, 0, 0, 0));
        assertEquals(-1, testLobComparisonImpl(null, Value.BLOB, 1, 1, 200, 210));
        assertEquals(-1, testLobComparisonImpl(null, Value.CLOB, 1, 1, 'a', 'b'));
        assertEquals(1, testLobComparisonImpl(null, Value.BLOB, 512, 512, 210, 200));
        assertEquals(1, testLobComparisonImpl(null, Value.CLOB, 512, 512, 'B', 'A'));
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:testValue")) {
            Database dh = ((Session) ((JdbcConnection) c).getSession()).getDatabase();
            assertEquals(1, testLobComparisonImpl(dh, Value.BLOB, 1_024, 1_024, 210, 200));
            assertEquals(1, testLobComparisonImpl(dh, Value.CLOB, 1_024, 1_024, 'B', 'A'));
            assertEquals(-1, testLobComparisonImpl(dh, Value.BLOB, 10_000, 10_000, 200, 210));
            assertEquals(-1, testLobComparisonImpl(dh, Value.CLOB, 10_000, 10_000, 'a', 'b'));
            assertEquals(0, testLobComparisonImpl(dh, Value.BLOB, 10_000, 10_000, 0, 0));
            assertEquals(0, testLobComparisonImpl(dh, Value.CLOB, 10_000, 10_000, 0, 0));
            assertEquals(-1, testLobComparisonImpl(dh, Value.BLOB, 1_000, 10_000, 0, 0));
            assertEquals(-1, testLobComparisonImpl(dh, Value.CLOB, 1_000, 10_000, 0, 0));
            assertEquals(1, testLobComparisonImpl(dh, Value.BLOB, 10_000, 1_000, 0, 0));
            assertEquals(1, testLobComparisonImpl(dh, Value.CLOB, 10_000, 1_000, 0, 0));
        }
    }

    private static int testLobComparisonImpl(DataHandler dh, int type, int size1, int size2, int suffix1,
            int suffix2) {
        byte[] bytes1 = new byte[size1];
        byte[] bytes2 = new byte[size2];
        if (size1 > 0) {
            bytes1[size1 - 1] = (byte) suffix1;
        }
        if (size2 > 0) {
            bytes2[size2 - 1] = (byte) suffix2;
        }
        Value lob1 = createLob(dh, type, bytes1);
        Value lob2 = createLob(dh, type, bytes2);
        return lob1.compareTypeSafe(lob2, null);
    }

    static Value createLob(DataHandler dh, int type, byte[] bytes) {
        if (dh == null) {
            return ValueLobDb.createSmallLob(type, bytes);
        }
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        if (type == Value.BLOB) {
            return dh.getLobStorage().createBlob(in, -1);
        } else {
            return dh.getLobStorage().createClob(new InputStreamReader(in, StandardCharsets.UTF_8), -1);
        }
    }

}
