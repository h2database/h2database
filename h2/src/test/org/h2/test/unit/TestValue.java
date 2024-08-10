/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import static org.h2.engine.Constants.MAX_ARRAY_CARDINALITY;
import static org.h2.engine.Constants.MAX_NUMERIC_PRECISION;
import static org.h2.engine.Constants.MAX_STRING_LENGTH;

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
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;
import org.h2.api.ErrorCode;
import org.h2.api.H2Type;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.jdbc.JdbcConnection;
import org.h2.store.DataHandler;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.Bits;
import org.h2.util.JdbcUtils;
import org.h2.util.LegacyDateTimeUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBlob;
import org.h2.value.ValueClob;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInterval;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueReal;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueToObjectConverter2;
import org.h2.value.ValueUuid;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;

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
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws SQLException {
        testBinaryAndUuid();
        testCastTrim();
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
        testTypeInfo();
        testH2Type();
        testHigherType();
    }

    private void testBinaryAndUuid() throws SQLException {
        try (Connection conn = getConnection("binaryAndUuid")) {
            UUID uuid = UUID.randomUUID();
            PreparedStatement prep;
            ResultSet rs;
            // Check conversion to byte[]
            prep = conn.prepareStatement("SELECT * FROM TABLE(X BINARY(16)=?)");
            prep.setObject(1, new Object[] { uuid });
            rs = prep.executeQuery();
            rs.next();
            assertTrue(Arrays.equals(Bits.uuidToBytes(uuid), (byte[]) rs.getObject(1)));
            // Check conversion to byte[]
            prep = conn.prepareStatement("SELECT * FROM TABLE(X VARBINARY=?)");
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

        v = ValueArray.get(new Value[] { ValueVarchar.get("hello"), ValueVarchar.get("world") }, null);
        TypeInfo typeInfo = TypeInfo.getTypeInfo(Value.ARRAY, 1L, 0, TypeInfo.TYPE_VARCHAR);
        assertEquals(2, v.getType().getPrecision());
        assertEquals(1, v.castTo(typeInfo, null).getType().getPrecision());
        v = ValueArray.get(new Value[]{ValueVarchar.get(""), ValueVarchar.get("")}, null);
        assertEquals(2, v.getType().getPrecision());
        assertEquals("ARRAY ['']", v.castTo(typeInfo, null).toString());

        v = ValueVarbinary.get(spaces.getBytes());
        typeInfo = TypeInfo.getTypeInfo(Value.VARBINARY, 10L, 0, null);
        assertEquals(100, v.getType().getPrecision());
        assertEquals(10, v.castTo(typeInfo, null).getType().getPrecision());
        assertEquals(10, v.castTo(typeInfo, null).getBytes().length);
        assertEquals(32, v.castTo(typeInfo, null).getBytes()[9]);
        assertEquals(10, v.castTo(typeInfo, null).getType().getPrecision());

        v = ValueClob.createSmall(spaces.getBytes(), 100);
        typeInfo = TypeInfo.getTypeInfo(Value.CLOB, 10L, 0, null);
        assertEquals(100, v.getType().getPrecision());
        assertEquals(10, v.castTo(typeInfo, null).getType().getPrecision());
        assertEquals(10, v.castTo(typeInfo, null).getString().length());
        assertEquals("          ", v.castTo(typeInfo, null).getString());
        assertEquals(10, v.castTo(typeInfo, null).getType().getPrecision());

        v = ValueBlob.createSmall(spaces.getBytes());
        typeInfo = TypeInfo.getTypeInfo(Value.BLOB, 10L, 0, null);
        assertEquals(100, v.getType().getPrecision());
        assertEquals(10, v.castTo(typeInfo, null).getType().getPrecision());
        assertEquals(10, v.castTo(typeInfo, null).getBytes().length);
        assertEquals(32, v.castTo(typeInfo, null).getBytes()[9]);
        assertEquals(10, v.castTo(typeInfo, null).getType().getPrecision());

        v = ValueVarchar.get(spaces);
        typeInfo = TypeInfo.getTypeInfo(Value.VARCHAR, 10L, 0, null);
        assertEquals(100, v.getType().getPrecision());
        assertEquals(10, v.castTo(typeInfo, null).getType().getPrecision());
        assertEquals("          ", v.castTo(typeInfo, null).getString());
        assertEquals("          ", v.castTo(typeInfo, null).getString());

    }

    private void testDataType() {
        testDataType(TypeInfo.TYPE_NULL, null);
        testDataType(TypeInfo.TYPE_NULL, Void.class);
        testDataType(TypeInfo.TYPE_NULL, void.class);
        testDataType(TypeInfo.getTypeInfo(Value.ARRAY, Integer.MAX_VALUE, 0, TypeInfo.TYPE_VARCHAR), String[].class);
        testDataType(TypeInfo.TYPE_VARCHAR, String.class);
        testDataType(TypeInfo.TYPE_INTEGER, Integer.class);
        testDataType(TypeInfo.TYPE_BIGINT, Long.class);
        testDataType(TypeInfo.TYPE_BOOLEAN, Boolean.class);
        testDataType(TypeInfo.TYPE_DOUBLE, Double.class);
        testDataType(TypeInfo.TYPE_TINYINT, Byte.class);
        testDataType(TypeInfo.TYPE_SMALLINT, Short.class);
        testDataType(TypeInfo.TYPE_REAL, Float.class);
        testDataType(TypeInfo.TYPE_VARBINARY, byte[].class);
        testDataType(TypeInfo.TYPE_UUID, UUID.class);
        testDataType(TypeInfo.TYPE_NULL, Void.class);
        testDataType(TypeInfo.TYPE_NUMERIC_FLOATING_POINT, BigDecimal.class);
        testDataType(TypeInfo.TYPE_DATE, Date.class);
        testDataType(TypeInfo.TYPE_TIME, Time.class);
        testDataType(TypeInfo.TYPE_TIMESTAMP, Timestamp.class);
        testDataType(TypeInfo.TYPE_TIMESTAMP, java.util.Date.class);
        testDataType(TypeInfo.TYPE_CLOB, java.io.Reader.class);
        testDataType(TypeInfo.TYPE_CLOB, java.sql.Clob.class);
        testDataType(TypeInfo.TYPE_BLOB, java.io.InputStream.class);
        testDataType(TypeInfo.TYPE_BLOB, java.sql.Blob.class);
        testDataType(TypeInfo.getTypeInfo(Value.ARRAY, Integer.MAX_VALUE, 0, TypeInfo.TYPE_JAVA_OBJECT),
                Object[].class);
        testDataType(TypeInfo.TYPE_JAVA_OBJECT, StringBuffer.class);
    }

    private void testDataType(TypeInfo type, Class<?> clazz) {
        assertEquals(type, ValueToObjectConverter2.classToType(clazz));
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
        int[] signum = {
                -1,
                -1,
                0,
                1,
                1,
                0
        };
        Value[] values = new Value[d.length];
        for (int i = 0; i < d.length; i++) {
            Value v = useFloat ? (Value) ValueReal.get((float) d[i])
                    : (Value) ValueDouble.get(d[i]);
            values[i] = v;
            assertTrue(values[i].compareTypeSafe(values[i], null, null) == 0);
            assertTrue(v.equals(v));
            assertEquals(signum[i], v.getSignum());
        }
        for (int i = 0; i < d.length - 1; i++) {
            assertTrue(values[i].compareTypeSafe(values[i+1], null, null) < 0);
            assertTrue(values[i + 1].compareTypeSafe(values[i], null, null) > 0);
            assertFalse(values[i].equals(values[i+1]));
        }
    }

    private void testTimestamp() {
        ValueTimestamp valueTs = ValueTimestamp.parse("2000-01-15 10:20:30.333222111", null);
        Timestamp ts = Timestamp.valueOf("2000-01-15 10:20:30.333222111");
        assertEquals(ts.toString(), valueTs.getString());
        assertEquals(ts, LegacyDateTimeUtils.toTimestamp(null,  null, valueTs));
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
        c.set(2018, 02, 25, 1, 59, 00);
        c.set(Calendar.MILLISECOND, 123);
        long expected = c.getTimeInMillis();
        ts = LegacyDateTimeUtils.toTimestamp(null,  null,
                ValueTimestamp.parse("2018-03-25 01:59:00.123123123 Europe/Berlin", null));
        assertEquals(expected, ts.getTime());
        assertEquals(123123123, ts.getNanos());
        ts = LegacyDateTimeUtils.toTimestamp(null, null,
                ValueTimestamp.parse("2018-03-25 01:59:00.123123123+01", null));
        assertEquals(expected, ts.getTime());
        assertEquals(123123123, ts.getNanos());
        expected += 60000; // 1 minute
        ts = LegacyDateTimeUtils.toTimestamp(null, null,
                ValueTimestamp.parse("2018-03-25 03:00:00.123123123 Europe/Berlin", null));
        assertEquals(expected, ts.getTime());
        assertEquals(123123123, ts.getNanos());
        ts = LegacyDateTimeUtils.toTimestamp(null, null,
                ValueTimestamp.parse("2018-03-25 03:00:00.123123123+02", null));
        assertEquals(expected, ts.getTime());
        assertEquals(123123123, ts.getNanos());
    }

    private void testArray() {
        ValueArray src = ValueArray.get(
                new Value[] {ValueVarchar.get("1"), ValueVarchar.get("22"), ValueVarchar.get("333")}, null);
        assertEquals(3, src.getType().getPrecision());
        assertSame(src, src.castTo(TypeInfo.getTypeInfo(Value.ARRAY, 3L, 0, TypeInfo.TYPE_VARCHAR), null));
        ValueArray exp = ValueArray.get(
                new Value[] {ValueVarchar.get("1"), ValueVarchar.get("22")}, null);
        Value got = src.castTo(TypeInfo.getTypeInfo(Value.ARRAY, 2L, 0, TypeInfo.TYPE_VARCHAR), null);
        assertEquals(exp, got);
        assertEquals(Value.VARCHAR, ((ValueArray) got).getComponentType().getValueType());
        exp = ValueArray.get(TypeInfo.TYPE_VARCHAR, new Value[0], null);
        got = src.castTo(TypeInfo.getTypeInfo(Value.ARRAY, 0L, 0, TypeInfo.TYPE_VARCHAR), null);
        assertEquals(exp, got);
        assertEquals(Value.VARCHAR, ((ValueArray) got).getComponentType().getValueType());
    }

    private void testUUID() {
        long maxHigh = 0, maxLow = 0, minHigh = -1L, minLow = -1L;
        for (int i = 0; i < 100; i++) {
            ValueUuid uuid = ValueUuid.getNewRandom(4);
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
        ValueJavaObject valObj = ValueJavaObject.getNoCopy(JdbcUtils.serialize(origUUID, null));
        ValueUuid valUUID = valObj.convertToUuid();
        assertEquals(uuidStr, valUUID.getString());
        assertEquals(origUUID, valUUID.getUuid());

        ValueJavaObject voString = ValueJavaObject.getNoCopy(JdbcUtils.serialize(
                new String("This is not a ValueUuid object"), null));
        assertThrows(ErrorCode.DESERIALIZATION_FAILED_1, () -> voString.convertToUuid());
    }

    private void testModulusDouble() {
        final ValueDouble vd1 = ValueDouble.get(12);
        assertThrows(ErrorCode.DIVISION_BY_ZERO_1, () -> vd1.modulus(ValueDouble.ZERO));
        ValueDouble vd2 = ValueDouble.get(10);
        ValueDouble vd3 = vd1.modulus(vd2);
        assertEquals(2, vd3.getDouble());
    }

    private void testModulusDecimal() {
        final ValueNumeric vd1 = ValueNumeric.get(new BigDecimal(12));
        assertThrows(ErrorCode.DIVISION_BY_ZERO_1, () -> vd1.modulus(ValueNumeric.ZERO));
        ValueNumeric vd2 = ValueNumeric.get(new BigDecimal(10));
        Value vd3 = vd1.modulus(vd2);
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
            Database dh = ((SessionLocal) ((JdbcConnection) c).getSession()).getDatabase();
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
        return lob1.compareTypeSafe(lob2, null, null);
    }

    private static Value createLob(DataHandler dh, int type, byte[] bytes) {
        if (dh == null) {
            return type == Value.BLOB ? ValueBlob.createSmall(bytes) : ValueClob.createSmall(bytes);
        }
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        if (type == Value.BLOB) {
            return dh.getLobStorage().createBlob(in, -1);
        } else {
            return dh.getLobStorage().createClob(new InputStreamReader(in, StandardCharsets.UTF_8), -1);
        }
    }

    private void testTypeInfo() {
        testTypeInfoCheck(Value.UNKNOWN, -1, -1, -1, TypeInfo.TYPE_UNKNOWN);
        assertThrows(ErrorCode.UNKNOWN_DATA_TYPE_1, () -> TypeInfo.getTypeInfo(Value.UNKNOWN));

        testTypeInfoCheck(Value.NULL, 1, 0, 4, TypeInfo.TYPE_NULL, TypeInfo.getTypeInfo(Value.NULL));

        testTypeInfoCheck(Value.BOOLEAN, 1, 0, 5, TypeInfo.TYPE_BOOLEAN, TypeInfo.getTypeInfo(Value.BOOLEAN));

        testTypeInfoCheck(Value.TINYINT, 8, 0, 4, TypeInfo.TYPE_TINYINT, TypeInfo.getTypeInfo(Value.TINYINT));
        testTypeInfoCheck(Value.SMALLINT, 16, 0, 6, TypeInfo.TYPE_SMALLINT, TypeInfo.getTypeInfo(Value.SMALLINT));
        testTypeInfoCheck(Value.INTEGER, 32, 0, 11, TypeInfo.TYPE_INTEGER, TypeInfo.getTypeInfo(Value.INTEGER));
        testTypeInfoCheck(Value.BIGINT, 64, 0, 20, TypeInfo.TYPE_BIGINT, TypeInfo.getTypeInfo(Value.BIGINT));

        testTypeInfoCheck(Value.REAL, 24, 0, 15, TypeInfo.TYPE_REAL, TypeInfo.getTypeInfo(Value.REAL));
        testTypeInfoCheck(Value.DOUBLE, 53, 0, 24, TypeInfo.TYPE_DOUBLE, TypeInfo.getTypeInfo(Value.DOUBLE));
        testTypeInfoCheck(Value.NUMERIC, MAX_NUMERIC_PRECISION, MAX_NUMERIC_PRECISION / 2, MAX_NUMERIC_PRECISION + 2,
                TypeInfo.TYPE_NUMERIC_FLOATING_POINT);

        testTypeInfoCheck(Value.TIME, 18, 9, 18, TypeInfo.TYPE_TIME, TypeInfo.getTypeInfo(Value.TIME));
        for (int s = 0; s <= 9; s++) {
            int d = s > 0 ? s + 9 : 8;
            testTypeInfoCheck(Value.TIME, d, s, d, TypeInfo.getTypeInfo(Value.TIME, 0, s, null));
        }
        testTypeInfoCheck(Value.DATE, 10, 0, 10, TypeInfo.TYPE_DATE, TypeInfo.getTypeInfo(Value.DATE));
        testTypeInfoCheck(Value.TIMESTAMP, 29, 9, 29, TypeInfo.TYPE_TIMESTAMP, TypeInfo.getTypeInfo(Value.TIMESTAMP));
        for (int s = 0; s <= 9; s++) {
            int d = s > 0 ? s + 20 : 19;
            testTypeInfoCheck(Value.TIMESTAMP, d, s, d, TypeInfo.getTypeInfo(Value.TIMESTAMP, 0, s, null));
        }
        testTypeInfoCheck(Value.TIMESTAMP_TZ, 35, 9, 35, TypeInfo.TYPE_TIMESTAMP_TZ,
                TypeInfo.getTypeInfo(Value.TIMESTAMP_TZ));
        for (int s = 0; s <= 9; s++) {
            int d = s > 0 ? s + 26 : 25;
            testTypeInfoCheck(Value.TIMESTAMP_TZ, d, s, d, TypeInfo.getTypeInfo(Value.TIMESTAMP_TZ, 0, s, null));
        }

        testTypeInfoCheck(Value.BINARY, 1, 0, 2, TypeInfo.getTypeInfo(Value.BINARY));
        testTypeInfoCheck(Value.VARBINARY, MAX_STRING_LENGTH, 0, MAX_STRING_LENGTH * 2,
                TypeInfo.getTypeInfo(Value.VARBINARY));
        testTypeInfoCheck(Value.BLOB, Long.MAX_VALUE, 0, Integer.MAX_VALUE, TypeInfo.getTypeInfo(Value.BLOB));
        testTypeInfoCheck(Value.CLOB, Long.MAX_VALUE, 0, Integer.MAX_VALUE, TypeInfo.getTypeInfo(Value.CLOB));

        testTypeInfoCheck(Value.VARCHAR, MAX_STRING_LENGTH, 0, MAX_STRING_LENGTH, TypeInfo.TYPE_VARCHAR,
                TypeInfo.getTypeInfo(Value.VARCHAR));
        testTypeInfoCheck(Value.CHAR, 1, 0, 1, TypeInfo.getTypeInfo(Value.CHAR));
        testTypeInfoCheck(Value.VARCHAR_IGNORECASE, MAX_STRING_LENGTH, 0, MAX_STRING_LENGTH,
                TypeInfo.getTypeInfo(Value.VARCHAR_IGNORECASE));

        testTypeInfoCheck(Value.ARRAY, MAX_ARRAY_CARDINALITY, 0, Integer.MAX_VALUE, TypeInfo.TYPE_ARRAY_UNKNOWN,
                TypeInfo.getTypeInfo(Value.ARRAY));
        testTypeInfoCheck(Value.ROW, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, TypeInfo.TYPE_ROW_EMPTY,
                TypeInfo.getTypeInfo(Value.ROW));

        testTypeInfoCheck(Value.JAVA_OBJECT, MAX_STRING_LENGTH, 0, MAX_STRING_LENGTH * 2, TypeInfo.TYPE_JAVA_OBJECT,
                TypeInfo.getTypeInfo(Value.JAVA_OBJECT));
        testTypeInfoCheck(Value.UUID, 16, 0, 36, TypeInfo.TYPE_UUID, TypeInfo.getTypeInfo(Value.UUID));
        testTypeInfoCheck(Value.GEOMETRY, MAX_STRING_LENGTH, 0, Integer.MAX_VALUE, TypeInfo.TYPE_GEOMETRY,
                TypeInfo.getTypeInfo(Value.GEOMETRY));
        testTypeInfoCheck(Value.ENUM, MAX_STRING_LENGTH, 0, MAX_STRING_LENGTH, TypeInfo.TYPE_ENUM_UNDEFINED,
                TypeInfo.getTypeInfo(Value.ENUM));

        testTypeInfoInterval1(Value.INTERVAL_YEAR);
        testTypeInfoInterval1(Value.INTERVAL_MONTH);
        testTypeInfoInterval1(Value.INTERVAL_DAY);
        testTypeInfoInterval1(Value.INTERVAL_HOUR);
        testTypeInfoInterval1(Value.INTERVAL_MINUTE);
        testTypeInfoInterval2(Value.INTERVAL_SECOND);
        testTypeInfoInterval1(Value.INTERVAL_YEAR_TO_MONTH);
        testTypeInfoInterval1(Value.INTERVAL_DAY_TO_HOUR);
        testTypeInfoInterval1(Value.INTERVAL_DAY_TO_MINUTE);
        testTypeInfoInterval2(Value.INTERVAL_DAY_TO_SECOND);
        testTypeInfoInterval1(Value.INTERVAL_HOUR_TO_MINUTE);
        testTypeInfoInterval2(Value.INTERVAL_HOUR_TO_SECOND);
        testTypeInfoInterval2(Value.INTERVAL_MINUTE_TO_SECOND);

        testTypeInfoCheck(Value.JSON, MAX_STRING_LENGTH, 0, MAX_STRING_LENGTH, TypeInfo.TYPE_JSON,
                TypeInfo.getTypeInfo(Value.JSON));
    }

    private void testTypeInfoInterval1(int type) {
        testTypeInfoCheck(type, 18, 0, ValueInterval.getDisplaySize(type, 18, 0), TypeInfo.getTypeInfo(type));
        for (int p = 1; p <= 18; p++) {
            testTypeInfoCheck(type, p, 0, ValueInterval.getDisplaySize(type, p, 0),
                    TypeInfo.getTypeInfo(type, p, 0, null));
        }
    }

    private void testTypeInfoInterval2(int type) {
        testTypeInfoCheck(type, 18, 9, ValueInterval.getDisplaySize(type, 18, 9), TypeInfo.getTypeInfo(type));
        for (int p = 1; p <= 18; p++) {
            for (int s = 0; s <= 9; s++) {
                testTypeInfoCheck(type, p, s, ValueInterval.getDisplaySize(type, p, s),
                        TypeInfo.getTypeInfo(type, p, s, null));
            }
        }
    }

    private void testTypeInfoCheck(int valueType, long precision, int scale, int displaySize, TypeInfo... typeInfos) {
        for (TypeInfo typeInfo : typeInfos) {
            testTypeInfoCheck(valueType, precision, scale, displaySize, typeInfo);
        }
    }

    private void testTypeInfoCheck(int valueType, long precision, int scale, int displaySize, TypeInfo typeInfo) {
        assertEquals(valueType, typeInfo.getValueType());
        assertEquals(precision, typeInfo.getPrecision());
        assertEquals(scale, typeInfo.getScale());
        assertEquals(displaySize, typeInfo.getDisplaySize());
    }

    private void testH2Type() {
        assertEquals(Value.CHAR, (int) H2Type.CHAR.getVendorTypeNumber());
        assertEquals(Value.VARCHAR, (int) H2Type.VARCHAR.getVendorTypeNumber());
        assertEquals(Value.CLOB, (int) H2Type.CLOB.getVendorTypeNumber());
        assertEquals(Value.VARCHAR_IGNORECASE, (int) H2Type.VARCHAR_IGNORECASE.getVendorTypeNumber());
        assertEquals(Value.BINARY, (int) H2Type.BINARY.getVendorTypeNumber());
        assertEquals(Value.VARBINARY, (int) H2Type.VARBINARY.getVendorTypeNumber());
        assertEquals(Value.BLOB, (int) H2Type.BLOB.getVendorTypeNumber());
        assertEquals(Value.BOOLEAN, (int) H2Type.BOOLEAN.getVendorTypeNumber());
        assertEquals(Value.TINYINT, (int) H2Type.TINYINT.getVendorTypeNumber());
        assertEquals(Value.SMALLINT, (int) H2Type.SMALLINT.getVendorTypeNumber());
        assertEquals(Value.INTEGER, (int) H2Type.INTEGER.getVendorTypeNumber());
        assertEquals(Value.BIGINT, (int) H2Type.BIGINT.getVendorTypeNumber());
        assertEquals(Value.NUMERIC, (int) H2Type.NUMERIC.getVendorTypeNumber());
        assertEquals(Value.REAL, (int) H2Type.REAL.getVendorTypeNumber());
        assertEquals(Value.DOUBLE, (int) H2Type.DOUBLE_PRECISION.getVendorTypeNumber());
        assertEquals(Value.DECFLOAT, (int) H2Type.DECFLOAT.getVendorTypeNumber());
        assertEquals(Value.DATE, (int) H2Type.DATE.getVendorTypeNumber());
        assertEquals(Value.TIME, (int) H2Type.TIME.getVendorTypeNumber());
        assertEquals(Value.TIME_TZ, (int) H2Type.TIME_WITH_TIME_ZONE.getVendorTypeNumber());
        assertEquals(Value.TIMESTAMP, (int) H2Type.TIMESTAMP.getVendorTypeNumber());
        assertEquals(Value.TIMESTAMP_TZ, (int) H2Type.TIMESTAMP_WITH_TIME_ZONE.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_YEAR, (int) H2Type.INTERVAL_YEAR.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_MONTH, (int) H2Type.INTERVAL_MONTH.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_DAY, (int) H2Type.INTERVAL_DAY.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_HOUR, (int) H2Type.INTERVAL_HOUR.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_MINUTE, (int) H2Type.INTERVAL_MINUTE.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_SECOND, (int) H2Type.INTERVAL_SECOND.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_YEAR_TO_MONTH, (int) H2Type.INTERVAL_YEAR_TO_MONTH.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_DAY_TO_HOUR, (int) H2Type.INTERVAL_DAY_TO_HOUR.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_DAY_TO_MINUTE, (int) H2Type.INTERVAL_DAY_TO_MINUTE.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_DAY_TO_SECOND, (int) H2Type.INTERVAL_DAY_TO_SECOND.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_HOUR_TO_MINUTE, (int) H2Type.INTERVAL_HOUR_TO_MINUTE.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_HOUR_TO_SECOND, (int) H2Type.INTERVAL_HOUR_TO_SECOND.getVendorTypeNumber());
        assertEquals(Value.INTERVAL_MINUTE_TO_SECOND, (int) H2Type.INTERVAL_MINUTE_TO_SECOND.getVendorTypeNumber());
        assertEquals(Value.JAVA_OBJECT, (int) H2Type.JAVA_OBJECT.getVendorTypeNumber());
        assertEquals(Value.ENUM, (int) H2Type.ENUM.getVendorTypeNumber());
        assertEquals(Value.GEOMETRY, (int) H2Type.GEOMETRY.getVendorTypeNumber());
        assertEquals(Value.JSON, (int) H2Type.JSON.getVendorTypeNumber());
        assertEquals(Value.UUID, (int) H2Type.UUID.getVendorTypeNumber());
        assertEquals(Value.ARRAY, (int) H2Type.array(H2Type.VARCHAR).getVendorTypeNumber());
        assertEquals(Value.ROW, (int) H2Type.row(H2Type.VARCHAR).getVendorTypeNumber());
    }

    private void testHigherType() {
        testHigherTypeNumeric(15L, 6, 10L, 1, 5L, 6);
        testHigherTypeNumeric(15L, 6, 5L, 6, 10L, 1);
        TypeInfo intArray10 = TypeInfo.getTypeInfo(Value.ARRAY, 10, 0, TypeInfo.TYPE_INTEGER);
        TypeInfo bigintArray1 = TypeInfo.getTypeInfo(Value.ARRAY, 1, 0, TypeInfo.TYPE_BIGINT);
        TypeInfo bigintArray10 = TypeInfo.getTypeInfo(Value.ARRAY, 10, 0, TypeInfo.TYPE_BIGINT);
        assertEquals(bigintArray10, TypeInfo.getHigherType(intArray10, bigintArray1));
        TypeInfo intArray10Array1 = TypeInfo.getTypeInfo(Value.ARRAY, 1, 0, intArray10);
        TypeInfo bigintArray1Array10 = TypeInfo.getTypeInfo(Value.ARRAY, 10, 0, bigintArray1);
        TypeInfo bigintArray10Array10 = TypeInfo.getTypeInfo(Value.ARRAY, 10, 0, bigintArray10);
        assertEquals(bigintArray10Array10, TypeInfo.getHigherType(intArray10Array1, bigintArray1Array10));
        assertEquals(bigintArray10Array10, TypeInfo.getHigherType(intArray10, bigintArray1Array10));
        TypeInfo bigintArray10Array1 = TypeInfo.getTypeInfo(Value.ARRAY, 1, 0, bigintArray10);
        assertEquals(bigintArray10Array1, TypeInfo.getHigherType(intArray10Array1, bigintArray1));
    }

    private void testHigherTypeNumeric(long expectedPrecision, int expectedScale, long precision1, int scale1,
            long precision2, int scale2) {
        assertEquals(TypeInfo.getTypeInfo(Value.NUMERIC, expectedPrecision, expectedScale, null),
                TypeInfo.getHigherType(TypeInfo.getTypeInfo(Value.NUMERIC, precision1, scale1, null),
                        TypeInfo.getTypeInfo(Value.NUMERIC, precision2, scale2, null)));
    }

}
