/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.UUID;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueResultSet;
import org.h2.value.ValueUuid;

/**
 * Tests features of values.
 */
public class TestValue extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        testValueResultSet();
        testDataType();
        testUUID();
        testDouble(false);
        testDouble(true);
    }

    private void testValueResultSet() throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("ID", Types.INTEGER, 0, 0);
        rs.addColumn("NAME", Types.VARCHAR, 255, 0);
        rs.addRow(1, "Hello");
        rs.addRow(2, "World");
        rs.addRow(3, "Peace");
        ValueResultSet v = ValueResultSet.getCopy(rs, 2);
        rs.beforeFirst();
        ResultSet rs2 = v.getResultSet();
        rs2.next();
        rs.next();
        assertEquals(rs.getInt(1), rs2.getInt(1));
        assertEquals(rs.getString(2), rs2.getString(2));
        rs2.next();
        rs.next();
        assertEquals(rs.getInt(1), rs2.getInt(1));
        assertEquals(rs.getString(2), rs2.getString(2));
        assertFalse(rs2.next());
        assertTrue(rs.next());
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
        testDataType(Value.BLOB, Value.ValueBlob.class);
        testDataType(Value.CLOB, Value.ValueClob.class);
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
            Value v = useFloat ? (Value) ValueFloat.get((float) d[i]) : (Value) ValueDouble.get(d[i]);
            values[i] = v;
            assertTrue(values[i].compareTypeSave(values[i], null) == 0);
            assertTrue(v.equals(v));
            assertEquals(i < 2 ? -1 : i > 2 ? 1 : 0, v.getSignum());
        }
        for (int i = 0; i < d.length - 1; i++) {
            assertTrue(values[i].compareTypeSave(values[i+1], null) < 0);
            assertTrue(values[i + 1].compareTypeSave(values[i], null) > 0);
            assertTrue(!values[i].equals(values[i+1]));
        }
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
    }

}
