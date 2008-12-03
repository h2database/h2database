/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.h2.test.TestBase;

/**
 * Updatable result set tests.
 */
public class TestUpdatableResultSet extends TestBase {

    public void test() throws SQLException {
        testUpdateLob();
        testScroll();
        testUpdateDeleteInsert();
        testUpdateDataType();
        testUpdateResetRead();
        deleteDb("updatableResultSet");
    }

    private void testUpdateLob() throws SQLException {
        deleteDb("updatableResultSet");
        Connection conn = getConnection("updatableResultSet");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE object_index (id integer primary key, object other, number integer)");

        PreparedStatement prep = conn.prepareStatement("INSERT INTO object_index (id,object)  VALUES (1,?)");
        prep.setObject(1, "hello", Types.JAVA_OBJECT);
        prep.execute();

        ResultSet rs = stat.executeQuery("SELECT object,id,number FROM object_index WHERE id =1");
        rs.next();
        assertEquals("hello", rs.getObject(1).toString());
        stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        rs = stat.executeQuery("SELECT object,id,number FROM object_index WHERE id =1");
        rs.next();
        assertEquals("hello", rs.getObject(1).toString());
        rs.updateInt(2, 1);
        rs.updateRow();
        rs.close();
        stat = conn.createStatement();
        rs = stat.executeQuery("SELECT object,id,number FROM object_index WHERE id =1");
        rs.next();
        assertEquals("hello", rs.getObject(1).toString());
        conn.close();
    }

    private void testUpdateResetRead() throws SQLException {
        deleteDb("updatableResultSet");
        Connection conn = getConnection("updatableResultSet");
        Statement stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        rs.updateInt(1, 10);
        rs.updateRow();
        rs.next();
        rs.updateString(2, "Welt");
        rs.updateRow();
        rs.beforeFirst();
        rs.next();
        assertEquals(10, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("Welt", rs.getString(2));
        conn.close();
    }

    private void testScroll() throws SQLException {
        deleteDb("updatableResultSet");
        Connection conn = getConnection("updatableResultSet");
        Statement stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World'), (3, 'Test')");

        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");

        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertEquals(rs.getRow(), 0);

        rs.next();
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getRow(), 1);

        rs.next();

        try {
            rs.insertRow();
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }

        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getRow(), 2);

        rs.next();
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertEquals(rs.getInt(1), 3);
        assertEquals(rs.getRow(), 3);

        assertFalse(rs.next());
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        assertEquals(rs.getRow(), 0);

        assertTrue(rs.first());
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getRow(), 1);

        assertTrue(rs.last());
        assertEquals(rs.getInt(1), 3);
        assertEquals(rs.getRow(), 3);

        assertTrue(rs.relative(0));
        assertEquals(rs.getRow(), 3);

        assertTrue(rs.relative(-1));
        assertEquals(rs.getRow(), 2);

        assertTrue(rs.relative(1));
        assertEquals(rs.getRow(), 3);

        assertFalse(rs.relative(100));
        assertTrue(rs.isAfterLast());

        assertFalse(rs.absolute(0));
        assertEquals(rs.getRow(), 0);

        assertTrue(rs.absolute(1));
        assertEquals(rs.getRow(), 1);

        assertTrue(rs.absolute(2));
        assertEquals(rs.getRow(), 2);

        assertTrue(rs.absolute(3));
        assertEquals(rs.getRow(), 3);

        assertFalse(rs.absolute(4));
        assertEquals(rs.getRow(), 0);

        try {
            assertFalse(rs.absolute(0));
            // actually, we allow it for compatibility
            // error("absolute 0 not allowed");
        } catch (SQLException e) {
            assertKnownException(e);
        }

        assertTrue(rs.absolute(3));
        assertEquals(rs.getRow(), 3);

        assertTrue(rs.absolute(-1));
        assertEquals(rs.getRow(), 3);

        assertFalse(rs.absolute(4));
        assertTrue(rs.isAfterLast());

        assertFalse(rs.absolute(5));
        assertTrue(rs.isAfterLast());

        assertTrue(rs.previous());
        assertEquals(rs.getRow(), 3);

        assertTrue(rs.previous());
        assertEquals(rs.getRow(), 2);

        conn.close();
    }

    private void testUpdateDataType() throws SQLException {
        deleteDb("updatableResultSet");
        Connection conn = getConnection("updatableResultSet");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255), "
                + "DEC DECIMAL(10,2), BOO BIT, BYE TINYINT, BIN BINARY(100), "
                + "D DATE, T TIME, TS TIMESTAMP, DB DOUBLE, R REAL, L BIGINT, "
                + "O_I INT, SH SMALLINT, CL CLOB, BL BLOB)");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(meta.getColumnClassName(1), "java.lang.Integer");
        assertEquals(meta.getColumnClassName(2), "java.lang.String");
        assertEquals(meta.getColumnClassName(3), "java.math.BigDecimal");
        assertEquals(meta.getColumnClassName(4), "java.lang.Boolean");
        assertEquals(meta.getColumnClassName(5), "java.lang.Byte");
        assertEquals(meta.getColumnClassName(6), "[B");
        assertEquals(meta.getColumnClassName(7), "java.sql.Date");
        assertEquals(meta.getColumnClassName(8), "java.sql.Time");
        assertEquals(meta.getColumnClassName(9), "java.sql.Timestamp");
        assertEquals(meta.getColumnClassName(10), "java.lang.Double");
        assertEquals(meta.getColumnClassName(11), "java.lang.Float");
        assertEquals(meta.getColumnClassName(12), "java.lang.Long");
        assertEquals(meta.getColumnClassName(13), "java.lang.Integer");
        assertEquals(meta.getColumnClassName(14), "java.lang.Short");
        assertEquals(meta.getColumnClassName(15), "java.io.Reader");
        assertEquals(meta.getColumnClassName(16), "java.io.InputStream");

        rs.moveToInsertRow();
        rs.updateInt(1, 0);
        rs.updateNull(2);
        rs.updateNull("DEC");
        // 'not set' values are set to null
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt(1, 1);
        rs.updateString(2, null);
        rs.updateBigDecimal(3, null);
        rs.updateBoolean(4, false);
        rs.updateByte(5, (byte) 0);
        rs.updateBytes(6, null);
        rs.updateDate(7, null);
        rs.updateTime(8, null);
        rs.updateTimestamp(9, null);
        rs.updateDouble(10, 0.0);
        rs.updateFloat(11, (float) 0.0);
        rs.updateLong(12, 0L);
        rs.updateObject(13, null);
        rs.updateShort(14, (short) 0);
        rs.updateCharacterStream(15, new StringReader("test"), 0);
        rs.updateBinaryStream(16, new ByteArrayInputStream(new byte[] { (byte) 0xff, 0x00 }), 0);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 2);
        rs.updateString("NAME", "+");
        rs.updateBigDecimal("DEC", new BigDecimal("1.2"));
        rs.updateBoolean("BOO", true);
        rs.updateByte("BYE", (byte) 0xff);
        rs.updateBytes("BIN", new byte[] { 0x00, (byte) 0xff });
        rs.updateDate("D", Date.valueOf("2005-09-21"));
        rs.updateTime("T", Time.valueOf("21:46:28"));
        rs.updateTimestamp("TS", Timestamp.valueOf("2005-09-21 21:47:09.567890123"));
        rs.updateDouble("DB", 1.725);
        rs.updateFloat("R", (float) 2.5);
        rs.updateLong("L", Long.MAX_VALUE);
        rs.updateObject("O_I", new Integer(10));
        rs.updateShort("SH", Short.MIN_VALUE);
        // auml, ouml, uuml
        rs.updateCharacterStream("CL", new StringReader("\u00ef\u00f6\u00fc"), 0);
        rs.updateBinaryStream("BL", new ByteArrayInputStream(new byte[] { (byte) 0xab, 0x12 }), 0);
        rs.insertRow();

        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID NULLS FIRST");
        rs.next();
        assertTrue(rs.getInt(1) == 0);
        assertTrue(rs.getString(2) == null && rs.wasNull());
        assertTrue(rs.getBigDecimal(3) == null && rs.wasNull());
        assertTrue(!rs.getBoolean(4) && rs.wasNull());
        assertTrue(rs.getByte(5) == 0 && rs.wasNull());
        assertTrue(rs.getBytes(6) == null && rs.wasNull());
        assertTrue(rs.getDate(7) == null && rs.wasNull());
        assertTrue(rs.getTime(8) == null && rs.wasNull());
        assertTrue(rs.getTimestamp(9) == null && rs.wasNull());
        assertTrue(rs.getDouble(10) == 0.0 && rs.wasNull());
        assertTrue(rs.getFloat(11) == 0.0 && rs.wasNull());
        assertTrue(rs.getLong(12) == 0 && rs.wasNull());
        assertTrue(rs.getObject(13) == null && rs.wasNull());
        assertTrue(rs.getShort(14) == 0 && rs.wasNull());
        assertTrue(rs.getCharacterStream(15) == null && rs.wasNull());
        assertTrue(rs.getBinaryStream(16) == null && rs.wasNull());

        rs.next();
        assertTrue(rs.getInt(1) == 1);
        assertTrue(rs.getString(2) == null && rs.wasNull());
        assertTrue(rs.getBigDecimal(3) == null && rs.wasNull());
        assertTrue(!rs.getBoolean(4) && !rs.wasNull());
        assertTrue(rs.getByte(5) == 0 && !rs.wasNull());
        assertTrue(rs.getBytes(6) == null && rs.wasNull());
        assertTrue(rs.getDate(7) == null && rs.wasNull());
        assertTrue(rs.getTime(8) == null && rs.wasNull());
        assertTrue(rs.getTimestamp(9) == null && rs.wasNull());
        assertTrue(rs.getDouble(10) == 0.0 && !rs.wasNull());
        assertTrue(rs.getFloat(11) == 0.0 && !rs.wasNull());
        assertTrue(rs.getLong(12) == 0 && !rs.wasNull());
        assertTrue(rs.getObject(13) == null && rs.wasNull());
        assertTrue(rs.getShort(14) == 0 && !rs.wasNull());
        assertEquals(rs.getString(15), "test");
        assertEquals(rs.getBytes(16), new byte[] { (byte) 0xff, 0x00 });

        rs.next();
        assertTrue(rs.getInt(1) == 2);
        assertEquals(rs.getString(2), "+");
        assertEquals(rs.getBigDecimal(3).toString(), "1.20");
        assertTrue(rs.getBoolean(4));
        assertTrue((rs.getByte(5) & 0xff) == 0xff);
        assertEquals(rs.getBytes(6), new byte[] { 0x00, (byte) 0xff });
        assertEquals(rs.getDate(7).toString(), "2005-09-21");
        assertEquals(rs.getTime(8).toString(), "21:46:28");
        assertEquals(rs.getTimestamp(9).toString(), "2005-09-21 21:47:09.567890123");
        assertTrue(rs.getDouble(10) == 1.725);
        assertTrue(rs.getFloat(11) == (float) 2.5);
        assertTrue(rs.getLong(12) == Long.MAX_VALUE);
        assertEquals(((Integer) rs.getObject(13)).intValue(), 10);
        assertTrue(rs.getShort(14) == Short.MIN_VALUE);
        // auml ouml uuml
        assertEquals(rs.getString(15), "\u00ef\u00f6\u00fc");
        assertEquals(rs.getBytes(16), new byte[] { (byte) 0xab, 0x12 });

        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
        conn.close();
    }

    private void testUpdateDeleteInsert() throws SQLException {
        deleteDb("updatableResultSet");
        Connection c1 = getConnection("updatableResultSet");
        Connection c2 = getConnection("updatableResultSet");
        Statement stat = c1.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        int max = 8;
        for (int i = 0; i < max; i++) {
            stat.execute("INSERT INTO TEST VALUES(" + i + ", 'Hello" + i + "')");
        }
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        int j = max;
        while (rs.next()) {
            int id = rs.getInt(1);
            if (id % 2 == 0) {
                Statement s2 = c2.createStatement();
                s2.execute("UPDATE TEST SET NAME = NAME || '+' WHERE ID = " + rs.getInt(1));
                if (id % 4 == 0) {
                    rs.refreshRow();
                }
                rs.updateString(2, "Updated " + rs.getString(2));
                rs.updateRow();
            } else {
                rs.deleteRow();
            }
            rs.moveToInsertRow();
            rs.updateString(2, "Inserted " + j);
            rs.updateInt(1, j);
            j += 2;
            rs.insertRow();
        }
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        while (rs.next()) {
            int id = rs.getInt(1);
            String name = rs.getString(2);
            assertEquals(id % 2, 0);
            if (id >= max) {
                assertEquals("Inserted " + id, rs.getString(2));
            } else {
                if (id % 4 == 0) {
                    assertEquals(rs.getString(2), "Updated Hello" + id + "+");
                } else {
                    assertEquals(rs.getString(2), "Updated Hello" + id);
                }
            }
            trace("id=" + id + " name=" + name);
        }
        c2.close();
        c1.close();

        // test scrollable result sets
        Connection conn = getConnection("updatableResultSet");
        for (int i = 0; i < 5; i++) {
            testScrollable(conn, i);
        }
        conn.close();
    }

    private void testScrollable(Connection conn, int rows) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("DELETE FROM TEST");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        for (int i = 0; i < rows; i++) {
            prep.setInt(1, i);
            prep.setString(2, "Data " + i);
            prep.execute();
        }
        Statement regular = conn.createStatement();
        testScrollResultSet(regular, ResultSet.TYPE_FORWARD_ONLY, rows);
        Statement scroll = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        testScrollResultSet(scroll, ResultSet.TYPE_SCROLL_INSENSITIVE, rows);
    }

    private void testScrollResultSet(Statement stat, int type, int rows) throws SQLException {
        boolean error = false;
        if (type == ResultSet.TYPE_FORWARD_ONLY) {
            error = true;
        }
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        assertEquals(rs.getType(), type);

        checkState(rs, true, false, false, rows == 0);
        for (int i = 0; i < rows; i++) {
            rs.next();
            checkState(rs, rows == 0, i == 0, i == rows - 1, rows == 0 || i == rows);
        }
        try {
            rs.beforeFirst();
            checkState(rs, true, false, false, rows == 0);
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
        try {
            rs.afterLast();
            checkState(rs, false, false, false, true);
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
        try {
            boolean valid = rs.first();
            assertEquals(valid, rows > 0);
            if (valid) {
                checkState(rs, false, true, rows == 1, rows == 0);
            }
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
        try {
            boolean valid = rs.last();
            assertEquals(valid, rows > 0);
            if (valid) {
                checkState(rs, false, rows == 1, true, rows == 0);
            }
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
    }

    private void checkState(ResultSet rs, boolean beforeFirst, boolean first, boolean last, boolean afterLast) throws SQLException {
        assertEquals(rs.isBeforeFirst(), beforeFirst);
        assertEquals(rs.isFirst(), first);
        assertEquals(rs.isLast(), last);
        assertEquals(rs.isAfterLast(), afterLast);
    }

}
