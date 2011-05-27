/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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

import org.h2.test.TestBase;

/**
 * Updatable result set tests.
 */
public class TestUpdatableResultSet extends TestBase {

    public void test() throws Exception {
        testScroll();
        testUpdateDeleteInsert();
        testUpdateDataType();
    }

    private void testScroll() throws Exception {
        deleteDb("updatableResultSet");
        Connection conn = getConnection("updatableResultSet");
        Statement stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World'), (3, 'Test')");

        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        check(rs.isBeforeFirst());
        checkFalse(rs.isAfterLast());
        check(rs.getRow(), 0);

        rs.next();
        checkFalse(rs.isBeforeFirst());
        checkFalse(rs.isAfterLast());
        check(rs.getInt(1), 1);
        check(rs.getRow(), 1);

        rs.next();
        checkFalse(rs.isBeforeFirst());
        checkFalse(rs.isAfterLast());
        check(rs.getInt(1), 2);
        check(rs.getRow(), 2);

        rs.next();
        checkFalse(rs.isBeforeFirst());
        checkFalse(rs.isAfterLast());
        check(rs.getInt(1), 3);
        check(rs.getRow(), 3);

        checkFalse(rs.next());
        checkFalse(rs.isBeforeFirst());
        check(rs.isAfterLast());
        check(rs.getRow(), 0);

        check(rs.first());
        check(rs.getInt(1), 1);
        check(rs.getRow(), 1);

        check(rs.last());
        check(rs.getInt(1), 3);
        check(rs.getRow(), 3);

        check(rs.relative(0));
        check(rs.getRow(), 3);

        check(rs.relative(-1));
        check(rs.getRow(), 2);

        check(rs.relative(1));
        check(rs.getRow(), 3);

        checkFalse(rs.relative(100));
        check(rs.isAfterLast());

        checkFalse(rs.absolute(0));
        check(rs.getRow(), 0);

        check(rs.absolute(1));
        check(rs.getRow(), 1);

        check(rs.absolute(2));
        check(rs.getRow(), 2);

        check(rs.absolute(3));
        check(rs.getRow(), 3);

        checkFalse(rs.absolute(4));
        check(rs.getRow(), 0);

        try {
            checkFalse(rs.absolute(0));
            // actually, we allow it for compatibility
            // error("absolute 0 not allowed");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }

        check(rs.absolute(3));
        check(rs.getRow(), 3);

        check(rs.absolute(-1));
        check(rs.getRow(), 3);

        checkFalse(rs.absolute(4));
        check(rs.isAfterLast());

        checkFalse(rs.absolute(5));
        check(rs.isAfterLast());

        check(rs.previous());
        check(rs.getRow(), 3);

        check(rs.previous());
        check(rs.getRow(), 2);

        conn.close();
    }

    private void testUpdateDataType() throws Exception {
        deleteDb("updatableResultSet");
        Connection conn = getConnection("updatableResultSet");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255), "
                + "DEC DECIMAL(10,2), BOO BIT, BYE TINYINT, BIN BINARY(100), "
                + "D DATE, T TIME, TS TIMESTAMP, DB DOUBLE, R REAL, L BIGINT, "
                + "O_I INT, SH SMALLINT, CL CLOB, BL BLOB)");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        ResultSetMetaData meta = rs.getMetaData();
        check(meta.getColumnClassName(1), "java.lang.Integer");
        check(meta.getColumnClassName(2), "java.lang.String");
        check(meta.getColumnClassName(3), "java.math.BigDecimal");
        check(meta.getColumnClassName(4), "java.lang.Boolean");
        check(meta.getColumnClassName(5), "java.lang.Byte");
        check(meta.getColumnClassName(6), "[B");
        check(meta.getColumnClassName(7), "java.sql.Date");
        check(meta.getColumnClassName(8), "java.sql.Time");
        check(meta.getColumnClassName(9), "java.sql.Timestamp");
        check(meta.getColumnClassName(10), "java.lang.Double");
        check(meta.getColumnClassName(11), "java.lang.Float");
        check(meta.getColumnClassName(12), "java.lang.Long");
        check(meta.getColumnClassName(13), "java.lang.Integer");
        check(meta.getColumnClassName(14), "java.lang.Short");
        check(meta.getColumnClassName(15), "java.sql.Clob");
        check(meta.getColumnClassName(16), "java.sql.Blob");

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
        rs.updateCharacterStream("CL", new StringReader("\u00ef\u00f6\u00fc"), 0); // auml
                                                                                    // ouml
                                                                                    // uuml
        rs.updateBinaryStream("BL", new ByteArrayInputStream(new byte[] { (byte) 0xab, 0x12 }), 0);
        rs.insertRow();

        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID NULLS FIRST");
        rs.next();
        check(rs.getInt(1) == 0);
        check(rs.getString(2) == null && rs.wasNull());
        check(rs.getBigDecimal(3) == null && rs.wasNull());
        check(!rs.getBoolean(4) && rs.wasNull());
        check(rs.getByte(5) == 0 && rs.wasNull());
        check(rs.getBytes(6) == null && rs.wasNull());
        check(rs.getDate(7) == null && rs.wasNull());
        check(rs.getTime(8) == null && rs.wasNull());
        check(rs.getTimestamp(9) == null && rs.wasNull());
        check(rs.getDouble(10) == 0.0 && rs.wasNull());
        check(rs.getFloat(11) == 0.0 && rs.wasNull());
        check(rs.getLong(12) == 0 && rs.wasNull());
        check(rs.getObject(13) == null && rs.wasNull());
        check(rs.getShort(14) == 0 && rs.wasNull());
        check(rs.getCharacterStream(15) == null && rs.wasNull());
        check(rs.getBinaryStream(16) == null && rs.wasNull());

        rs.next();
        check(rs.getInt(1) == 1);
        check(rs.getString(2) == null && rs.wasNull());
        check(rs.getBigDecimal(3) == null && rs.wasNull());
        check(!rs.getBoolean(4) && !rs.wasNull());
        check(rs.getByte(5) == 0 && !rs.wasNull());
        check(rs.getBytes(6) == null && rs.wasNull());
        check(rs.getDate(7) == null && rs.wasNull());
        check(rs.getTime(8) == null && rs.wasNull());
        check(rs.getTimestamp(9) == null && rs.wasNull());
        check(rs.getDouble(10) == 0.0 && !rs.wasNull());
        check(rs.getFloat(11) == 0.0 && !rs.wasNull());
        check(rs.getLong(12) == 0 && !rs.wasNull());
        check(rs.getObject(13) == null && rs.wasNull());
        check(rs.getShort(14) == 0 && !rs.wasNull());
        check(rs.getString(15), "test");
        check(rs.getBytes(16), new byte[] { (byte) 0xff, 0x00 });

        rs.next();
        check(rs.getInt(1) == 2);
        check(rs.getString(2), "+");
        check(rs.getBigDecimal(3).toString(), "1.20");
        check(rs.getBoolean(4));
        check((rs.getByte(5) & 0xff) == 0xff);
        check(rs.getBytes(6), new byte[] { 0x00, (byte) 0xff });
        check(rs.getDate(7).toString(), "2005-09-21");
        check(rs.getTime(8).toString(), "21:46:28");
        check(rs.getTimestamp(9).toString(), "2005-09-21 21:47:09.567890123");
        check(rs.getDouble(10) == 1.725);
        check(rs.getFloat(11) == (float) 2.5);
        check(rs.getLong(12) == Long.MAX_VALUE);
        check(((Integer) rs.getObject(13)).intValue(), 10);
        check(rs.getShort(14) == Short.MIN_VALUE);
        check(rs.getString(15), "\u00ef\u00f6\u00fc"); // auml ouml uuml
        check(rs.getBytes(16), new byte[] { (byte) 0xab, 0x12 });

        checkFalse(rs.next());
        stat.execute("DROP TABLE TEST");
        conn.close();
    }

    private void testUpdateDeleteInsert() throws Exception {
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
            check(id % 2, 0);
            if (id >= max) {
                check("Inserted " + id, rs.getString(2));
            } else {
                if (id % 4 == 0) {
                    check(rs.getString(2), "Updated Hello" + id + "+");
                } else {
                    check(rs.getString(2), "Updated Hello" + id);
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

    void testScrollable(Connection conn, int rows) throws Exception {
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

    void testScrollResultSet(Statement stat, int type, int rows) throws Exception {
        boolean error = false;
        if (type == ResultSet.TYPE_FORWARD_ONLY) {
            error = true;
        }
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        check(rs.getType(), type);

        checkState(rs, true, false, false, rows == 0);
        for (int i = 0; i < rows; i++) {
            rs.next();
            checkState(rs, rows == 0, i == 0, i == rows - 1, (rows == 0 || i == rows));
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
            check(valid, rows > 0);
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
            check(valid, rows > 0);
            if (valid) {
                checkState(rs, false, rows == 1, true, rows == 0);
            }
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
    }

    void checkState(ResultSet rs, boolean beforeFirst, boolean first, boolean last, boolean afterLast) throws Exception {
        check(rs.isBeforeFirst(), beforeFirst);
        check(rs.isFirst(), first);
        check(rs.isLast(), last);
        check(rs.isAfterLast(), afterLast);
    }

}
