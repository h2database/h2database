/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import org.h2.test.TestBase;

public class TestPreparedStatement extends TestBase {

    static final int LOB_SIZE = 4000, LOB_SIZE_BIG = 512 * 1024;

    public void test() throws Exception {

        deleteDb("preparedStatement");
        Connection conn = getConnection("preparedStatement");
        testTempView(conn);
        testInsertFunction(conn);
        testPrepareRecompile(conn);
        testMaxRowsChange(conn);
        testUnknownDataType(conn);
        testCancelReuse(conn);        
        testCoalesce(conn);
        testPreparedStatementMetaData(conn);
        testDate(conn);
        testArray(conn);
        testUUIDGeneratedKeys(conn);
        testSetObject(conn);
        testPreparedSubquery(conn);
        testLikeIndex(conn);
        testCasewhen(conn);
        testSubquery(conn);
        testObject(conn);
        if (config.jdk14) {
            testIdentity(conn);
        }
        testDataTypes(conn);
        testBlob(conn);
        testClob(conn);
        testParameterMetaData(conn);
        conn.close();
    }
    
    private void testTempView(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        stat.execute("CREATE TABLE TEST(FLD INT PRIMARY KEY)");
        stat.execute("INSERT INTO TEST VALUES(1)");
        stat.execute("INSERT INTO TEST VALUES(2)");
        prep = conn.prepareStatement("select FLD FROM " 
                + "(select FLD FROM (SELECT FLD  FROM TEST WHERE FLD = ?) AS TBL2 "
                + "WHERE TBL2.FLD = ?) AS TBL3 WHERE TBL3.FLD = ?");
        prep.setInt(1, 1);
        prep.setInt(2, 1);
        prep.setInt(3, 1);
        ResultSet rs = prep.executeQuery();
        rs.next();
        check(1, rs.getInt(1));
        prep.setInt(1, 2);
        prep.setInt(2, 2);
        prep.setInt(3, 2);
        rs = prep.executeQuery();
        rs.next();
        check(2, rs.getInt(1));
        stat.execute("DROP TABLE TEST");
    }
    
    private void testInsertFunction(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;
        
        stat.execute("CREATE TABLE TEST(ID INT, H BINARY)");
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, HASH('SHA256', STRINGTOUTF8(?), 5))");
        prep.setInt(1, 1);
        prep.setString(2, "One");
        prep.execute();
        prep.setInt(1, 2);
        prep.setString(2, "Two");
        prep.execute();
        rs = stat.executeQuery("SELECT COUNT(DISTINCT H) FROM TEST");
        rs.next();
        check(rs.getInt(1), 2);
        
        stat.execute("DROP TABLE TEST");
    }
    
    private void testPrepareRecompile(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;
        
        prep = conn.prepareStatement("SELECT COUNT(*) FROM DUAL WHERE ? IS NULL");
        prep.setString(1, null);
        prep.executeQuery();
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("DROP TABLE TEST");
        prep.setString(1, null);
        prep.executeQuery();
        prep.setString(1, "X");
        rs = prep.executeQuery();
        rs.next();
        check(rs.getInt(1), 0);

        stat.execute("CREATE TABLE t1 (c1 INT, c2 VARCHAR(10))");
        stat.execute("INSERT INTO t1 SELECT X, CONCAT('Test', X)  FROM SYSTEM_RANGE(1, 5);");
        prep = conn.prepareStatement("SELECT c1, c2 FROM t1 WHERE c1 = ?");
        prep.setInt(1, 1);
        prep.executeQuery();
        stat.execute("CREATE TABLE t2 (x int PRIMARY KEY)");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        rs.next();
        check(rs.getInt(1), 2);
        prep.setInt(1, 3);
        rs = prep.executeQuery();
        rs.next();
        check(rs.getInt(1), 3);
        stat.execute("DROP TABLE t1, t2");
        
    }
    
    private void testMaxRowsChange(Connection conn) throws Exception {
        PreparedStatement prep = conn.prepareStatement("SELECT * FROM SYSTEM_RANGE(1, 100)");
        ResultSet rs;
        for (int j = 1; j < 20; j++) {
            prep.setMaxRows(j);
            rs = prep.executeQuery();
            for (int i = 0; i < j; i++) {
                check(rs.next());
            }
            checkFalse(rs.next());
        }
    }
    
    private void testUnknownDataType(Connection conn) throws Exception {
        try {
            PreparedStatement prep = conn.prepareStatement(
            "SELECT * FROM (SELECT ? FROM DUAL)");
            prep.setInt(1, 1);
            prep.execute();
            error("Must fail");
        } catch (SQLException e) {
            // expected
        }
        PreparedStatement prep = conn.prepareStatement("SELECT -?");
        prep.setInt(1, 1);
        prep.execute();
        prep = conn.prepareStatement("SELECT ?-?");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.execute();
    }
    
    private void testCancelReuse(Connection conn) throws Exception {
        conn.createStatement().execute("CREATE ALIAS YIELD FOR \"java.lang.Thread.yield\"");
        final PreparedStatement prep = conn.prepareStatement("SELECT YIELD() FROM SYSTEM_RANGE(1, 1000000) LIMIT ?");
        prep.setInt(1, 100000000);
        Thread t = new Thread() {
            public void run() {
                try {
                    prep.execute();
                } catch (SQLException e) {
                    // ignore
                }
            }
        };
        t.start();
        Thread.sleep(10);
        try {
            prep.cancel();
        } catch (SQLException e) {
            this.checkNotGeneralException(e);
        }
        prep.setInt(1, 1);
        ResultSet rs = prep.executeQuery();
        check(rs.next());
        check(rs.getInt(1), 0);
        checkFalse(rs.next());
    }
    
    private void testCoalesce(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.executeUpdate("create table test(tm timestamp)");
        stat.executeUpdate("insert into test values(current_timestamp)");
        PreparedStatement prep = conn.prepareStatement("update test set tm = coalesce(?,tm)");
        prep.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
        prep.executeUpdate();
        stat.executeUpdate("drop table test");
    }

    private void testPreparedStatementMetaData(Connection conn) throws Exception {
        PreparedStatement prep = conn.prepareStatement("select * from table(x int = ?, name varchar = ?)");
        ResultSetMetaData meta = prep.getMetaData();
        check(meta.getColumnCount(), 2);
        check(meta.getColumnTypeName(1), "INTEGER");
        check(meta.getColumnTypeName(2), "VARCHAR");
        prep = conn.prepareStatement("call 1");
        meta = prep.getMetaData();
        check(meta.getColumnCount(), 1);
        check(meta.getColumnTypeName(1), "INTEGER");
    }

    private void testArray(Connection conn) throws Exception {
        PreparedStatement prep = conn.prepareStatement("select * from table(x int = ?) order by x");
        prep.setObject(1, new Object[] { new BigDecimal("1"), "2" });
        ResultSet rs = prep.executeQuery();
        rs.next();
        check(rs.getString(1), "1");
        rs.next();
        check(rs.getString(1), "2");
        checkFalse(rs.next());
    }

    private void testUUIDGeneratedKeys(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST_UUID(id UUID DEFAULT random_UUID() PRIMARY KEY)");
        stat.execute("INSERT INTO TEST_UUID() VALUES()");
        ResultSet rs = stat.getGeneratedKeys();
        rs.next();
        byte[] data = rs.getBytes(1);
        check(data.length, 16);
        stat.execute("DROP TABLE TEST_UUID");
    }

    private void testSetObject(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT, DATA BINARY, JAVA OTHER)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");
        prep.setInt(1, 1);
        prep.setObject(2, new Integer(11));
        prep.setObject(3, null);
        prep.execute();
        prep.setInt(1, 2);
        prep.setObject(2, new Integer(101), Types.OTHER);
        prep.setObject(3, new Integer(103), Types.OTHER);
        prep.execute();
        PreparedStatement p2 = conn.prepareStatement("SELECT * FROM TEST ORDER BY ID");
        ResultSet rs = p2.executeQuery();
        rs.next();
        Object o = rs.getObject(2);
        check(o instanceof byte[]);
        check(rs.getObject(3) == null);
        rs.next();
        o = rs.getObject(2);
        check(o instanceof byte[]);
        o = rs.getObject(3);
        check(o instanceof Integer);
        check(((Integer) o).intValue(), 103);
        checkFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testDate(Connection conn) throws Exception {
        PreparedStatement prep = conn.prepareStatement("SELECT ?");
        Timestamp ts = Timestamp.valueOf("2001-02-03 04:05:06");
        prep.setObject(1, new java.util.Date(ts.getTime()));
        ResultSet rs = prep.executeQuery();
        rs.next();
        Timestamp ts2 = rs.getTimestamp(1);
        check(ts.toString(), ts2.toString());
    }

    private void testPreparedSubquery(Connection conn) throws Exception {
        Statement s = conn.createStatement();
        s.executeUpdate("CREATE TABLE TEST(ID IDENTITY, FLAG BIT)");
        s.executeUpdate("INSERT INTO TEST(ID, FLAG) VALUES(0, FALSE)");
        s.executeUpdate("INSERT INTO TEST(ID, FLAG) VALUES(1, FALSE)");
        PreparedStatement u = conn.prepareStatement("SELECT ID, FLAG FROM TEST ORDER BY ID");
        PreparedStatement p = conn.prepareStatement("UPDATE TEST SET FLAG=true WHERE ID=(SELECT ?)");
        p.clearParameters();
        p.setLong(1, 0);
        check(p.executeUpdate(), 1);
        p.clearParameters();
        p.setLong(1, 1);
        check(p.executeUpdate(), 1);
        ResultSet rs = u.executeQuery();
        check(rs.next());
        check(rs.getInt(1), 0);
        check(rs.getBoolean(2));
        check(rs.next());
        check(rs.getInt(1), 1);
        check(rs.getBoolean(2));

        p = conn.prepareStatement("SELECT * FROM TEST WHERE EXISTS(SELECT * FROM TEST WHERE ID=?)");
        p.setInt(1, -1);
        rs = p.executeQuery();
        checkFalse(rs.next());
        p.setInt(1, 1);
        rs = p.executeQuery();
        check(rs.next());

        s.executeUpdate("DROP TABLE IF EXISTS TEST");
    }

    private void testParameterMetaData(Connection conn) throws Exception {
        PreparedStatement prep = conn.prepareStatement("SELECT ?, ?, ? FROM DUAL");
        ParameterMetaData pm = prep.getParameterMetaData();
        check(pm.getParameterClassName(1), "java.lang.String");
        check(pm.getParameterTypeName(1), "VARCHAR");
        check(pm.getParameterCount(), 3);
        check(pm.getParameterMode(1), ParameterMetaData.parameterModeIn);
        check(pm.getParameterType(1), Types.VARCHAR);
        check(pm.getPrecision(1), 0);
        check(pm.getScale(1), 0);
        check(pm.isNullable(1), ResultSetMetaData.columnNullableUnknown);
        check(pm.isSigned(1), true);
        try {
            pm.getPrecision(0);
            error("should fail");
        } catch (SQLException e) {
            // ok
        }
        try {
            pm.getPrecision(4);
            error("should fail");
        } catch (SQLException e) {
            // ok
        }
        prep.close();
        try {
            pm.getPrecision(1);
            error("should fail");
        } catch (SQLException e) {
            // ok
        }
    }

    private void testLikeIndex(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        stat.execute("create index idxname on test(name);");
        PreparedStatement prep, prepExe;

        prep = conn.prepareStatement("EXPLAIN SELECT * FROM TEST WHERE NAME LIKE ?");
        check(prep.getParameterMetaData().getParameterCount(), 1);
        prepExe = conn.prepareStatement("SELECT * FROM TEST WHERE NAME LIKE ?");
        prep.setString(1, "%orld");
        prepExe.setString(1, "%orld");
        ResultSet rs = prep.executeQuery();
        rs.next();
        String plan = rs.getString(1);
        check(plan.indexOf("TABLE_SCAN") >= 0);
        rs = prepExe.executeQuery();
        rs.next();
        check(rs.getString(2), "World");
        checkFalse(rs.next());

        prep.setString(1, "H%");
        prepExe.setString(1, "H%");
        rs = prep.executeQuery();
        rs.next();
        String plan1 = rs.getString(1);
        check(plan1.indexOf("IDXNAME") >= 0);
        rs = prepExe.executeQuery();
        rs.next();
        check(rs.getString(2), "Hello");
        checkFalse(rs.next());

        stat.execute("DROP TABLE IF EXISTS TEST");
    }

    private void testCasewhen(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1),(2),(3)");
        PreparedStatement prep;
        ResultSet rs;
        prep = conn.prepareStatement("EXPLAIN SELECT COUNT(*) FROM TEST WHERE CASEWHEN(ID=1, ID, ID)=? GROUP BY ID");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        rs.next();
        String plan = rs.getString(1);
        trace(plan);
        rs.close();
        prep = conn
                .prepareStatement("EXPLAIN SELECT COUNT(*) FROM TEST WHERE CASE ID WHEN 1 THEN ID WHEN 2 THEN ID ELSE ID END=? GROUP BY ID");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        rs.next();
        plan = rs.getString(1);
        trace(plan);

        prep = conn.prepareStatement("SELECT COUNT(*) FROM TEST WHERE CASEWHEN(ID=1, ID, ID)=? GROUP BY ID");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        check(rs.next());
        check(rs.getInt(1), 1);
        checkFalse(rs.next());

        prep = conn
                .prepareStatement("SELECT COUNT(*) FROM TEST WHERE CASE ID WHEN 1 THEN ID WHEN 2 THEN ID ELSE ID END=? GROUP BY ID");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        check(rs.next());
        check(rs.getInt(1), 1);
        checkFalse(rs.next());

        prep = conn.prepareStatement("SELECT * FROM TEST WHERE ? IS NULL");
        prep.setString(1, "Hello");
        rs = prep.executeQuery();
        checkFalse(rs.next());
        try {
            prep = conn.prepareStatement("select ? from dual union select ? from dual");
            error("expected error");
        } catch (SQLException e) {
            checkNotGeneralException(e);
            // ok
        }
        prep = conn.prepareStatement("select cast(? as varchar) from dual union select ? from dual");
        check(prep.getParameterMetaData().getParameterCount(), 2);
        prep.setString(1, "a");
        prep.setString(2, "a");
        rs = prep.executeQuery();
        rs.next();
        check(rs.getString(1), "a");
        check(rs.getString(1), "a");
        checkFalse(rs.next());

        stat.execute("DROP TABLE TEST");
    }

    private void testSubquery(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1),(2),(3)");
        PreparedStatement prep = conn.prepareStatement("select x.id, ? from "
                + "(select * from test where id in(?, ?)) x " + "where x.id*2 <>  ?");
        check(prep.getParameterMetaData().getParameterCount(), 4);
        prep.setInt(1, 0);
        prep.setInt(2, 1);
        prep.setInt(3, 2);
        prep.setInt(4, 4);
        ResultSet rs = prep.executeQuery();
        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getInt(2), 0);
        checkFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testDataTypes(Connection conn) throws Exception {
        conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;
        trace("Create tables");
        stat.execute("CREATE TABLE T_INT(ID INT PRIMARY KEY,VALUE INT)");
        stat.execute("CREATE TABLE T_VARCHAR(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        stat.execute("CREATE TABLE T_DECIMAL_0(ID INT PRIMARY KEY,VALUE DECIMAL(30,0))");
        stat.execute("CREATE TABLE T_DECIMAL_10(ID INT PRIMARY KEY,VALUE DECIMAL(20,10))");
        stat.execute("CREATE TABLE T_DATETIME(ID INT PRIMARY KEY,VALUE DATETIME)");
        prep = conn.prepareStatement("INSERT INTO T_INT VALUES(?,?)", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        prep.setInt(1, 1);
        prep.setInt(2, 0);
        prep.executeUpdate();
        prep.setInt(1, 2);
        prep.setInt(2, -1);
        prep.executeUpdate();
        prep.setInt(1, 3);
        prep.setInt(2, 3);
        prep.executeUpdate();
        prep.setInt(1, 4);
        prep.setNull(2, Types.INTEGER);
        prep.executeUpdate();
        prep.setInt(1, 5);
        prep.setBigDecimal(2, new java.math.BigDecimal("0"));
        prep.executeUpdate();
        prep.setInt(1, 6);
        prep.setString(2, "-1");
        prep.executeUpdate();
        prep.setInt(1, 7);
        prep.setObject(2, new Integer(3));
        prep.executeUpdate();
        prep.setObject(1, "8");
        // should throw an exception
        prep.setObject(2, null);
        // some databases don't allow calling setObject with null (no data type)
        prep.executeUpdate();
        prep.setInt(1, 9);
        prep.setObject(2, new Integer(-4), Types.VARCHAR);
        prep.executeUpdate();
        prep.setInt(1, 10);
        prep.setObject(2, "5", Types.INTEGER);
        prep.executeUpdate();
        prep.setInt(1, 11);
        prep.setObject(2, null, Types.INTEGER);
        prep.executeUpdate();
        prep.setInt(1, 12);
        prep.setBoolean(2, true);
        prep.executeUpdate();
        prep.setInt(1, 13);
        prep.setBoolean(2, false);
        prep.executeUpdate();
        prep.setInt(1, 14);
        prep.setByte(2, (byte) -20);
        prep.executeUpdate();
        prep.setInt(1, 15);
        prep.setByte(2, (byte) 100);
        prep.executeUpdate();
        prep.setInt(1, 16);
        prep.setShort(2, (short) 30000);
        prep.executeUpdate();
        prep.setInt(1, 17);
        prep.setShort(2, (short) (-30000));
        prep.executeUpdate();
        prep.setInt(1, 18);
        prep.setLong(2, Integer.MAX_VALUE);
        prep.executeUpdate();
        prep.setInt(1, 19);
        prep.setLong(2, Integer.MIN_VALUE);
        prep.executeUpdate();

        check(stat.execute("SELECT * FROM T_INT ORDER BY ID"));
        rs = stat.getResultSet();
        testResultSetOrdered(rs, new String[][] { { "1", "0" }, { "2", "-1" }, { "3", "3" }, { "4", null },
                { "5", "0" }, { "6", "-1" }, { "7", "3" }, { "8", null }, { "9", "-4" }, { "10", "5" }, { "11", null },
                { "12", "1" }, { "13", "0" }, { "14", "-20" }, { "15", "100" }, { "16", "30000" }, { "17", "-30000" },
                { "18", "" + Integer.MAX_VALUE }, { "19", "" + Integer.MIN_VALUE }, });

        prep = conn.prepareStatement("INSERT INTO T_DECIMAL_0 VALUES(?,?)");
        prep.setInt(1, 1);
        prep.setLong(2, Long.MAX_VALUE);
        prep.executeUpdate();
        prep.setInt(1, 2);
        prep.setLong(2, Long.MIN_VALUE);
        prep.executeUpdate();
        prep.setInt(1, 3);
        prep.setFloat(2, 10);
        prep.executeUpdate();
        prep.setInt(1, 4);
        prep.setFloat(2, -20);
        prep.executeUpdate();
        prep.setInt(1, 5);
        prep.setFloat(2, 30);
        prep.executeUpdate();
        prep.setInt(1, 6);
        prep.setFloat(2, -40);
        prep.executeUpdate();

        rs = stat.executeQuery("SELECT VALUE FROM T_DECIMAL_0 ORDER BY ID");
        checkBigDecimal(rs, new String[] { "" + Long.MAX_VALUE, "" + Long.MIN_VALUE, "10", "-20", "30", "-40" });

        // getMoreResults
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1)");
        prep = conn.prepareStatement("SELECT * FROM TEST");
        // just to check if it doesn't throw an exception - it may be null
        prep.getMetaData();
        check(prep.execute());
        rs = prep.getResultSet();
        checkFalse(prep.getMoreResults());
        try {
            // supposed to be closed now
            rs.next();
            error("getMoreResults didn't close this result set");
        } catch (SQLException e) {
            trace("no error - getMoreResults is supposed to close the result set");
        }
        check(prep.getUpdateCount() == -1);
        prep = conn.prepareStatement("DELETE FROM TEST");
        prep.executeUpdate();
        checkFalse(prep.getMoreResults());
        check(prep.getUpdateCount() == -1);
    }

    private void testObject(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        PreparedStatement prep = conn
                .prepareStatement("SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM TEST");
        prep.setObject(1, new Boolean(true));
        prep.setObject(2, "Abc");
        prep.setObject(3, new BigDecimal("10.2"));
        prep.setObject(4, new Byte((byte) 0xff));
        prep.setObject(5, new Short(Short.MAX_VALUE));
        prep.setObject(6, new Integer(Integer.MIN_VALUE));
        prep.setObject(7, new Long(Long.MAX_VALUE));
        prep.setObject(8, new Float(Float.MAX_VALUE));
        prep.setObject(9, new Double(Double.MAX_VALUE));
        prep.setObject(10, java.sql.Date.valueOf("2001-02-03"));
        prep.setObject(11, java.sql.Time.valueOf("04:05:06"));
        prep.setObject(12, java.sql.Timestamp.valueOf("2001-02-03 04:05:06.123456789"));
        prep.setObject(13, new java.util.Date(java.sql.Date.valueOf("2001-02-03").getTime()));
        prep.setObject(14, new byte[] { 10, 20, 30 });
        prep.setObject(15, new Character('a'));
        prep.setObject(16, "2001-01-02", Types.DATE);
        // converting to null seems strange...
        prep.setObject(17, "2001-01-02", Types.NULL);
        prep.setObject(18, "3.725", Types.DOUBLE);
        prep.setObject(19, "23:22:21", Types.TIME);
        prep.setObject(20, new java.math.BigInteger("12345"), Types.OTHER);
        rs = prep.executeQuery();
        rs.next();
        check(rs.getObject(1).equals(new Boolean(true)));
        check(rs.getObject(2).equals("Abc"));
        check(rs.getObject(3).equals(new BigDecimal("10.2")));
        check(rs.getObject(4).equals(new Byte((byte) 0xff)));
        check(rs.getObject(5).equals(new Short(Short.MAX_VALUE)));
        check(rs.getObject(6).equals(new Integer(Integer.MIN_VALUE)));
        check(rs.getObject(7).equals(new Long(Long.MAX_VALUE)));
        check(rs.getObject(8).equals(new Float(Float.MAX_VALUE)));
        check(rs.getObject(9).equals(new Double(Double.MAX_VALUE)));
        check(rs.getObject(10).equals(java.sql.Date.valueOf("2001-02-03")));
        check(rs.getObject(11).toString(), "04:05:06");
        check(rs.getObject(11).equals(java.sql.Time.valueOf("04:05:06")));
        check(rs.getObject(12).equals(java.sql.Timestamp.valueOf("2001-02-03 04:05:06.123456789")));
        check(rs.getObject(13).equals(java.sql.Timestamp.valueOf("2001-02-03 00:00:00")));
        check((byte[]) rs.getObject(14), new byte[] { 10, 20, 30 });
        check(rs.getObject(15).equals(new Character('a')));
        check(rs.getObject(16).equals(java.sql.Date.valueOf("2001-01-02")));
        check(rs.getObject(17) == null && rs.wasNull());
        check(rs.getObject(18).equals(new Double(3.725)));
        check(rs.getObject(19).equals(java.sql.Time.valueOf("23:22:21")));
        check(rs.getObject(20).equals(new java.math.BigInteger("12345")));

        // } else if(x instanceof java.io.Reader) {
        // return session.createLob(Value.CLOB,
        // TypeConverter.getInputStream((java.io.Reader)x), 0);
        // } else if(x instanceof java.io.InputStream) {
        // return session.createLob(Value.BLOB, (java.io.InputStream)x, 0);
        // } else {
        // return ValueBytes.get(TypeConverter.serialize(x));

        stat.execute("DROP TABLE TEST");

    }

    private void testIdentity(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE SEQUENCE SEQ");
        stat.execute("CREATE TABLE TEST(ID INT)");
        PreparedStatement prep;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)");
        prep.execute();
        ResultSet rs = prep.getGeneratedKeys();
        rs.next();
        check(rs.getInt(1), 1);
        checkFalse(rs.next());
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", Statement.RETURN_GENERATED_KEYS);
        prep.execute();
        rs = prep.getGeneratedKeys();
        rs.next();
        check(rs.getInt(1), 2);
        checkFalse(rs.next());
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", new int[] { 1 });
        prep.execute();
        rs = prep.getGeneratedKeys();
        rs.next();
        check(rs.getInt(1), 3);
        checkFalse(rs.next());
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", new String[] { "ID" });
        prep.execute();
        rs = prep.getGeneratedKeys();
        rs.next();
        check(rs.getInt(1), 4);
        checkFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    int getLength() throws Exception {
        return getSize(LOB_SIZE, LOB_SIZE_BIG);
    }

    void testBlob(Connection conn) throws Exception {
        trace("testBlob");
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;
        stat.execute("CREATE TABLE T_BLOB(ID INT PRIMARY KEY,V1 BLOB,V2 BLOB)");
        trace("table created");
        prep = conn.prepareStatement("INSERT INTO T_BLOB VALUES(?,?,?)");

        prep.setInt(1, 1);
        prep.setBytes(2, null);
        prep.setNull(3, Types.BINARY);
        prep.executeUpdate();

        prep.setInt(1, 2);
        prep.setBinaryStream(2, null, 0);
        prep.setNull(3, Types.BLOB);
        prep.executeUpdate();

        int length = getLength();
        byte[] big1 = new byte[length];
        byte[] big2 = new byte[length];
        for (int i = 0; i < big1.length; i++) {
            big1[i] = (byte) ((i * 11) % 254);
            big2[i] = (byte) ((i * 17) % 251);
        }

        prep.setInt(1, 3);
        prep.setBytes(2, big1);
        prep.setBytes(3, big2);
        prep.executeUpdate();

        prep.setInt(1, 4);
        ByteArrayInputStream buffer;
        buffer = new ByteArrayInputStream(big2);
        prep.setBinaryStream(2, buffer, big2.length);
        buffer = new ByteArrayInputStream(big1);
        prep.setBinaryStream(3, buffer, big1.length);
        prep.executeUpdate();
        try {
            buffer.close();
            trace("buffer not closed");
        } catch (IOException e) {
            trace("buffer closed");
        }

        prep.setInt(1, 5);
        buffer = new ByteArrayInputStream(big2);
        prep.setObject(2, buffer, Types.BLOB, 0);
        buffer = new ByteArrayInputStream(big1);
        prep.setObject(3, buffer);
        prep.executeUpdate();

        rs = stat.executeQuery("SELECT ID, V1, V2 FROM T_BLOB ORDER BY ID");

        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getBytes(2) == null && rs.wasNull());
        check(rs.getBytes(3) == null && rs.wasNull());

        rs.next();
        check(rs.getInt(1), 2);
        check(rs.getBytes(2) == null && rs.wasNull());
        check(rs.getBytes(3) == null && rs.wasNull());

        rs.next();
        check(rs.getInt(1), 3);
        check(rs.getBytes(2), big1);
        check(rs.getBytes(3), big2);

        rs.next();
        check(rs.getInt(1), 4);
        check(rs.getBytes(2), big2);
        check(rs.getBytes(3), big1);

        rs.next();
        check(rs.getInt(1), 5);
        check(rs.getBytes(2), big2);
        check(rs.getBytes(3), big1);

        checkFalse(rs.next());
    }

    void testClob(Connection conn) throws Exception {
        trace("testClob");
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;
        stat.execute("CREATE TABLE T_CLOB(ID INT PRIMARY KEY,V1 CLOB,V2 CLOB)");
        StringBuffer asciiBuffer = new StringBuffer();
        int len = getLength();
        for (int i = 0; i < len; i++) {
            asciiBuffer.append((char) ('a' + (i % 20)));
        }
        String ascii1 = asciiBuffer.toString();
        String ascii2 = "Number2 " + ascii1;
        prep = conn.prepareStatement("INSERT INTO T_CLOB VALUES(?,?,?)");

        prep.setInt(1, 1);
        prep.setString(2, null);
        prep.setNull(3, Types.CLOB);
        prep.executeUpdate();

        prep.clearParameters();
        prep.setInt(1, 2);
        prep.setAsciiStream(2, null, 0);
        prep.setCharacterStream(3, null, 0);
        prep.executeUpdate();

        prep.clearParameters();
        prep.setInt(1, 3);
        prep.setCharacterStream(2, new StringReader(ascii1), ascii1.length());
        prep.setCharacterStream(3, null, 0);
        prep.setAsciiStream(3, new ByteArrayInputStream(ascii2.getBytes()), ascii2.length());
        prep.executeUpdate();

        prep.clearParameters();
        prep.setInt(1, 4);
        prep.setNull(2, Types.CLOB);
        prep.setString(2, ascii2);
        prep.setCharacterStream(3, null, 0);
        prep.setNull(3, Types.CLOB);
        prep.setString(3, ascii1);
        prep.executeUpdate();

        prep.clearParameters();
        prep.setInt(1, 5);
        prep.setObject(2, new StringReader(ascii1));
        prep.setObject(3, new StringReader(ascii2), Types.CLOB, 0);
        prep.executeUpdate();

        rs = stat.executeQuery("SELECT ID, V1, V2 FROM T_CLOB ORDER BY ID");

        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getCharacterStream(2) == null && rs.wasNull());
        check(rs.getAsciiStream(3) == null && rs.wasNull());

        rs.next();
        check(rs.getInt(1), 2);
        check(rs.getString(2) == null && rs.wasNull());
        check(rs.getString(3) == null && rs.wasNull());

        rs.next();
        check(rs.getInt(1), 3);
        check(rs.getString(2), ascii1);
        check(rs.getString(3), ascii2);

        rs.next();
        check(rs.getInt(1), 4);
        check(rs.getString(2), ascii2);
        check(rs.getString(3), ascii1);

        rs.next();
        check(rs.getInt(1), 5);
        check(rs.getString(2), ascii1);
        check(rs.getString(3), ascii2);

        checkFalse(rs.next());
        check(prep.getWarnings() == null);
        prep.clearWarnings();
        check(prep.getWarnings() == null);
        check(conn == prep.getConnection());
    }

    void checkBigDecimal(ResultSet rs, String[] value) throws Exception {
        for (int i = 0; i < value.length; i++) {
            String v = value[i];
            check(rs.next());
            java.math.BigDecimal x = rs.getBigDecimal(1);
            trace("v=" + v + " x=" + x);
            if (v == null) {
                check(x == null);
            } else {
                check(x.compareTo(new java.math.BigDecimal(v)) == 0);
            }
        }
        check(!rs.next());
    }

}
