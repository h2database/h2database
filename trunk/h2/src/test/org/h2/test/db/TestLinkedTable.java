/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.h2.test.TestBase;

public class TestLinkedTable extends TestBase {

    public void test() throws Exception {
        // testLinkAutoAdd();
        testLinkOtherSchema();
        testLinkDrop();
        testLinkSchema();
        testLinkEmitUpdates();
        testLinkTable();
        testLinkTwoTables();
    }
    
    // this is not a bug, it is the documented behavior
//    private void testLinkAutoAdd() throws Exception {
//        Class.forName("org.h2.Driver");
//        Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
//        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
//        Statement sa = ca.createStatement();
//        Statement sb = cb.createStatement();
//        sa.execute("CREATE TABLE ONE (X NUMBER)");
//        sb.execute("CALL LINK_SCHEMA('GOOD', '', 'jdbc:h2:mem:one', 'sa', 'sa', 'PUBLIC'); ");
//        sb.executeQuery("SELECT * FROM GOOD.ONE");
//        sa.execute("CREATE TABLE TWO (X NUMBER)");
//        sb.executeQuery("SELECT * FROM GOOD.TWO"); // FAILED
//        ca.close();
//        cb.close();
//    }
    
    private void testLinkOtherSchema() throws Exception {
        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
        Statement sa = ca.createStatement();
        Statement sb = cb.createStatement();
        sa.execute("CREATE TABLE GOOD (X NUMBER)");
        sa.execute("CREATE SCHEMA S");
        sa.execute("CREATE TABLE S.BAD (X NUMBER)");
        sb.execute("CALL LINK_SCHEMA('G', '', 'jdbc:h2:mem:one', 'sa', 'sa', 'PUBLIC'); ");
        sb.execute("CALL LINK_SCHEMA('B', '', 'jdbc:h2:mem:one', 'sa', 'sa', 'S'); ");
        sb.executeQuery("SELECT * FROM G.GOOD");  //OK
        sb.executeQuery("SELECT * FROM B.BAD");  // FAILED
        ca.close();
        cb.close();
    }
    
    private void testLinkTwoTables() throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("CREATE SCHEMA Y");
        stat.execute("CREATE TABLE A( C INT)");
        stat.execute("INSERT INTO A VALUES(1)");
        stat.execute("CREATE TABLE Y.A (C INT)");
        stat.execute("INSERT INTO Y.A VALUES(2)");
        Connection conn2 = DriverManager.getConnection("jdbc:h2:mem:two");
        Statement stat2 = conn2.createStatement();
        stat2.execute("CREATE LINKED TABLE one('org.h2.Driver', 'jdbc:h2:mem:one', 'sa', 'sa', 'Y.A');");
        stat2.execute("CREATE LINKED TABLE two('org.h2.Driver', 'jdbc:h2:mem:one', 'sa', 'sa', 'A');");
        ResultSet rs = stat2.executeQuery("SELECT * FROM one");
        rs.next();
        check(rs.getInt(1), 2);
        rs = stat2.executeQuery("SELECT * FROM two");
        rs.next();
        check(rs.getInt(1), 1);
        conn.close();
        conn2.close();
    }

    private void testLinkDrop() throws Exception {
        Class.forName("org.h2.Driver");
        Connection connA = DriverManager.getConnection("jdbc:h2:mem:a");
        Statement statA = connA.createStatement();
        statA.execute("CREATE TABLE TEST(ID INT)");
        Connection connB = DriverManager.getConnection("jdbc:h2:mem:b");
        Statement statB = connB.createStatement();
        statB.execute("CREATE LINKED TABLE TEST_LINK('', 'jdbc:h2:mem:a', '', '', 'TEST')");
        connA.close();
        // the connection should be closed now
        // (and the table should disappear because the last connection was
        // closed)
        statB.execute("DROP TABLE TEST_LINK");
        connA = DriverManager.getConnection("jdbc:h2:mem:a");
        statA = connA.createStatement();
        // table should not exist now
        statA.execute("CREATE TABLE TEST(ID INT)");
        connA.close();
        connB.close();
    }

    private void testLinkEmitUpdates() throws Exception {
        deleteDb("linked1");
        deleteDb("linked2");
        Class.forName("org.h2.Driver");

        Connection conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/linked1", "sa1", "abc");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");

        Connection conn2 = DriverManager.getConnection("jdbc:h2:" + baseDir + "/linked2", "sa2", "def");
        Statement stat2 = conn2.createStatement();
        String link = "CREATE LINKED TABLE TEST_LINK_U('', 'jdbc:h2:" + baseDir
                + "/linked1', 'sa1', 'abc', 'TEST') EMIT UPDATES";
        stat2.execute(link);
        link = "CREATE LINKED TABLE TEST_LINK_DI('', 'jdbc:h2:" + baseDir + "/linked1', 'sa1', 'abc', 'TEST')";
        stat2.execute(link);
        stat2.executeUpdate("INSERT INTO TEST_LINK_U VALUES(1, 'Hello')");
        stat2.executeUpdate("INSERT INTO TEST_LINK_DI VALUES(2, 'World')");
        try {
            stat2.executeUpdate("UPDATE TEST_LINK_U SET ID=ID+1");
            error("unexpected success");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        stat2.executeUpdate("UPDATE TEST_LINK_DI SET ID=ID+1");
        stat2.executeUpdate("UPDATE TEST_LINK_U SET NAME=NAME || ID");
        ResultSet rs;
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        check(rs.getInt(1), 2);
        check(rs.getString(2), "Hello2");
        rs.next();
        check(rs.getInt(1), 3);
        check(rs.getString(2), "World3");
        checkFalse(rs.next());

        conn.close();
        conn2.close();
    }

    private void testLinkSchema() throws Exception {
        deleteDb("linked1");
        deleteDb("linked2");
        Class.forName("org.h2.Driver");

        Connection conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/linked1", "sa1", "abc");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST1(ID INT PRIMARY KEY)");

        Connection conn2 = DriverManager.getConnection("jdbc:h2:" + baseDir + "/linked2", "sa2", "def");
        Statement stat2 = conn2.createStatement();
        String link = "CALL LINK_SCHEMA('LINKED', '', 'jdbc:h2:" + baseDir + "/linked1', 'sa1', 'abc', 'PUBLIC')";
        stat2.execute(link);
        stat2.executeQuery("SELECT * FROM LINKED.TEST1");

        stat.execute("CREATE TABLE TEST2(ID INT PRIMARY KEY)");
        stat2.execute(link);
        stat2.executeQuery("SELECT * FROM LINKED.TEST1");
        stat2.executeQuery("SELECT * FROM LINKED.TEST2");

        conn.close();
        conn2.close();
    }

    private void testLinkTable() throws Exception {
        deleteDb("linked1");
        deleteDb("linked2");
        Class.forName("org.h2.Driver");

        Connection conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/linked1", "sa1", "abc");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TEMP TABLE TEST_TEMP(ID INT PRIMARY KEY)");
        stat
                .execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(200), XT TINYINT, XD DECIMAL(10,2), XTS TIMESTAMP, XBY BINARY(255), XBO BIT, XSM SMALLINT, XBI BIGINT, XBL BLOB, XDA DATE, XTI TIME, XCL CLOB, XDO DOUBLE)");
        stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");
        stat
                .execute("INSERT INTO TEST VALUES(0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
        stat
                .execute("INSERT INTO TEST VALUES(1, 'Hello', -1, 10.30, '2001-02-03 11:22:33.4455', X'FF0102', TRUE, 3000, 1234567890123456789, X'1122AA', DATE '0002-01-01', TIME '00:00:00', 'J\u00fcrg', 2.25)");
        testRow(stat, "TEST");
        stat
                .execute("INSERT INTO TEST VALUES(2, 'World', 30, 100.05, '2005-12-31 12:34:56.789', X'FFEECC33', FALSE, 1, -1234567890123456789, X'4455FF', DATE '9999-12-31', TIME '23:59:59', 'George', -2.5)");
        testRow(stat, "TEST");
        stat.execute("SELECT * FROM TEST_TEMP");
        conn.close();

        conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/linked1", "sa1", "abc");
        stat = conn.createStatement();
        testRow(stat, "TEST");
        try {
            stat.execute("SELECT * FROM TEST_TEMP");
            error("temp table must not be persistent");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        conn.close();

        conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/linked2", "sa2", "def");
        stat = conn.createStatement();
        stat.execute("CREATE LINKED TABLE IF NOT EXISTS LINK_TEST('org.h2.Driver', 'jdbc:h2:" + baseDir
                + "/linked1', 'sa1', 'abc', 'TEST')");
        stat.execute("CREATE LINKED TABLE IF NOT EXISTS LINK_TEST('org.h2.Driver', 'jdbc:h2:" + baseDir
                + "/linked1', 'sa1', 'abc', 'TEST')");
        testRow(stat, "LINK_TEST");
        ResultSet rs = stat.executeQuery("SELECT * FROM LINK_TEST");
        ResultSetMetaData meta = rs.getMetaData();
        check(10, meta.getPrecision(1));
        check(200, meta.getPrecision(2));

        conn.close();
        conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/linked2", "sa2", "def");
        stat = conn.createStatement();

        stat
                .execute("INSERT INTO LINK_TEST VALUES(3, 'Link Test', 30, 100.05, '2005-12-31 12:34:56.789', X'FFEECC33', FALSE, 1, -1234567890123456789, X'4455FF', DATE '9999-12-31', TIME '23:59:59', 'George', -2.5)");

        rs = stat.executeQuery("SELECT COUNT(*) FROM LINK_TEST");
        rs.next();
        check(rs.getInt(1), 4);

        rs = stat.executeQuery("SELECT COUNT(*) FROM LINK_TEST WHERE NAME='Link Test'");
        rs.next();
        check(rs.getInt(1), 1);

        int uc = stat.executeUpdate("DELETE FROM LINK_TEST WHERE ID=3");
        check(uc, 1);

        rs = stat.executeQuery("SELECT COUNT(*) FROM LINK_TEST");
        rs.next();
        check(rs.getInt(1), 3);

        rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='LINK_TEST'");
        rs.next();
        check(rs.getString("TABLE_TYPE"), "TABLE LINK");

        rs.next();
        rs = stat.executeQuery("SELECT * FROM LINK_TEST WHERE ID=0");
        rs.next();
        check(rs.getString("NAME") == null && rs.wasNull());
        check(rs.getString("XT") == null && rs.wasNull());
        check(rs.getInt("ID") == 0 && !rs.wasNull());
        check(rs.getBigDecimal("XD") == null && rs.wasNull());
        check(rs.getTimestamp("XTS") == null && rs.wasNull());
        check(rs.getBytes("XBY") == null && rs.wasNull());
        check(!rs.getBoolean("XBO") && rs.wasNull());
        check(rs.getShort("XSM") == 0 && rs.wasNull());
        check(rs.getLong("XBI") == 0 && rs.wasNull());
        check(rs.getString("XBL") == null && rs.wasNull());
        check(rs.getString("XDA") == null && rs.wasNull());
        check(rs.getString("XTI") == null && rs.wasNull());
        check(rs.getString("XCL") == null && rs.wasNull());
        check(rs.getString("XDO") == null && rs.wasNull());
        checkFalse(rs.next());

        stat.execute("DROP TABLE LINK_TEST");

        stat.execute("CREATE LINKED TABLE LINK_TEST('org.h2.Driver', 'jdbc:h2:" + baseDir
                + "/linked1', 'sa1', 'abc', '(SELECT COUNT(*) FROM TEST)')");
        rs = stat.executeQuery("SELECT * FROM LINK_TEST");
        rs.next();
        check(rs.getInt(1), 3);
        checkFalse(rs.next());

        conn.close();

        deleteDb("linked1");
        deleteDb("linked2");
    }

    void testRow(Statement stat, String name) throws Exception {
        ResultSet rs = stat.executeQuery("SELECT * FROM " + name + " WHERE ID=1");
        rs.next();
        check(rs.getString("NAME"), "Hello");
        check(rs.getByte("XT"), -1);
        BigDecimal bd = rs.getBigDecimal("XD");
        check(bd.equals(new BigDecimal("10.30")));
        Timestamp ts = rs.getTimestamp("XTS");
        String s = ts.toString();
        check(s, "2001-02-03 11:22:33.4455");
        check(ts.equals(Timestamp.valueOf("2001-02-03 11:22:33.4455")));
        check(rs.getBytes("XBY"), new byte[] { (byte) 255, (byte) 1, (byte) 2 });
        check(rs.getBoolean("XBO"));
        check(rs.getShort("XSM"), 3000);
        check(rs.getLong("XBI"), 1234567890123456789L);
        check(rs.getString("XBL"), "1122aa");
        check(rs.getString("XDA"), "0002-01-01");
        check(rs.getString("XTI"), "00:00:00");
        check(rs.getString("XCL"), "J\u00fcrg");
        check(rs.getString("XDO"), "2.25");

    }

}
