/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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

/**
 * Tests the linked table feature (CREATE LINKED TABLE).
 */
public class TestLinkedTable extends TestBase {
    
    /**
     * Run just this test.
     * 
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        // testLinkAutoAdd();
        testSharedConnection();
        testMultipleSchemas();
        testReadOnlyLinkedTable();
        testLinkOtherSchema();
        testLinkDrop();
        testLinkSchema();
        testLinkEmitUpdates();
        testLinkTable();
        testLinkTwoTables();
    }

    // this is not a bug, it is the documented behavior
//    private void testLinkAutoAdd() throws SQLException {
//        Class.forName("org.h2.Driver");
//        Connection ca = 
//            DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
//        Connection cb = 
//            DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
//        Statement sa = ca.createStatement();
//        Statement sb = cb.createStatement();
//        sa.execute("CREATE TABLE ONE (X NUMBER)");
//        sb.execute(
//            "CALL LINK_SCHEMA('GOOD', '', " +
//            "'jdbc:h2:mem:one', 'sa', 'sa', 'PUBLIC'); ");
//        sb.executeQuery("SELECT * FROM GOOD.ONE");
//        sa.execute("CREATE TABLE TWO (X NUMBER)");
//        sb.executeQuery("SELECT * FROM GOOD.TWO"); // FAILED
//        ca.close();
//        cb.close();
//    }
    
    private void testSharedConnection() throws SQLException {
        if (config.memory) {
            return;
        }
        org.h2.Driver.load();
        deleteDb("linkedTable");
        String url = getURL("linkedTable", true);
        String user = getUser();
        String password = getPassword();
        Connection ca = getConnection(url, user, password);
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE TEST(ID INT)");
        ca.close();
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
        Statement sb = cb.createStatement();
        sb.execute("CREATE LINKED TABLE T1(NULL, '" + url + ";OPEN_NEW=TRUE', '"+user+"', '"+password+"', 'TEST')");
        sb.execute("CREATE LINKED TABLE T2(NULL, '" + url + ";OPEN_NEW=TRUE', '"+user+"', '"+password+"', 'TEST')");
        sb.execute("DROP ALL OBJECTS");
        cb.close();
    }
    
    private void testMultipleSchemas() throws SQLException {
        org.h2.Driver.load();
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
        Statement sa = ca.createStatement();
        Statement sb = cb.createStatement();
        sa.execute("CREATE TABLE TEST(ID INT)");
        sa.execute("CREATE SCHEMA P");
        sa.execute("CREATE TABLE P.TEST(X INT)");
        sa.execute("INSERT INTO TEST VALUES(1)");
        sa.execute("INSERT INTO P.TEST VALUES(2)");
        try {
            sb.execute("CREATE LINKED TABLE T(NULL, 'jdbc:h2:mem:one', 'sa', 'sa', 'TEST')");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        sb.execute("CREATE LINKED TABLE T(NULL, 'jdbc:h2:mem:one', 'sa', 'sa', 'PUBLIC', 'TEST')");
        sb.execute("CREATE LINKED TABLE T2(NULL, 'jdbc:h2:mem:one', 'sa', 'sa', 'P', 'TEST')");
        assertSingleValue(sb, "SELECT * FROM T", 1);
        assertSingleValue(sb, "SELECT * FROM T2", 2);
        sa.execute("DROP ALL OBJECTS");
        sb.execute("DROP ALL OBJECTS");
        ca.close();
        cb.close();
    }
    
    private void testReadOnlyLinkedTable() throws SQLException {
        org.h2.Driver.load();
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
        Statement sa = ca.createStatement();
        Statement sb = cb.createStatement();
        sa.execute("CREATE TABLE TEST(ID INT)");
        sa.execute("INSERT INTO TEST VALUES(1)");
        String[] suffix = new String[]{"", "READONLY", "EMIT UPDATES"};
        for (int i = 0; i < suffix.length; i++) {
            String sql = "CREATE LINKED TABLE T(NULL, 'jdbc:h2:mem:one', 'sa', 'sa', 'TEST')" + suffix[i];
            sb.execute(sql);
            sb.executeQuery("SELECT * FROM T");
            String[] update = new String[]{"DELETE FROM T", "INSERT INTO T VALUES(2)", "UPDATE T SET ID = 3"};
            for (int j = 0; j < update.length; j++) {
                try {
                    sb.execute(update[j]);
                    if (i == 1) {
                        fail();
                    }
                } catch (SQLException e) {
                    if (i == 1) {
                        assertKnownException(e);
                    } else {
                        throw e;
                    }
                }
            }
            sb.execute("DROP TABLE T");
        }
        ca.close();
        cb.close();
    }

    private void testLinkOtherSchema() throws SQLException {
        org.h2.Driver.load();
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
        Statement sa = ca.createStatement();
        Statement sb = cb.createStatement();
        sa.execute("CREATE TABLE GOOD (X NUMBER)");
        sa.execute("CREATE SCHEMA S");
        sa.execute("CREATE TABLE S.BAD (X NUMBER)");
        sb.execute("CALL LINK_SCHEMA('G', '', 'jdbc:h2:mem:one', 'sa', 'sa', 'PUBLIC'); ");
        sb.execute("CALL LINK_SCHEMA('B', '', 'jdbc:h2:mem:one', 'sa', 'sa', 'S'); ");
        // OK
        sb.executeQuery("SELECT * FROM G.GOOD");
        // FAILED
        sb.executeQuery("SELECT * FROM B.BAD");
        ca.close();
        cb.close();
    }

    private void testLinkTwoTables() throws SQLException {
        org.h2.Driver.load();
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
        stat2.execute("CREATE LINKED TABLE two('org.h2.Driver', 'jdbc:h2:mem:one', 'sa', 'sa', 'PUBLIC.A');");
        ResultSet rs = stat2.executeQuery("SELECT * FROM one");
        rs.next();
        assertEquals(rs.getInt(1), 2);
        rs = stat2.executeQuery("SELECT * FROM two");
        rs.next();
        assertEquals(rs.getInt(1), 1);
        conn.close();
        conn2.close();
    }

    private void testLinkDrop() throws SQLException {
        org.h2.Driver.load();
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

    private void testLinkEmitUpdates() throws SQLException {
        deleteDb("linked1");
        deleteDb("linked2");
        org.h2.Driver.load();

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
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        stat2.executeUpdate("UPDATE TEST_LINK_DI SET ID=ID+1");
        stat2.executeUpdate("UPDATE TEST_LINK_U SET NAME=NAME || ID");
        ResultSet rs;

        rs = stat2.executeQuery("SELECT * FROM TEST_LINK_DI ORDER BY ID");
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getString(2), "Hello2");
        rs.next();
        assertEquals(rs.getInt(1), 3);
        assertEquals(rs.getString(2), "World3");
        assertFalse(rs.next());

        rs = stat2.executeQuery("SELECT * FROM TEST_LINK_U ORDER BY ID");
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getString(2), "Hello2");
        rs.next();
        assertEquals(rs.getInt(1), 3);
        assertEquals(rs.getString(2), "World3");
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getString(2), "Hello2");
        rs.next();
        assertEquals(rs.getInt(1), 3);
        assertEquals(rs.getString(2), "World3");
        assertFalse(rs.next());

        conn.close();
        conn2.close();
    }

    private void testLinkSchema() throws SQLException {
        deleteDb("linked1");
        deleteDb("linked2");
        org.h2.Driver.load();

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

    private void testLinkTable() throws SQLException {
        deleteDb("linked1");
        deleteDb("linked2");
        org.h2.Driver.load();

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
            fail("temp table must not be persistent");
        } catch (SQLException e) {
            assertKnownException(e);
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
        assertEquals(10, meta.getPrecision(1));
        assertEquals(200, meta.getPrecision(2));

        conn.close();
        conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/linked2", "sa2", "def");
        stat = conn.createStatement();

        stat
                .execute("INSERT INTO LINK_TEST VALUES(3, 'Link Test', 30, 100.05, '2005-12-31 12:34:56.789', X'FFEECC33', FALSE, 1, -1234567890123456789, X'4455FF', DATE '9999-12-31', TIME '23:59:59', 'George', -2.5)");

        rs = stat.executeQuery("SELECT COUNT(*) FROM LINK_TEST");
        rs.next();
        assertEquals(rs.getInt(1), 4);

        rs = stat.executeQuery("SELECT COUNT(*) FROM LINK_TEST WHERE NAME='Link Test'");
        rs.next();
        assertEquals(rs.getInt(1), 1);

        int uc = stat.executeUpdate("DELETE FROM LINK_TEST WHERE ID=3");
        assertEquals(uc, 1);

        rs = stat.executeQuery("SELECT COUNT(*) FROM LINK_TEST");
        rs.next();
        assertEquals(rs.getInt(1), 3);

        rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='LINK_TEST'");
        rs.next();
        assertEquals(rs.getString("TABLE_TYPE"), "TABLE LINK");

        rs.next();
        rs = stat.executeQuery("SELECT * FROM LINK_TEST WHERE ID=0");
        rs.next();
        assertTrue(rs.getString("NAME") == null && rs.wasNull());
        assertTrue(rs.getString("XT") == null && rs.wasNull());
        assertTrue(rs.getInt("ID") == 0 && !rs.wasNull());
        assertTrue(rs.getBigDecimal("XD") == null && rs.wasNull());
        assertTrue(rs.getTimestamp("XTS") == null && rs.wasNull());
        assertTrue(rs.getBytes("XBY") == null && rs.wasNull());
        assertTrue(!rs.getBoolean("XBO") && rs.wasNull());
        assertTrue(rs.getShort("XSM") == 0 && rs.wasNull());
        assertTrue(rs.getLong("XBI") == 0 && rs.wasNull());
        assertTrue(rs.getString("XBL") == null && rs.wasNull());
        assertTrue(rs.getString("XDA") == null && rs.wasNull());
        assertTrue(rs.getString("XTI") == null && rs.wasNull());
        assertTrue(rs.getString("XCL") == null && rs.wasNull());
        assertTrue(rs.getString("XDO") == null && rs.wasNull());
        assertFalse(rs.next());

        stat.execute("DROP TABLE LINK_TEST");

        stat.execute("CREATE LINKED TABLE LINK_TEST('org.h2.Driver', 'jdbc:h2:" + baseDir
                + "/linked1', 'sa1', 'abc', '(SELECT COUNT(*) FROM TEST)')");
        rs = stat.executeQuery("SELECT * FROM LINK_TEST");
        rs.next();
        assertEquals(rs.getInt(1), 3);
        assertFalse(rs.next());

        conn.close();

        deleteDb("linked1");
        deleteDb("linked2");
    }

    private void testRow(Statement stat, String name) throws SQLException {
        ResultSet rs = stat.executeQuery("SELECT * FROM " + name + " WHERE ID=1");
        rs.next();
        assertEquals(rs.getString("NAME"), "Hello");
        assertEquals(rs.getByte("XT"), -1);
        BigDecimal bd = rs.getBigDecimal("XD");
        assertTrue(bd.equals(new BigDecimal("10.30")));
        Timestamp ts = rs.getTimestamp("XTS");
        String s = ts.toString();
        assertEquals(s, "2001-02-03 11:22:33.4455");
        assertTrue(ts.equals(Timestamp.valueOf("2001-02-03 11:22:33.4455")));
        assertEquals(rs.getBytes("XBY"), new byte[] { (byte) 255, (byte) 1, (byte) 2 });
        assertTrue(rs.getBoolean("XBO"));
        assertEquals(rs.getShort("XSM"), 3000);
        assertEquals(rs.getLong("XBI"), 1234567890123456789L);
        assertEquals(rs.getString("XBL"), "1122aa");
        assertEquals(rs.getString("XDA"), "0002-01-01");
        assertEquals(rs.getString("XTI"), "00:00:00");
        assertEquals(rs.getString("XCL"), "J\u00fcrg");
        assertEquals(rs.getString("XDO"), "2.25");

    }

}
