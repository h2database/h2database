/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.TimeZone;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.DateTimeUtils;

/**
 * Tests the client info
 */
public class TestConnection extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testSetSupportedClientInfo();
        testSetUnsupportedClientInfo();
        testGetUnsupportedClientInfo();
        testSetSupportedClientInfoProperties();
        testSetUnsupportedClientInfoProperties();
        testSetInternalProperty();
        testSetInternalPropertyToInitialValue();
        testTransactionIsolationSetAndGet();
        testSetGetSchema();
        testCommitOnAutoCommitSetRunner();
        testRollbackOnAutoCommitSetRunner();
        testChangeTransactionLevelCommitRunner();
        testLockTimeout();
        testIgnoreUnknownSettings();
        testTimeZone();
    }

    private void testSetInternalProperty() throws SQLException {
        // Use MySQL-mode since this allows all property names
        // (apart from h2 internal names).
        Connection conn = getConnection("clientInfoMySQL;MODE=MySQL");

        assertThrows(SQLClientInfoException.class, conn).setClientInfo("numServers", "SomeValue");
        assertThrows(SQLClientInfoException.class, conn).setClientInfo("server23", "SomeValue");
        conn.close();
    }

    /**
     * Test that no exception is thrown if the client info of a connection
     * managed in a connection pool is reset to its initial values.
     *
     * This is needed when using h2 in websphere liberty.
     */
    private void testSetInternalPropertyToInitialValue() throws SQLException {
        // Use MySQL-mode since this allows all property names
        // (apart from h2 internal names).
        Connection conn = getConnection("clientInfoMySQL;MODE=MySQL");
        String numServersPropertyName = "numServers";
        String numServers = conn.getClientInfo(numServersPropertyName);
        conn.setClientInfo(numServersPropertyName, numServers);
        assertEquals(conn.getClientInfo(numServersPropertyName), numServers);
        conn.close();
    }

    private void testSetUnsupportedClientInfoProperties() throws SQLException {
        Connection conn = getConnection("clientInfo");
        Properties properties = new Properties();
        properties.put("ClientUser", "someUser");
        assertThrows(SQLClientInfoException.class, conn).setClientInfo(properties);
        conn.close();
    }

    private void testSetSupportedClientInfoProperties() throws SQLException {
        Connection conn = getConnection("clientInfoDB2;MODE=DB2");
        conn.setClientInfo("ApplicationName", "Connection Test");

        Properties properties = new Properties();
        properties.put("ClientUser", "someUser");
        conn.setClientInfo(properties);
        // old property should have been removed
        assertNull(conn.getClientInfo("ApplicationName"));
        // new property has been set
        assertEquals(conn.getClientInfo("ClientUser"), "someUser");
        conn.close();
    }

    private void testSetSupportedClientInfo() throws SQLException {
        Connection conn = getConnection("clientInfoDB2;MODE=DB2");
        conn.setClientInfo("ApplicationName", "Connection Test");

        assertEquals(conn.getClientInfo("ApplicationName"), "Connection Test");
        conn.close();
    }

    private void testSetUnsupportedClientInfo() throws SQLException {
        Connection conn = getConnection("clientInfoDB2;MODE=DB2");
        assertThrows(SQLClientInfoException.class, conn).setClientInfo(
                "UnsupportedName", "SomeValue");
        conn.close();
    }

    private void testGetUnsupportedClientInfo() throws SQLException {
        Connection conn = getConnection("clientInfo");
        assertNull(conn.getClientInfo("UnknownProperty"));
        conn.close();
    }

    private void testTransactionIsolationSetAndGet() throws Exception {
        deleteDb("transactionIsolation");
        try (Connection conn = getConnection("transactionIsolation")) {
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, conn.getTransactionIsolation());
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, conn.getTransactionIsolation());
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            assertEquals(Connection.TRANSACTION_REPEATABLE_READ,
                    conn.getTransactionIsolation());
            conn.setTransactionIsolation(Constants.TRANSACTION_SNAPSHOT);
            assertEquals(Constants.TRANSACTION_SNAPSHOT,
                    conn.getTransactionIsolation());
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());
        } finally {
            deleteDb("transactionIsolation");
        }
    }

    private void testCommitOnAutoCommitSetRunner() throws Exception {
        assertFalse("Default value must be false", SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT);
        testCommitOnAutoCommitSet(false);
        try {
            SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT = true;
            testCommitOnAutoCommitSet(true);
        } finally {
            SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT = false;
        }

    }

    private void testCommitOnAutoCommitSet(boolean expectedPropertyEnabled) throws Exception {
        assertEquals(SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT, expectedPropertyEnabled);
        Connection conn = getConnection("clientInfo");
        conn.setAutoCommit(false);
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        int index = 1;
        prep.setInt(index++, 1);
        prep.setString(index++, "test1");
        prep.execute();
        conn.commit();
        // no error expected

        conn.setAutoCommit(true);
        index = 1;
        prep.setInt(index++, 2);
        prep.setString(index++, "test2");
        if (expectedPropertyEnabled) {
            prep.execute();
            try {
                conn.commit();
                throw new AssertionError("SQLException expected");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("commit()"));
                assertEquals(ErrorCode.METHOD_DISABLED_ON_AUTOCOMMIT_TRUE, e.getErrorCode());
            }
            ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM TEST");
            rs.next();
            assertTrue(rs.getInt(1) == 2);
            rs.close();
        } else {
            prep.execute();
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM TEST");
            rs.next();
            assertTrue(rs.getInt(1) == 2);
            rs.close();
        }

        conn.close();
        prep.close();
    }

    private void testChangeTransactionLevelCommitRunner() throws Exception {
        assertFalse("Default value must be false", SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT);
        testChangeTransactionLevelCommit(false);
        testChangeTransactionLevelCommit(true);
        try {
            SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT = true;
            testChangeTransactionLevelCommit(true);
            testChangeTransactionLevelCommit(false);
        } finally {
            SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT = false;
        }
    }

    private void testChangeTransactionLevelCommit(boolean setAutoCommit) throws Exception {
        Connection conn = getConnection("clientInfo");
        conn.setAutoCommit(setAutoCommit);
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        int index = 1;
        prep.setInt(index++, 1);
        prep.setString(index++, "test1");
        prep.execute();
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

        conn.createStatement().executeQuery("SELECT COUNT(*) FROM TEST");
        // throws exception if TransactionIsolation did not commit

        conn.close();
        prep.close();
    }

    private void testRollbackOnAutoCommitSetRunner() throws Exception {
        assertFalse("Default value must be false", SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT);
        testRollbackOnAutoCommitSet(false);
        try {
            SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT = true;
            testRollbackOnAutoCommitSet(true);
        } finally {
            SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT = false;
        }
    }

    private void testRollbackOnAutoCommitSet(boolean expectedPropertyEnabled) throws Exception {
        assertEquals(SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT, expectedPropertyEnabled);
        Connection conn = getConnection("clientInfo");
        conn.setAutoCommit(false);
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        int index = 1;
        prep.setInt(index++, 1);
        prep.setString(index++, "test1");
        prep.execute();
        conn.rollback();
        // no error expected


        conn.setAutoCommit(true);
        index = 1;
        prep.setInt(index++, 2);
        prep.setString(index++, "test2");
        if (expectedPropertyEnabled) {
            prep.execute();
            try {
                conn.rollback();
                throw new AssertionError("SQLException expected");
            } catch (SQLException e) {
                assertEquals(ErrorCode.METHOD_DISABLED_ON_AUTOCOMMIT_TRUE, e.getErrorCode());
                assertTrue(e.getMessage().contains("rollback()"));
            }
            ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM TEST");
            rs.next();
            int count = rs.getInt(1);
            assertTrue("Found " +count + " rows",  count == 1);
            rs.close();
        } else {
            prep.execute();
            // rollback is permitted, however has no effects in autocommit=true
            conn.rollback();
            ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM TEST");
            rs.next();
            int count = rs.getInt(1);
            assertTrue("Found " + count + " rows",  count == 1);
            rs.close();
        }

        conn.close();
        prep.close();
    }

    private void testSetGetSchema() throws SQLException {
        deleteDb("schemaSetGet");
        Connection conn = getConnection("schemaSetGet");
        Statement s = conn.createStatement();
        s.executeUpdate("create schema my_test_schema");
        s.executeUpdate("create table my_test_schema.my_test_table(id int, nave varchar) as values (1, 'a')");
        assertEquals("PUBLIC", conn.getSchema());
        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, s, "select * from my_test_table");
        assertThrows(ErrorCode.SCHEMA_NOT_FOUND_1, conn).setSchema("my_test_table");
        conn.setSchema("MY_TEST_SCHEMA");
        assertEquals("MY_TEST_SCHEMA", conn.getSchema());
        try (ResultSet rs = s.executeQuery("select * from my_test_table")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("a", rs.getString(2));
            assertFalse(rs.next());
        }
        assertThrows(ErrorCode.SCHEMA_NOT_FOUND_1, conn).setSchema("NON_EXISTING_SCHEMA");
        assertEquals("MY_TEST_SCHEMA", conn.getSchema());
        s.executeUpdate("create schema \"otheR_schEma\"");
        s.executeUpdate("create table \"otheR_schEma\".my_test_table(id int, nave varchar) as values (2, 'b')");
        conn.setSchema("otheR_schEma");
        assertEquals("otheR_schEma", conn.getSchema());
        try (ResultSet rs = s.executeQuery("select * from my_test_table")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("b", rs.getString(2));
            assertFalse(rs.next());
        }
        s.execute("SET SCHEMA \"MY_TEST_SCHEMA\"");
        assertEquals("MY_TEST_SCHEMA", conn.getSchema());
        s.close();
        conn.close();
        deleteDb("schemaSetGet");
    }

    private void testLockTimeout() throws SQLException {
        deleteDb("lockTimeout");
        try (Connection conn1 = getConnection("lockTimeout");
                Connection conn2 = getConnection("lockTimeout;LOCK_TIMEOUT=6000")) {
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(false);
            Statement s1 = conn1.createStatement();
            Statement s2 = conn2.createStatement();
            s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, V INT) AS VALUES (1, 2)");
            conn1.commit();
            s2.execute("INSERT INTO TEST VALUES (2, 4)");
            s1.execute("UPDATE TEST SET V = 3 WHERE ID = 1");
            s2.execute("SET LOCK_TIMEOUT 50");
            long n = System.nanoTime();
            assertThrows(ErrorCode.LOCK_TIMEOUT_1, s2).execute("UPDATE TEST SET V = 4 WHERE ID = 1");
            if (System.nanoTime() - n > 5_000_000_000L) {
                fail("LOCK_TIMEOUT wasn't set");
            }
        } finally {
            deleteDb("lockTimeout");
        }
    }

    private void testIgnoreUnknownSettings() throws SQLException {
        deleteDb("ignoreUnknownSettings");
        assertThrows(ErrorCode.UNSUPPORTED_SETTING_1, () -> getConnection("ignoreUnknownSettings;A=1"));
        try (Connection c = getConnection("ignoreUnknownSettings;IGNORE_UNKNOWN_SETTINGS=TRUE;A=1")) {
        } finally {
            deleteDb("ignoreUnknownSettings");
        }
    }

    private void testTimeZone() throws SQLException {
        deleteDb("timeZone");
        String tz1 = "Europe/London", tz2 = "Europe/Paris", tz3 = "Asia/Tokyo";
        try (Connection c = getConnection("timeZone")) {
            TimeZone tz = TimeZone.getDefault();
            try {
                TimeZone.setDefault(TimeZone.getTimeZone(tz1));
                DateTimeUtils.resetCalendar();
                try (Connection c1 = getConnection("timeZone")) {
                    TimeZone.setDefault(TimeZone.getTimeZone(tz2));
                    DateTimeUtils.resetCalendar();
                    try (Connection c2 = getConnection("timeZone");
                            Connection c3 = getConnection("timeZone;TIME ZONE=" + tz3)) {
                        checkTimeZone(tz1, c1);
                        checkTimeZone(tz2, c2);
                        checkTimeZone(tz3, c3);
                    }
                }
            } finally {
                TimeZone.setDefault(tz);
                DateTimeUtils.resetCalendar();
            }
        } finally {
            deleteDb("timeZone");
        }
    }

    private void checkTimeZone(String expected, Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME = 'TIME ZONE'");
        rs.next();
        assertEquals(expected, rs.getString(1));
    }

}
