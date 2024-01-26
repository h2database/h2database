/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Tests for the {@link Statement#getGeneratedKeys()}.
 */
public class TestGetGeneratedKeys extends TestDb {

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        deleteDb("getGeneratedKeys");
        Connection conn = getConnection("getGeneratedKeys");
        testBatchAndMergeInto(conn);
        testPrimaryKey(conn);
        testInsertWithSelect(conn);
        testUpdate(conn);
        testMergeUsing(conn);
        testWrongStatement(conn);
        testMultithreaded(conn);
        testNameCase(conn);
        testColumnNotFound(conn);

        testPrepareStatement_Execute(conn);
        testPrepareStatement_ExecuteBatch(conn);
        testPrepareStatement_ExecuteLargeBatch(conn);
        testPrepareStatement_ExecuteLargeUpdate(conn);
        testPrepareStatement_ExecuteUpdate(conn);
        testPrepareStatement_int_Execute(conn);
        testPrepareStatement_int_ExecuteBatch(conn);
        testPrepareStatement_int_ExecuteLargeBatch(conn);
        testPrepareStatement_int_ExecuteLargeUpdate(conn);
        testPrepareStatement_int_ExecuteUpdate(conn);
        testPrepareStatement_intArray_Execute(conn);
        testPrepareStatement_intArray_ExecuteBatch(conn);
        testPrepareStatement_intArray_ExecuteLargeBatch(conn);
        testPrepareStatement_intArray_ExecuteLargeUpdate(conn);
        testPrepareStatement_intArray_ExecuteUpdate(conn);
        testPrepareStatement_StringArray_Execute(conn);
        testPrepareStatement_StringArray_ExecuteBatch(conn);
        testPrepareStatement_StringArray_ExecuteLargeBatch(conn);
        testPrepareStatement_StringArray_ExecuteLargeUpdate(conn);
        testPrepareStatement_StringArray_ExecuteUpdate(conn);

        testStatementExecute(conn);
        testStatementExecute_int(conn);
        testStatementExecute_intArray(conn);
        testStatementExecute_StringArray(conn);
        testStatementExecuteLargeUpdate(conn);
        testStatementExecuteLargeUpdate_int(conn);
        testStatementExecuteLargeUpdate_intArray(conn);
        testStatementExecuteLargeUpdate_StringArray(conn);
        testStatementExecuteUpdate(conn);
        testStatementExecuteUpdate_int(conn);
        testStatementExecuteUpdate_intArray(conn);
        testStatementExecuteUpdate_StringArray(conn);

        conn.close();
        deleteDb("getGeneratedKeys");
    }

    /**
     * Test for batch updates and MERGE INTO operator.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testBatchAndMergeInto(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID BIGINT AUTO_INCREMENT, UID UUID DEFAULT RANDOM_UUID(), V INT)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (?), (?)",
                Statement.RETURN_GENERATED_KEYS);
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.addBatch();
        prep.setInt(1, 3);
        prep.setInt(1, 4);
        prep.addBatch();
        prep.executeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals("BIGINT", meta.getColumnTypeName(1));
        assertEquals("UUID", meta.getColumnTypeName(2));
        rs.next();
        assertEquals(1L, rs.getLong(1));
        UUID u1 = (UUID) rs.getObject(2);
        assertNotNull(u1);
        rs.next();
        assertEquals(2L, rs.getLong(1));
        UUID u2 = (UUID) rs.getObject(2);
        assertNotNull(u2);
        rs.next();
        assertEquals(3L, rs.getLong(1));
        UUID u3 = (UUID) rs.getObject(2);
        assertNotNull(u3);
        rs.next();
        assertEquals(4L, rs.getLong(1));
        UUID u4 = (UUID) rs.getObject(2);
        assertNotNull(u4);
        assertFalse(rs.next());
        assertFalse(u1.equals(u2));
        assertFalse(u2.equals(u3));
        assertFalse(u3.equals(u4));
        prep = conn.prepareStatement("MERGE INTO TEST(ID, V) KEY(ID) VALUES (?, ?)",
                Statement.RETURN_GENERATED_KEYS);
        prep.setInt(1, 2);
        prep.setInt(2, 10);
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(u2, rs.getObject(2));
        assertFalse(rs.next());
        prep.setInt(1, 5);
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        rs.next();
        assertEquals(Long.class, rs.getObject(1).getClass());
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test for PRIMARY KEY columns.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrimaryKey(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID BIGINT PRIMARY KEY, V INT)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(ID, V) VALUES (?, ?)",
                Statement.RETURN_GENERATED_KEYS);
        prep.setLong(1, 10);
        prep.setInt(2, 100);
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        rs.next();
        assertEquals(10L, rs.getLong(1));
        assertFalse(rs.next());
        assertEquals(1, rs.getMetaData().getColumnCount());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for INSERT ... SELECT operator.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testInsertWithSelect(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT, V INT NOT NULL)");

        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) SELECT 10",
                Statement.RETURN_GENERATED_KEYS);
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));
        rs.close();

        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for UPDATE operator.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT, V INT NOT NULL)");
        stat.execute("INSERT INTO TEST(V) VALUES 10");
        PreparedStatement prep = conn.prepareStatement("UPDATE TEST SET V = ? WHERE V = ?",
                Statement.RETURN_GENERATED_KEYS);
        prep.setInt(1, 20);
        prep.setInt(2, 10);
        assertEquals(1, prep.executeUpdate());
        ResultSet rs = prep.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for MERGE USING operator.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testMergeUsing(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE SOURCE (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + " UID INT NOT NULL UNIQUE, V INT NOT NULL)");
        stat.execute("CREATE TABLE DESTINATION (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + " UID INT NOT NULL UNIQUE, V INT NOT NULL)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO SOURCE(UID, V) VALUES (?, ?)");
        for (int i = 1; i <= 100; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i * 10 + 5);
            ps.executeUpdate();
        }
        // Insert first half of a rows with different values
        ps = conn.prepareStatement("INSERT INTO DESTINATION(UID, V) VALUES (?, ?)");
        for (int i = 1; i <= 50; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i * 10);
            ps.executeUpdate();
        }
        // And merge second half into it, first half will be updated with a new values
        ps = conn.prepareStatement(
                "MERGE INTO DESTINATION USING SOURCE ON (DESTINATION.UID = SOURCE.UID)"
                        + " WHEN MATCHED THEN UPDATE SET V = SOURCE.V"
                        + " WHEN NOT MATCHED THEN INSERT (UID, V) VALUES (SOURCE.UID, SOURCE.V)",
                Statement.RETURN_GENERATED_KEYS);
        // All rows should be either updated or inserted
        assertEquals(100, ps.executeUpdate());
        ResultSet rs = ps.getGeneratedKeys();
        for (int i = 1; i <= 100; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getLong(1));
        }
        assertFalse(rs.next());
        rs.close();
        // Check merged data
        rs = stat.executeQuery("SELECT ID, UID, V FROM DESTINATION ORDER BY ID");
        for (int i = 1; i <= 100; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getLong(1));
            assertEquals(i, rs.getInt(2));
            assertEquals(i * 10 + 5, rs.getInt(3));
        }
        assertFalse(rs.next());
        stat.execute("DROP TABLE SOURCE");
        stat.execute("DROP TABLE DESTINATION");
    }

    /**
     * Test method for incompatible statements.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testWrongStatement(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT, V INT)");
        stat.execute("INSERT INTO TEST(V) VALUES 10, 20, 30");
        stat.execute("DELETE FROM TEST WHERE V = 10", Statement.RETURN_GENERATED_KEYS);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("TRUNCATE TABLE TEST", Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for shared connection between several statements in different
     * threads.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testMultithreaded(final Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT, V INT NOT NULL)");
        final int count = 4, iterations = 10_000;
        Thread[] threads = new Thread[count];
        final long[] keys = new long[count * iterations];
        for (int i = 0; i < count; i++) {
            final int num = i;
            threads[num] = new Thread("getGeneratedKeys-" + num) {
                @Override
                public void run() {
                    try {
                        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (?)",
                                Statement.RETURN_GENERATED_KEYS);
                        for (int i = 0; i < iterations; i++) {
                            int value = iterations * num + i;
                            prep.setInt(1, value);
                            prep.execute();
                            ResultSet rs = prep.getGeneratedKeys();
                            rs.next();
                            keys[value] = rs.getLong(1);
                            rs.close();
                        }
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                }
            };
        }
        for (int i = 0; i < count; i++) {
            threads[i].start();
        }
        for (int i = 0; i < count; i++) {
            threads[i].join();
        }
        ResultSet rs = stat.executeQuery("SELECT V, ID FROM TEST ORDER BY V");
        for (int i = 0; i < keys.length; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
            assertEquals(keys[i], rs.getLong(2));
        }
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for case of letters in column names.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testNameCase(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "\"id\" UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        // Test columns with only difference in case
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)",
                new String[] { "id", "ID" });
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("id", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(1L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        // Test lower case name of upper case column
        stat.execute("ALTER TABLE TEST DROP COLUMN \"id\"");
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", new String[] { "id" });
        testNameCase1(prep, 2L, true);
        // Test upper case name of lower case column
        stat.execute("ALTER TABLE TEST ALTER COLUMN ID RENAME TO \"id\"");
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (30)", new String[] { "ID" });
        testNameCase1(prep, 3L, false);
        stat.execute("DROP TABLE TEST");
    }

    private void testNameCase1(PreparedStatement prep, long id, boolean upper) throws SQLException {
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals(upper ? "ID" : "id", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(id, rs.getLong(1));
        assertFalse(rs.next());
        rs.close();
    }

    /**
     * Test method for column not found exception.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testColumnNotFound(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT, V INT NOT NULL)");
        assertThrows(ErrorCode.COLUMN_NOT_FOUND_1, stat).execute("INSERT INTO TEST(V) VALUES (1)", //
                new int[] { 0 });
        assertThrows(ErrorCode.COLUMN_NOT_FOUND_1, stat).execute("INSERT INTO TEST(V) VALUES (1)", //
                new int[] { 3 });
        assertThrows(ErrorCode.COLUMN_NOT_FOUND_1, stat).execute("INSERT INTO TEST(V) VALUES (1)", //
                new String[] { "X" });
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String)}
     * .{@link PreparedStatement#execute()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_Execute(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)");
        prep.execute();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String)}
     * .{@link PreparedStatement#executeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_ExecuteBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)");
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String)}
     * .{@link PreparedStatement#executeLargeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_ExecuteLargeBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)");
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String)}
     * .{@link PreparedStatement#executeLargeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_ExecuteLargeUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)");
        prep.executeLargeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String)}
     * .{@link PreparedStatement#executeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_ExecuteUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)");
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int)}
     * .{@link PreparedStatement#execute()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_int_Execute(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL, OTHER INT DEFAULT 0)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)",
                Statement.NO_GENERATED_KEYS);
        prep.execute();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int)}
     * .{@link PreparedStatement#executeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_int_ExecuteBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL, OTHER INT DEFAULT 0)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)",
                Statement.NO_GENERATED_KEYS);
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(3L, rs.getLong("ID"));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertEquals(UUID.class, rs.getObject("UID").getClass());
        assertEquals(UUID.class, rs.getObject("UID", UUID.class).getClass());
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(4L, rs.getLong("ID"));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertEquals(UUID.class, rs.getObject("UID").getClass());
        assertEquals(UUID.class, rs.getObject("UID", UUID.class).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Connection#prepareStatement(String, int)}
     * .{@link PreparedStatement#executeLargeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_int_ExecuteLargeBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL, OTHER INT DEFAULT 0)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)",
                Statement.NO_GENERATED_KEYS);
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(3L, rs.getLong("ID"));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertEquals(UUID.class, rs.getObject("UID").getClass());
        assertEquals(UUID.class, rs.getObject("UID", UUID.class).getClass());
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(4L, rs.getLong("ID"));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertEquals(UUID.class, rs.getObject("UID").getClass());
        assertEquals(UUID.class, rs.getObject("UID", UUID.class).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int)}
     * .{@link PreparedStatement#executeLargeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_int_ExecuteLargeUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL, OTHER INT DEFAULT 0)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)",
                Statement.NO_GENERATED_KEYS);
        prep.executeLargeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int)}
     * .{@link PreparedStatement#executeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_int_ExecuteUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL, OTHER INT DEFAULT 0)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)",
                Statement.NO_GENERATED_KEYS);
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int[])}
     * .{@link PreparedStatement#execute()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_intArray_Execute(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)", new int[0]);
        prep.execute();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", new int[] { 1, 2 });
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (30)", new int[] { 2, 1 });
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (40)", new int[] { 2 });
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int[])}
     * .{@link PreparedStatement#executeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_intArray_ExecuteBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)", new int[0]);
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", new int[] { 1, 2 });
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (30)", new int[] { 2, 1 });
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(5L, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(6L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (40)", new int[] { 2 });
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int[])}
     * .{@link PreparedStatement#executeLargeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_intArray_ExecuteLargeBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)", new int[0]);
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", new int[] { 1, 2 });
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (30)", new int[] { 2, 1 });
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(5L, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(6L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (40)", new int[] { 2 });
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int[])}
     * .{@link PreparedStatement#executeLargeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_intArray_ExecuteLargeUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)", new int[0]);
        prep.executeLargeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", new int[] { 1, 2 });
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (30)", new int[] { 2, 1 });
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (40)", new int[] { 2 });
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int[])}
     * .{@link PreparedStatement#executeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_intArray_ExecuteUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)", new int[0]);
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", new int[] { 1, 2 });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (30)", new int[] { 2, 1 });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (40)", new int[] { 2 });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, String[])}
     * .{@link PreparedStatement#execute()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_StringArray_Execute(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)", new String[0]);
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", new String[] { "ID", "UID" });
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (30)", new String[] { "UID", "ID" });
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (40)", new String[] { "UID" });
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, String[])}
     * .{@link PreparedStatement#executeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_StringArray_ExecuteBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)", new String[0]);
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", new String[] { "ID", "UID" });
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (30)", new String[] { "UID", "ID" });
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(5L, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(6L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (40)", new String[] { "UID" });
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, String[])}
     * .{@link PreparedStatement#executeLargeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_StringArray_ExecuteLargeBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)", new String[0]);
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", new String[] { "ID", "UID" });
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (30)", new String[] { "UID", "ID" });
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(5L, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(6L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (40)", new String[] { "UID" });
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, String[])}
     * .{@link PreparedStatement#executeLargeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_StringArray_ExecuteLargeUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)", new String[0]);
        prep.executeLargeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", new String[] { "ID", "UID" });
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (30)", new String[] { "UID", "ID" });
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (40)", new String[] { "UID" });
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Connection#prepareStatement(String, String[])}
     * .{@link PreparedStatement#executeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_StringArray_ExecuteUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (10)", new String[0]);
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (20)", new String[] { "ID", "UID" });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (30)", new String[] { "UID", "ID" });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(V) VALUES (40)", new String[] { "UID" });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#execute(String)}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecute(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        stat.execute("INSERT INTO TEST(V) VALUES (10)");
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#execute(String, int)}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecute_int(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL, OTHER INT DEFAULT 0)");
        stat.execute("INSERT INTO TEST(V) VALUES (10)", Statement.NO_GENERATED_KEYS);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(V) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#execute(String, int[])}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecute_intArray(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        stat.execute("INSERT INTO TEST(V) VALUES (10)", new int[0]);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(V) VALUES (20)", new int[] { 1, 2 });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(V) VALUES (30)", new int[] { 2, 1 });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(V) VALUES (40)", new int[] { 2 });
        rs = stat.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeUpdate(String, String[])}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecute_StringArray(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        stat.execute("INSERT INTO TEST(V) VALUES (10)", new String[0]);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(V) VALUES (20)", new String[] { "ID", "UID" });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(V) VALUES (30)", new String[] { "UID", "ID" });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(V) VALUES (40)", new String[] { "UID" });
        rs = stat.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeLargeUpdate(String)}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteLargeUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        stat.executeLargeUpdate("INSERT INTO TEST(V) VALUES (10)");
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeLargeUpdate(String, int)}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteLargeUpdate_int(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL, OTHER INT DEFAULT 0)");
        stat.executeLargeUpdate("INSERT INTO TEST(V) VALUES (10)", Statement.NO_GENERATED_KEYS);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(V) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeLargeUpdate(String, int[])}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteLargeUpdate_intArray(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        stat.executeLargeUpdate("INSERT INTO TEST(V) VALUES (10)", new int[0]);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(V) VALUES (20)", new int[] { 1, 2 });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(V) VALUES (30)", new int[] { 2, 1 });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(V) VALUES (40)", new int[] { 2 });
        rs = stat.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeLargeUpdate(String, String[])}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteLargeUpdate_StringArray(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        stat.executeLargeUpdate("INSERT INTO TEST(V) VALUES (10)", new String[0]);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(V) VALUES (20)", new String[] { "ID", "UID" });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(V) VALUES (30)", new String[] { "UID", "ID" });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(V) VALUES (40)", new String[] { "UID" });
        rs = stat.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeUpdate(String)}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        stat.executeUpdate("INSERT INTO TEST(V) VALUES (10)");
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeUpdate(String, int)}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteUpdate_int(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL, OTHER INT DEFAULT 0)");
        stat.executeUpdate("INSERT INTO TEST(V) VALUES (10)", Statement.NO_GENERATED_KEYS);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(V) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeUpdate(String, int[])}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteUpdate_intArray(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        stat.executeUpdate("INSERT INTO TEST(V) VALUES (10)", new int[0]);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(V) VALUES (20)", new int[] { 1, 2 });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(V) VALUES (30)", new int[] { 2, 1 });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(V) VALUES (40)", new int[] { 2 });
        rs = stat.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeUpdate(String, String[])}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteUpdate_StringArray(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), V INT NOT NULL)");
        stat.executeUpdate("INSERT INTO TEST(V) VALUES (10)", new String[0]);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(V) VALUES (20)", new String[] { "ID", "UID" });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(V) VALUES (30)", new String[] { "UID", "ID" });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(V) VALUES (40)", new String[] { "UID" });
        rs = stat.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

}
