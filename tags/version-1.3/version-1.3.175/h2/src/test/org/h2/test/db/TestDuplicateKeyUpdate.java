/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;

/**
 * Tests for the ON DUPLICATE KEY UPDATE in the Insert class.
 */
public class TestDuplicateKeyUpdate extends TestBase {

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
        deleteDb("duplicateKeyUpdate");
        Connection conn = getConnection("duplicateKeyUpdate;MODE=MySQL");
        testDuplicateOnPrimary(conn);
        testDuplicateOnUnique(conn);
        testDuplicateCache(conn);
        testDuplicateExpression(conn);
        conn.close();
        deleteDb("duplicateKeyUpdate");
    }

    private void testDuplicateOnPrimary(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("CREATE TABLE `table_test` (\n" +
                "  `id` bigint(20) NOT NULL ,\n" +
                "  `a_text` varchar(254) NOT NULL,\n" +
                "  `some_text` varchar(254) NULL,\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ;");

        stat.execute("INSERT INTO table_test ( id, a_text, some_text ) VALUES " +
                "(1, 'aaaaaaaaaa', 'aaaaaaaaaa'), " +
                "(2, 'bbbbbbbbbb', 'bbbbbbbbbb'), "+
                "(3, 'cccccccccc', 'cccccccccc'), " +
                "(4, 'dddddddddd', 'dddddddddd'), " +
                "(5, 'eeeeeeeeee', 'eeeeeeeeee')");

        stat.execute("INSERT INTO table_test ( id , a_text, some_text ) " +
                "VALUES (1, 'zzzzzzzzzz', 'abcdefghij') " +
                "ON DUPLICATE KEY UPDATE some_text='UPDATE'");

        rs = stat.executeQuery("SELECT some_text FROM table_test where id = 1");
        rs.next();
        assertEquals("UPDATE", rs.getNString(1));

        stat.execute("INSERT INTO table_test ( id , a_text, some_text ) " +
                "VALUES (3, 'zzzzzzzzzz', 'SOME TEXT') " +
                "ON DUPLICATE KEY UPDATE some_text=values(some_text)");
        rs = stat.executeQuery("SELECT some_text FROM table_test where id = 3");
        rs.next();
        assertEquals("SOME TEXT", rs.getNString(1));
    }

    private void testDuplicateOnUnique(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("CREATE TABLE `table_test2` (\n" + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "  `a_text` varchar(254) NOT NULL,\n" + "  `some_text` varchar(254) NOT NULL,\n"
                + "  `updatable_text` varchar(254) NULL,\n" + "  PRIMARY KEY (`id`)\n" + ") ;");

        stat.execute("CREATE UNIQUE INDEX index_name \n" + "ON table_test2 (a_text, some_text);");

        stat.execute("INSERT INTO table_test2 ( a_text, some_text, updatable_text ) VALUES ('a', 'a', '1')");
        stat.execute("INSERT INTO table_test2 ( a_text, some_text, updatable_text ) VALUES ('b', 'b', '2')");
        stat.execute("INSERT INTO table_test2 ( a_text, some_text, updatable_text ) VALUES ('c', 'c', '3')");
        stat.execute("INSERT INTO table_test2 ( a_text, some_text, updatable_text ) VALUES ('d', 'd', '4')");
        stat.execute("INSERT INTO table_test2 ( a_text, some_text, updatable_text ) VALUES ('e', 'e', '5')");

        stat.execute("INSERT INTO table_test2 ( a_text, some_text ) " +
                "VALUES ('e', 'e') ON DUPLICATE KEY UPDATE updatable_text='UPDATE'");

        rs = stat.executeQuery("SELECT updatable_text FROM table_test2 where a_text = 'e'");
        rs.next();
        assertEquals("UPDATE", rs.getNString(1));

        stat.execute("INSERT INTO table_test2 (a_text, some_text, updatable_text ) " +
                "VALUES ('b', 'b', 'test') " +
                "ON DUPLICATE KEY UPDATE updatable_text=values(updatable_text)");
        rs = stat.executeQuery("SELECT updatable_text FROM table_test2 where a_text = 'b'");
        rs.next();
        assertEquals("test", rs.getNString(1));
    }

    private void testDuplicateCache(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("CREATE TABLE `table_test3` (\n" +
                "  `id` bigint(20) NOT NULL ,\n" +
                "  `a_text` varchar(254) NOT NULL,\n" +
                "  `some_text` varchar(254) NULL,\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ;");

        stat.execute("INSERT INTO table_test3 ( id, a_text, some_text ) " +
                "VALUES (1, 'aaaaaaaaaa', 'aaaaaaaaaa')");

        stat.execute("INSERT INTO table_test3 ( id , a_text, some_text ) " +
                "VALUES (1, 'zzzzzzzzzz', 'SOME TEXT') " +
                "ON DUPLICATE KEY UPDATE some_text=values(some_text)");
        rs = stat.executeQuery("SELECT some_text FROM table_test3 where id = 1");
        rs.next();
        assertEquals("SOME TEXT", rs.getNString(1));

        // Execute twice the same query to use the one from cache without
        // parsing, caused the values parameter to be seen as ambiguous
        stat.execute("INSERT INTO table_test3 ( id , a_text, some_text ) " +
                "VALUES (1, 'zzzzzzzzzz', 'SOME TEXT') " +
                "ON DUPLICATE KEY UPDATE some_text=values(some_text)");
        rs = stat.executeQuery("SELECT some_text FROM table_test3 where id = 1");
        rs.next();
        assertEquals("SOME TEXT", rs.getNString(1));
    }

    private void testDuplicateExpression(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("CREATE TABLE `table_test4` (\n" +
                "  `id` bigint(20) NOT NULL ,\n" +
                "  `a_text` varchar(254) NOT NULL,\n" +
                "  `some_value` int(10) NULL,\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ;");

        stat.execute("INSERT INTO table_test4 ( id, a_text, some_value ) " +
                "VALUES (1, 'aaaaaaaaaa', 5)");
        stat.execute("INSERT INTO table_test4 ( id, a_text, some_value ) " +
                "VALUES (2, 'aaaaaaaaaa', 5)");

        stat.execute("INSERT INTO table_test4 ( id , a_text, some_value ) VALUES (1, 'b', 1) " +
                "ON DUPLICATE KEY UPDATE some_value=some_value + values(some_value)");
        stat.execute("INSERT INTO table_test4 ( id , a_text, some_value ) VALUES (1, 'b', 1) " +
                "ON DUPLICATE KEY UPDATE some_value=some_value + 100");
        stat.execute("INSERT INTO table_test4 ( id , a_text, some_value ) VALUES (2, 'b', 1) " +
                "ON DUPLICATE KEY UPDATE some_value=values(some_value) + 1");
        rs = stat.executeQuery("SELECT some_value FROM table_test4 where id = 1");
        rs.next();
        assertEquals(106, rs.getInt(1));
        rs = stat.executeQuery("SELECT some_value FROM table_test4 where id = 2");
        rs.next();
        assertEquals(2, rs.getInt(1));
    }

}
