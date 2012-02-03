/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.StringTokenizer;
import org.h2.fulltext.FullText;
import org.h2.store.fs.FileSystem;
import org.h2.test.TestBase;

/**
 * Fulltext search tests.
 */
public class TestFullText extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testTransaction(false);
        testCreateDrop();
        if (config.memory) {
            return;
        }
        testMultiThreaded();
        testStreamLob();
        test(false, "VARCHAR");
        test(false, "CLOB");
        testPerformance(false);
        testReopen(false);
        String luceneFullTextClassName = "org.h2.fulltext.FullTextLucene";
        try {
            Class.forName(luceneFullTextClassName);
            testTransaction(true);
            test(true, "VARCHAR");
            test(true, "CLOB");
            testPerformance(true);
            testReopen(true);
        } catch (ClassNotFoundException e) {
            println("Class not found, not tested: " + luceneFullTextClassName);
            // ok
        } catch (NoClassDefFoundError e) {
            println("Class not found, not tested: " + luceneFullTextClassName);
            // ok
        }
        FullText.closeAll();
        deleteDb("fullText");
        deleteDb("fullTextReopen");
    }

    private void testTransaction(boolean lucene) throws SQLException {
        if (config.memory) {
            return;
        }
        String prefix = lucene ? "FTL" : "FT";
        deleteDb("fullTextTransaction");
        FileSystem.getInstance(baseDir).deleteRecursive(baseDir + "/fullTextTransaction", false);
        Connection conn = getConnection("fullTextTransaction");
        Statement stat = conn.createStatement();
        String className = lucene ? "FullTextLucene" : "FullText";
        stat.execute("CREATE ALIAS IF NOT EXISTS " + prefix + "_INIT FOR \"org.h2.fulltext." + className + ".init\"");
        stat.execute("CALL " + prefix + "_INIT()");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello World')");
        stat.execute("CALL " + prefix + "_CREATE_INDEX('PUBLIC', 'TEST', NULL)");
        stat.execute("UPDATE TEST SET NAME=NULL WHERE ID=1");
        stat.execute("UPDATE TEST SET NAME='Hello World' WHERE ID=1");
        conn.setAutoCommit(false);
        stat.execute("insert into test values(2, 'Hello Moon!')");
        conn.rollback();
        conn.close();
        conn = getConnection("fullTextTransaction");
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM " + prefix + "_SEARCH('Hello', 0, 0)");
        assertTrue(rs.next());
        rs = stat.executeQuery("SELECT * FROM " + prefix + "_SEARCH('Moon', 0, 0)");
        assertFalse(rs.next());
        FullText.dropAll(conn);
        conn.close();
        deleteDb("fullTextTransaction");
    }

    private void testMultiThreaded() throws Exception {
        deleteDb("fullText");
        final boolean[] stop = { false };
        final Exception[] exception = { null };
        int len = 2;
        Thread[] threads = new Thread[len];
        for (int i = 0; i < len; i++) {
            // final Connection conn =
            // getConnection("fullText;MULTI_THREADED=1;LOCK_TIMEOUT=10000");
            final Connection conn = getConnection("fullText");
            Statement stat = conn.createStatement();
            stat.execute("CREATE ALIAS IF NOT EXISTS FT_INIT FOR \"org.h2.fulltext.FullText.init\"");
            stat.execute("CALL FT_INIT()");
            stat.execute("CREATE ALIAS IF NOT EXISTS FT_INIT FOR \"org.h2.fulltext.FullText.init\"");
            stat.execute("CALL FT_INIT()");
            final String tableName = "TEST" + i;
            stat.execute("CREATE TABLE " + tableName + "(ID INT PRIMARY KEY, DATA VARCHAR)");
            FullText.createIndex(conn, "PUBLIC", tableName, null);
            threads[i] = new Thread() {
                public void run() {
                    try {
                        PreparedStatement prep = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?)");
                        Random random = new Random();
                        int x = 0;
                        while (!stop[0]) {
                            StringBuilder buff = new StringBuilder();
                            for (int j = 0; j < 1000; j++) {
                                buff.append(" " + random.nextInt(10000));
                                buff.append(" x" + j);
                            }
                            prep.setInt(1, x);
                            prep.setString(2, buff.toString());
                            prep.execute();
                            x++;
                        }
                        conn.close();
                    } catch (SQLException e) {
                        exception[0] = e;
                    }
                }
            };
        }
        for (Thread t : threads) {
            t.start();
        }
        Thread.sleep(1000);
        stop[0] = true;
        for (Thread t : threads) {
            t.join();
        }
        if (exception[0] != null) {
            throw exception[0];
        }

    }

    private void testStreamLob() throws SQLException {
        deleteDb("fullText");
        Connection conn = getConnection("fullText");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS IF NOT EXISTS FT_INIT FOR \"org.h2.fulltext.FullText.init\"");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, DATA CLOB)");
        FullText.createIndex(conn, "PUBLIC", "TEST", null);
        conn.setAutoCommit(false);
        stat.execute("insert into test values(1, 'Hello Moon!')");
        conn.rollback();
        conn.setAutoCommit(true);
        stat.execute("insert into test values(0, 'Hello World!')");
        PreparedStatement prep = conn.prepareStatement("insert into test values(1, ?)");
        final int length = 1024 * 1024;
        prep.setCharacterStream(1, new Reader() {
            int remaining = length;
            public void close() {
                // ignore
            }
            public int read(char[] buff, int off, int len) {
                if (remaining >= len) {
                    remaining -= len;
                    return len;
                }
                remaining = -1;
                return -1;
            }
        }, length);
        prep.execute();
        ResultSet rs = stat.executeQuery("SELECT * FROM FT_SEARCH('World', 0, 0)");
        assertTrue(rs.next());
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('Moon', 0, 0)");
        assertFalse(rs.next());
        FullText.dropAll(conn);
        conn.close();
        deleteDb("fullText");
    }

    private void testCreateDrop() throws SQLException {
        deleteDb("fullText");
        FileSystem.getInstance(baseDir).deleteRecursive(baseDir + "/fullText", false);
        Connection conn = getConnection("fullText");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS IF NOT EXISTS FT_INIT FOR \"org.h2.fulltext.FullText.init\"");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        for (int i = 0; i < 10; i++) {
            FullText.createIndex(conn, "PUBLIC", "TEST", null);
            FullText.dropIndex(conn, "PUBLIC", "TEST");
        }
        conn.close();
        deleteDb("fullText");
        FileSystem.getInstance(baseDir).deleteRecursive(baseDir + "/fullText", false);
    }

    private void testReopen(boolean lucene) throws SQLException {
        String prefix = lucene ? "FTL" : "FT";
        deleteDb("fullTextReopen");
        FileSystem.getInstance(baseDir).deleteRecursive(baseDir + "/fullTextReopen", false);
        Connection conn = getConnection("fullTextReopen");
        Statement stat = conn.createStatement();
        String className = lucene ? "FullTextLucene" : "FullText";
        stat.execute("CREATE ALIAS IF NOT EXISTS " + prefix + "_INIT FOR \"org.h2.fulltext." + className + ".init\"");
        stat.execute("CALL " + prefix + "_INIT()");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello World')");
        stat.execute("CALL " + prefix + "_CREATE_INDEX('PUBLIC', 'TEST', NULL)");
        stat.execute("UPDATE TEST SET NAME=NULL WHERE ID=1");
        stat.execute("UPDATE TEST SET NAME='Hello World' WHERE ID=1");
        conn.close();

        conn = getConnection("fullTextReopen");
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM " + prefix + "_SEARCH('Hello', 0, 0)");
        assertTrue(rs.next());
        stat.executeQuery("SELECT * FROM " + prefix + "_SEARCH(NULL, 0, 0)");
        stat.execute("INSERT INTO TEST VALUES(2, NULL)");
        conn.close();

        FullText.closeAll();
        conn = getConnection("fullTextReopen");
        stat = conn.createStatement();
        stat.execute("INSERT INTO TEST VALUES(3, 'Hello')");
        conn.close();
    }

    private void testPerformance(boolean lucene) throws SQLException {
        deleteDb("fullText");
        FileSystem.getInstance(baseDir).deleteRecursive(baseDir + "/fullText", false);
        Connection conn = getConnection("fullText");
        String prefix = lucene ? "FTL" : "FT";
        Statement stat = conn.createStatement();
        String className = lucene ? "FullTextLucene" : "FullText";
        stat.execute("CREATE ALIAS IF NOT EXISTS " + prefix + "_INIT FOR \"org.h2.fulltext." + className + ".init\"");
        stat.execute("CALL " + prefix + "_INIT()");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST AS SELECT * FROM INFORMATION_SCHEMA.HELP");
        stat.execute("ALTER TABLE TEST ALTER COLUMN ID INT NOT NULL");
        stat.execute("CREATE PRIMARY KEY ON TEST(ID)");
        long time = System.currentTimeMillis();
        stat.execute("CALL " + prefix + "_CREATE_INDEX('PUBLIC', 'TEST', NULL)");
        println("create " + prefix + ": " + (System.currentTimeMillis() - time));
        PreparedStatement prep = conn.prepareStatement("SELECT * FROM " + prefix + "_SEARCH(?, 0, 0)");
        time = System.currentTimeMillis();
        ResultSet rs = stat.executeQuery("SELECT TEXT FROM TEST");
        int count = 0;
        while (rs.next()) {
            String text = rs.getString(1);
            StringTokenizer tokenizer = new StringTokenizer(text, " ()[].,;:-+*/!?=<>{}#@'\"~$_%&|");
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                if (word.length() < 10) {
                    continue;
                }
                prep.setString(1, word);
                ResultSet rs2 = prep.executeQuery();
                while (rs2.next()) {
                    rs2.getString(1);
                    count++;
                }
            }
        }
        println("search " + prefix + ": " + (System.currentTimeMillis() - time) + " count: " + count);
        stat.execute("CALL " + prefix + "_DROP_ALL()");
        conn.close();
    }

    private void test(boolean lucene, String dataType) throws SQLException {
        deleteDb("fullText");
        Connection conn = getConnection("fullText");
        String prefix = lucene ? "FTL_" : "FT_";
        Statement stat = conn.createStatement();
        String className = lucene ? "FullTextLucene" : "FullText";
        stat.execute("CREATE ALIAS IF NOT EXISTS " + prefix + "INIT FOR \"org.h2.fulltext." + className + ".init\"");
        stat.execute("CALL " + prefix + "INIT()");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME " + dataType + ")");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello World')");
        stat.execute("CALL " + prefix + "CREATE_INDEX('PUBLIC', 'TEST', NULL)");
        ResultSet rs;
        rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('Hello', 0, 0)");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=1", rs.getString(1));
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('Hallo', 0, 0)");
        assertFalse(rs.next());
        stat.execute("INSERT INTO TEST VALUES(2, 'Hallo Welt')");
        rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('Hello', 0, 0)");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=1", rs.getString(1));
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('Hallo', 0, 0)");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=2", rs.getString(1));
        assertFalse(rs.next());

        stat.execute("CALL " + prefix + "REINDEX()");
        rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('Hello', 0, 0)");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=1", rs.getString(1));
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('Hallo', 0, 0)");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=2", rs.getString(1));
        assertFalse(rs.next());

        stat.execute("INSERT INTO TEST VALUES(3, 'Hello World')");
        stat.execute("INSERT INTO TEST VALUES(4, 'Hello World')");
        stat.execute("INSERT INTO TEST VALUES(5, 'Hello World')");

        rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('World', 0, 0) ORDER BY QUERY");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=1", rs.getString(1));
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=3", rs.getString(1));
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=4", rs.getString(1));
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=5", rs.getString(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('World', 1, 0)");
        rs.next();
        assertTrue(rs.getString(1).startsWith("\"PUBLIC\".\"TEST\" WHERE \"ID\"="));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('World', 0, 2) ORDER BY QUERY");
        rs.next();
        assertTrue(rs.getString(1).startsWith("\"PUBLIC\".\"TEST\" WHERE \"ID\"="));
        rs.next();
        assertTrue(rs.getString(1).startsWith("\"PUBLIC\".\"TEST\" WHERE \"ID\"="));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('World', 2, 1) ORDER BY QUERY");
        rs.next();
        assertTrue(rs.getString(1).startsWith("\"PUBLIC\".\"TEST\" WHERE \"ID\"="));
        rs.next();
        assertTrue(rs.getString(1).startsWith("\"PUBLIC\".\"TEST\" WHERE \"ID\"="));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('1', 0, 0)");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=1", rs.getString(1));
        assertFalse(rs.next());

        if (lucene) {
            rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('NAME:Hallo', 0, 0)");
            rs.next();
            assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=2", rs.getString(1));
            assertFalse(rs.next());
        }

        conn.close();

        conn = getConnection("fullText");
        stat = conn.createStatement();
        rs = stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('World', 0, 0)");

        stat.execute("CALL " + prefix + "DROP_ALL()");
        stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('World', 2, 1)");
        stat.execute("CALL " + prefix + "DROP_ALL()");

        conn.close();

    }
}
