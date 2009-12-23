/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import org.h2.jdbc.JdbcConnection;
import org.h2.test.TestBase;
import org.h2.util.SortedProperties;

/**
 * Test the serialized (server-less) mode.
 */
public class TestFileLockSerialized extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        println("testLeftLogFiles");
        testLeftLogFiles();
        println("testWrongDatabaseInstanceOnReconnect");
        testWrongDatabaseInstanceOnReconnect();
        println("testCache()");
        testCache();
        println("testBigDatabase(false)");
        testBigDatabase(false);
        println("testBigDatabase(true)");
        testBigDatabase(true);
        println("testCheckpointInUpdateRaceCondition");
        testCheckpointInUpdateRaceCondition();
        println("testConcurrentUpdates");
        testConcurrentUpdates();
        println("testThreeMostlyReaders true");
        testThreeMostlyReaders(true);
        println("testThreeMostlyReaders false");
        testThreeMostlyReaders(false);
        println("testTwoReaders");
        testTwoReaders();
        println("testTwoWriters");
        testTwoWriters();
        println("testPendingWrite");
        testPendingWrite();
        println("testKillWriter");
        testKillWriter();
        println("testConcurrentReadWrite");
        testConcurrentReadWrite();
    }

    private void testThreeMostlyReaders(final boolean write) throws Exception {
        boolean longRun = false;
        deleteDb("fileLockSerialized");
        final String url = "jdbc:h2:" + baseDir + "/fileLockSerialized;FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE";

        Connection c = DriverManager.getConnection(url);
        c.createStatement().execute("create table test(id int) as select 1");
        c.close();

        final int len = 10;
        final Exception[] ex = new Exception[1];
        final boolean[] stop = new boolean[1];
        Thread[] threads = new Thread[len];
        for (int i = 0; i < len; i++) {
            Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        Connection c = DriverManager.getConnection(url);
                        PreparedStatement p = c.prepareStatement("select * from test where id = ?");
                        while (!stop[0]) {
                            Thread.sleep(100);
                            if (write) {
                                if (Math.random() > 0.9) {
                                    c.createStatement().execute("update test set id = id");
                                }
                            }
                            p.setInt(1, 1);
                            p.executeQuery();
                            p.clearParameters();
                        }
                        c.close();
                    } catch (Exception e) {
                        ex[0] = e;
                    }
                }
            });
            t.start();
            threads[i] = t;
        }
        if (longRun) {
            Thread.sleep(40000);
        } else {
            Thread.sleep(1000);
        }
        stop[0] = true;
        for (int i = 0; i < len; i++) {
            threads[i].join();
        }
        if (ex[0] != null) {
            throw ex[0];
        }
        DriverManager.getConnection(url).close();
    }

    private void testTwoReaders() throws Exception {
        deleteDb("fileLockSerialized");
        String url = "jdbc:h2:" + baseDir + "/fileLockSerialized;FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE";
        Connection conn1 = DriverManager.getConnection(url);
        conn1.createStatement().execute("create table test(id int)");
        Connection conn2 = DriverManager.getConnection(url);
        Statement stat2 = conn2.createStatement();
        stat2.execute("drop table test");
        stat2.execute("create table test(id identity) as select 1");
        conn2.close();
        conn1.close();
        DriverManager.getConnection(url).close();
    }

    private void testTwoWriters() throws Exception {
        deleteDb("fileLockSerialized");
        String url = "jdbc:h2:" + baseDir + "/fileLockSerialized";
        final String writeUrl = url + ";FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE";
        final boolean[] stop = new boolean[1];
        Connection conn = DriverManager.getConnection(writeUrl, "sa", "sa");
        conn.createStatement().execute("create table test(id identity) as select x from system_range(1, 100)");
        conn.close();
        new Thread() {
            public void run() {
                while (!stop[0]) {
                    try {
                        Thread.sleep(10);
                        Connection conn = DriverManager.getConnection(writeUrl, "sa", "sa");
                        conn.createStatement().execute("select * from test");
                        conn.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        }.start();
        Thread.sleep(20);
        for (int i = 0; i < 2; i++) {
            conn = DriverManager.getConnection(writeUrl, "sa", "sa");
            Statement stat = conn.createStatement();
            stat.execute("drop table test");
            stat.execute("create table test(id identity) as select x from system_range(1, 100)");
            conn.createStatement().execute("select * from test");
            conn.close();
        }
        stop[0] = true;
        Thread.sleep(100);
        conn = DriverManager.getConnection(writeUrl, "sa", "sa");
        conn.createStatement().execute("select * from test");
        conn.close();
    }

    private void testPendingWrite() throws Exception {
        deleteDb("fileLockSerialized");
        String url = "jdbc:h2:" + baseDir + "/fileLockSerialized";
        String writeUrl = url + ";FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE;WRITE_DELAY=0";

        Connection conn = DriverManager.getConnection(writeUrl, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");
        Thread.sleep(100);
        String propFile = baseDir + "/fileLockSerialized.lock.db";
        SortedProperties p = SortedProperties.loadProperties(propFile);
        p.setProperty("changePending", "true");
        p.setProperty("modificationDataId", "1000");
        OutputStream out = new FileOutputStream(propFile, false);
        try {
            p.store(out, "test");
        } finally {
            out.close();
        }
        Thread.sleep(100);
        stat.execute("select * from test");
        conn.close();
    }

    private void testKillWriter() throws Exception {
        deleteDb("fileLockSerialized");
        String url = "jdbc:h2:" + baseDir + "/fileLockSerialized";
        String writeUrl = url + ";FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE;WRITE_DELAY=0";

        Connection conn = DriverManager.getConnection(writeUrl, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");
        ((JdbcConnection) conn).setPowerOffCount(1);
        try {
            stat.execute("insert into test values(1)");
            fail();
        } catch (SQLException e) {
            // ignore
        }

        Connection conn2 = DriverManager.getConnection(writeUrl, "sa", "sa");
        Statement stat2 = conn2.createStatement();
        stat2.execute("insert into test values(1)");
        printResult(stat2, "select * from test");

        conn2.close();

        try {
            conn.close();
        } catch (SQLException e) {
            // ignore
        }
    }

    private void testConcurrentReadWrite() throws Exception {
        deleteDb("fileLockSerialized");

        String url = "jdbc:h2:" + baseDir + "/fileLockSerialized";
        String writeUrl = url + ";FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE";
        // ;TRACE_LEVEL_SYSTEM_OUT=3
        // String readUrl = writeUrl + ";ACCESS_MODE_LOG=R;ACCESS_MODE_DATA=R";

        trace(" create database");
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(writeUrl, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");

        Connection conn3 = DriverManager.getConnection(writeUrl, "sa", "sa");
        PreparedStatement prep3 = conn3.prepareStatement("insert into test values(?)");

        Connection conn2 = DriverManager.getConnection(writeUrl, "sa", "sa");
        Statement stat2 = conn2.createStatement();
        printResult(stat2, "select * from test");

        stat2.execute("create local temporary table temp(name varchar) not persistent");
        printResult(stat2, "select * from temp");

        trace(" insert row 1");
        stat.execute("insert into test values(1)");
        trace(" insert row 2");
        prep3.setInt(1, 2);
        prep3.execute();
        printResult(stat2, "select * from test");
        printResult(stat2, "select * from temp");

        conn.close();
        conn2.close();
        conn3.close();
    }

    private void printResult(Statement stat, String sql) throws SQLException {
        trace("  query: " + sql);
        ResultSet rs = stat.executeQuery(sql);
        int rowCount = 0;
        while (rs.next()) {
            trace("   " + rs.getString(1));
            rowCount++;
        }
        trace("   " + rowCount + " row(s)");
    }

    private void testConcurrentUpdates() throws Exception {
        boolean longRun = false;
        if (longRun) {
            for (int waitTime = 100; waitTime < 10000; waitTime += 20) {
                for (int howManyThreads = 1; howManyThreads < 10; howManyThreads++) {
                    testConcurrentUpdates(waitTime, howManyThreads, waitTime * howManyThreads * 10);
                }
            }
        } else {
            testConcurrentUpdates(100, 4, 2000);
        }
    }


    private void testConcurrentUpdates(final int waitTime, int howManyThreads, int runTime) throws Exception {
        println("testConcurrentUpdates waitTime: " + waitTime + " howManyThreads: " + howManyThreads + " runTime: " + runTime);
        deleteDb("fileLockSerialized");
        final String url = "jdbc:h2:" + baseDir + "/fileLockSerialized;FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE;" +
                "AUTO_RECONNECT=TRUE;MAX_LENGTH_INPLACE_LOB=8192;COMPRESS_LOB=DEFLATE;LOG=2;CACHE_SIZE=65536";

        Connection c = DriverManager.getConnection(url);
        c.createStatement().execute("create table test(id int)");
        c.createStatement().execute("insert into test values(1)");
        c.close();

        final long endTime = System.currentTimeMillis() + runTime;
        final Exception[] ex = new Exception[1];
        final Connection[] connList = new Connection[howManyThreads];
        final boolean[] stop = new boolean[1];
        final int[] lastInt = { 1 };
        Thread[] threads = new Thread[howManyThreads];
        for (int i = 0; i < howManyThreads; i++) {
            final int finalNrOfConnection = i;
            Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        Connection c = DriverManager.getConnection(url);
                        connList[finalNrOfConnection] = c;
                        while (!stop[0]) {
                            ResultSet rs = c.createStatement().executeQuery("select * from test");
                            rs.next();
                            if (rs.getInt(1) != lastInt[0]) {
                                throw new Exception(finalNrOfConnection + "  Expected: " + lastInt[0] + " got " + rs.getInt(1));
                            }
                            Thread.sleep(waitTime);
                            if (Math.random() > 0.7) {
                                int newLastInt = (int) (Math.random() * 1000);
                                c.createStatement().execute("update test set id = " + newLastInt);
                                lastInt[0] = newLastInt;
                            }
                        }
                        c.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        ex[0] = e;
                    }
                }
            });
            t.start();
            threads[i] = t;
        }
        while ((ex[0] == null) && (System.currentTimeMillis() < endTime)) {
            Thread.sleep(10);
        }

        stop[0] = true;
        for (int i = 0; i < howManyThreads; i++) {
            threads[i].join();
        }
        if (ex[0] != null) {
            throw ex[0];
        }
        DriverManager.getConnection(url).close();
        deleteDb("fileLockSerialized");
    }

    /**
     * If a checkpoint occurs between beforeWriting and checkWritingAllowed
     * then the result of checkWritingAllowed is READ_ONLY, which is wrong.
     *
     * Also, if a checkpoint started before beforeWriting, and ends between
     * between beforeWriting and checkWritingAllowed, then the same error occurs.
     *
     * @throws Exception
     */
    private void testCheckpointInUpdateRaceCondition() throws Exception {
        boolean longRun = false;
        deleteDb("fileLockSerialized");
        String url = "jdbc:h2:" + baseDir + "/fileLockSerialized;FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE";

        Connection conn = DriverManager.getConnection(url);
        conn.createStatement().execute("create table test(id int)");
        conn.createStatement().execute("insert into test values(1)");
        for (int i = 0; i < (longRun ? 10000 : 5); i++) {
            Thread.sleep(402);
            conn.createStatement().execute("update test set id = " + i);
        }
        conn.close();
        deleteDb("fileLockSerialized");
    }

    /**
     * Caches must be cleared. Session.reconnect only closes the DiskFile (which is
     * associated with the cache) if there is one session
     */
    private void testCache() throws Exception {
        deleteDb("fileLockSerialized");

        String urlShared = "jdbc:h2:" + baseDir + "/fileLockSerialized;FILE_LOCK=SERIALIZED";

        Connection connShared1 = DriverManager.getConnection(urlShared);
        Statement statement1   = connShared1.createStatement();
        Connection connShared2 = DriverManager.getConnection(urlShared);
        Statement statement2   = connShared2.createStatement();

        statement1.execute("create table test1(id int)");
        statement1.execute("insert into test1 values(1)");

        ResultSet rs = statement1.executeQuery("select id from test1");
        rs.close();
        rs = statement2.executeQuery("select id from test1");
        rs.close();

        statement1.execute("update test1 set id=2");
        Thread.sleep(500);

        rs = statement2.executeQuery("select id from test1");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        rs.close();

        connShared1.close();
        connShared2.close();
        deleteDb("fileLockSerialized");
    }

    private void testWrongDatabaseInstanceOnReconnect() throws Exception {
        deleteDb("fileLockSerialized");

        String urlShared = "jdbc:h2:" + baseDir + "/fileLockSerialized;FILE_LOCK=SERIALIZED";
        String urlForNew = urlShared + ";OPEN_NEW=TRUE";

        Connection connShared1 = DriverManager.getConnection(urlShared);
        Statement statement1   = connShared1.createStatement();
        Connection connShared2 = DriverManager.getConnection(urlShared);
        Connection connNew     = DriverManager.getConnection(urlForNew);
        statement1.execute("create table test1(id int)");
        connShared1.close();
        connShared2.close();
        connNew.close();
        deleteDb("fileLockSerialized");
    }

    private void testBigDatabase(boolean withCache) throws Exception {
        boolean longRun = false;
        final int howMuchRows = longRun ? 2000000 : 500000;
        deleteDb("fileLockSerialized");
        int cacheSizeKb = withCache ? 5000 : 0;

        final String url = "jdbc:h2:" + baseDir + "/fileLockSerialized;FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE;CACHE_SIZE=" + cacheSizeKb;
        final boolean[] importFinished = { false };
        final Exception[] ex = new Exception[1];
        final Thread importUpdate = new Thread() {
            public void run() {
                try {
                    Connection conn = DriverManager.getConnection(url);
                    Statement stat = conn.createStatement();
                    stat.execute("create table test(id int, id2 int)");
                    for (int i = 0; i < howMuchRows; i++) {
                        stat.execute("insert into test values(" + i + ", " + i + ")");
                    }
                    importFinished[0] = true;
                    Thread.sleep(5000);
                    stat.execute("update test set id2=999 where id=500");
                    conn.close();
                } catch (Exception e) {
                    ex[0] = e;
                } finally {
                    importFinished[0] = true;
                }
            }
        };
        importUpdate.start();

        Thread select = new Thread() {
            public void run() {
                try {
                    Connection conn = DriverManager.getConnection(url);
                    Statement stat = conn.createStatement();
                    while (!importFinished[0]) {
                        Thread.sleep(100);
                    }
                    Thread.sleep(1000);
                    ResultSet rs = stat.executeQuery("select id2 from test where id=500");
                    assertTrue(rs.next());
                    assertEquals(500, rs.getInt(1));
                    rs.close();
                    importUpdate.join();
                    Thread.sleep(1000);
                    rs = stat.executeQuery("select id2 from test where id=500");
                    assertTrue(rs.next());
                    assertEquals(999, rs.getInt(1));
                    rs.close();
                    conn.close();
                } catch (Exception e) {
                    ex[0] = e;
                }
            }
        };
        select.start();
        importUpdate.join();
        select.join();
        if (ex[0] != null) {
            throw ex[0];
        }
        deleteDb("fileLockSerialized");
    }

    private void testLeftLogFiles() throws Exception {
        deleteDb("fileLockSerialized");

        // without serialized
        String url;
        if (config.pageStore) {
            url = "jdbc:h2:" + baseDir + "/fileLockSerialized;PAGE_STORE=TRUE";
        } else {
            url = "jdbc:h2:" + baseDir + "/fileLockSerialized;PAGE_STORE=FALSE";
        }
        Connection conn = DriverManager.getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("insert into test values(0)");
        conn.close();
        List<String> filesWithoutSerialized = Arrays.asList(new File(baseDir).list());
        deleteDb("fileLockSerialized");

        // with serialized
        if (config.pageStore) {
            url = "jdbc:h2:" + baseDir + "/fileLockSerialized;FILE_LOCK=SERIALIZED;PAGE_STORE=TRUE";
        } else {
            url = "jdbc:h2:" + baseDir + "/fileLockSerialized;FILE_LOCK=SERIALIZED;PAGE_STORE=FALSE";
        }
        conn = DriverManager.getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id int)");
        Thread.sleep(500);
        stat.execute("insert into test values(0)");
        conn.close();

        List<String> filesWithSerialized = Arrays.asList(new File(baseDir).list());
        if (filesWithoutSerialized.size() !=  filesWithSerialized.size()) {
            for (int i = 0; i < filesWithoutSerialized.size(); i++) {
                if (!filesWithSerialized.contains(filesWithoutSerialized.get(i))) {
                    System.out.println("File left from 'without serialized' mode: " + filesWithoutSerialized.get(i));
                }
            }
            for (int i = 0; i < filesWithSerialized.size(); i++) {
                if (!filesWithoutSerialized.contains(filesWithSerialized.get(i))) {
                    System.out.println("File left from 'with serialized' mode: " + filesWithSerialized.get(i));
                }
            }
            fail("With serialized it must create the same files than without serialized");
        }
        deleteDb("fileLockSerialized");
    }

}
