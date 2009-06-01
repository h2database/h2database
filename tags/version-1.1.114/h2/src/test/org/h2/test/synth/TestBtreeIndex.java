/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;

/**
 * A b-tree index test.
 */
public class TestBtreeIndex extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        Random random = new Random();
        for (int i = 0; i < getSize(1, 4); i++) {
            testAddDelete();
            int seed = random.nextInt();
            testCase(seed);
        }
    }

    private void testAddDelete() throws SQLException {
        deleteDb("index");
        Connection conn = getConnection("index");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID bigint primary key)");
        int count = 1000;
        stat.execute("insert into test select x from system_range(1, " + count + ")");
        if (!config.memory) {
            conn.close();
            conn = getConnection("index");
            stat = conn.createStatement();
        }
        for (int i = 1; i < count; i++) {
            ResultSet rs = stat.executeQuery("select * from test order by id");
            for (int j = i; rs.next(); j++) {
                assertEquals(j, rs.getInt(1));
            }
            stat.execute("delete from test where id =" + i);
        }
        stat.execute("drop all objects delete files");
        conn.close();
    }

    public void testCase(int seed) throws SQLException {
        String old = baseDir;
        baseDir = TestBase.getTestDir("index");
        testOne(seed);
        baseDir = old;
    }

    private void testOne(int seed) throws SQLException {
        org.h2.Driver.load();
        printTime("testIndex " + seed);
        Random random = new Random(seed);
        int distinct, prefixLength;
        if (random.nextBoolean()) {
            distinct = random.nextInt(8000) + 1;
            prefixLength = random.nextInt(8000) + 1;
        } else if (random.nextBoolean()) {
            distinct = random.nextInt(16000) + 1;
            prefixLength = random.nextInt(100) + 1;
        } else {
            distinct = random.nextInt(10) + 1;
            prefixLength = random.nextInt(10) + 1;
        }
        boolean delete = random.nextBoolean();
        StringBuilder buff = new StringBuilder();
        for (int j = 0; j < prefixLength; j++) {
            buff.append("x");
        }
        String prefix = buff.toString();
        DeleteDbFiles.execute(baseDir, null, true);
        Connection conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/index", "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE a(text VARCHAR PRIMARY KEY)");
        PreparedStatement prepInsert = conn.prepareStatement("INSERT INTO a VALUES(?)");
        PreparedStatement prepDelete = conn.prepareStatement("DELETE FROM a WHERE text=?");
        PreparedStatement prepDeleteAllButOne = conn.prepareStatement("DELETE FROM a WHERE text <> ?");
        int count = 0;
        for (int i = 0; i < 1000; i++) {
            int y = random.nextInt(distinct);
            try {
                prepInsert.setString(1, prefix + y);
                prepInsert.executeUpdate();
                count++;
            } catch (SQLException e) {
                if (e.getSQLState().equals("23001")) {
                    // ignore
                } else {
                    TestBase.logError("error", e);
                    break;
                }
            }
            if (delete && random.nextInt(10) == 1) {
                if (random.nextInt(4) == 1) {
                    try {
                        prepDeleteAllButOne.setString(1, prefix + y);
                        int deleted = prepDeleteAllButOne.executeUpdate();
                        if (deleted < count - 1) {
                            System.out.println("ERROR deleted:" + deleted);
                            System.out.println("new TestBtreeIndex().");
                        }
                        count -= deleted;
                    } catch (SQLException e) {
                        TestBase.logError("error", e);
                        break;
                    }
                } else {
                    try {
                        prepDelete.setString(1, prefix + y);
                        int deleted = prepDelete.executeUpdate();
                        if (deleted > 1) {
                            System.out.println("ERROR deleted:" + deleted);
                            System.out.println("seed: " + seed);
                        }
                        count -= deleted;
                    } catch (SQLException e) {
                        TestBase.logError("error", e);
                        break;
                    }
                }
            }
        }
        ResultSet rs = conn.createStatement().executeQuery("SELECT text FROM a ORDER BY text");
        int testCount = 0;
        while (rs.next()) {
            testCount++;
        }
        if (testCount != count) {
            System.out.println("ERROR count:" + count + " testCount:" + testCount);
            System.out.println("seed: " + seed);
        }
        rs = conn.createStatement().executeQuery("SELECT text, count(*) FROM a GROUP BY text HAVING COUNT(*)>1");
        if (rs.next()) {
            System.out.println("ERROR");
            System.out.println("seed: " + seed);
        }
        conn.close();
    }

}
