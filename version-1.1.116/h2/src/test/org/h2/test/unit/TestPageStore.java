/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.h2.test.TestBase;

/**
 * Test the page store.
 */
public class TestPageStore extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        System.setProperty("h2.pageStore", "true");
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testCreateIndexLater();
        testFuzzOperations();
    }

    private void testCreateIndexLater() throws SQLException {
        deleteDb("pageStore");
        Connection conn = getConnection("pageStore");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(NAME VARCHAR) AS SELECT 1");
        stat.execute("CREATE INDEX IDX_N ON TEST(NAME)");
        stat.execute("INSERT INTO TEST SELECT X FROM SYSTEM_RANGE(20, 100)");
        stat.execute("INSERT INTO TEST SELECT X FROM SYSTEM_RANGE(1000, 1100)");
        stat.execute("SHUTDOWN IMMEDIATELY");
        try {
            conn.close();
        } catch (SQLException e) {
            // ignore
        }
        conn = getConnection("pageStore");
        conn.close();
    }

    private void testFuzzOperations() throws SQLException {
        int best = Integer.MAX_VALUE;
        for (int i = 0; i < 10; i++) {
            int x = testFuzzOperationsSeed(i, 10);
            if (x >= 0 && x < best) {
                best = x;
                fail("op:" + x + " seed:" + i);
            }
        }
    }

    private int testFuzzOperationsSeed(int seed, int len) throws SQLException {
        deleteDb("pageStore");
        Connection conn = getConnection("pageStore");
        Statement stat = conn.createStatement();
        log("DROP TABLE IF EXISTS TEST;");
        stat.execute("DROP TABLE IF EXISTS TEST");
        log("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR DEFAULT 'Hello World');");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR DEFAULT 'Hello World')");
        Set<Integer> rows = new TreeSet<Integer>();
        Random random = new Random(seed);
        for (int i = 0; i < len; i++) {
            int op = random.nextInt(3);
            Integer x = new Integer(random.nextInt(100));
            switch(op) {
            case 0:
                if (!rows.contains(x)) {
                    log("insert into test(id) values(" + x + ");");
                    stat.execute("INSERT INTO TEST(ID) VALUES("+ x + ");");
                    rows.add(x);
                }
                break;
            case 1:
                if (rows.contains(x)) {
                    log("delete from test where id=" + x + ";");
                    stat.execute("DELETE FROM TEST WHERE ID=" + x);
                    rows.remove(x);
                }
                break;
            case 2:
                conn.close();
                conn = getConnection("pageStore");
                stat = conn.createStatement();
                ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
                log("--reconnect");
                for (int test : rows) {
                    if (!rs.next()) {
                        log("error: expected next");
                        conn.close();
                        return i;
                    }
                    int y = rs.getInt(1);
                    // System.out.println(" " + x);
                    if (y != test) {
                        log("error: " + y + " <> " + test);
                        conn.close();
                        return i;
                    }
                }
                if (rs.next()) {
                    log("error: unexpected next");
                    conn.close();
                    return i;
                }
            }
        }
        conn.close();
        return -1;
    }

    private void log(String m) {
        trace("   " + m);
    }

}
