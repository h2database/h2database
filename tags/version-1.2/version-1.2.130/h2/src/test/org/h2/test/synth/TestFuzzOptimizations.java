/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.h2.test.TestBase;
import org.h2.test.db.Db;
import org.h2.test.db.Db.Prepared;

/**
 * This test executes random SQL statements to test if optimizations are working
 * correctly.
 */
public class TestFuzzOptimizations extends TestBase {

    private Connection conn;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        deleteDb("optimizations");
        conn = getConnection("optimizations");
        testGroupSorted();
        testInSelect();
        conn.close();
        deleteDb("optimizations");
    }

    private void testInSelect() {
        Db db = new Db(conn);
        db.execute("CREATE TABLE TEST(A INT, B INT)");
        db.execute("CREATE INDEX IDX ON TEST(A)");
        db.execute("INSERT INTO TEST SELECT X/4, MOD(X, 4) FROM SYSTEM_RANGE(1, 16)");
        db.execute("UPDATE TEST SET A = NULL WHERE A = 0");
        db.execute("UPDATE TEST SET B = NULL WHERE B = 0");
        Random random = new Random();
        long seed = random.nextLong();
        println("seed: " + seed);
        for (int i = 0; i < 100; i++) {
            String column = random.nextBoolean() ? "A" : "B";
            String value = new String[] { "NULL", "0", "A", "B" }[random.nextInt(4)];
            String compare = random.nextBoolean() ? "A" : "B";
            int x = random.nextInt(3);
            String sql1 = "SELECT * FROM TEST T WHERE " + column + "+0 " +
                "IN(SELECT " + value + " FROM TEST I WHERE I." + compare + "=?) ORDER BY 1, 2";
            String sql2 = "SELECT * FROM TEST T WHERE " + column + " " +
                "IN(SELECT " + value + " FROM TEST I WHERE I." + compare + "=?) ORDER BY 1, 2";
            List<Map<String, Object>> a = db.prepare(sql1).set(x).query();
            List<Map<String, Object>> b = db.prepare(sql2).set(x).query();
            assertTrue(a.equals(b));
        }
        db.execute("DROP TABLE TEST");
    }

    private void testGroupSorted() {
        Db db = new Db(conn);
        db.execute("CREATE TABLE TEST(A INT, B INT, C INT)");
        Random random = new Random();
        long seed = random.nextLong();
        println("seed: " + seed);
        for (int i = 0; i < 100; i++) {
            Prepared p = db.prepare("INSERT INTO TEST VALUES(?, ?, ?)");
            p.set(new String[] { null, "0", "1", "2" }[random.nextInt(4)]);
            p.set(new String[] { null, "0", "1", "2" }[random.nextInt(4)]);
            p.set(new String[] { null, "0", "1", "2" }[random.nextInt(4)]);
            p.execute();
        }
        int len = getSize(1000, 3000);
        for (int i = 0; i < len / 10; i++) {
            db.execute("CREATE TABLE TEST_INDEXED AS SELECT * FROM TEST");
            int jLen = 1 + random.nextInt(2);
            for (int j = 0; j < jLen; j++) {
                String x = "CREATE INDEX IDX" + j + " ON TEST_INDEXED(";
                int kLen = 1 + random.nextInt(2);
                for (int k = 0; k < kLen; k++) {
                    if (k > 0) {
                        x += ",";
                    }
                    x += new String[] { "A", "B", "C" }[random.nextInt(3)];
                }
                db.execute(x + ")");
            }
            for (int j = 0; j < 10; j++) {
                String x = "SELECT ";
                for (int k = 0; k < 3; k++) {
                    if (k > 0) {
                        x += ",";
                    }
                    x += new String[] { "SUM(A)", "MAX(B)", "AVG(C)", "COUNT(B)" }[random.nextInt(4)];
                    x += " S" + k;
                }
                x += " FROM ";
                String group = " GROUP BY ";
                int kLen = 1 + random.nextInt(2);
                for (int k = 0; k < kLen; k++) {
                    if (k > 0) {
                        group += ",";
                    }
                    group += new String[] { "A", "B", "C" }[random.nextInt(3)];
                }
                group += " ORDER BY 1, 2, 3";
                List<Map<String, Object>> a = db.query(x + "TEST" + group);
                List<Map<String, Object>> b = db.query(x + "TEST_INDEXED" + group);
                assertEquals(a.toString(), b.toString());
                assertTrue(a.equals(b));
            }
            db.execute("DROP TABLE TEST_INDEXED");
        }
        db.execute("DROP TABLE TEST");
    }

}
