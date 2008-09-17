/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeSet;

import org.h2.constant.SysProperties;
import org.h2.test.TestBase;

/**
 * Test various optimizations (query cache, optimization for MIN(..), and
 * MAX(..)).
 */
public class TestOptimizations extends TestBase {

    public void test() throws Exception {
        testMinMaxNullOptimization();
        if (config.networked) {
            return;
        }
        testOptimizeInJoin();
        testMultiColumnRangeQuery();
        testDistinctOptimization();
        testQueryCacheTimestamp();
        testQueryCacheSpeed();
        testQueryCache(true);
        testQueryCache(false);
        testIn();
        testMinMaxCountOptimization(true);
        testMinMaxCountOptimization(false);
    }
    
    private void testOptimizeInJoin() throws Exception {
        boolean old = SysProperties.optimizeInJoin;
        SysProperties.optimizeInJoin = true;

        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();

        stat.execute("create table test(id int primary key)");
        stat.execute("insert into test select x from system_range(1, 1000)");
        ResultSet rs = stat.executeQuery("explain select * from test where id in (400, 300)");
        rs.next();
        String plan = rs.getString(1);
        if (plan.indexOf("/* PUBLIC.PRIMARY_KEY_") < 0) {
            fail("Expected using the primary key, got: " + plan);
        }
        conn.close();
        
        SysProperties.optimizeInJoin = old;
    }
    
    private void testMinMaxNullOptimization() throws Exception {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        Random random = new Random(1);
        int len = getSize(50, 500);
        for (int i = 0; i < len; i++) {
            stat.execute("drop table if exists test");
            stat.execute("create table test(x int)");
            if (random.nextBoolean()) {
                int count = random.nextBoolean() ? 1 : 1 + random.nextInt(len);
                if (count > 0) {
                    stat.execute("insert into test select null from system_range(1, " + count + ")");
                }
            }
            int maxExpected = -1;
            int minExpected = -1;
            if (random.nextInt(10) != 1) {
                minExpected = 1;
                maxExpected = 1 + random.nextInt(len);
                stat.execute("insert into test select x from system_range(1, " + maxExpected + ")");
            }
            String sql = "create index idx on test(x";
            if (random.nextBoolean()) {
                sql += " desc";
            }
            if (random.nextBoolean()) {
                if (random.nextBoolean()) {
                    sql += " nulls first";
                } else {
                    sql += " nulls last";
                }
            }
            sql += ")";
            stat.execute(sql);
            ResultSet rs = stat.executeQuery("explain select min(x), max(x) from test");
            rs.next();
            if (!config.mvcc) {
                String plan = rs.getString(1);
                assertTrue(plan.indexOf("direct") > 0);
            }
            rs = stat.executeQuery("select min(x), max(x) from test");
            rs.next();
            int min = rs.getInt(1);
            if (rs.wasNull()) {
                min = -1;
            }
            int max = rs.getInt(2);
            if (rs.wasNull()) {
                max = -1;
            }
            assertEquals(minExpected, min);
            assertEquals(maxExpected, max);
        }
        conn.close();
    }

    private void testMultiColumnRangeQuery() throws Exception {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Logs(id INT PRIMARY KEY, type INT)");
        stat.execute("CREATE unique INDEX type_index ON Logs(type, id)");
        stat.execute("INSERT INTO Logs SELECT X, MOD(X, 3) FROM SYSTEM_RANGE(1, 1000)");
        stat.execute("ANALYZE SAMPLE_SIZE 0");
        ResultSet rs;
        rs = stat.executeQuery("EXPLAIN SELECT id FROM Logs WHERE id < 100 and type=2 AND id<100");
        rs.next();
        String plan = rs.getString(1);
        assertTrue(plan.indexOf("TYPE_INDEX") > 0);
        conn.close();
    }

    private void testDistinctOptimization() throws Exception {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR, TYPE INT)");
        stat.execute("CREATE INDEX IDX_TEST_TYPE ON TEST(TYPE)");
        Random random = new Random(1);
        int len = getSize(10000, 100000);
        int[] groupCount = new int[10];
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.setString(2, "Hello World");
            int type = random.nextInt(10);
            groupCount[type]++;
            prep.setInt(3, type);
            prep.execute();
        }
        ResultSet rs;
        rs = stat.executeQuery("SELECT TYPE, COUNT(*) FROM TEST GROUP BY TYPE ORDER BY TYPE");
        for (int i = 0; rs.next(); i++) {
            assertEquals(i, rs.getInt(1));
            assertEquals(groupCount[i], rs.getInt(2));
        }
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT DISTINCT TYPE FROM TEST ORDER BY TYPE");
        for (int i = 0; rs.next(); i++) {
            assertEquals(i, rs.getInt(1));
        }
        assertFalse(rs.next());
        stat.execute("ANALYZE");
        rs = stat.executeQuery("SELECT DISTINCT TYPE FROM TEST ORDER BY TYPE");
        for (int i = 0; i < 10; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
        }
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT DISTINCT TYPE FROM TEST ORDER BY TYPE LIMIT 5 OFFSET 2");
        for (int i = 2; i < 7; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
        }
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT DISTINCT TYPE FROM TEST ORDER BY TYPE LIMIT 0 OFFSET 0 SAMPLE_SIZE 3");
        // must have at least one row
        assertTrue(rs.next());
        for (int i = 0; i < 3; i++) {
            rs.getInt(1);
            if (i > 0 && !rs.next()) {
                break;
            }
        }
        assertFalse(rs.next());
        conn.close();
    }

    private void testQueryCacheTimestamp() throws Exception {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        PreparedStatement prep = conn.prepareStatement("SELECT CURRENT_TIMESTAMP()");
        ResultSet rs = prep.executeQuery();
        rs.next();
        String a = rs.getString(1);
        Thread.sleep(50);
        rs = prep.executeQuery();
        rs.next();
        String b = rs.getString(1);
        assertFalse(a.equals(b));
        conn.close();
    }

    private void testQueryCacheSpeed() throws Exception {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        // if h2.optimizeInJoin is enabled, the following query can not be improved
        if (!SysProperties.optimizeInJoin) {
            testQuerySpeed(stat,
                    "select sum(x) from system_range(1, 10000) a where a.x in (select b.x from system_range(1, 30) b)");
        }
        testQuerySpeed(stat,
                "select sum(a.n), sum(b.x) from system_range(1, 100) b, (select sum(x) n from system_range(1, 4000)) a");
        conn.close();
    }

    private void testQuerySpeed(Statement stat, String sql) throws Exception {
        stat.execute("set OPTIMIZE_REUSE_RESULTS 0");
        stat.execute(sql);
        long time = System.currentTimeMillis();
        stat.execute(sql);
        time = System.currentTimeMillis() - time;
        stat.execute("set OPTIMIZE_REUSE_RESULTS 1");
        stat.execute(sql);
        long time2 = System.currentTimeMillis();
        stat.execute(sql);
        time2 = System.currentTimeMillis() - time2;
        if (time2 > time * 2) {
            fail("not optimized: " + time + " optimized: " + time2 + " sql:" + sql);
        }
    }

    private void testQueryCache(boolean optimize) throws Exception {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        if (optimize) {
            stat.execute("set OPTIMIZE_REUSE_RESULTS 1");
        } else {
            stat.execute("set OPTIMIZE_REUSE_RESULTS 0");
        }
        stat.execute("create table test(id int)");
        stat.execute("create table test2(id int)");
        stat.execute("insert into test values(1), (1), (2)");
        stat.execute("insert into test2 values(1)");
        PreparedStatement prep = conn.prepareStatement("select * from test where id = (select id from test2)");
        ResultSet rs1 = prep.executeQuery();
        rs1.next();
        assertEquals(rs1.getInt(1), 1);
        rs1.next();
        assertEquals(rs1.getInt(1), 1);
        assertFalse(rs1.next());

        stat.execute("update test2 set id = 2");
        ResultSet rs2 = prep.executeQuery();
        rs2.next();
        assertEquals(rs2.getInt(1), 2);

        conn.close();
    }

    private void testMinMaxCountOptimization(boolean memory) throws Exception {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create " + (memory ? "memory" : "") + " table test(id int primary key, value int)");
        stat.execute("create index idx_value_id on test(value, id);");
        int len = getSize(1000, 10000);
        HashMap map = new HashMap();
        TreeSet set = new TreeSet();
        Random random = new Random(1);
        for (int i = 0; i < len; i++) {
            if (i == len / 2) {
                if (!config.memory) {
                    conn.close();
                    conn = getConnection("optimizations");
                    stat = conn.createStatement();
                }
            }
            switch (random.nextInt(10)) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
                if (random.nextInt(1000) == 1) {
                    stat.execute("insert into test values(" + i + ", null)");
                    map.put(new Integer(i), null);
                } else {
                    int value = random.nextInt();
                    stat.execute("insert into test values(" + i + ", " + value + ")");
                    map.put(new Integer(i), new Integer(value));
                    set.add(new Integer(value));
                }
                break;
            case 6:
            case 7:
            case 8: {
                if (map.size() > 0) {
                    for (int j = random.nextInt(i), k = 0; k < 10; k++, j++) {
                        if (map.containsKey(new Integer(j))) {
                            Integer x = (Integer) map.remove(new Integer(j));
                            if (x != null) {
                                set.remove(x);
                            }
                            stat.execute("delete from test where id=" + j);
                        }
                    }
                }
                break;
            }
            case 9: {
                ArrayList list = new ArrayList(map.values());
                int count = list.size();
                Integer min = null, max = null;
                if (count > 0) {
                    min = (Integer) set.first();
                    max = (Integer) set.last();
                }
                ResultSet rs = stat.executeQuery("select min(value), max(value), count(*) from test");
                rs.next();
                Integer minDb = (Integer) rs.getObject(1);
                Integer maxDb = (Integer) rs.getObject(2);
                int countDb = rs.getInt(3);
                assertEquals(minDb, min);
                assertEquals(maxDb, max);
                assertEquals(countDb, count);
                break;
            }
            default:
            }
        }
        conn.close();
    }

    private void testIn() throws Exception {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello')");
        stat.execute("insert into test values(2, 'World')");
        PreparedStatement prep;
        ResultSet rs;

        prep = conn.prepareStatement("select * from test t1 where t1.id in(?)");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), "Hello");
        assertFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 where t1.id in(?, ?) order by id");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), "Hello");
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getString(2), "World");
        assertFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 where t1.id in(select t2.id from test t2 where t2.id=?)");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getString(2), "World");
        assertFalse(rs.next());

        prep = conn
                .prepareStatement("select * from test t1 where t1.id in(select t2.id from test t2 where t2.id=? and t1.id<>t2.id)");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        assertFalse(rs.next());

        prep = conn
                .prepareStatement("select * from test t1 where t1.id in(select t2.id from test t2 where t2.id in(cast(?+10 as varchar)))");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        assertFalse(rs.next());

        conn.close();
    }

}
