/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeSet;
import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;
import org.h2.util.New;
import org.h2.util.StringUtils;
import org.h2.util.Task;

/**
 * Test various optimizations (query cache, optimization for MIN(..), and
 * MAX(..)).
 */
public class TestOptimizations extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        deleteDb("optimizations");
        testExplainRoundTrip();
        testOrderByExpression();
        testGroupSubquery();
        testAnalyzeLob();
        testLike();
        testExistsSubquery();
        testQueryCacheConcurrentUse();
        testQueryCacheResetParams();
        testRowId();
        testSortIndex();
        testAutoAnalyze();
        testInAndBetween();
        testNestedIn();
        testConstantIn1();
        testConstantIn2();
        testNestedInSelectAndLike();
        testNestedInSelect();
        testInSelectJoin();
        testMinMaxNullOptimization();
        if (config.networked) {
            return;
        }
        testOptimizeInJoinSelect();
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
        testOrderedIndexes();
        testConvertOrToIn();
        deleteDb("optimizations");
    }

    private void testExplainRoundTrip() throws Exception {
        Connection conn = getConnection("optimizations");
        assertExplainRoundTrip(conn, "select x from dual where x > any(select x from dual)");
        conn.close();
    }

    private void assertExplainRoundTrip(Connection conn, String sql) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("explain " + sql);
        rs.next();
        String plan = rs.getString(1).toLowerCase();
        plan = plan.replaceAll("\\s+", " ");
        plan = plan.replaceAll("/\\*[^\\*]*\\*/", "");
        plan = plan.replaceAll("\\s+", " ");
        plan = StringUtils.replaceAll(plan, "system_range(1, 1)", "dual");
        plan = plan.replaceAll("\\( ", "\\(");
        plan = plan.replaceAll(" \\)", "\\)");
        assertEquals(plan, sql);
    }

    private void testOrderByExpression() throws Exception {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello'), (2, 'Hello'), (3, 'Hello')");
        ResultSet rs;
        rs = stat.executeQuery(
                "explain select name from test where name='Hello' order by name");
        rs.next();
        String plan = rs.getString(1);
        assertContains(plan, "tableScan");
        stat.execute("drop table test");
        conn.close();
    }

    private void testGroupSubquery() throws Exception {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table t1(id int)");
        stat.execute("create table t2(id int)");
        stat.execute("insert into t1 values(2), (2), (3)");
        stat.execute("insert into t2 values(2), (3)");
        stat.execute("create index t1id_index on t1(id)");
        ResultSet rs;
        rs = stat.executeQuery("select id, (select count(*) from t2 " +
                "where t2.id = t1.id) cc from t1 group by id order by id");
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        rs.next();
        stat.execute("drop table t1, t2");
        conn.close();
    }

    private void testAnalyzeLob() throws Exception {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(v varchar, b binary, cl clob, bl blob) as " +
                "select ' ', '00', ' ', '00' from system_range(1, 100)");
        stat.execute("analyze");
        ResultSet rs = stat.executeQuery("select column_name, selectivity " +
                "from information_schema.columns where table_name='TEST'");
        rs.next();
        assertEquals("V", rs.getString(1));
        assertEquals(1, rs.getInt(2));
        rs.next();
        assertEquals("B", rs.getString(1));
        assertEquals(1, rs.getInt(2));
        rs.next();
        assertEquals("CL", rs.getString(1));
        assertEquals(100, rs.getInt(2));
        rs.next();
        assertEquals("BL", rs.getString(1));
        assertEquals(100, rs.getInt(2));
        stat.execute("drop table test");
        conn.close();
    }

    private void testLike() throws Exception {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(name varchar primary key) as select x from system_range(1, 10)");
        ResultSet rs = stat.executeQuery("explain select * from test where name like ? || '%' {1: 'Hello'}");
        rs.next();
        // ensure the ID = 10 part is evaluated first
        assertContains(rs.getString(1), "PRIMARY_KEY_");
        stat.execute("drop table test");
        conn.close();
    }

    private void testExistsSubquery() throws Exception {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int) as select x from system_range(1, 10)");
        ResultSet rs = stat.executeQuery("explain select * from test where exists(select 1 from test, test, test) and id = 10");
        rs.next();
        // ensure the ID = 10 part is evaluated first
        assertContains(rs.getString(1), "WHERE (ID = 10)");
        stat.execute("drop table test");
        conn.close();
    }

    private void testQueryCacheConcurrentUse() throws Exception {
        final Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, data clob)");
        stat.execute("insert into test values(0, space(10000))");
        stat.execute("insert into test values(1, space(10001))");
        Task[] tasks = new Task[2];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new Task() {
                @Override
                public void call() throws Exception {
                    PreparedStatement prep = conn.prepareStatement("select * from test where id = ?");
                    while (!stop) {
                        int x = (int) (Math.random() * 2);
                        prep.setInt(1, x);
                        ResultSet rs = prep.executeQuery();
                        rs.next();
                        String data = rs.getString(2);
                        if (data.length() != 10000 + x) {
                            throw new Exception(data.length() + " != " + x);
                        }
                        rs.close();
                    }
                }
            };
            tasks[i].execute();
        }
        Thread.sleep(1000);
        for (Task t : tasks) {
            t.get();
        }
        stat.execute("drop table test");
        conn.close();
    }

    private void testQueryCacheResetParams() throws SQLException {
        Connection conn = getConnection("optimizations");
        PreparedStatement prep;
        prep = conn.prepareStatement("select ?");
        prep.setString(1, "Hello");
        prep.execute();
        prep.close();
        prep = conn.prepareStatement("select ?");
        assertThrows(ErrorCode.PARAMETER_NOT_SET_1, prep).execute();
        prep.close();
        conn.close();
    }

    private void testRowId() throws SQLException {
        if (config.memory) {
            return;
        }
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("create table test(data varchar)");
        stat.execute("select min(_rowid_ + 1) from test");
        stat.execute("insert into test(_rowid_, data) values(10, 'Hello')");
        stat.execute("insert into test(data) values('World')");
        stat.execute("insert into test(_rowid_, data) values(20, 'Hello')");
        stat.execute("merge into test(_rowid_, data) key(_rowid_) values(20, 'Hallo')");
        rs = stat.executeQuery("select _rowid_, data from test order by _rowid_");
        rs.next();
        assertEquals(10, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(11, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        rs.next();
        assertEquals(21, rs.getInt(1));
        assertEquals("Hallo", rs.getString(2));
        assertFalse(rs.next());
        stat.execute("drop table test");

        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(0, 'Hello')");
        stat.execute("insert into test values(3, 'Hello')");
        stat.execute("insert into test values(2, 'Hello')");

        rs = stat.executeQuery("explain select * from test where _rowid_ = 2");
        rs.next();
        assertContains(rs.getString(1), ".tableScan: _ROWID_ =");

        rs = stat.executeQuery("explain select * from test where _rowid_ > 2");
        rs.next();
        assertContains(rs.getString(1), ".tableScan: _ROWID_ >");

        rs = stat.executeQuery("explain select * from test order by _rowid_");
        rs.next();
        assertContains(rs.getString(1), "/* index sorted */");
        rs = stat.executeQuery("select _rowid_, * from test order by _rowid_");
        rs.next();
        assertEquals(0, rs.getInt(1));
        assertEquals(0, rs.getInt(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals(3, rs.getInt(2));

        stat.execute("drop table test");
        conn.close();
    }

    private void testSortIndex() throws SQLException {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("drop table test if exists");
        stat.execute("create table test(id int)");
        stat.execute("create index idx_id_desc on test(id desc)");
        stat.execute("create index idx_id_asc on test(id)");
        ResultSet rs;

        rs = stat.executeQuery("explain select * from test where id > 10 order by id");
        rs.next();
        assertTrue(rs.getString(1).indexOf("IDX_ID_ASC") >= 0);

        rs = stat.executeQuery("explain select * from test where id < 10 order by id desc");
        rs.next();
        assertTrue(rs.getString(1).indexOf("IDX_ID_DESC") >= 0);

        rs.next();
        stat.execute("drop table test");
        conn.close();
    }

    private void testAutoAnalyze() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select value from information_schema.settings where name='analyzeAuto'");
        int auto = rs.next() ? rs.getInt(1) : 0;
        if (auto != 0) {
            stat.execute("create table test(id int)");
            stat.execute("create user onlyInsert password ''");
            stat.execute("grant insert on test to onlyInsert");
            Connection conn2 = getConnection("optimizations", "onlyInsert", getPassword(""));
            Statement stat2 = conn2.createStatement();
            stat2.execute("insert into test select x from system_range(1, " + (auto + 10) + ")");
            conn2.close();
        }
        conn.close();
    }

    private void testInAndBetween() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        ResultSet rs;
        stat.execute("create table test(id int, name varchar)");
        stat.execute("create index idx_name on test(id, name)");
        stat.execute("insert into test values(1, 'Hello'), (2, 'World')");
        rs = stat.executeQuery("select * from test where id between 1 and 3 and name in ('World')");
        assertTrue(rs.next());
        rs = stat.executeQuery("select * from test where id between 1 and 3 and name in (select 'World')");
        assertTrue(rs.next());
        stat.execute("drop table test");
        conn.close();
    }

    private void testNestedIn() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("create table accounts(id integer primary key, status varchar(255), tag varchar(255))");
        stat.execute("insert into accounts values (31, 'X', 'A')");
        stat.execute("create table parent(id int)");
        stat.execute("insert into parent values(31)");
        stat.execute("create view test_view as select a.status, a.tag from accounts a, parent t where a.id = t.id");
        rs = stat.executeQuery("select * from test_view where status='X' and tag in ('A','B')");
        assertTrue(rs.next());
        rs = stat.executeQuery("select * from (select a.status, a.tag " +
                "from accounts a, parent t where a.id = t.id) x where status='X' and tag in ('A','B')");
        assertTrue(rs.next());

        stat.execute("create table test(id int primary key, name varchar(255))");
        stat.execute("create unique index idx_name on test(name, id)");
        stat.execute("insert into test values(1, 'Hello'), (2, 'World')");
        rs = stat.executeQuery("select * from (select * from test) where id=1 and name in('Hello', 'World')");
        assertTrue(rs.next());
        stat.execute("drop table test");

        conn.close();
    }

    private void testConstantIn1() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();

        stat.execute("create table test(id int primary key, name varchar(255))");
        stat.execute("insert into test values(1, 'Hello'), (2, 'World')");
        assertSingleValue(stat, "select count(*) from test where name in ('Hello', 'World', 1)", 2);
        assertSingleValue(stat, "select count(*) from test where name in ('Hello', 'World')", 2);
        assertSingleValue(stat, "select count(*) from test where name in ('Hello', 'Not')", 1);
        stat.execute("drop table test");

        conn.close();
    }

    private void testConstantIn2() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations;IGNORECASE=TRUE");
        Statement stat = conn.createStatement();

        stat.executeUpdate("CREATE TABLE testValues (x VARCHAR(50))");
        stat.executeUpdate("INSERT INTO testValues (x) SELECT 'foo' x");
        ResultSet resultSet;
        resultSet = stat.executeQuery("SELECT x FROM testValues WHERE x IN ('foo')");
        assertTrue(resultSet.next());
        resultSet = stat.executeQuery("SELECT x FROM testValues WHERE x IN ('FOO')");
        assertTrue(resultSet.next());
        resultSet = stat.executeQuery("SELECT x FROM testValues WHERE x IN ('foo','bar')");
        assertTrue(resultSet.next());
        resultSet = stat.executeQuery("SELECT x FROM testValues WHERE x IN ('FOO','bar')");
        assertTrue(resultSet.next());
        
        conn.close();
    }
    
    private void testNestedInSelect() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("create table test(id int primary key, name varchar) as select 1, 'Hello'");
        stat.execute("select * from (select * from test) where id=1 and name in('Hello', 'World')");

        stat.execute("drop table test");

        stat.execute("create table test(id int, name varchar) as select 1, 'Hello'");
        stat.execute("create index idx2 on test(id, name)");
        rs = stat.executeQuery("select count(*) from test where id=1 and name in('Hello', 'x')");
        rs.next();
        assertEquals(1, rs.getInt(1));

        conn.close();
    }

    private void testNestedInSelectAndLike() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();

        stat.execute("create table test(id int primary key)");
        stat.execute("insert into test values(2)");
        ResultSet rs = stat.executeQuery("select * from test where id in(1, 2)");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        stat.execute("create table test2(id int primary key hash)");
        stat.execute("insert into test2 values(2)");
        rs = stat.executeQuery("select * from test where id in(1, 2)");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        PreparedStatement prep;
        prep = conn.prepareStatement("SELECT * FROM DUAL A WHERE A.X IN (SELECT B.X FROM DUAL B WHERE B.X LIKE ?)");
        prep.setString(1, "1");
        prep.execute();
        prep = conn.prepareStatement("SELECT * FROM DUAL A WHERE A.X IN (SELECT B.X FROM DUAL B WHERE B.X IN (?, ?))");
        prep.setInt(1, 1);
        prep.setInt(2, 1);
        prep.executeQuery();
        conn.close();
    }

    private void testInSelectJoin() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(a int, b int, c int, d int) " +
                "as select 1, 1, 1, 1 from dual;");
        ResultSet rs;
        PreparedStatement prep;
        prep = conn.prepareStatement("SELECT 2 FROM TEST A "
                + "INNER JOIN (SELECT DISTINCT B.C AS X FROM TEST B "
                + "WHERE B.D = ?2) V ON 1=1 WHERE (A = ?1) AND (B = V.X)");
        prep.setInt(1, 1);
        prep.setInt(2, 1);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertFalse(rs.next());

        prep = conn.prepareStatement(
                "select 2 from test a where a=? and b in(" +
                "select b.c from test b where b.d=?)");
        prep.setInt(1, 1);
        prep.setInt(2, 1);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertFalse(rs.next());
        conn.close();
    }


    private void testOptimizeInJoinSelect() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table item(id int primary key)");
        stat.execute("insert into item values(1)");
        stat.execute("create alias opt for \"" +
                getClass().getName() +
                ".optimizeInJoinSelect\"");
        PreparedStatement prep = conn.prepareStatement(
                "select * from item where id in (select x from opt())");
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }

    /**
     * This method is called via reflection from the database.
     *
     * @return a result set
     */
    public static ResultSet optimizeInJoinSelect() {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("X", Types.INTEGER, 0, 0);
        rs.addRow(1);
        return rs;
    }

    private void testOptimizeInJoin() throws SQLException {
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
    }

    private void testMinMaxNullOptimization() throws SQLException {
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

    private void testMultiColumnRangeQuery() throws SQLException {
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

    private void testDistinctOptimization() throws SQLException {
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
        rs = stat.executeQuery("SELECT DISTINCT TYPE FROM TEST ORDER BY TYPE LIMIT -1 OFFSET 0 SAMPLE_SIZE 3");
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

    private void testQueryCacheSpeed() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        testQuerySpeed(stat,
                "select sum(a.n), sum(b.x) from system_range(1, 100) b, (select sum(x) n from system_range(1, 4000)) a");
        conn.close();
    }

    private void testQuerySpeed(Statement stat, String sql) throws SQLException {
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

    private void testQueryCache(boolean optimize) throws SQLException {
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
        assertEquals(1, rs1.getInt(1));
        rs1.next();
        assertEquals(1, rs1.getInt(1));
        assertFalse(rs1.next());

        stat.execute("update test2 set id = 2");
        ResultSet rs2 = prep.executeQuery();
        rs2.next();
        assertEquals(2, rs2.getInt(1));

        conn.close();
    }

    private void testMinMaxCountOptimization(boolean memory) throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create " + (memory ? "memory" : "") + " table test(id int primary key, value int)");
        stat.execute("create index idx_value_id on test(value, id);");
        int len = getSize(1000, 10000);
        HashMap<Integer, Integer> map = New.hashMap();
        TreeSet<Integer> set = new TreeSet<Integer>();
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
                    map.put(i, value);
                    set.add(value);
                }
                break;
            case 6:
            case 7:
            case 8: {
                if (map.size() > 0) {
                    for (int j = random.nextInt(i), k = 0; k < 10; k++, j++) {
                        if (map.containsKey(j)) {
                            Integer x = map.remove(j);
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
                ArrayList<Integer> list = New.arrayList(map.values());
                int count = list.size();
                Integer min = null, max = null;
                if (count > 0) {
                    min = set.first();
                    max = set.last();
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

    private void testIn() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;

        assertFalse(stat.executeQuery("select * from dual where x in()").next());
        assertFalse(stat.executeQuery("select * from dual where null in(1)").next());
        assertFalse(stat.executeQuery("select * from dual where null in(null)").next());
        assertFalse(stat.executeQuery("select * from dual where null in(null, 1)").next());

        assertFalse(stat.executeQuery("select * from dual where 1+x in(3, 4)").next());
        assertFalse(stat.executeQuery("select * from dual d1, dual d2 where d1.x in(3, 4)").next());

        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello')");
        stat.execute("insert into test values(2, 'World')");

        prep = conn.prepareStatement("select * from test t1 where t1.id in(?)");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 where t1.id in(?, ?) order by id");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        assertFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 where t1.id "
                + "in(select t2.id from test t2 where t2.id=?)");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        assertFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 where t1.id "
                + "in(select t2.id from test t2 where t2.id=? and t1.id<>t2.id)");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        assertFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 where t1.id "
                + "in(select t2.id from test t2 where t2.id in(cast(?+10 as varchar)))");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        assertFalse(rs.next());

        conn.close();
    }

    /**
     * Where there are multiple indices, and we have an ORDER BY, select the
     * index that already has the required ordering.
     */
    private void testOrderedIndexes() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE my_table(K1 INT, K2 INT, VAL VARCHAR, PRIMARY KEY(K1, K2))");
        stat.execute("CREATE INDEX my_index ON my_table(K1, VAL)");
        ResultSet rs = stat.executeQuery(
                "EXPLAIN PLAN FOR SELECT * FROM my_table WHERE K1=7 ORDER BY K1, VAL");
        rs.next();
        assertContains(rs.getString(1), "/* PUBLIC.MY_INDEX: K1 = 7 */");

        stat.execute("DROP TABLE my_table");

        // where we have two covering indexes, make sure
        // we choose the one that covers more
        stat.execute("CREATE TABLE my_table(K1 INT, K2 INT, VAL VARCHAR)");
        stat.execute("CREATE INDEX my_index1 ON my_table(K1, K2)");
        stat.execute("CREATE INDEX my_index2 ON my_table(K1, K2, VAL)");
        rs = stat.executeQuery(
                "EXPLAIN PLAN FOR SELECT * FROM my_table WHERE K1=7 ORDER BY K1, K2, VAL");
        rs.next();
        assertContains(rs.getString(1), "/* PUBLIC.MY_INDEX2: K1 = 7 */");

        conn.close();
    }

    private void testConvertOrToIn() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();

        stat.execute("create table test(id int primary key, name varchar(255))");
        stat.execute("insert into test values(1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5')");

        ResultSet rs = stat.executeQuery("EXPLAIN PLAN FOR SELECT * FROM test WHERE ID=1 OR ID=2 OR ID=3 OR ID=4 OR ID=5");
        rs.next();
        assertContains(rs.getString(1), "ID IN(1, 2, 3, 4, 5)");

        rs = stat.executeQuery("SELECT COUNT(*) FROM test WHERE ID=1 OR ID=2 OR ID=3 OR ID=4 OR ID=5");
        rs.next();
        assertEquals(5, rs.getInt(1));

        conn.close();
    }
}
