/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Various small performance tests.
 */
public class TestSpeed extends TestBase {

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

        deleteDb("speed");
        Connection conn;

        conn = getConnection("speed");

        // conn =
        // getConnection("speed;ASSERT=0;MAX_MEMORY_ROWS=1000000;MAX_LOG_SIZE=1000");

        // Class.forName("org.hsqldb.jdbcDriver");
        // conn = DriverManager.getConnection("jdbc:hsqldb:speed");

        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        int len = getSize(1, 10000);
        for (int i = 0; i < len; i++) {
            stat.execute("SELECT ID, NAME FROM TEST ORDER BY ID");
        }

        // drop table if exists test;
        // CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
        // @LOOP 100000 INSERT INTO TEST VALUES(?, 'Hello');
        // @LOOP 100000 SELECT * FROM TEST WHERE ID = ?;

        // stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME
        // VARCHAR(255))");
        // for(int i=0; i<1000; i++) {
        // stat.execute("INSERT INTO TEST VALUES("+i+", 'Hello')");
        // }
        // stat.execute("CREATE TABLE TEST_A(ID INT PRIMARY KEY, NAME
        // VARCHAR(255))");
        // stat.execute("INSERT INTO TEST_A VALUES(0, 'Hello')");
        long time = System.currentTimeMillis();
        // for(int i=1; i<8000; i*=2) {
        // stat.execute("INSERT INTO TEST_A SELECT ID+"+i+", NAME FROM TEST_A");
        //
        // // stat.execute("INSERT INTO TEST_A VALUES("+i+", 'Hello')");
        // }
        // for(int i=0; i<4; i++) {
        // ResultSet rs = stat.executeQuery("SELECT * FROM TEST_A");
        // while(rs.next()) {
        // rs.getInt(1);
        // rs.getString(2);
        // }
        // }
        // System.out.println(System.currentTimeMillis()-time);

        //
        // stat.execute("CREATE TABLE TEST_B(ID INT PRIMARY KEY, NAME
        // VARCHAR(255))");
        // for(int i=0; i<80000; i++) {
        // stat.execute("INSERT INTO TEST_B VALUES("+i+", 'Hello')");
        // }

        // conn.close();
        // System.exit(0);
        // int testParser;
        // java -Xrunhprof:cpu=samples,depth=8 -cp . org.h2.test.TestAll
        //
        // stat.execute("CREATE TABLE TEST(ID INT)");
        // stat.execute("INSERT INTO TEST VALUES(1)");
        // ResultSet rs = stat.executeQuery("SELECT ID OTHER_ID FROM TEST");
        // rs.next();
        // rs.getString("ID");
        // stat.execute("DROP TABLE TEST");

        // long time = System.currentTimeMillis();

        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE CACHED TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");

        int max = getSize(1, 10000);
        for (int i = 0; i < max; i++) {
            prep.setInt(1, i);
            prep.setString(2,
                    "abchelloasdfaldsjflajdflajdslfoajlskdfkjasdfadsfasdfadsfadfsalksdjflasjflajsdlkfjaksdjflkskd" + i);
            prep.execute();
        }

        // System.exit(0);
        // System.out.println("END "+Value.cacheHit+" "+Value.cacheMiss);

        time = System.currentTimeMillis() - time;
        trace(time + " insert");

        // if(true) return;

        // if(config.log) {
        // System.gc();
        // System.gc();
        // log("mem="+(Runtime.getRuntime().totalMemory() -
        //     Runtime.getRuntime().freeMemory())/1024);
        // }

        // conn.close();

        time = System.currentTimeMillis();

        prep = conn.prepareStatement("UPDATE TEST SET NAME='Another data row which is long' WHERE ID=?");
        for (int i = 0; i < max; i++) {
            prep.setInt(1, i);
            prep.execute();

            // System.out.println("updated "+i);
            // stat.execute("UPDATE TEST SET NAME='Another data row which is
            // long' WHERE ID="+i);
            // ResultSet rs = stat.executeQuery("SELECT * FROM TEST WHERE
            // ID="+i);
            // if(!rs.next()) {
            // throw new AssertionError("hey! i="+i);
            // }
            // if(rs.next()) {
            // throw new AssertionError("hey! i="+i);
            // }
        }
        // for(int i=0; i<max; i++) {
        // stat.execute("DELETE FROM TEST WHERE ID="+i);
        // ResultSet rs = stat.executeQuery("SELECT * FROM TEST WHERE ID="+i);
        // if(rs.next()) {
        // throw new AssertionError("hey!");
        // }
        // }

        time = System.currentTimeMillis() - time;
        trace(time + " update");

        conn.close();
        time = System.currentTimeMillis() - time;
        trace(time + " close");
        deleteDb("speed");
    }

    // private void testOuterJoin() throws SQLException {
    // Class.forName("org.h2.jdbc.jdbcDriver");
    // Connection conn = DriverManager.getConnection("jdbc:h2:test");

    // Class.forName("org.hsqldb.jdbcDriver");
    // Connection conn = DriverManager.getConnection("jdbc:hsqldb:test");
    // Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:.");

    // Statement stat = conn.createStatement();
    //
    // int len = getSize(1, 10000);

    // create table test(id int primary key, name varchar(255))
    // insert into test values(1, 'b')
    // insert into test values(2, 'c')
    // insert into test values(3, 'a')
    // select * from test order by name desc
    // select min(id)+max(id) from test
    // select abs(-1), id from test order by name desc

    // select id from test group by id

    // long start = System.currentTimeMillis();
    //
    // stat.executeUpdate("DROP TABLE IF EXISTS TEST");
    // stat.executeUpdate("CREATE TABLE Test(" + "Id INTEGER PRIMARY KEY, "
    // + "FirstName VARCHAR(20), " + "Name VARCHAR(50), "
    // + "ZIP INTEGER)");
    //
    //
    // stat.execute("create table a(a1 varchar(1), a2 int)");
    // stat.execute("create table b(b1 varchar(1), b2 int)");
    // stat.execute("insert into a values(null, 12)");
    // stat.execute("insert into a values('a', 22)");
    // stat.execute("insert into a values('b', 32)");
    // stat.execute("insert into b values(null, 14)");
    // stat.execute("insert into b values('a', 14)");
    // stat.execute("insert into b values('c', 15)");

    // create table a(a1 varchar(1), a2 int);
    // create table b(b1 varchar(1), b2 int);
    // insert into a values(null, 12);
    // insert into a values('a', 22);
    // insert into a values('b', 32);
    // insert into b values(null, 14);
    // insert into b values('a', 14);
    // insert into b values('c', 15);

    // query(stat, "select * from a left outer join b on a.a1=b.b1");

    // should be 3 rows
    // query(stat, "select * from a left outer join b on ((a.a1=b.b1) or (a.a1
    // is null and b.b1 is null))");
    // A1 A2 B1 B2
    // null 12 null 14
    // a 22 a 14
    // b 32 null null

    // should be 3 rows
    // query(stat, "select * from a left outer join b on ((a.a1=b.b1) or (a.a1
    // is null and b.b1 is null))");
    // A1 A2 B1 B2
    // 12 14
    // a 22 a 14
    // b 32

    // should be 2 rows
    // query(stat, "select * from a left outer join b on (1=1) where
    // ((a.a1=b.b1) or (a.a1 is null and b.b1 is null))");
    // A1 A2 B1 B2
    // 12 14
    // a 22 a 14

    // should be 1 row
    // query(stat, "select * from a left outer join b on (1=1) where
    // a.a1=b.b1");

    // should be 3 rows
    // query(stat, "select * from a left outer join b on a.a1=b.b1 where
    // (1=1)");

    // if(true) return;

    // query(stat, "SELECT T1.ID, T2.ID FROM TEST T1, TEST T2 WHERE T1.ID >
    // T2.ID");

    // PreparedStatement prep;
    //
    // prep = conn
    // .prepareStatement("INSERT INTO Test
    // VALUES(?,'Julia','Peterson-Clancy',?)");

    // query(stat, "SELECT * FROM TEST WHERE NAME LIKE 'Ju%'");

    // long time = System.currentTimeMillis();
    //
    // for (int i = 0; i < len; i++) {
    // prep.setInt(1, i);
    // prep.setInt(2, i);
    // prep.execute();
    // query(stat, "SELECT * FROM TEST");
    // if(i % 2 == 0) {
    // stat.executeUpdate("INSERT INTO Test
    // VALUES("+i+",'Julia','Peterson-Clancy',"+i+")");
    // } else {
    // stat.executeUpdate("INSERT INTO TEST
    // VALUES("+i+",'Julia','Peterson-Clancy',"+i+")");
    // }
    // }

    // query(stat, "SELECT ABS(-1) FROM TEST");

    // conn.close();
    // if(true) return;

    // stat.executeUpdate("UPDATE Test SET Name='Hans' WHERE Id=1");
    // query(stat, "SELECT * FROM Test WHERE Id=1");
    // stat.executeUpdate("DELETE FROM Test WHERE Id=1");

    // query(stat, "SELECT * FROM TEST");

    // conn.close();
    //
    // if(true) {
    // return;
    // }

    // query(stat, "SELECT * FROM TEST WHERE ID = 182");
    /*
     * for(int i=0; i<len; i++) { query(stat, "SELECT * FROM TEST WHERE ID =
     * "+i); }
     */

    // System.out.println("insert=" + (System.currentTimeMillis() - time));
    // conn.setAutoCommit(false);
    // prep = conn.prepareStatement("UPDATE Test SET FirstName='Hans' WHERE
    // Id=?");
    //
    // time = System.currentTimeMillis();
    //
    // for (int i = 0; i < len; i++) {
    // prep.setInt(1, i);
    // if(i%10 == 0) {
    // System.out.println(i+" ");
    // }
    // prep.execute();
    // stat.executeUpdate("UPDATE Test SET FirstName='Hans' WHERE Id="+i);
    // if(i==5) conn.close();
    // query(stat, "SELECT * FROM TEST");
    // }
    // conn.rollback();
    // System.out.println("update=" + (System.currentTimeMillis() - time));
    //
    // prep = conn.prepareStatement("SELECT * FROM Test WHERE Id=?");
    //
    // time = System.currentTimeMillis();
    //
    // for (int i = 0; i < len; i++) {
    // prep.setInt(1, i);
    // prep.execute();
    // // stat.executeQuery("SELECT * FROM Test WHERE Id="+i);
    // }
    // System.out.println("select=" + (System.currentTimeMillis() - time));
    // query(stat, "SELECT * FROM TEST");
    // prep = conn.prepareStatement("DELETE FROM Test WHERE Id=?");
    //
    // time = System.currentTimeMillis();
    //
    // for (int i = 0; i < len; i++) {
    // // stat.executeUpdate("DELETE FROM Test WHERE Id="+i);
    // prep.setInt(1, i);
    // //System.out.println("delete "+i);
    // prep.execute();
    // // query(stat, "SELECT * FROM TEST");
    // }
    // System.out.println("delete=" + (System.currentTimeMillis() - time));
    // System.out.println("total=" + (System.currentTimeMillis() - start));
    // stat.executeUpdate("DROP TABLE Test");
    //
    // conn.close();
    /*
     * stat.executeUpdate("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE DATE)");
     * stat.executeUpdate("INSERT INTO TEST VALUES(1, DATE '2004-12-19')");
     * stat.executeUpdate("INSERT INTO TEST VALUES(2, DATE '2004-12-20')");
     * query(stat, "SELECT * FROM TEST WHERE VALUE > DATE '2004-12-19'");
     */
    /*
     * stat.executeUpdate("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE
     * BINARY(10))"); stat.executeUpdate("INSERT INTO TEST VALUES(1, X'0011')");
     * stat.executeUpdate("INSERT INTO TEST VALUES(2, X'01FFAA')"); query(stat,
     * "SELECT * FROM TEST WHERE VALUE > X'0011'");
     */
    /*
     * stat.executeUpdate("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME
     * VARCHAR(255))"); stat.executeUpdate("INSERT INTO TEST VALUES(1,
     * 'Hallo')"); stat.executeUpdate("INSERT INTO TEST VALUES(2, 'World')");
     */
    /*
     * stat.executeUpdate("CREATE UNIQUE INDEX TEST_NAME ON TEST(NAME)");
     * stat.executeUpdate("DROP INDEX TEST_NAME"); stat.executeUpdate("INSERT
     * INTO TEST VALUES(2, 'Hallo')"); stat.executeUpdate("DELETE FROM TEST");
     * for(int i=0; i <100; i++) { stat.executeUpdate("INSERT INTO TEST
     * VALUES("+i+", 'Test"+i+"')"); }
     */
    /*
     * query(stat, "SELECT T1.ID, T1.NAME FROM TEST T1"); query(stat, "SELECT
     * T1.ID, T1.NAME, T2.ID, T2.NAME FROM TEST T1, TEST T2"); query(stat,
     * "SELECT T1.ID, T1.NAME, T2.ID, T2.NAME FROM TEST T1, TEST T2 WHERE T1.ID =
     * T2.ID");
     */
    /*
     * query(stat, "SELECT * FROM TEST WHERE ID = 1");
     * stat.executeUpdate("DELETE FROM TEST WHERE ID = 2"); query(stat, "SELECT *
     * FROM TEST WHERE ID < 10"); query(stat, "SELECT * FROM TEST WHERE ID =
     * 2"); stat.executeUpdate("UPDATE TEST SET NAME = 'World' WHERE ID = 5");
     * query(stat, "SELECT * FROM TEST WHERE ID = 5"); query(stat, "SELECT *
     * FROM TEST WHERE ID < 10");
     */
    // }
    // private static void query(Statement stat, String sql) throws SQLException
    // {
    // System.out.println("--------- " + sql);
    // ResultSet rs = stat.executeQuery(sql);
    // ResultSetMetaData meta = rs.getMetaData();
    // while (rs.next()) {
    // for (int i = 0; i < meta.getColumnCount(); i++) {
    // System.out.print("[" + meta.getColumnLabel(i + 1) + "]="
    // + rs.getString(i + 1) + " ");
    // }
    // System.out.println();
    // }
    // }
}
