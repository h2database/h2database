/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;

public class TestMVCC extends TestBase {
    
    Connection c1, c2;
    Statement s1, s2;

    public void test() throws Exception {
        testSetMode();
        testCases();
    }

    private void testSetMode() throws Exception {
        DeleteDbFiles.execute(null, "test", true);
        Class.forName("org.h2.Driver");
        c1 = DriverManager.getConnection("jdbc:h2:test", "sa", "sa");
        Statement stat = c1.createStatement();
        ResultSet rs = stat.executeQuery("select * from information_schema.settings where name='MVCC'");
        rs.next();
        check("FALSE", rs.getString("VALUE"));
        try {
            stat.execute("SET MVCC TRUE");
            error("Unexpected success");
        } catch (SQLException e) {
            check(ErrorCode.CANNOT_CHANGE_SETTING_WHEN_OPEN_1, e.getErrorCode());
        }
        rs = stat.executeQuery("select * from information_schema.settings where name='MVCC'");
        rs.next();
        check("FALSE", rs.getString("VALUE"));        
        c1.close();
    }
    
    private void testCases() throws Exception {
        if (!config.mvcc) {
            return;
        }
        // TODO Prio 1: make unit test work (remaining problem: optimization for select min/max)
        // TODO Prio 1: document: exclusive table lock still used when altering tables, adding indexes, select ... for update; table level locks are checked
        // TODO Prio 1: free up disk space (for deleted rows and old versions of updated rows) on commit
        // TODO Prio 1: ScanIndex: never remove uncommitted data from cache (lost sessionId)
        // TODO Prio 1: Test with Hibernate
        // TODO Prio 2: if MVCC is used, rows of transactions need to fit in memory
        // TODO Prio 2: write the log only when committed; remove restriction at Record.canRemove
        // TODO Prio 2: getRowCount: different row count for different indexes (MultiVersionIndex)
        // TODO Prio 2: getRowCount: different row count for different sessions: TableLink (use different connections?)
        // TODO Prio 2: getFirst / getLast in MultiVersionIndex
        // TODO Prio 2: snapshot isolation (currently read-committed, not repeatable read)

        // TODO test: one thread appends, the other selects new data (select * from test where id > ?) and deletes

        DeleteDbFiles.execute(null, "test", true);
        Class.forName("org.h2.Driver");
        c1 = DriverManager.getConnection("jdbc:h2:test;MVCC=TRUE;LOCK_TIMEOUT=10", "sa", "sa");
        s1 = c1.createStatement();
        c2 = DriverManager.getConnection("jdbc:h2:test;MVCC=TRUE;LOCK_TIMEOUT=10", "sa", "sa");
        s2 = c2.createStatement();
        c1.setAutoCommit(false);
        c2.setAutoCommit(false);
        
        // it should not be possible to drop a table when an uncommitted transaction changed something
        s1.execute("create table test(id int primary key)");
        s1.execute("insert into test values(1)");
        try {
            s2.execute("drop table test");
            error("Unexpected success");
        } catch (SQLException e) {
            // lock timeout expected
            checkNotGeneralException(e);
        }
        c1.rollback();
        s2.execute("drop table test");
        c2.rollback();
        
        // referential integrity problem
        s1.execute("create table a (id integer identity not null, code varchar(10) not null, primary key(id))");
        s1.execute("create table b (name varchar(100) not null, a integer, primary key(name), foreign key(a) references a(id))");        
        s1.execute("insert into a(code) values('one')");        
        try {
             s2.execute("insert into b values('un B', 1)");
            error("Unexpected success");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        c2.commit();
        c1.rollback();
        s1.execute("drop table a, b");
        c2.commit();
        
        // select for update should do an exclusive lock, even with mvcc
        s1.execute("create table test(id int primary key, name varchar(255))");
        s1.execute("insert into test values(1, 'y')");
        c1.commit();
        s2.execute("select * from test for update");
        try {
            s1.execute("insert into test values(2, 'x')");
            error("Unexpected success");
        } catch (SQLException e) {
            // lock timeout expected
            checkNotGeneralException(e);
        }
        c2.rollback();
        s1.execute("drop table test");
        c1.commit();
        c2.commit();
        
        s1.execute("create table test(id int primary key, name varchar(255))");
        s2.execute("insert into test values(4, 'Hello')");
        c2.rollback();
        test(s1, "select count(*) from test where name = 'Hello'", "0");
        test(s2, "select count(*) from test where name = 'Hello'", "0");
        c1.commit();
        c2.commit();
        s1.execute("DROP TABLE TEST");
        
        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        s1.execute("INSERT INTO TEST VALUES(1, 'Test')");
        c1.commit();
        test(s1, "select max(id) from test", "1");
        s1.execute("INSERT INTO TEST VALUES(2, 'World')");
        c1.rollback();
        test(s1, "select max(id) from test", "1");
        c1.commit();
        c2.commit();
        s1.execute("DROP TABLE TEST");

        
        s1.execute("create table test as select * from table(id int=(1, 2))");
        s1.execute("update test set id=1 where id=1");
        s1.execute("select max(id) from test");
        test(s1, "select max(id) from test", "2");
        c1.commit();
        c2.commit();
        s1.execute("DROP TABLE TEST");

        s1.execute("CREATE TABLE TEST(ID INT)");
        s1.execute("INSERT INTO TEST VALUES(1)");
        c1.commit();
        test(s2, "SELECT COUNT(*) FROM TEST", "1");
        s1.executeUpdate("DELETE FROM TEST");
        test(s2, "SELECT COUNT(*) FROM TEST", "1");
        test(s1, "SELECT COUNT(*) FROM TEST", "0");
        c1.commit();
        test(s2, "SELECT COUNT(*) FROM TEST", "0");
        c1.commit();
        c2.commit();
        s1.execute("DROP TABLE TEST");

        s1.execute("CREATE TABLE TEST(ID INT)");
        s1.execute("INSERT INTO TEST VALUES(1)");
        c1.commit();
        s1.execute("DELETE FROM TEST");
        test(s1, "SELECT COUNT(*) FROM TEST", "0");
        c1.commit();
        test(s1, "SELECT COUNT(*) FROM TEST", "0");
        s1.execute("INSERT INTO TEST VALUES(1)");
        s1.execute("DELETE FROM TEST");
        c1.commit();
        test(s1, "SELECT COUNT(*) FROM TEST", "0");
        s1.execute("DROP TABLE TEST");
        
        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World')");
        test(s2, "SELECT COUNT(*) FROM TEST", "0");
        c1.commit();
        test(s2, "SELECT COUNT(*) FROM TEST", "2");
        s1.execute("INSERT INTO TEST VALUES(3, '!')");
        c1.rollback();
        test(s2, "SELECT COUNT(*) FROM TEST", "2");
        s1.execute("DROP TABLE TEST");
        c1.commit();

        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        s1.execute("DELETE FROM TEST");
        test(s2, "SELECT COUNT(*) FROM TEST", "0");
        c1.commit();
        test(s2, "SELECT COUNT(*) FROM TEST", "0");
        s1.execute("DROP TABLE TEST");
        c1.commit();

        s1.execute("CREATE TABLE TEST(ID INT IDENTITY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        test(s2, "SELECT COUNT(*) FROM TEST", "0");
        test(s1, "SELECT COUNT(*) FROM TEST", "1");
        s1.execute("DROP TABLE TEST");
        c1.commit();

        s1.execute("CREATE TABLE TEST(ID INT IDENTITY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        s1.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        s1.execute("DROP TABLE TEST");
        c1.commit();
        
        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        c1.commit();
        s1.execute("DELETE FROM TEST WHERE ID=1");
        c1.rollback();
        s1.execute("DROP TABLE TEST");
        c1.commit();
        
        Random random = new Random(1);
        s1.execute("CREATE TABLE TEST(ID INT IDENTITY, NAME VARCHAR)");
        Statement s;
        Connection c;
        for (int i = 0; i < 1000; i++) {
            if (random.nextBoolean()) {
                s = s1;
                c = c1;
            } else {
                s = s2;
                c = c2;
            }
            switch (random.nextInt(5)) {
            case 0:
                s.execute("INSERT INTO TEST(NAME) VALUES('Hello')");
                break;
            case 1:
                s.execute("UPDATE TEST SET NAME=" + i + " WHERE ID=" + random.nextInt(i));
                break;
            case 2:
                s.execute("DELETE FROM TEST WHERE ID=" + random.nextInt(i));
                break;
            case 3:
                c.commit();
                break;
            case 4:
                c.rollback();
                break;
            default:
            }
            s1.execute("SELECT * FROM TEST ORDER BY ID");
            s2.execute("SELECT * FROM TEST ORDER BY ID");
        }
        c2.rollback();
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();

        random = new Random(1);
        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        for (int i = 0; i < 1000; i++) {
            if (random.nextBoolean()) {
                s = s1;
                c = c1;
            } else {
                s = s2;
                c = c2;
            }
            switch (random.nextInt(5)) {
            case 0:
                s.execute("INSERT INTO TEST VALUES(" + i + ", 'Hello')");
                break;
            case 1:
                try {
                    s.execute("UPDATE TEST SET NAME=" + i + " WHERE ID=" + random.nextInt(i));
                } catch (SQLException e) {
                    check(e.getErrorCode(), ErrorCode.CONCURRENT_UPDATE_1);
                }
                break;
            case 2:
                s.execute("DELETE FROM TEST WHERE ID=" + random.nextInt(i));
                break;
            case 3:
                c.commit();
                break;
            case 4:
                c.rollback();
                break;
            default:
            }
            s1.execute("SELECT * FROM TEST ORDER BY ID");
            s2.execute("SELECT * FROM TEST ORDER BY ID");
        }
        c2.rollback();
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();
        
        s1.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        test(s2, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'", "0");
        test(s1, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'", "1");
        c1.commit();
        test(s2, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'", "1");
        test(s2, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'", "1");
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();
        
        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        test(s2, "SELECT COUNT(*) FROM TEST WHERE ID<100", "0");
        test(s1, "SELECT COUNT(*) FROM TEST WHERE ID<100", "1");
        c1.commit();
        test(s2, "SELECT COUNT(*) FROM TEST WHERE ID<100", "1");
        test(s2, "SELECT COUNT(*) FROM TEST WHERE ID<100", "1");
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();
        
        s1.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR, PRIMARY KEY(ID, NAME))");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        c1.commit();
        test(s2, "SELECT NAME FROM TEST WHERE ID=1", "Hello");
        s1.execute("UPDATE TEST SET NAME = 'Hallo' WHERE ID=1");
        test(s2, "SELECT NAME FROM TEST WHERE ID=1", "Hello");
        test(s1, "SELECT NAME FROM TEST WHERE ID=1", "Hallo");
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();
        
        
        s1.execute("create table test(id int primary key, name varchar(255))");
        s1.execute("insert into test values(1, 'Hello'), (2, 'World')");
        c1.commit();
        try {
            s1.execute("update test set id=2 where id=1");
            error("unexpected success");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        ResultSet rs = s1.executeQuery("select * from test order by id");
        check(rs.next());
        check(rs.getInt(1), 1);
        check(rs.getString(2), "Hello");
        check(rs.next());
        check(rs.getInt(1), 2);
        check(rs.getString(2), "World");
        checkFalse(rs.next());
        
        rs = s2.executeQuery("select * from test order by id");
        check(rs.next());
        check(rs.getInt(1), 1);
        check(rs.getString(2), "Hello");
        check(rs.next());
        check(rs.getInt(1), 2);
        check(rs.getString(2), "World");
        checkFalse(rs.next());
        s1.execute("drop table test");
        c1.commit();
        c2.commit();
        
        c1.close();
        c2.close();
        
        
        
    }

    private void test(Statement stat, String sql, String expected) throws Exception {
        ResultSet rs = stat.executeQuery(sql);
        if (rs.next()) {
            String s = rs.getString(1);
            if (expected == null) {
                throw new Error("expected: no rows, got: " + s);
            } else if (!expected.equals(s)) {
                throw new Error("expected: " + expected + ", got: " + s);
            }
        } else {
            if (expected != null) {
                throw new Error("expected: " + expected + ", got: no rows");
            }
        }
        // TODO Auto-generated method stub
        
    }
}
