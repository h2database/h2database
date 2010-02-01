/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import org.h2.test.TestBase;
import org.h2.util.New;

/**
 * Transactional tests, including transaction isolation tests, and tests related
 * to savepoints.
 */
public class TestTransaction extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        testRollback();
        testRollback2();
        testForUpdate();
        testSetTransaction();
        testReferential();
        testSavepoint();
        testIsolation();
        deleteDb("transaction");
    }

    private void testForUpdate() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        conn.setAutoCommit(false);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key) as select 1");
        conn.commit();
        PreparedStatement prep = conn.prepareStatement("select * from test for update");
        prep.execute();
        // releases the lock
        conn.commit();
        prep.execute();
        Connection conn2 = getConnection("transaction");
        conn2.setAutoCommit(false);
        try {
            conn2.createStatement().execute("select * from test");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        conn2.close();
        conn.close();
    }

    private void testRollback() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("create index idx_id on test(id)");
        stat.execute("insert into test values(1), (1), (1)");
        if (!config.memory) {
            conn.close();
            conn = getConnection("transaction");
            stat = conn.createStatement();
        }
        conn.setAutoCommit(false);
        stat.execute("delete from test");
        conn.rollback();
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        assertResultRowCount(3, rs);
        rs = stat.executeQuery("select * from test where id = 1");
        assertResultRowCount(3, rs);
        conn.close();

        conn = getConnection("transaction");
        stat = conn.createStatement();
        stat.execute("create table master(id int) as select 1");
        stat.execute("create table child1(id int references master(id) on delete cascade)");
        stat.execute("insert into child1 values(1), (1), (1)");
        stat.execute("create table child2(id int references master(id)) as select 1");
        if (!config.memory) {
            conn.close();
            conn = getConnection("transaction");
            stat = conn.createStatement();
        }
        stat = conn.createStatement();
        try {
            stat.execute("delete from master");
            fail();
        } catch (SQLException ex) {
            // ok
            conn.rollback();
        }
        rs = stat.executeQuery("select * from master where id=1");
        assertResultRowCount(1, rs);
        rs = stat.executeQuery("select * from child1");
        assertResultRowCount(3, rs);
        rs = stat.executeQuery("select * from child1 where id=1");
        assertResultRowCount(3, rs);
        conn.close();
    }

    private void testRollback2() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("create index idx_id on test(id)");
        stat.execute("insert into test values(1), (1)");
        if (!config.memory) {
            conn.close();
            conn = getConnection("transaction");
            stat = conn.createStatement();
        }
        conn.setAutoCommit(false);
        stat.execute("delete from test");
        conn.rollback();
        ResultSet rs;
        rs = stat.executeQuery("select * from test where id = 1");
        assertResultRowCount(2, rs);
        conn.close();

        conn = getConnection("transaction");
        stat = conn.createStatement();
        stat.execute("create table master(id int) as select 1");
        stat.execute("create table child1(id int references master(id) on delete cascade)");
        stat.execute("insert into child1 values(1), (1)");
        stat.execute("create table child2(id int references master(id)) as select 1");
        if (!config.memory) {
            conn.close();
            conn = getConnection("transaction");
            stat = conn.createStatement();
        }
        stat = conn.createStatement();
        try {
            stat.execute("delete from master");
            fail();
        } catch (SQLException ex) {
            // ok
        }
        rs = stat.executeQuery("select * from master where id=1");
        assertResultRowCount(1, rs);
        rs = stat.executeQuery("select * from child1 where id=1");
        assertResultRowCount(2, rs);
        conn.close();
    }

    private void testSetTransaction() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        conn.setAutoCommit(false);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("insert into test values(1)");
        stat.execute("set @x = 1");
        conn.commit();
        assertSingleValue(stat, "select id from test", 1);
        assertSingleValue(stat, "call @x", 1);

        stat.execute("update test set id=2");
        stat.execute("set @x = 2");
        conn.rollback();
        assertSingleValue(stat, "select id from test", 1);
        assertSingleValue(stat, "call @x", 2);

        conn.close();
    }

    private void testReferential() throws SQLException {
        deleteDb("transaction");
        Connection c1 = getConnection("transaction");
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();
        s1.execute("drop table if exists a");
        s1.execute("drop table if exists b");
        s1.execute("create table a (id integer identity not null, code varchar(10) not null, primary key(id))");
        s1.execute("create table b (name varchar(100) not null, a integer, primary key(name), foreign key(a) references a(id))");
        Connection c2 = getConnection("transaction");
        c2.setAutoCommit(false);
        s1.executeUpdate("insert into A(code) values('one')");
        Statement s2 = c2.createStatement();
        try {
            s2.executeUpdate("insert into B values('two', 1)");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        c2.commit();
        c1.rollback();
        c1.close();
        c2.close();
    }

    private void testSavepoint() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST0(ID IDENTITY, NAME VARCHAR)");
        stat.execute("CREATE TABLE TEST1(NAME VARCHAR, ID IDENTITY, X TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
        conn.setAutoCommit(false);
        int[] count = new int[2];
        int[] countCommitted = new int[2];
        int[] countSave = new int[2];
        int len = getSize(2000, 10000);
        Random random = new Random(10);
        Savepoint sp = null;
        for (int i = 0; i < len; i++) {
            int tableId = random.nextInt(2);
            String table = "TEST" + tableId;
            int op = random.nextInt(6);
            switch (op) {
            case 0:
                stat.execute("INSERT INTO " + table + "(NAME) VALUES('op" + i + "')");
                count[tableId]++;
                break;
            case 1:
                if (count[tableId] > 0) {
                    stat.execute("DELETE FROM " + table + " WHERE ID=SELECT MIN(ID) FROM " + table);
                    count[tableId]--;
                }
                break;
            case 2:
                sp = conn.setSavepoint();
                countSave[0] = count[0];
                countSave[1] = count[1];
                break;
            case 3:
                if (sp != null) {
                    conn.rollback(sp);
                    count[0] = countSave[0];
                    count[1] = countSave[1];
                }
                break;
            case 4:
                conn.commit();
                sp = null;
                countCommitted[0] = count[0];
                countCommitted[1] = count[1];
                break;
            case 5:
                conn.rollback();
                sp = null;
                count[0] = countCommitted[0];
                count[1] = countCommitted[1];
                break;
            default:
            }
            checkTableCount(stat, "TEST0", count[0]);
            checkTableCount(stat, "TEST1", count[1]);
        }
        conn.close();
    }

    private void checkTableCount(Statement stat, String tableName, int count) throws SQLException {
        ResultSet rs;
        rs = stat.executeQuery("SELECT COUNT(*) FROM " + tableName);
        rs.next();
        assertEquals(count, rs.getInt(1));
    }

    private void testIsolation() throws SQLException {
        Connection conn = getConnection("transaction");
        trace("default TransactionIsolation=" + conn.getTransactionIsolation());
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        assertTrue(conn.getTransactionIsolation() == Connection.TRANSACTION_READ_COMMITTED);
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertTrue(conn.getTransactionIsolation() == Connection.TRANSACTION_SERIALIZABLE);
        Statement stat = conn.createStatement();
        assertTrue(conn.getAutoCommit());
        conn.setAutoCommit(false);
        assertFalse(conn.getAutoCommit());
        conn.setAutoCommit(true);
        assertTrue(conn.getAutoCommit());
        test(stat, "CREATE TABLE TEST(ID INT PRIMARY KEY)");
        conn.commit();
        test(stat, "INSERT INTO TEST VALUES(0)");
        conn.rollback();
        testValue(stat, "SELECT COUNT(*) FROM TEST", "1");
        conn.setAutoCommit(false);
        test(stat, "DELETE FROM TEST");
        // testValue("SELECT COUNT(*) FROM TEST", "0");
        conn.rollback();
        testValue(stat, "SELECT COUNT(*) FROM TEST", "1");
        conn.commit();
        conn.setAutoCommit(true);
        testNestedResultSets(conn);
        conn.setAutoCommit(false);
        testNestedResultSets(conn);
        conn.close();
    }

    private void testNestedResultSets(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        test(stat, "CREATE TABLE NEST1(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        test(stat, "CREATE TABLE NEST2(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        DatabaseMetaData meta = conn.getMetaData();
        ArrayList<String> result = New.arrayList();
        ResultSet rs1, rs2;
        rs1 = meta.getTables(null, null, "NEST%", null);
        while (rs1.next()) {
            String table = rs1.getString("TABLE_NAME");
            rs2 = meta.getColumns(null, null, table, null);
            while (rs2.next()) {
                String column = rs2.getString("COLUMN_NAME");
                trace("Table: " + table + " Column: " + column);
                result.add(table + "." + column);
            }
        }
        if (result.size() != 4) {
            fail("Wrong result, should be NEST1.ID, NEST1.NAME, NEST2.ID, NEST2.NAME but is " + result);
        }
        result = New.arrayList();
        test(stat, "INSERT INTO NEST1 VALUES(1,'A')");
        test(stat, "INSERT INTO NEST1 VALUES(2,'B')");
        test(stat, "INSERT INTO NEST2 VALUES(1,'1')");
        test(stat, "INSERT INTO NEST2 VALUES(2,'2')");
        Statement s1 = conn.createStatement();
        Statement s2 = conn.createStatement();
        rs1 = s1.executeQuery("SELECT * FROM NEST1 ORDER BY ID");
        while (rs1.next()) {
            rs2 = s2.executeQuery("SELECT * FROM NEST2 ORDER BY ID");
            while (rs2.next()) {
                String v1 = rs1.getString("VALUE");
                String v2 = rs2.getString("VALUE");
                result.add(v1 + "/" + v2);
            }
        }
        if (result.size() != 4) {
            fail("Wrong result, should be A/1, A/2, B/1, B/2 but is " + result);
        }
        result = New.arrayList();
        rs1 = s1.executeQuery("SELECT * FROM NEST1 ORDER BY ID");
        rs2 = s1.executeQuery("SELECT * FROM NEST2 ORDER BY ID");
        try {
            rs1.next();
            fail("next worked on a closed result set");
        } catch (SQLException e) {
            assertKnownException(e);
        }
        // this is already closed, so but closing again should no do any harm
        rs1.close();
        while (rs2.next()) {
            String v1 = rs2.getString("VALUE");
            result.add(v1);
        }
        if (result.size() != 2) {
            fail("Wrong result, should be A, B but is " + result);
        }
        test(stat, "DROP TABLE NEST1");
        test(stat, "DROP TABLE NEST2");
    }

    private void testValue(Statement stat, String sql, String data) throws SQLException {
        ResultSet rs = stat.executeQuery(sql);
        rs.next();
        String s = rs.getString(1);
        if (s == null ? (data != null) : (!s.equals(data))) {
            fail("s= " + s + " should be: " + data);
        }
    }

    private void test(Statement stat, String sql) throws SQLException {
        trace(sql);
        stat.execute(sql);
    }

}
