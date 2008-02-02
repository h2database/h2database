/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Random;
import java.util.Vector;

import org.h2.test.TestBase;

/**
 * Transactional tests, including transaction isolation tests, and tests related to savepoints.
 */
public class TestTransaction extends TestBase {

    public void test() throws Exception {
        testReferential();
        testSavepoint();
        testIsolation();
    }

    private void testReferential() throws Exception {
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
            error("Unexpected success");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        c2.commit();
        c1.rollback();
        c1.close();
        c2.close();
    }

    public void testSavepoint() throws Exception {
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
            }
            checkTableCount(stat, "TEST0", count[0]);
            checkTableCount(stat, "TEST1", count[1]);
        }
        conn.close();
    }

    private void checkTableCount(Statement stat, String tableName, int count) throws Exception {
        ResultSet rs;
        rs = stat.executeQuery("SELECT COUNT(*) FROM " + tableName);
        rs.next();
        check(count, rs.getInt(1));
    }

    public void testIsolation() throws Exception {
        Connection conn = getConnection("transaction");
        trace("default TransactionIsolation=" + conn.getTransactionIsolation());
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        check(conn.getTransactionIsolation() == Connection.TRANSACTION_READ_COMMITTED);
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        check(conn.getTransactionIsolation() == Connection.TRANSACTION_SERIALIZABLE);
        Statement stat = conn.createStatement();
        check(conn.getAutoCommit());
        conn.setAutoCommit(false);
        checkFalse(conn.getAutoCommit());
        conn.setAutoCommit(true);
        check(conn.getAutoCommit());
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

    void testNestedResultSets(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        test(stat, "CREATE TABLE NEST1(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        test(stat, "CREATE TABLE NEST2(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        DatabaseMetaData meta = conn.getMetaData();
        Vector result;
        ResultSet rs1, rs2;
        result = new Vector();
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
            error("Wrong result, should be NEST1.ID, NEST1.NAME, NEST2.ID, NEST2.NAME but is " + result);
        }
        result = new Vector();
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
            error("Wrong result, should be A/1, A/2, B/1, B/2 but is " + result);
        }
        result = new Vector();
        rs1 = s1.executeQuery("SELECT * FROM NEST1 ORDER BY ID");
        rs2 = s1.executeQuery("SELECT * FROM NEST2 ORDER BY ID");
        try {
            rs1.next();
            error("next worked on a closed result set");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        // this is already closed, so but closing again should no do any harm
        rs1.close();
        while (rs2.next()) {
            String v1 = rs2.getString("VALUE");
            result.add(v1);
        }
        if (result.size() != 2) {
            error("Wrong result, should be A, B but is " + result);
        }
        test(stat, "DROP TABLE NEST1");
        test(stat, "DROP TABLE NEST2");
    }

    void testValue(Statement stat, String sql, String data) throws Exception {
        ResultSet rs = stat.executeQuery(sql);
        rs.next();
        String s = rs.getString(1);
        if (s == null ? (data != null) : (!s.equals(data))) {
            error("s= " + s + " should be: " + data);
        }
    }

    void test(Statement stat, String sql) throws Exception {
        trace(sql);
        stat.execute(sql);
    }

}
