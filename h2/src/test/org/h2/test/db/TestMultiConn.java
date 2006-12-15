/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
 * Initial Developer: H2 Group 
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.DatabaseEventListener;
import org.h2.test.TestBase;

/**
 * @author Thomas
 */

public class TestMultiConn extends TestBase implements DatabaseEventListener {

    public void test() throws Exception {
//        testCommitRollback();
        try {
        testConcurrentOpen();
        } catch(SQLException e) {
            e.printStackTrace();
        System.exit(0); }
//        testThreeThreads();
    }
    
    private static int wait;
    
    private void testThreeThreads() throws Exception {
        deleteDb("multiConn");
        final Connection conn1 = getConnection("multiConn");
        final Connection conn2 = getConnection("multiConn");
        final Connection conn3 = getConnection("multiConn");
        conn1.setAutoCommit(false);
        conn2.setAutoCommit(false);
        conn3.setAutoCommit(false);
        final Statement s1 = conn1.createStatement();
        final Statement s2 = conn2.createStatement();
        final Statement s3 = conn3.createStatement();
        s1.execute("CREATE TABLE TEST1(ID INT)");
        s2.execute("CREATE TABLE TEST2(ID INT)");
        s3.execute("CREATE TABLE TEST3(ID INT)");
        s1.execute("INSERT INTO TEST1 VALUES(1)");
        s2.execute("INSERT INTO TEST2 VALUES(2)");
        s3.execute("INSERT INTO TEST3 VALUES(3)");
        s1.execute("SET LOCK_TIMEOUT 1000");
        s2.execute("SET LOCK_TIMEOUT 1000");
        s3.execute("SET LOCK_TIMEOUT 1000");
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    s3.execute("INSERT INTO TEST2 VALUES(4)");
                    conn3.commit();
                } catch(SQLException e) {
                    e.printStackTrace();
                }
            }
        });
        t1.start();
        Thread.sleep(20);
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                try {
                    s2.execute("INSERT INTO TEST1 VALUES(5)");
                    conn2.commit();
                } catch(SQLException e) {
                    e.printStackTrace();
                }
            }
        });
        t2.start();
        Thread.sleep(20);
        conn1.commit();
        t2.join(100);
        t1.join(100);
        ResultSet rs = s1.executeQuery("SELECT * FROM TEST1 ORDER BY ID");
        rs.next();
        check(rs.getInt(1), 1);
        rs.next();
        check(rs.getInt(1), 5);
        checkFalse(rs.next());
        conn1.close();
        conn2.close();
        conn3.close();
    }
    
    private void testConcurrentOpen() throws Exception {
        if(config.memory) {
            return;
        }
        deleteDb("multiConn");        
        Connection conn = getConnection("multiConn");
        conn.createStatement().execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        conn.createStatement().execute("INSERT INTO TEST VALUES(0, 'Hello'), (1, 'World')");
        conn.createStatement().execute("SHUTDOWN");
        conn.close();
        final String listener = getClass().getName();
        Runnable r = new Runnable() {
            public void run() {
                try {
                    Connection c1 = getConnection("multiConn;DATABASE_EVENT_LISTENER='"+listener+"';file_lock=socket");
                    c1.close();
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
        };
        Thread thread = new Thread(r);
        thread.start();
        Thread.sleep(10);
        Connection c2 = getConnection("multiConn;file_lock=socket");
        c2.close();
        thread.join();
    }
    

    public void diskSpaceIsLow(long stillAvailable) throws SQLException {
    }

    public void exceptionThrown(SQLException e) {
    }

    public void setProgress(int state, String name, int x, int max) {
        while(wait > 0) {
            try {
                Thread.sleep(wait);
                wait = 0;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void closingDatabase() {
    }    
    
    private void testCommitRollback() throws Exception {
        deleteDb("multiConn");
        Connection c1 = getConnection("multiConn");
        Connection c2 = getConnection("multiConn");
        c1.setAutoCommit(false);
        c2.setAutoCommit(false);
        Statement s1 = c1.createStatement();
        s1.execute("DROP TABLE IF EXISTS MULTI_A");
        s1.execute("CREATE TABLE MULTI_A(ID INT, NAME VARCHAR(255))");
        s1.execute("INSERT INTO MULTI_A VALUES(0, '0-insert-A')");
        Statement s2 = c2.createStatement();
        s1.execute("DROP TABLE IF EXISTS MULTI_B");
        s1.execute("CREATE TABLE MULTI_B(ID INT, NAME VARCHAR(255))");
        s2.execute("INSERT INTO MULTI_B VALUES(0, '1-insert-B')");
        c1.commit();
        c2.rollback();
        s1.execute("INSERT INTO MULTI_A VALUES(1, '0-insert-C')");
        s2.execute("INSERT INTO MULTI_B VALUES(1, '1-insert-D')");
        c1.rollback();
        c2.commit();
        c1.close();
        c2.close();
        
        if(!config.memory) {
            Connection conn = getConnection("multiConn");
            ResultSet rs;
            rs = conn.createStatement().executeQuery("SELECT * FROM MULTI_A ORDER BY ID");
            rs.next();
            check(rs.getString("NAME"), "0-insert-A" );
            checkFalse(rs.next());
            rs = conn.createStatement().executeQuery("SELECT * FROM MULTI_B ORDER BY ID");
            rs.next();
            check(rs.getString("NAME"), "1-insert-D" );
            checkFalse(rs.next());
            conn.close();
        }

    }

    public void init(String url) {
    }

}
