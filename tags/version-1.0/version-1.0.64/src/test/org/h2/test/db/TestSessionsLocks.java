/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

public class TestSessionsLocks extends TestBase {

    public void test() throws Exception {
        testCancelStatement();
        testLocks();
    }

    private void testLocks() throws Exception {
        deleteDb("sessionsLocks");
        Connection conn = getConnection("sessionsLocks;MULTI_THREADED=1");
        Statement stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from information_schema.locks order by session_id");
        checkFalse(rs.next());
        Connection conn2 = getConnection("sessionsLocks");
        Statement stat2 = conn2.createStatement();
        stat2.execute("create table test(id int primary key, name varchar)");
        conn2.setAutoCommit(false);
        stat2.execute("insert into test values(1, 'Hello')");
        rs = stat.executeQuery("select * from information_schema.locks order by session_id");
        rs.next();
        check("PUBLIC", rs.getString("TABLE_SCHEMA"));
        check("TEST", rs.getString("TABLE_NAME"));
        rs.getString("SESSION_ID");
        if (config.mvcc) {
            check("READ", rs.getString("LOCK_TYPE"));
        } else {
            check("WRITE", rs.getString("LOCK_TYPE"));
        }
        checkFalse(rs.next());
        conn2.commit();
        conn2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        stat2.execute("SELECT * FROM TEST");
        rs = stat.executeQuery("select * from information_schema.locks order by session_id");
        if (!config.mvcc) {
            rs.next();
            check("PUBLIC", rs.getString("TABLE_SCHEMA"));
            check("TEST", rs.getString("TABLE_NAME"));
            rs.getString("SESSION_ID");
            check("READ", rs.getString("LOCK_TYPE"));
        }
        checkFalse(rs.next());
        conn2.commit();
        rs = stat.executeQuery("select * from information_schema.locks order by session_id");
        checkFalse(rs.next());
        conn.close();
        conn2.close();
    }

    public void testCancelStatement() throws Exception {
        deleteDb("sessionsLocks");
        Connection conn = getConnection("sessionsLocks;MULTI_THREADED=1");
        Statement stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from information_schema.sessions order by SESSION_START, ID");
        rs.next();
        int sessionId = rs.getInt("ID");
        rs.getString("USER_NAME");
        rs.getTimestamp("SESSION_START");
        rs.getString("STATEMENT");
        rs.getTimestamp("STATEMENT_START");
        checkFalse(rs.next());
        Connection conn2 = getConnection("sessionsLocks");
        final Statement stat2 = conn2.createStatement();
        rs = stat.executeQuery("select * from information_schema.sessions order by SESSION_START, ID");
        check(rs.next());
        check(sessionId, rs.getInt("ID"));
        check(rs.next());
        int otherId = rs.getInt("ID");
        check(otherId != sessionId);
        checkFalse(rs.next());
        stat2.execute("set throttle 1");
        final boolean[] done = new boolean[1];
        Runnable runnable = new Runnable() {
            public void run() {
                try {
                    stat2.execute("select count(*) from system_range(1, 10000000) t1, system_range(1, 10000000) t2");
                    new Error("Unexpected success").printStackTrace();
                } catch (SQLException e) {
                    done[0] = true;
                }
            }
        };
        new Thread(runnable).start();
        while (true) {
            Thread.sleep(1000);
            rs = stat.executeQuery("CALL CANCEL_SESSION(" + otherId + ")");
            rs.next();
            if (rs.getBoolean(1)) {
                Thread.sleep(100);
                check(done[0]);
                break;
            } else {
                System.out.println("no statement is executing yet");
            }
        }

        conn2.close();
        conn.close();
    }

}
