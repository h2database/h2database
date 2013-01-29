/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.todo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.tools.DeleteDbFiles;

/**
 * A test with an undo log size of 2 GB.
 */
public class TestUndoLogLarge {

    /**
     * Run just this test.
     *
     * @param args ignored
     */
    public static void main(String... args) throws Exception {
        // System.setProperty("h2.largeTransactions", "true");
        TestUndoLogLarge.test();
    }

    private static void test() throws SQLException {
        DeleteDbFiles.execute("data", "test", true);
        Connection conn = DriverManager.getConnection("jdbc:h2:data/test");
        Statement stat = conn.createStatement();
        stat.execute("set max_operation_memory 100");
        stat.execute("set max_memory_undo 100");
        stat.execute("create table test(id identity, name varchar)");
        conn.setAutoCommit(false);
        PreparedStatement prep = conn.prepareStatement(
                "insert into test(name) values(space(1024*1024))");
        long time = System.currentTimeMillis();
        for (int i = 0; i < 2500; i++) {
            prep.execute();
            long now = System.currentTimeMillis();
            if (now > time + 5000) {
                System.out.println(i);
                time = now + 5000;
            }
        }
        conn.rollback();
        conn.close();
    }

}
