/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.todo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.h2.tools.DeleteDbFiles;

/**
 * A test to reproduce out of memory using a large operation.
 */
public class TestUndoLogMemory {

    /**
     * Run just this test.
     *
     * @param args ignored
     */
    public static void main(String... args) throws Exception {
        System.setProperty("h2.largeTransactions", "true");
        
        // -Xmx1m -XX:+HeapDumpOnOutOfMemoryError
        DeleteDbFiles.execute("data", "test", true);
        Connection conn = DriverManager.getConnection("jdbc:h2:data/test");
        Statement stat = conn.createStatement();
        stat.execute("set cache_size 32");
        stat.execute("SET max_operation_memory 100");
        stat.execute("SET max_memory_undo 100");

        // also a problem: tables without unique index
        stat.execute("create table test(id int)");
        stat.execute("insert into test select x from system_range(1, 100000)");

        stat.execute("drop table test");
        stat.execute("create table test(id int primary key)");

        // INSERT problem
        stat.execute(
            "insert into test select x from system_range(1, 400000)");
        stat.execute("delete from test");

        // DELETE problem
        stat.execute(
            "insert into test select x from system_range(1, 50000)");
        stat.execute(
             "insert into test select x from system_range(50001, 100000)");
        stat.execute(
            "insert into test select x from system_range(100001, 150000)");
        stat.execute("delete from test");

        conn.close();
    }
}
