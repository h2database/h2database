/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.tools.Restore;

public class TestBackup extends TestBase {

    public void test() throws Exception {
        if(config.memory || config.logMode == 0) {
            return;
        }
        testBackup();
    }

    private void testBackup() throws Exception {
        deleteDb("backup");
        Connection conn1, conn2, conn3;
        Statement stat1, stat2, stat3;
        conn1 = getConnection("backup");
        stat1 = conn1.createStatement();
        stat1.execute("create table test(id int primary key, name varchar(255))");
        stat1.execute("insert into test values(1, 'first'), (2, 'second')");
        stat1.execute("create table testlob(id int primary key, b blob, c clob)");
        stat1.execute("insert into testlob values(1, space(10000), repeat('00', 10000))");
        conn2 = getConnection("backup");
        stat2 = conn2.createStatement();
        stat2.execute("insert into test values(3, 'third')");
        conn2.setAutoCommit(false);
        stat2.execute("insert into test values(4, 'fourth (uncommitted)')");
        stat2.execute("insert into testlob values(2, ' ', '00')");
        
        stat1.execute("backup to '" + BASE_DIR + "/backup.zip'");
        conn2.rollback();
        
        Restore.execute(BASE_DIR + "/backup.zip", BASE_DIR, "restored", true);
        conn3 = getConnection("restored");
        stat3 = conn3.createStatement();
        compareDatabases(stat1, stat3);

        conn1.close();
        conn2.close();
        conn3.close();
    }

}

