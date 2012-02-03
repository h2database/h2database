/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.tools.Backup;
import org.h2.tools.Restore;
import org.h2.util.IOUtils;

/**
 * Test for the BACKUP SQL statement.
 */
public class TestBackup extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        if (config.memory) {
            return;
        }
        testBackupRestoreLobStatement();
        testBackupRestoreLob();
        testBackup();
        deleteDb("backup");
    }

    private void testBackupRestoreLob() throws SQLException {
        deleteDb("backup");
        Connection conn = getConnection("backup");
        conn.createStatement().execute("create table test(x clob) as select space(10000)");
        conn.close();
        Backup.execute(getTestDir("") + "/backup.zip", getTestDir(""), "backup", true);
        deleteDb("backup");
        Restore.execute(getTestDir("") + "/backup.zip", getTestDir(""), "backup", true);
    }

    private void testBackupRestoreLobStatement() throws SQLException {
        deleteDb("backup");
        Connection conn = getConnection("backup");
        conn.createStatement().execute("create table test(x clob) as select space(10000)");
        conn.createStatement().execute("backup to '"+getTestDir("") + "/backup.zip"+"'");
        conn.close();
        deleteDb("backup");
        Restore.execute(getTestDir("") + "/backup.zip", getTestDir(""), "backup", true);
    }

    private void testBackup() throws SQLException {
        deleteDb("backup");
        deleteDb("restored");
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

        stat1.execute("backup to '" + baseDir + "/backup.zip'");
        conn2.rollback();
        assertEqualDatabases(stat1, stat2);

        Restore.execute(baseDir + "/backup.zip", baseDir, "restored", true);
        conn3 = getConnection("restored");
        stat3 = conn3.createStatement();
        assertEqualDatabases(stat1, stat3);

        conn1.close();
        conn2.close();
        conn3.close();
        deleteDb("restored");
        IOUtils.delete(baseDir + "/backup.zip");
    }

}
