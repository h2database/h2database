/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.Trigger;
import org.h2.constant.ErrorCode;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.ChangeFileEncryption;
import org.h2.tools.Recover;
import org.h2.util.Task;

/**
 * Tests the RUNSCRIPT SQL statement.
 */
public class TestRunscript extends TestBase implements Trigger {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testDropReferencedUserDefinedFunction();
        testDropCascade();
        testRunscriptFromClasspath();
        testCancelScript();
        testEncoding();
        testClobPrimaryKey();
        test(false);
        test(true);
        deleteDb("runscript");
    }

    private void testDropReferencedUserDefinedFunction() throws Exception {
        deleteDb("runscript");
        Connection conn;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("create alias int_decode for \"java.lang.Integer.decode\"");
        stat.execute("create table test(x varchar, y int as int_decode(x))");
        stat.execute("script simple drop to '" + getBaseDir() + "/backup.sql'");
        stat.execute("runscript from '" + getBaseDir() + "/backup.sql'");
        conn.close();
    }

    private void testDropCascade() throws Exception {
        deleteDb("runscript");
        Connection conn;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("create table b(x int)");
        stat.execute("create view a as select * from b");
        stat.execute("script simple drop to '" + getBaseDir() + "/backup.sql'");
        stat.execute("runscript from '" + getBaseDir() + "/backup.sql'");
        conn.close();
    }

    private void testRunscriptFromClasspath() throws Exception {
        deleteDb("runscript");
        Connection conn;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("runscript from 'classpath:/org/h2/samples/newsfeed.sql'");
        stat.execute("select * from version");
        conn.close();
    }

    private void testCancelScript() throws Exception {
        deleteDb("runscript");
        Connection conn;
        conn = getConnection("runscript");
        final Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key) as select x from system_range(1, 20000)");
        stat.execute("script simple drop to '"+getBaseDir()+"/backup.sql'");
        stat.execute("set throttle 1000");
        // need to wait a bit (throttle is only used every 50 ms)
        Thread.sleep(200);
        final String dir = getBaseDir();
        Task task;
        task = new Task() {
            public void call() throws SQLException {
                stat.execute("script simple drop to '"+dir+"/backup2.sql'");
            }
        };
        task.execute();
        Thread.sleep(200);
        stat.cancel();
        SQLException e = (SQLException) task.getException();
        assertTrue(e != null);
        assertEquals(ErrorCode.STATEMENT_WAS_CANCELED, e.getErrorCode());

        stat.execute("set throttle 1000");
        // need to wait a bit (throttle is only used every 50 ms)
        Thread.sleep(100);

        task = new Task() {
            public void call() throws SQLException {
                stat.execute("runscript from '"+dir+"/backup.sql'");
            }
        };
        task.execute();
        Thread.sleep(100);
        stat.cancel();
        e = (SQLException) task.getException();
        assertEquals(ErrorCode.STATEMENT_WAS_CANCELED, e.getErrorCode());

        conn.close();
        FileUtils.delete(getBaseDir() + "/backup.sql");
        FileUtils.delete(getBaseDir() + "/backup2.sql");
    }

    private void testEncoding() throws SQLException {
        deleteDb("runscript");
        Connection conn;
        Statement stat;
        conn = getConnection("runscript");
        stat = conn.createStatement();
        stat.execute("create table \"t\u00f6\"(id int)");
        stat.execute("script to '"+getBaseDir()+"/backup.sql'");
        stat.execute("drop all objects");
        stat.execute("runscript from '"+getBaseDir()+"/backup.sql'");
        stat.execute("select * from \"t\u00f6\"");
        stat.execute("script to '"+getBaseDir()+"/backup.sql' charset 'UTF-8'");
        stat.execute("drop all objects");
        stat.execute("runscript from '"+getBaseDir()+"/backup.sql' charset 'UTF-8'");
        stat.execute("select * from \"t\u00f6\"");
        conn.close();
        FileUtils.delete(getBaseDir() + "/backup.sql");
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param a the value
     * @return the absolute value
     */
    public static int test(int a) {
        return Math.abs(a);
    }

    private void testClobPrimaryKey() throws SQLException {
        deleteDb("runscript");
        Connection conn;
        Statement stat;
        conn = getConnection("runscript");
        stat = conn.createStatement();
        stat.execute("create table test(id int not null, data clob) as select 1, space(4100)");
        // the primary key for SYSTEM_LOB_STREAM used to be named like this
        stat.execute("create primary key primary_key_e on test(id)");
        stat.execute("script to '" + getBaseDir() + "/backup.sql'");
        conn.close();
        deleteDb("runscript");
        conn = getConnection("runscript");
        stat = conn.createStatement();
        stat.execute("runscript from '" + getBaseDir() + "/backup.sql'");
        conn.close();
        deleteDb("runscriptRestore");
        FileUtils.delete(getBaseDir() + "/backup.sql");
    }

    private void test(boolean password) throws SQLException {
        deleteDb("runscript");
        Connection conn1, conn2;
        Statement stat1, stat2;
        conn1 = getConnection("runscript");
        stat1 = conn1.createStatement();
        stat1.execute("create table test (id identity, name varchar(12))");
        stat1.execute("insert into test (name) values ('first'), ('second')");
        stat1.execute("create table test2(id int primary key) as select x from system_range(1, 5000)");
        stat1.execute("create sequence testSeq start with 100 increment by 10");
        stat1.execute("create alias myTest for \"" + getClass().getName() + ".test\"");
        stat1.execute("create trigger myTrigger before insert on test nowait call \"" + getClass().getName() + "\"");
        stat1.execute("create view testView as select * from test where 1=0 union all " +
                "select * from test where 0=1");
        stat1.execute("create user testAdmin salt '00' hash '01' admin");
        stat1.execute("create schema testSchema authorization testAdmin");
        stat1.execute("create table testSchema.parent(id int primary key, name varchar)");
        stat1.execute("create index idxname on testSchema.parent(name)");
        stat1.execute("create table testSchema.child(id int primary key, " +
                "parentId int, name varchar, foreign key(parentId) references parent(id))");
        stat1.execute("create user testUser salt '02' hash '03'");
        stat1.execute("create role testRole");
        stat1.execute("grant all on testSchema.child to testUser");
        stat1.execute("grant select, insert on testSchema.parent to testRole");
        stat1.execute("grant testRole to testUser");
        stat1.execute("create table blob (value blob)");
        PreparedStatement prep = conn1.prepareStatement("insert into blob values (?)");
        prep.setBytes(1, new byte[65536]);
        prep.execute();
        String sql = "script to '" + getBaseDir() + "/backup.2.sql'";
        if (password) {
            sql += " CIPHER AES PASSWORD 't1e2s3t4'";
        }
        stat1.execute(sql);

        deleteDb("runscriptRestore");
        conn2 = getConnection("runscriptRestore");
        stat2 = conn2.createStatement();
        sql = "runscript from '" + getBaseDir() + "/backup.2.sql'";
        if (password) {
            sql += " CIPHER AES PASSWORD 'wrongPassword'";
        }
        if (password) {
            assertThrows(ErrorCode.FILE_ENCRYPTION_ERROR_1, stat2).
                    execute(sql);
        }
        sql = "runscript from '" + getBaseDir() + "/backup.2.sql'";
        if (password) {
            sql += " CIPHER AES PASSWORD 't1e2s3t4'";
        }
        stat2.execute(sql);
        stat2.execute("script to '" + getBaseDir() + "/backup.3.sql'");

        assertEqualDatabases(stat1, stat2);

        if (!config.memory && !config.reopen) {
            conn1.close();

            if (config.cipher != null) {
                ChangeFileEncryption.execute(getBaseDir(), "runscript", config.cipher, getFilePassword().toCharArray(), null, true);
            }
            Recover.execute(getBaseDir(), "runscript");

            deleteDb("runscriptRestoreRecover");
            Connection conn3 = getConnection("runscriptRestoreRecover");
            Statement stat3 = conn3.createStatement();
            stat3.execute("runscript from '" + getBaseDir() + "/runscript.h2.sql'");
            conn3.close();
            conn3 = getConnection("runscriptRestoreRecover");
            stat3 = conn3.createStatement();

            if (config.cipher != null) {
                ChangeFileEncryption.execute(getBaseDir(), "runscript", config.cipher, null, getFilePassword().toCharArray(), true);
            }

            conn1 = getConnection("runscript");
            stat1 = conn1.createStatement();

            assertEqualDatabases(stat1, stat3);
            conn3.close();
        }

        assertEqualDatabases(stat1, stat2);

        conn1.close();
        conn2.close();
        deleteDb("runscriptRestore");
        deleteDb("runscriptRestoreRecover");
        FileUtils.delete(getBaseDir() + "/backup.2.sql");
        FileUtils.delete(getBaseDir() + "/backup.3.sql");

    }

    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type) {
        if (!before) {
            throw new InternalError("before:" + before);
        }
        if (type != INSERT) {
            throw new InternalError("type:" + type);
        }
    }

    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {
        // nothing to do
    }

    public void close() {
        // ignore
    }

    public void remove() {
        // ignore
    }

}
