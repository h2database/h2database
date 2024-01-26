/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Recover;

/**
 * Tests database recovery.
 */
public class TestRecovery extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public boolean isEnabled() {
        if (config.memory) {
            return false;
        }
        return true;
    }

    @Override
    public void test() throws Exception {
        testRecoverClob();
        testRecoverFulltext();
        testCompressedAndUncompressed();
        testRunScript();
        testRunScript2();
    }

    private void testRecoverClob() throws Exception {
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int, data clob)");
        stat.execute("insert into test values(1, space(100000))");
        conn.close();
        Recover.main("-dir", getBaseDir(), "-db", "recovery");
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        conn = getConnection(
                "recovery;init=runscript from '" +
                getBaseDir() + "/recovery.h2.sql'");
        stat = conn.createStatement();
        stat.execute("select * from test");
        conn.close();
    }

    private void testRecoverFulltext() throws Exception {
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS IF NOT EXISTS FTL_INIT FOR 'org.h2.fulltext.FullTextLucene.init'");
        stat.execute("CALL FTL_INIT()");
        stat.execute("create table test(id int primary key, name varchar) as " +
                "select 1, 'Hello'");
        stat.execute("CALL FTL_CREATE_INDEX('PUBLIC', 'TEST', 'NAME')");
        conn.close();
        Recover.main("-dir", getBaseDir(), "-db", "recovery");
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        conn = getConnection(
                "recovery;init=runscript from '" +
                getBaseDir() + "/recovery.h2.sql'");
        conn.close();
    }


    private void testCompressedAndUncompressed() throws SQLException {
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        DeleteDbFiles.execute(getBaseDir(), "recovery2", true);
        org.h2.Driver.load();
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, data clob)");
        stat.execute("insert into test values(1, space(10000))");
        stat.execute("insert into test values(2, space(10000))");
        conn.close();
        Recover rec = new Recover();
        rec.runTool("-dir", getBaseDir(), "-db", "recovery");
        Connection conn2 = getConnection("recovery2");
        Statement stat2 = conn2.createStatement();
        String name = "recovery.h2.sql";
        stat2.execute("runscript from '" + getBaseDir() + "/" + name + "'");
        stat2.execute("select * from test");
        conn2.close();

        conn = getConnection("recovery");
        stat = conn.createStatement();
        conn2 = getConnection("recovery2");
        stat2 = conn2.createStatement();

        assertEqualDatabases(stat, stat2);
        conn.close();
        conn2.close();
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        DeleteDbFiles.execute(getBaseDir(), "recovery2", true);
    }

    private void testRunScript() throws Exception {
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        DeleteDbFiles.execute(getBaseDir(), "recovery2", true);
        org.h2.Driver.load();
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("create table \"Joe\"\"s Table\" as " +
                "select 1");
        stat.execute("create table test as " +
                "select * from system_range(1, 100)");
        stat.execute("create view \"TEST VIEW OF TABLE TEST\" as " +
                "select * from test");
        stat.execute("create table a(id int primary key) as " +
                "select * from system_range(1, 100)");
        stat.execute("create table b(id int primary key references a(id)) as " +
                "select * from system_range(1, 100)");
        stat.execute("create table lob(c clob, b blob) as " +
                "select space(10000) || 'end', SECURE_RAND(10000)");
        stat.execute("create table d(d varchar) as " +
                "select space(10000) || 'end'");
        stat.execute("alter table a add foreign key(id) references b(id)");
        // all rows have the same value - so that SCRIPT can't re-order the rows
        stat.execute("create table e(id varchar) as " +
                "select space(10) from system_range(1, 1000)");
        stat.execute("create index idx_e_id on e(id)");
        conn.close();

        Recover rec = new Recover();
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        rec.setOut(new PrintStream(buff, false, "UTF-8"));
        rec.runTool("-dir", getBaseDir(), "-db", "recovery", "-trace");
        String out = buff.toString(StandardCharsets.UTF_8);
        assertContains(out, "Created file");

        Connection conn2 = getConnection("recovery2");
        Statement stat2 = conn2.createStatement();
        String name = "recovery.h2.sql";

        stat2.execute("runscript from '" + getBaseDir() + "/" + name + "'");
        stat2.execute("select * from test");
        conn2.close();

        conn = getConnection("recovery");
        stat = conn.createStatement();
        conn2 = getConnection("recovery2");
        stat2 = conn2.createStatement();

        assertEqualDatabases(stat, stat2);
        conn.close();
        conn2.close();

        Recover.execute(getBaseDir(), "recovery");

        deleteDb("recovery");
        deleteDb("recovery2");
        FileUtils.delete(getBaseDir() + "/recovery.h2.sql");
        String dir = getBaseDir() + "/recovery.lobs.db";
        FileUtils.deleteRecursive(dir, false);
    }

    private void testRunScript2() throws Exception {
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        DeleteDbFiles.execute(getBaseDir(), "recovery2", true);
        org.h2.Driver.load();
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("SET COLLATION EN");
        stat.execute("CREATE TABLE TEST(A VARCHAR)");
        conn.close();

        final Recover recover = new Recover();
        final ByteArrayOutputStream buff = new ByteArrayOutputStream(); // capture the console output
        recover.setOut(new PrintStream(buff, false, "UTF-8"));
        recover.runTool("-dir", getBaseDir(), "-db", "recovery", "-trace");
        String consoleOut = buff.toString(StandardCharsets.UTF_8);
        assertContains(consoleOut, "Created file");

        Connection conn2 = getConnection("recovery2");
        Statement stat2 = conn2.createStatement();

        stat2.execute("runscript from '" + getBaseDir() + "/recovery.h2.sql'");
        stat2.execute("select * from test");
        conn2.close();

        conn = getConnection("recovery");
        stat = conn.createStatement();
        conn2 = getConnection("recovery2");
        stat2 = conn2.createStatement();
        assertEqualDatabases(stat, stat2);
        conn.close();
        conn2.close();

        deleteDb("recovery");
        deleteDb("recovery2");
        FileUtils.delete(getBaseDir() + "/recovery.h2.sql");
        String dir = getBaseDir() + "/recovery.lobs.db";
        FileUtils.deleteRecursive(dir, false);
    }
}
