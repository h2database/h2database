/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Recover;

/**
 * Tests database recovery.
 */
public class TestRecovery extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        testRunScript();
    }

    private void testRunScript() throws SQLException {
        DeleteDbFiles.execute(baseDir, "recovery", true);
        DeleteDbFiles.execute(baseDir, "recovery2", true);
        org.h2.Driver.load();
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("create table test as select * from system_range(1, 100)");
        stat.execute("create table a(id int primary key) as select * from system_range(1, 100)");
        stat.execute("create table b(id int references a(id)) as select * from system_range(1, 100)");
        stat.execute("create table c(d clob) as select space(10000) || 'end'");
        stat.execute("create table d(d varchar) as select space(10000) || 'end'");
        stat.execute("alter table a add foreign key(id) references b(id)");
        // all rows have the same value - so that SCRIPT can't re-order the rows
        stat.execute("create table e(id varchar) as select space(10) from system_range(1, 1000)");
        stat.execute("create index idx_e_id on e(id)");
        conn.close();

        Recover rec = new Recover();
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        rec.setOut(new PrintStream(buff));
        rec.runTool("-dir", baseDir, "-db", "recovery", "-trace");
        String out = new String(buff.toByteArray());
        assertTrue(out.indexOf("Created file") >= 0);

        Connection conn2 = getConnection("recovery2", "diff", "");
        Statement stat2 = conn2.createStatement();
        String name = "recovery.h2.sql";

        stat2.execute("runscript from '" + baseDir + "/" + name + "'");
        stat2.execute("select * from test");
        stat2.execute("drop user diff");
        conn2.close();

        conn = getConnection("recovery");
        stat = conn.createStatement();
        conn2 = getConnection("recovery2");
        stat2 = conn2.createStatement();

        assertEqualDatabases(stat, stat2);
        conn.close();
        conn2.close();

        Recover.execute(baseDir, "recovery");

    }

}
