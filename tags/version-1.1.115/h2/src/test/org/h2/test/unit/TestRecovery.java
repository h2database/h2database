/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.constant.SysProperties;
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
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        DeleteDbFiles.execute(baseDir, "recovery", true);
        org.h2.Driver.load();
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("create table test as select * from system_range(1, 100)");
        stat.execute("create table a(id int primary key) as select * from system_range(1, 100)");
        stat.execute("create table b(id int references a(id)) as select * from system_range(1, 100)");
        stat.execute("alter table a add foreign key(id) references b(id)");
        conn.close();

        Recover.execute(baseDir, "recovery");
        DeleteDbFiles.execute(baseDir, "recovery", true);

        conn = getConnection("recovery", "diff", "");
        stat = conn.createStatement();
        String name = "recovery.data.sql";
        if (SysProperties.PAGE_STORE) {
            name = "recovery.h2.sql";
        }

        stat.execute("runscript from '" + baseDir + "/" + name + "'");
        stat.execute("select * from test");
        conn.close();
    }

}
