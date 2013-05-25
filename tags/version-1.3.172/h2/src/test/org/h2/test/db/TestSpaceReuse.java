/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Tests if disk space is reused after deleting many rows.
 */
public class TestSpaceReuse extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("spaceReuse");
        long first = 0, now = 0;
        for (int i = 0; i < 10; i++) {
            Connection conn = getConnection("spaceReuse");
            Statement stat = conn.createStatement();
            stat.execute("create table if not exists t(i int)");
            stat.execute("insert into t select x from system_range(1, 500)");
            conn.close();
            conn = getConnection("spaceReuse");
            conn.createStatement().execute("delete from t");
            conn.close();
            now = new File(getBaseDir() + "/spaceReuse.data.db").length();
            if (first == 0) {
                first = now;
            }
        }
        if (now > first) {
            fail("first: " + first + " now: " + now);
        }
        deleteDb("spaceReuse");
    }

}
