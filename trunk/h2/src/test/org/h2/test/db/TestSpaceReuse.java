/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.*;
import java.sql.*;

import org.h2.test.TestBase;

public class TestSpaceReuse extends TestBase {

    public void test() throws Exception {
        if(config.memory) {
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
            now = new File(baseDir + "/spaceReuse.data.db").length();
            if(first == 0) {
                first = now;
            }
        }
        if(now > first) {
            this.error("first: " + first + " now: " + now);
        }
    }

}
