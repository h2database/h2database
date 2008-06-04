/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2.test.TestBase;

/**
 * Tests for the two-phase-commit feature.
 */
public class TestTwoPhaseCommit extends TestBase {
    public void test() throws Exception {
        if (config.memory || config.networked || config.logMode == 0) {
            return;
        }

        deleteDb("twoPhaseCommit");

        prepare();
        openWith(true);
        test(true);

        prepare();
        openWith(false);
        test(false);
    }

    void test(boolean rolledBack) throws Exception {
        Connection conn = getConnection("twoPhaseCommit");
        Statement stat = conn.createStatement();
        stat.execute("SET WRITE_DELAY 0");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), "Hello");
        if (!rolledBack) {
            rs.next();
            assertEquals(rs.getInt(1), 2);
            assertEquals(rs.getString(2), "World");
        }
        assertFalse(rs.next());
        conn.close();
    }

    void openWith(boolean rollback) throws Exception {
        Connection conn = getConnection("twoPhaseCommit");
        Statement stat = conn.createStatement();
        ArrayList list = new ArrayList();
        ResultSet rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.IN_DOUBT");
        while (rs.next()) {
            list.add(rs.getString("TRANSACTION"));
        }
        for (int i = 0; i < list.size(); i++) {
            String s = (String) list.get(i);
            if (rollback) {
                stat.execute("ROLLBACK TRANSACTION " + s);
            } else {
                stat.execute("COMMIT TRANSACTION " + s);
            }
        }
        conn.close();
    }

    void prepare() throws Exception {
        deleteDb("twoPhaseCommit");
        Connection conn = getConnection("twoPhaseCommit");
        Statement stat = conn.createStatement();
        stat.execute("SET WRITE_DELAY 0");
        conn.setAutoCommit(false);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.commit();
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        stat.execute("PREPARE COMMIT XID_TEST_TRANSACTION_WITH_LONG_NAME");
        crash(conn);
    }
}
