/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;

/**
 * Tests out of memory situations. The database must not get corrupted, and
 * transactions must stay atomic.
 */
public class TestOutOfMemory extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        if (config.memory || config.mvcc) {
            return;
        }
        for (int i = 0; i < 5; i++) {
            System.gc();
        }
        deleteDb("outOfMemory");
        Connection conn = getConnection("outOfMemory");
        Statement stat = conn.createStatement();
        stat.execute("drop all objects");
        stat.execute("create table stuff (id int, text varchar as space(100) || id)");
        stat.execute("insert into stuff(id) select x from system_range(1, 3000)");
        PreparedStatement prep = conn.prepareStatement("update stuff set text = text || ' upd'");
        prep.execute();
        stat.execute("checkpoint");
        eatMemory(80);
        try {
            try {
                prep.execute();
                fail();
            } catch (SQLException e) {
                assertEquals(ErrorCode.OUT_OF_MEMORY, e.getErrorCode());
                try {
                    conn.close();
                    fail();
                } catch (SQLException e2) {
                    assertEquals(ErrorCode.DATABASE_IS_CLOSED, e2.getErrorCode());
                }
            }
            freeMemory();
            conn = null;
            conn = getConnection("outOfMemory");
            stat = conn.createStatement();
            ResultSet rs = stat.executeQuery("select count(*) from stuff");
            rs.next();
            assertEquals(3000, rs.getInt(1));
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                // out of memory will close the database
                assertKnownException(e);
            }
        }
        deleteDb("outOfMemory");
    }

}
