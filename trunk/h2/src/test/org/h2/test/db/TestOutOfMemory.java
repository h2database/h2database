/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
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

    @Override
    public void test() throws SQLException {
        if (config.memory || config.mvcc) {
            return;
        }
        for (int i = 0; i < 5; i++) {
            System.gc();
        }
        deleteDb("outOfMemory");
        Connection conn = getConnection("outOfMemory;MAX_OPERATION_MEMORY=1000000");
        Statement stat = conn.createStatement();
        stat.execute("drop all objects");
        stat.execute("create table stuff (id int, text varchar as space(100) || id)");
        stat.execute("insert into stuff(id) select x from system_range(1, 3000)");
        PreparedStatement prep = conn.prepareStatement("update stuff set text = text || space(1000) || id");
        prep.execute();
        stat.execute("checkpoint");
        eatMemory(80);
        try {
            try {
                prep.execute();
                fail();
            } catch (SQLException e) {
                assertEquals(ErrorCode.OUT_OF_MEMORY, e.getErrorCode());
            }
            assertThrows(ErrorCode.DATABASE_IS_CLOSED, conn).close();
            freeMemory();
            conn = null;
            conn = getConnection("outOfMemory");
            stat = conn.createStatement();
            ResultSet rs = stat.executeQuery("select count(*) from stuff");
            rs.next();
            assertEquals(3000, rs.getInt(1));
        } catch (OutOfMemoryError e) {
            freeMemory();
            // out of memory not detected
            throw (Error) new AssertionError("Out of memory not detected").initCause(e);
        } finally {
            freeMemory();
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // out of memory will / may close the database
                    assertKnownException(e);
                }
            }
        }
        deleteDb("outOfMemory");
    }

}
