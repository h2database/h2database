/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Tests for MATERIALIZED VIEW.
 */
public class TestMaterializedView extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws SQLException {
        deleteDb("materializedview");
        test1();
        deleteDb("materializedview");
    }

    private void test1() throws SQLException {
        Connection conn = getConnection("materializedview");
        Statement stat = conn.createStatement();
        stat.execute("create table test(a int, b int)");
        stat.execute("insert into test values(1, 1)");
        stat.execute("create materialized view test_view as select a, b from test");
        ResultSet rs = stat.executeQuery("select * from test_view");
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getInt(2), 1);
        assertFalse(rs.next());
        stat.execute("insert into test values(2, 2)");
        stat.execute("refresh materialized view test_view");
        rs = stat.executeQuery("select * from test_view");
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getInt(2), 1);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getInt(2), 2);
        assertFalse(rs.next());
        // cannot drop table until the materialized view is dropped
        assertThrows(ErrorCode.CANNOT_DROP_2, () -> {
            stat.execute("drop table test");
        });
        stat.execute("drop materialized view test_view");
        stat.execute("drop table test");
        conn.close();
    }

}
