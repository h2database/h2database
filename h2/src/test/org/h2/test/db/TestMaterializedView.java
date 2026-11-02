/*
 * Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
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
        testRefreshChecksSelectRight();
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

    private void testRefreshChecksSelectRight() throws SQLException {
        Connection conn = getConnection("materializedview");
        Statement stat = conn.createStatement();
        stat.execute("create user bob password 'bob'");
        stat.execute("create table secret(id int, data varchar)");
        stat.execute("insert into secret values(1, 'old')");
        stat.execute("grant select on secret to bob");
        stat.execute("create schema bobs authorization bob");

        Connection bobConn = getConnection("materializedview", "bob", "bob");
        Statement bob = bobConn.createStatement();
        bob.execute("create materialized view bobs.mv as select data from secret");

        // admin revokes bob's access and changes the underlying data
        stat.execute("revoke select on secret from bob");
        stat.execute("update secret set data = 'new' where id = 1");

        // refresh must not re-read a table bob can no longer select
        assertThrows(ErrorCode.NOT_ENOUGH_RIGHTS_FOR_1, () -> {
            bob.execute("refresh materialized view bobs.mv");
        });
        // the stale snapshot is kept, the forbidden current value is not exposed
        ResultSet rs = bob.executeQuery("select data from bobs.mv");
        assertTrue(rs.next());
        assertEquals("old", rs.getString(1));

        // granting the right back lets bob refresh again
        stat.execute("grant select on secret to bob");
        bob.execute("refresh materialized view bobs.mv");
        rs = bob.executeQuery("select data from bobs.mv");
        assertTrue(rs.next());
        assertEquals("new", rs.getString(1));

        // an admin may always refresh
        stat.execute("update secret set data = 'newest' where id = 1");
        stat.execute("refresh materialized view bobs.mv");

        bobConn.close();
        stat.execute("drop materialized view bobs.mv");
        stat.execute("drop schema bobs");
        stat.execute("drop table secret");
        stat.execute("drop user bob");
        conn.close();
    }

}
