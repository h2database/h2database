/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.test.TestBase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test the impact of DROP VIEW statements on dependent views.
 */
public class TestViewDropView extends TestBase {

    private Connection conn;
    private Statement stat;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        deleteDb("alter");
        conn = getConnection("alter");
        stat = conn.createStatement();

        testDropViewDefaultBehaviour();
        testDropViewRestrict();
        testDropViewCascade();
        testCreateOrReplaceView();
        testCreateOrReplaceViewWithNowInvalidDependentViews();
        testCreateOrReplaceForceViewWithNowInvalidDependentViews();

        conn.close();
        deleteDb("alter");
    }

    private void testDropViewDefaultBehaviour() throws SQLException {
        createTestData();

        try {
            // Should fail because have dependencies
            stat.execute("drop view v1");
            if (SysProperties.DROP_RESTRICT) {
                fail();
            }
        } catch (SQLException e) {
            if (!SysProperties.DROP_RESTRICT) {
                assertEquals(ErrorCode.CANNOT_DROP_2, e.getErrorCode());
            }
        }

        if (SysProperties.DROP_RESTRICT) {
            checkViewRemainsValid();
        }
    }

    private void testDropViewRestrict() throws SQLException {
        createTestData();

        try {
            // Should fail because have dependencies
            stat.execute("drop view v1 restrict");
            fail();
        } catch (SQLException e) {
            assertEquals(ErrorCode.CANNOT_DROP_2, e.getErrorCode());
        }

        checkViewRemainsValid();
    }

    private void testDropViewCascade() throws SQLException {
        createTestData();

        stat.execute("drop view v1 cascade");

        try {
            stat.execute("select * from v1");
            fail("Exception should be thrown - v1 should be deleted");
        } catch (SQLException e) {
            assertEquals(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, e.getErrorCode());
        }

        try {
            stat.execute("select * from v2");
            fail("Exception should be thrown - v2 should be deleted");
        } catch (SQLException e) {
            assertEquals(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, e.getErrorCode());
        }

        try {
            stat.execute("select * from v3");
            fail("Exception should be thrown - v3 should be deleted");
        } catch (SQLException e) {
            assertEquals(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, e.getErrorCode());
        }

        stat.execute("drop table test");
    }

    private void testCreateOrReplaceView() throws SQLException {
        createTestData();

        stat.execute("create or replace view v1 as select a as b, b as a, c from test");

        checkViewRemainsValid();
    }

    private void testCreateOrReplaceViewWithNowInvalidDependentViews() throws SQLException {
        createTestData();

        try {
            // v2 and v3 need more than just "c", so we should get an error
            stat.execute("create or replace view v1 as select c from test");
            fail("Exception should be thrown - dependent views need more columns than just 'c'");
        } catch (SQLException e) {
            assertEquals(ErrorCode.CANNOT_DROP_2, e.getErrorCode());
        }

        // Make sure our old views come back ok
        checkViewRemainsValid();
    }

    private void testCreateOrReplaceForceViewWithNowInvalidDependentViews() throws SQLException {
        createTestData();

        // v2 and v3 need more than just "c", but we want to force the creation of v1 anyway
        stat.execute("create or replace force view v1 as select c from test");

        try {
            // now v2 and v3 are broken, but they still exist -- if there is any value to that...?
            ResultSet rs = stat.executeQuery("select b from v2");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());

        } catch (SQLException e) {
            assertEquals(ErrorCode.COLUMN_NOT_FOUND_1, e.getErrorCode());
        }

        stat.execute("drop table test");
    }

    private void createTestData() throws SQLException {
        stat.execute("drop all objects");
        stat.execute("create table test(a int, b int, c int)");
        stat.execute("insert into test(a, b, c) values (1, 2, 3)");
        stat.execute("create view v1 as select a as b, b as a from test");
        // child of v1
        stat.execute("create view v2 as select * from v1");
        // child of v2
        stat.execute("create view v3 as select * from v2");
    }

    private void checkViewRemainsValid() throws SQLException {
        ResultSet rs = stat.executeQuery("select b from v1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("select b from v2");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("select b from test");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        stat.execute("drop table test");

        ResultSet d = conn.getMetaData().getTables(null, null, null, null);
        while (d.next()) {
            if (!d.getString(2).equals("INFORMATION_SCHEMA")) {
                fail("Should have no tables left in the database, not: " + d.getString(2) + "." + d.getString(3));
            }
        }
    }
}
