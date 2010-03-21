/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;
import org.h2.constant.ErrorCode;

/**
 * Test the impact of ALTER TABLE statements on views.
 */
public class TestViewAlterTable extends TestBase {

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

        testDropColumnWithoutViews();
        testViewsAreWorking();
        testAlterTableDropColumnNotInView();
        testAlterTableDropColumnInView();
        testAlterTableAddColumnWithView();
        testAlterTableAlterColumnDataTypeWithView();
        testSelectStar();
        testJoinAndAlias();
        testSubSelect();
        testForeignKey();

        conn.close();
        deleteDb("alter");
    }

    private void testDropColumnWithoutViews() throws SQLException {
        stat.execute("create table test(a int, b int, c int)");
        stat.execute("alter table test drop column c");
        stat.execute("drop table test");
    }

    private void testViewsAreWorking() throws SQLException {
        createTestData();
        checkViewRemainsValid();
    }

    private void testAlterTableDropColumnNotInView() throws SQLException {
        createTestData();
        stat.execute("alter table test drop column c");
        checkViewRemainsValid();
    }

    private void testAlterTableDropColumnInView() throws SQLException {
        // simple
        stat.execute("create table test(id identity, name varchar) as select x, 'Hello'");
        stat.execute("create view test_view as select * from test");
        try {
            stat.execute("alter table test drop name");
            fail();
        } catch (SQLException e) {
            assertEquals(ErrorCode.VIEW_IS_INVALID_2, e.getErrorCode());
        }
        ResultSet rs = stat.executeQuery("select * from test_view");
        assertTrue(rs.next());
        stat.execute("drop view test_view");
        stat.execute("drop table test");

        // nested
        createTestData();
        try {
            stat.execute("alter table test drop column a");
            fail("Should throw exception because V1 uses column A");
        } catch (SQLException e) {
            assertEquals(ErrorCode.VIEW_IS_INVALID_2, e.getErrorCode());
        }
        stat.execute("drop table test");
    }

    private void testAlterTableAddColumnWithView() throws SQLException {
        createTestData();
        stat.execute("alter table test add column d int");
        checkViewRemainsValid();
    }

    private void testAlterTableAlterColumnDataTypeWithView() throws SQLException {
        createTestData();
        stat.execute("alter table test alter b char(1)");
        checkViewRemainsValid();
    }

    private void testSelectStar() throws SQLException {
        createTestData();
        stat.execute("create view v4 as select * from test");
        stat.execute("alter table test add d int default 6");
        try {
            stat.executeQuery("select d from v4");
            // H2 doesn't remember v4 as 'select * from test',
            // it instead remembers each individual column that was in 'test' when the
            // view was originally created. This is consistent with PostgreSQL.
        } catch (SQLException e) {
            assertEquals(ErrorCode.COLUMN_NOT_FOUND_1, e.getErrorCode());
        }
        checkViewRemainsValid();
    }

    private void testJoinAndAlias() throws SQLException {
        createTestData();
        stat.execute("create view v4 as select v1.a dog, v3.a cat from v1 join v3 on v1.b = v3.a");
        // should make no difference
        stat.execute("alter table test add d int default 6");
        ResultSet rs = stat.executeQuery("select cat, dog from v4");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertFalse(rs.next());
        checkViewRemainsValid();
    }

    private void testSubSelect() throws SQLException {
        createTestData();
        stat.execute("create view v4 as select * from v3 where a in (select b from v2)");
        // should make no difference
        stat.execute("alter table test add d int default 6");
        ResultSet rs = stat.executeQuery("select a from v4");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        checkViewRemainsValid();
    }

    private void testForeignKey() throws SQLException {
        createTestData();
        stat.execute("create table test2(z int, a int, primary key(z), foreign key (a) references TEST(a))");
        stat.execute("insert into test2(z, a) values (99, 1)");
        // should make no difference
        stat.execute("alter table test add d int default 6");
        ResultSet rs = stat.executeQuery("select z from test2");
        assertTrue(rs.next());
        assertEquals(99, rs.getInt(1));
        assertFalse(rs.next());
        stat.execute("drop table test2");
        checkViewRemainsValid();
    }

    private void createTestData() throws SQLException {
        stat.execute("create table test(a int, b int, c int)");
        stat.execute("insert into test(a, b, c) values (1, 2, 3)");
        stat.execute("create view v1 as select a as b, b as a from test");
        // child of v1
        stat.execute("create view v2 as select * from v1");
        // sibling of v1
        stat.execute("create view v3 as select a from test");
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
