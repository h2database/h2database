/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Test ALTER statements.
 */
public class TestAlter extends TestBase {

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

    @Override
    public void test() throws Exception {
        deleteDb("alter");
        conn = getConnection("alter");
        stat = conn.createStatement();
        testAlterTableAlterColumnAsSelfColumn();
        testAlterTableDropColumnWithReferences();
        testAlterTableAlterColumnWithConstraint();
        testAlterTableAlterColumn();
        testAlterTableDropIdentityColumn();
        testAlterTableAddColumnIfNotExists();
        testAlterTableAddMultipleColumns();
        testAlterTableAlterColumn2();
        testAlterTableAddColumnBefore();
        testAlterTableAddColumnAfter();
        testAlterTableModifyColumn();
        conn.close();
        deleteDb("alter");
    }

    private void testAlterTableAlterColumnAsSelfColumn() throws SQLException {
        stat.execute("create table test(id int, name varchar)");
        stat.execute("alter table test alter column id int as id+1");
        stat.execute("insert into test values(1, 'Hello')");
        stat.execute("update test set name='World'");
        ResultSet rs = stat.executeQuery("select * from test");
        rs.next();
        assertEquals(3, rs.getInt(1));
        stat.execute("drop table test");
    }

    private void testAlterTableDropColumnWithReferences() throws SQLException {
        stat.execute("create table parent(id int, b int)");
        stat.execute("create table child(p int primary key)");
        stat.execute("alter table child add foreign key(p) references parent(id)");
        stat.execute("alter table parent drop column id");
        stat.execute("drop table parent");
        stat.execute("drop table child");

        stat.execute("create table test(id int, name varchar(255))");
        stat.execute("alter table test add constraint x check (id > name)");

        // the constraint references multiple columns
        assertThrows(ErrorCode.COLUMN_IS_REFERENCED_1, stat).
                execute("alter table test drop column id");

        stat.execute("drop table test");

        stat.execute("create table test(id int, name varchar(255))");
        stat.execute("alter table test add constraint x unique(id, name)");

        // the constraint references multiple columns
        assertThrows(ErrorCode.COLUMN_IS_REFERENCED_1, stat).
                execute("alter table test drop column id");

        stat.execute("drop table test");

        stat.execute("create table test(id int, name varchar(255))");
        stat.execute("alter table test add constraint x check (id > 1)");
        stat.execute("alter table test drop column id");
        stat.execute("drop table test");

        stat.execute("create table test(id int, name varchar(255))");
        stat.execute("alter table test add constraint x check (name > 'TEST.ID')");
        // previous versions of H2 used sql.indexOf(columnName)
        // to check if the column is referenced
        stat.execute("alter table test drop column id");
        stat.execute("drop table test");

        stat.execute("create table test(id int, name varchar(255))");
        stat.execute("alter table test add constraint x unique(id)");
        stat.execute("alter table test drop column id");
        stat.execute("drop table test");

    }

    /**
     * Tests a bug we used to have where altering the name of a column that had
     * a check constraint that referenced itself would result in not being able
     * to re-open the DB.
     */
    private void testAlterTableAlterColumnWithConstraint() throws SQLException {
        if (config.memory) {
            return;
        }
        stat.execute("create table test(id int check(id in (1,2)) )");
        stat.execute("alter table test alter id rename to id2");
        // disconnect and reconnect
        conn.close();
        conn = getConnection("alter");
        stat = conn.createStatement();
        stat.execute("insert into test values(1)");
        assertThrows(ErrorCode.CHECK_CONSTRAINT_VIOLATED_1, stat).
            execute("insert into test values(3)");
        stat.execute("drop table test");
    }

    private void testAlterTableDropIdentityColumn() throws SQLException {
        stat.execute("create table test(id int auto_increment, name varchar)");
        stat.execute("alter table test drop column id");
        ResultSet rs = stat.executeQuery("select * from INFORMATION_SCHEMA.SEQUENCES");
        assertFalse(rs.next());
        stat.execute("drop table test");

        stat.execute("create table test(id int auto_increment, name varchar)");
        stat.execute("alter table test drop column name");
        rs = stat.executeQuery("select * from INFORMATION_SCHEMA.SEQUENCES");
        assertTrue(rs.next());
        stat.execute("drop table test");
    }

    private void testAlterTableAlterColumn() throws SQLException {
        stat.execute("create table t(x varchar) as select 'x'");
        assertThrows(ErrorCode.DATA_CONVERSION_ERROR_1, stat).
                execute("alter table t alter column x int");
        stat.execute("drop table t");
        stat.execute("create table t(id identity, x varchar) as select null, 'x'");
        assertThrows(ErrorCode.DATA_CONVERSION_ERROR_1, stat).
                execute("alter table t alter column x int");
        stat.execute("drop table t");
    }

    private void testAlterTableAddColumnIfNotExists() throws SQLException {
        stat.execute("create table t(x varchar) as select 'x'");
        stat.execute("alter table t add if not exists x int");
        stat.execute("drop table t");
        stat.execute("create table t(x varchar) as select 'x'");
        stat.execute("alter table t add if not exists y int");
        stat.execute("select x, y from t");
        stat.execute("drop table t");
    }

    private void testAlterTableAddMultipleColumns() throws SQLException {
        stat.execute("create table t(x varchar) as select 'x'");
        stat.execute("alter table t add (y int, z varchar)");
        stat.execute("drop table t");
        stat.execute("create table t(x varchar) as select 'x'");
        stat.execute("alter table t add (y int)");
        stat.execute("drop table t");
    }

    // column and field names must be upper-case due to getMetaData sensitivity
    private void testAlterTableAddColumnBefore() throws SQLException {
        stat.execute("create table T(X varchar)");
        stat.execute("alter table T add Y int before X");
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet rs = dbMeta.getColumns(null, null, "T", null);
        assertTrue(rs.next());
        assertEquals("Y", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("X", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        stat.execute("drop table T");
    }

    // column and field names must be upper-case due to getMetaData sensitivity
    private void testAlterTableAddColumnAfter() throws SQLException {
        stat.execute("create table T(X varchar)");
        stat.execute("alter table T add Y int after X");
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet rs = dbMeta.getColumns(null, null, "T", null);
        assertTrue(rs.next());
        assertEquals("X", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("Y", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        stat.execute("drop table T");
    }

    private void testAlterTableAlterColumn2() throws SQLException {
        // ensure that increasing a VARCHAR columns length takes effect because
        // we optimize this case
        stat.execute("create table t(x varchar(2)) as select 'x'");
        stat.execute("alter table t alter column x varchar(20)");
        stat.execute("insert into t values('Hello')");
        stat.execute("drop table t");
    }

    private void testAlterTableModifyColumn() throws SQLException {
        stat.execute("create table t(x int)");
        stat.execute("alter table t modify column x varchar(20)");
        stat.execute("insert into t values('Hello')");
        stat.execute("drop table t");
    }
}
