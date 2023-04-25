/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.engine.SessionLocal;
import org.h2.jdbc.JdbcConnection;
import org.h2.schema.Sequence;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Test ALTER statements.
 */
public class TestAlter extends TestDb {

    private Connection conn;
    private Statement stat;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        deleteDb(getTestName());
        conn = getConnection(getTestName());
        stat = conn.createStatement();
        testAlterTableRenameConstraint();
        testAlterTableDropColumnWithReferences();
        testAlterTableDropMultipleColumns();
        testAlterTableAddColumnIdentity();
        testAlterTableDropIdentityColumn();
        testAlterTableAddColumnIfNotExists();
        testAlterTableAddMultipleColumns();
        testAlterTableAddColumnBefore();
        testAlterTableAddColumnAfter();
        testAlterTableAddMultipleColumnsBefore();
        testAlterTableAddMultipleColumnsAfter();
        conn.close();
        deleteDb(getTestName());
    }

    private void testAlterTableDropColumnWithReferences() throws SQLException {
        stat.execute("create table parent(id int primary key, b int)");
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

    private void testAlterTableDropMultipleColumns() throws SQLException {
        stat.execute("create table test(id int, b varchar, c int, d int)");
        stat.execute("alter table test drop column b, c");
        stat.execute("alter table test drop d");
        stat.execute("drop table test");
        // Test-Case: Same as above but using brackets (Oracle style)
        stat.execute("create table test(id int, b varchar, c int, d int)");
        stat.execute("alter table test drop column (b, c)");
        assertThrows(ErrorCode.COLUMN_NOT_FOUND_1, stat).
            execute("alter table test drop column b");
        stat.execute("alter table test drop (d)");
        stat.execute("drop table test");
        // Test-Case: Error if dropping all columns
        stat.execute("create table test(id int, name varchar, name2 varchar)");
        assertThrows(ErrorCode.CANNOT_DROP_LAST_COLUMN, stat).
            execute("alter table test drop column id, name, name2");
        stat.execute("drop table test");
    }

    private void testAlterTableRenameConstraint() throws SQLException {
        stat.execute("create table test(id int, name varchar(255))");
        stat.execute("alter table test add constraint x check (id > name)");
        stat.execute("alter table test rename constraint x to x2");
        stat.execute("drop table test");
    }

    private void testAlterTableDropIdentityColumn() throws SQLException {
        Session iface = ((JdbcConnection) stat.getConnection()).getSession();
        if (!(iface instanceof SessionLocal)) {
            return;
        }
        Collection<Sequence> allSequences = ((SessionLocal) iface).getDatabase().getMainSchema().getAllSequences();
        stat.execute("create table test(id int auto_increment, name varchar)");
        stat.execute("alter table test drop column id");
        assertEquals(0, allSequences.size());
        stat.execute("drop table test");

        stat.execute("create table test(id int auto_increment, name varchar)");
        stat.execute("alter table test drop column name");
        assertEquals(1, allSequences.size());
        stat.execute("drop table test");
    }

    private void testAlterTableAddColumnIdentity() throws SQLException {
        stat.execute("create table t(x varchar)");
        stat.execute("alter table t add id bigint generated by default as identity(start with 5 increment by 5)"
                + " default on null");
        stat.execute("insert into t values (null, null)");
        stat.execute("insert into t values (null, null)");
        ResultSet rs = stat.executeQuery("select id from t order by id");
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        assertFalse(rs.next());
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
    private void testAlterTableAddMultipleColumnsBefore() throws SQLException {
        stat.execute("create table T(X varchar)");
        stat.execute("alter table T add (Y int, Z int) before X");
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet rs = dbMeta.getColumns(null, null, "T", null);
        assertTrue(rs.next());
        assertEquals("Y", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("Z", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("X", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        stat.execute("drop table T");
    }

    // column and field names must be upper-case due to getMetaData sensitivity
    private void testAlterTableAddMultipleColumnsAfter() throws SQLException {
        stat.execute("create table T(X varchar)");
        stat.execute("alter table T add (Y int, Z int) after X");
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet rs = dbMeta.getColumns(null, null, "T", null);
        assertTrue(rs.next());
        assertEquals("X", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("Y", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("Z", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        stat.execute("drop table T");
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

}
