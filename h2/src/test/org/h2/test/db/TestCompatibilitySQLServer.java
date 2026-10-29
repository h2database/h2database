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
 * Test MSSQLServer compatibility mode.
 */
public class TestCompatibilitySQLServer extends TestDb {

    /**
     * Run just this test.
     *
     * @param s ignored
     */
    public static void main(String... s) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.testFromMain();
    }

    @Override
    public void test() throws Exception {
        deleteDb("sqlserver");

        final Connection conn = getConnection("sqlserver;MODE=MSSQLServer");
        try {
            testDiscardTableHints(conn);
            testDiscardDmlTableHints(conn);
            testPrimaryKeyIdentity(conn);
        } finally {
            conn.close();
            deleteDb("sqlserver");
        }
    }

    private void testDiscardDmlTableHints(Connection conn) throws SQLException {
        try (Statement stat = conn.createStatement()) {
            stat.execute("create table target(id int primary key, val int)");
            stat.execute("create table source(id int primary key, val int)");
            stat.execute("insert into target values (1, 10), (2, 20)");
            stat.execute("insert into source values (1, 30), (3, 40)");

            assertEquals(1, stat.executeUpdate("delete from target with (rowlock) where id = 2"));
            assertSingleValue(stat, "select count(*) from target", 1);

            assertEquals(2, stat.executeUpdate("merge into target with (holdlock) as t "
                    + "using source as s on t.id = s.id "
                    + "when matched then update set val = s.val "
                    + "when not matched then insert (id, val) values (s.id, s.val)"));
            assertSingleValue(stat, "select val from target where id = 1", 30);
            assertSingleValue(stat, "select val from target where id = 3", 40);

            assertEquals(1, stat.executeUpdate("merge into target with (holdlock, index = id) "
                    + "using (values (1, 31)) as s(id, val) on target.id = s.id "
                    + "when matched then update set val = s.val"));
            assertSingleValue(stat, "select val from target where id = 1", 31);

            assertEquals(1, stat.executeUpdate("merge into target "
                    + "with s(id, val) as (values (4, 50)) select id, val from s"));
            assertSingleValue(stat, "select val from target where id = 4", 50);

            stat.execute("set mode regular");
            assertThrows(ErrorCode.SYNTAX_ERROR_1, stat).executeUpdate("delete from target with (rowlock) where id = 1");
            assertThrows(ErrorCode.SYNTAX_ERROR_2, stat).executeUpdate("merge into target with (holdlock) as t "
                    + "using source as s on t.id = s.id when matched then update set val = s.val");
            stat.execute("set mode MSSQLServer");
        }
    }

    private void testDiscardTableHints(Connection conn) throws SQLException {
        final Statement stat = conn.createStatement();

        stat.execute("create table parent(id int primary key, name varchar(255))");
        stat.execute("create table child(" +
                            "id int primary key, " +
                            "parent_id int, " +
                            "name varchar(255), " +
                            "foreign key (parent_id) references public.parent(id))");

        stat.execute("select * from parent");
        stat.execute("select * from parent with(nolock)");
        stat.execute("select * from parent with(nolock, index = id)");
        stat.execute("select * from parent with(nolock, index(id, name))");

        stat.execute("select * from parent p " +
                            "join child ch on ch.parent_id = p.id");
        stat.execute("select * from parent p with(nolock) " +
                            "join child ch with(nolock) on ch.parent_id = p.id");
        stat.execute("select * from parent p with(nolock) " +
                            "join child ch with(nolock, index = id) on ch.parent_id = p.id");
        stat.execute("select * from parent p with(nolock) " +
                            "join child ch with(nolock, index(id, name)) on ch.parent_id = p.id");
    }

    private void testPrimaryKeyIdentity(Connection conn) throws SQLException {
        final Statement stat = conn.createStatement();

        // IDENTITY after PRIMARY KEY is an undocumented syntax of MS SQL
        stat.execute("create table test(id int primary key identity, expected_id int)");
        stat.execute("insert into test (expected_id) VALUES (1), (2), (3)");

        final ResultSet results = stat.executeQuery("select * from test");
        while (results.next()) {
            assertEquals(results.getInt("expected_id"), results.getInt("id"));
        }

        stat.execute("create table test2 (id int primary key not null identity)");
    }

}
