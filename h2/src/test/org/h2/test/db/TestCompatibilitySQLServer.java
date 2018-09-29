package org.h2.test.db;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
        test.test();
    }

    @Override
    public void test() throws Exception {
        testDiscardTableHints();
        testUseIdentityAsAutoIncrementAlias();
    }

    private void testDiscardTableHints() throws SQLException {
        deleteDb("sqlserver");

        Connection conn = getConnection("sqlserver;MODE=MSSQLServer");
        Statement stat = conn.createStatement();

        stat.execute("create table parent(id int primary key, name varchar(255))");
        stat.execute("create table child(id int primary key, parent_id int, name varchar(255), foreign key (parent_id) references public.parent(id))");

        assertSupportedSyntax(stat, "select * from parent");
        assertSupportedSyntax(stat, "select * from parent with(nolock)");
        assertSupportedSyntax(stat, "select * from parent with(nolock, index = id)");
        assertSupportedSyntax(stat, "select * from parent with(nolock, index(id, name))");

        assertSupportedSyntax(stat, "select * from parent p join child ch on ch.parent_id = p.id");
        assertSupportedSyntax(stat, "select * from parent p with(nolock) join child ch with(nolock) on ch.parent_id = p.id");
        assertSupportedSyntax(stat, "select * from parent p with(nolock) join child ch with(nolock, index = id) on ch.parent_id = p.id");
        assertSupportedSyntax(stat, "select * from parent p with(nolock) join child ch with(nolock, index(id, name)) on ch.parent_id = p.id");
    }

    private void testUseIdentityAsAutoIncrementAlias() throws SQLException {
        deleteDb("sqlserver");

        Connection conn = getConnection("sqlserver;MODE=MSSQLServer");
        Statement stat = conn.createStatement();

        stat.execute("create table test(id int primary key identity, expected_id int)");
        stat.execute("insert into test (expected_id) VALUES (1), (2), (3)");

        final ResultSet results = stat.executeQuery("select * from test");
        while (results.next()) {
            assertEquals(results.getInt("expected_id"), results.getInt("id"));
        }

        assertSupportedSyntax(stat, "create table test2 (id int primary key not null identity)");
    }

    private void assertSupportedSyntax(Statement stat, String sql) {
        try {
            stat.execute(sql);
        } catch (SQLException ex) {
            fail("Failed to execute SQL statement: " + sql);
        }
    }
}
