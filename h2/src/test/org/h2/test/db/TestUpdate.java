package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.jdbc.JdbcSQLSyntaxErrorException;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

public class TestUpdate extends TestDb {

    private Statement stat;

    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        // test.config.traceTest = true;
        test.testFromMain();
    }

    @Override
    public void test() throws Exception {
        setup();
        testUpdateWithFromClause();
        testUpdateWithoutFromClause();
        testUpdateWithFromClauseThrowException();
        deleteDb("update");
    }

    public void setup() throws Exception {
        deleteDb("update");
        Connection conn = getConnection("update");
        stat = conn.createStatement();
        stat.execute("create table foo (id int, val varchar)");
        stat.execute("create table bar (id int, val varchar)");
    }

    public void testUpdateWithFromClause() throws Exception {
        stat.execute("delete from foo");
        stat.execute("insert into foo values (1, 'foo1'), (2,'foo2'), (3, 'foo3')");
        stat.execute("insert into bar values (1, 'bar1'), (3, 'bar3'), (4, 'bar4')");
        stat.execute("update foo set val = bar.val from bar where foo.id = bar.id");
        ResultSet rs = stat.executeQuery("select * from foo order by 1");
        rs.next();
        assertEquals("bar1", rs.getString(2));
        rs.next();
        assertEquals("foo2", rs.getString(2));
        rs.next();
        assertEquals("bar3", rs.getString(2));
        rs.close();
    }

    public void testUpdateWithFromClauseThrowException() throws JdbcSQLSyntaxErrorException {
        try {
            stat.execute("delete from foo");
            stat.execute("insert into foo values (1, 'foo1'), (2,'foo2'), (3, 'foo3')");
            stat.execute("insert into bar values (1, 'bar1'), (3, 'bar3'), (4, 'bar4')");
        } catch (SQLException e) {
            //
        }
        // can't use bar table's column name in set clause left side.
        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, stat,
                "update foo set bar.val = foo.val from bar where foo.id = bar.id");
    }

    public void testUpdateWithoutFromClause() throws Exception {
        stat.execute("delete from foo");
        stat.execute("insert into foo values (1, 'foo1'), (2,'foo2'), (3, 'foo3')");
        stat.execute("insert into bar values (1, 'bar1'), (3, 'bar3')");
        stat.execute("update foo set val = 'updated2' where foo.id = 2");
        ResultSet rs = stat.executeQuery("select * from foo order by 1");
        rs.next();
        assertEquals("foo1", rs.getString(2));
        rs.next();
        assertEquals("updated2", rs.getString(2));
        rs.next();
        assertEquals("foo3", rs.getString(2));
        rs.close();
    }
}
