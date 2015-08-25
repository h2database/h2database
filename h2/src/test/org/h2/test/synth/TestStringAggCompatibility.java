package org.h2.test.synth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.test.TestBase;

/**
 * Test for check compatibility with posgresql function string_agg()
 *
 * */
public class TestStringAggCompatibility extends TestBase {

    public static final String STRING_AGG_DB = "stringAgg";

    private Connection conn;

    public static void main(String[] args) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        deleteDb(STRING_AGG_DB);

        conn = getConnection(STRING_AGG_DB);

        prepareDb();





        testWhenOrderByMissing();
        testWithOrderBy();
    }

    private void testWithOrderBy() throws SQLException {
        ResultSet result = query("select string_agg(b, ', ' order by b desc) from stringAgg group by a; ");

        assertTrue(result.next());
        assertEquals("3, 2, 1", result.getString(1));
    }

    private void testWhenOrderByMissing() throws SQLException {
        ResultSet result = query("select string_agg(b, ', ') from stringAgg group by a; ");

        assertTrue(result.next());
        assertEquals("1, 2, 3", result.getString(1));
    }


    private ResultSet query(String q) throws SQLException {
        PreparedStatement st = conn.prepareStatement(q);

        st.execute();

        return st.getResultSet();
    }

    private void prepareDb() throws SQLException {
        exec("create table stringAgg(\n" +
                " a int not null,\n" +
                " b varchar(50) not null\n" +
                ");");

           exec("insert into stringAgg values(1, '1')");
           exec("insert into stringAgg values(1, '2')");
           exec("insert into stringAgg values(1, '3')");

    }

    private void exec(String sql) throws SQLException {
        conn.prepareStatement(sql).execute();
    }
}
