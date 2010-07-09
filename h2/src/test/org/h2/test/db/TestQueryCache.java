package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.h2.constant.SysProperties;
import org.h2.test.TestBase;

/**
 * Tests the query cache.
 */
public class TestQueryCache extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        System.setProperty("h2.queryCacheSize", "10");
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        if (SysProperties.QUERY_CACHE_SIZE <= 0) {
            return;
        }
        deleteDb("queryCache");
        Connection conn = getConnection("queryCache");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int, name varchar) as select x, space(100) from system_range(1, 1000)");
        PreparedStatement prep = conn.prepareStatement("select count(*) from test t1, test t2");
        long time;
        ResultSet rs;
        long first = 0;
        for (int i = 0; i < 4; i++) {
            // this should both ensure results are not re-used
            // stat.execute("set mode regular");
            // stat.execute("create table x()");
            // stat.execute("drop table x");
            time = System.currentTimeMillis();
            prep = conn.prepareStatement("select count(*) from test t1, test t2");
            rs = prep.executeQuery();
            rs = stat.executeQuery("select count(*) from test t1, test t2");
            rs.next();
            int c = rs.getInt(1);
            assertEquals(1000000, c);
            time = System.currentTimeMillis() - time;
            if (first == 0) {
                first = time;
            } else {
                assertSmaller(time, first);
            }
        }
        conn.close();
        deleteDb("queryCache");
    }

}
