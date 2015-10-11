/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.command.dml.OptimizerHints;
import org.h2.test.TestBase;

/**
 * Test for optimizer hints.
 *
 * @author Sergi Vladykin
 */
public class TestOptimizerHints extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        if (config.networked) {
            return;
        }
        deleteDb("testOptimizerHints");
        Connection conn = getConnection("testOptimizerHints");
        Statement s = conn.createStatement();

        s.execute("create table t1(id int)");
        s.execute("create table t2(id int, ref_id int)");

        s.execute("insert into t1 values(1),(2),(3)");
        s.execute("insert into t2 values(1,2),(2,3),(3,4),(4,6),(5,1),(6,4)");

        s.execute("create unique index idx1_id on t1(id)");
        s.execute("create index idx2_id on t2(id)");
        s.execute("create index idx2_ref_id on t2(ref_id)");

        enableJoinReordering(false);

        try {
            String plan;

            plan = plan(s, "select * from t1, t2 where t1.id = t2.ref_id");
            assertTrue(plan, plan.contains("INNER JOIN PUBLIC.T2"));

            plan = plan(s, "select * from t2, t1 where t1.id = t2.ref_id");
            assertTrue(plan, plan.contains("INNER JOIN PUBLIC.T1"));

            plan = plan(s, "select * from t2, t1 where t1.id = 1");
            assertTrue(plan, plan.contains("INNER JOIN PUBLIC.T1"));

            plan = plan(s, "select * from t2, t1 where t1.id = t2.ref_id and t2.id = 1");
            assertTrue(plan, plan.contains("INNER JOIN PUBLIC.T1"));

            plan = plan(s, "select * from t1, t2 where t1.id = t2.ref_id and t2.id = 1");
            assertTrue(plan, plan.contains("INNER JOIN PUBLIC.T2"));
        } finally {
            enableJoinReordering(true);
        }
        deleteDb("testOptimizerHints");
    }

    /**
     * @param enable Enabled.
     */
    private void enableJoinReordering(boolean enable) {
        OptimizerHints hints = new OptimizerHints();
        hints.setJoinReorderEnabled(enable);
        OptimizerHints.set(hints);
    }

    /**
     * @param s Statement.
     * @param query Query.
     * @return Plan.
     * @throws SQLException If failed.
     */
    private String plan(Statement s, String query) throws SQLException {
        ResultSet rs = s.executeQuery("explain " + query);
        assertTrue(rs.next());
        String plan = rs.getString(1);
        rs.close();
        return plan;
    }
}
