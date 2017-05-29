/*
 * Copyright 2016 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Bernhard Haumacher
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Regression test for #227 ensuring that the database does not get corrupted if
 * a single access thread has been interrupted.
 */
public class TestInterrupt extends TestBase {

    private static final String DB = "interrupt";
    
	private Connection c1;
    private Statement s1;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.mvcc = true;
        test.test();
    }

    @Override
    public void test() throws SQLException, InterruptedException {
        testInterrupt();
        deleteDb(DB);
    }

    private void testInterrupt() throws SQLException, InterruptedException {
        deleteDb(DB);
        final String mode = config.mvcc ? "MVCC=TRUE;" : "";
		c1 = getConnection(DB + ";" + mode + "LOCK_TIMEOUT=10");
        s1 = c1.createStatement();
        c1.setAutoCommit(false);

        // update same key problem
        s1.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR, PRIMARY KEY(ID))");
        
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        c1.commit();
        
        final SQLException[] abort = {null};
        final int[] cnt = {1};
        
        Thread job = new Thread() {
        	@Override
			public void run() {
        		try {
        			tryRun();
				} catch (SQLException ex) {
					abort[0] = ex;
				}
        	}

			private void tryRun() throws SQLException {
				Connection con = getConnection(DB + ";" + mode + "LOCK_TIMEOUT=10");
				try {
					withConnection(con);
				} finally {
					con.close();
				}
			}

			private void withConnection(Connection con) throws SQLException {
				Statement s2 = con.createStatement();
				try {
					withStatement(con, s2);
				} finally {
					s2.close();
				}
			}

			private void withStatement(Connection con, Statement statement) throws SQLException {
				con.setAutoCommit(false);
				while (true) {
					int n = cnt[0] + 1;
					statement.execute("INSERT INTO TEST VALUES(" + n + ", 'Hello')");
					con.commit();
					cnt[0] = n;
				}
			}
        };
		job.start();

        Thread.sleep(10);
        assertResult("Hello", s1, "SELECT NAME FROM TEST WHERE ID=1");

        job.interrupt();
        job.join();
        
//        assertTrue("Job did not terminate with SQLException.", abort[0] != null);
//        assertTrue("Job did not terminate with interrupt.", abort[0].getCause() instanceof AccessInterruptedException);
        
        // Check that the database can still be accessed from a non-interrupted thread.
        assertResult("Hello", s1, "SELECT NAME FROM TEST WHERE ID=1");
        
        // Check that the data the interrupted job has last written can be accessed.
        assertResult("Hello", s1, "SELECT NAME FROM TEST WHERE ID=" + cnt[0] + "");
    }

}
