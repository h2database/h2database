/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.constant.SysProperties;
import org.h2.store.fs.FileSystemMemory;
import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.MathUtils;

/**
 * This test executes random SQL statements generated using the BNF tool.
 */
public class TestRandomSQL extends TestBase {

    private int dbId;
    private int seed;
    private boolean exitOnError = true;
    private BnfRandom bnfRandom;
    private int success, total;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    private void processException(String sql, SQLException e) {
        if (e.getSQLState().equals("HY000")) {
            TestBase.logError("new TestRandomSQL().init(test).testCase(" + seed + ");  // FAIL: " + e.toString() + " sql: " + sql, e);
            if (exitOnError) {
                System.exit(0);
            }
        }
    }

    private String getDatabaseName() {
        if (config.big) {
            return "dataRandomSQL/randomSql" + dbId;
        }
        return "memFS:/randomSql" + dbId;
        // return "dataRandomSQL/randomSql" + dbId+";TRACE_LEVEL_FILE=3";
    }

    private Connection connect() throws SQLException {
        while (true) {
            try {
                return getConnection(getDatabaseName());
            } catch (SQLException e) {
                dbId--;
                try {
                    deleteDb();
                } catch (Exception e2) {
                    // ignore
                }
                dbId++;
                try {
                    deleteDb();
                } catch (Exception e2) {
                    // ignore
                }
                dbId++;
                try {
                    deleteDb();
                } catch (SQLException e2) {
                    dbId++;
                    deleteDb();
                }
            }
        }

    }

    private void deleteDb() throws SQLException {
        String name = getDatabaseName();
        if (name.startsWith(FileSystemMemory.PREFIX)) {
            DeleteDbFiles.execute("memFS:/", name, true);
        } else {
            DeleteDbFiles.execute(baseDir, name, true);
        }
    }

    public TestBase init(TestAll conf) throws Exception {
        super.init(conf);
        bnfRandom = new BnfRandom();
        return this;
    }

    private void testWithSeed() throws SQLException {
        bnfRandom.setSeed(seed);
        Connection conn = null;
        try {
            conn = connect();
        } catch (SQLException e) {
            processException("connect", e);
            conn = connect();
        }
        Statement stat = conn.createStatement();

        for (int i = 0; i < bnfRandom.getStatementCount(); i++) {
            String sql = bnfRandom.getRandomSQL();
            if (sql != null) {
                try {
                    Thread.yield();
                    total++;
                    if (total % 100 == 0) {
                        printTime("total: " + total + " success: " + (100 * success / total) + "%");
                    }
                    stat.execute(sql);
                    success++;
                } catch (SQLException e) {
                    processException(sql, e);
                }
            }
        }
        try {
            conn.close();
        } catch (SQLException e) {
            processException("conn.close", e);
        }
    }

    public void testCase(int i) throws SQLException {
        String old = SysProperties.getScriptDirectory();
        try {
            System.setProperty(SysProperties.H2_SCRIPT_DIRECTORY, "dataScript/");
            seed = i;
            printTime("seed: " + seed);
            try {
                deleteDb();
            } catch (SQLException e) {
                processException("deleteDb", e);
            }
            testWithSeed();
        } finally {
            System.setProperty(SysProperties.H2_SCRIPT_DIRECTORY, old);
        }
        try {
            deleteDb();
        } catch (SQLException e) {
            processException("deleteDb", e);
        }
    }

    public void test() throws SQLException {
        if (config.networked) {
            return;
        }
        int len = getSize(2, 6);
        exitOnError = false;
        for (int a = 0; a < len; a++) {
            int s = MathUtils.randomInt(Integer.MAX_VALUE);
            testCase(s);
        }
    }

}
