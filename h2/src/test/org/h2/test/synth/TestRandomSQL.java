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
import java.util.ArrayList;

import org.h2.bnf.Bnf;
import org.h2.bnf.RuleHead;
import org.h2.constant.SysProperties;
import org.h2.store.fs.FileSystemMemory;
import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.RandomUtils;

/**
 * This test executes random SQL statements generated using the BNF tool.
 */
public class TestRandomSQL extends TestBase {

    private int dbId;
    private boolean showSQL;
    private ArrayList<RuleHead> statements;
    private int seed;
    private boolean exitOnError = true;
    private Bnf bnfSyntax;
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
        bnfSyntax = Bnf.getInstance(null);
        bnfSyntax.linkStatements();
        statements = bnfSyntax.getStatements();

        // go backwards so we can append at the end
        for (int i = statements.size() - 1; i >= 0; i--) {
            RuleHead r = statements.get(i);
            String topic = r.getTopic();
            int weight = 0;
            if (topic.equals("select")) {
                weight = 10;
            } else if (topic.equals("createtable")) {
                weight = 20;
            } else if (topic.equals("insert")) {
                weight = 5;
            } else if (topic.startsWith("update")) {
                weight = 3;
            } else if (topic.startsWith("delete")) {
                weight = 3;
            } else if (topic.startsWith("drop")) {
                weight = 2;
            }
            if (showSQL) {
                System.out.println(r.getTopic());
            }
            for (int j = 0; j < weight; j++) {
                statements.add(r);
            }
        }
        return this;
    }

    private void testWithSeed(Bnf bnf) throws SQLException {
        bnf.getRandom().setSeed(seed);
        Connection conn = null;
        try {
            conn = connect();
        } catch (SQLException e) {
            processException("connect", e);
            conn = connect();
        }
        Statement stat = conn.createStatement();
        for (int i = 0; i < statements.size(); i++) {
            int sid = bnf.getRandom().nextInt(statements.size());
            RuleHead r = statements.get(sid);
            String rand = r.getRule().random(bnf, 0).trim();
            if (rand.length() > 0) {
                try {
                    Thread.yield();
                    if (rand.indexOf("TRACE_LEVEL_") < 0 && rand.indexOf("COLLATION") < 0
                            && rand.indexOf("SCRIPT ") < 0 && rand.indexOf("CSVWRITE") < 0
                            && rand.indexOf("BACKUP") < 0 && rand.indexOf("DB_CLOSE_DELAY") < 0) {
                        if (showSQL) {
                            System.out.println(i + "  " + rand);
                        }
                        total++;
                        if (total % 100 == 0) {
                            printTime("total: " + total + " success: " + (100 * success / total) + "%");
                        }
                        stat.execute(rand);
                        success++;
                    }
                } catch (SQLException e) {
                    processException(rand, e);
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
            testWithSeed(bnfSyntax);
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
        showSQL = false;
        for (int a = 0; a < len; a++) {
            int s = RandomUtils.nextInt(Integer.MAX_VALUE);
            testCase(s);
        }
    }

}
