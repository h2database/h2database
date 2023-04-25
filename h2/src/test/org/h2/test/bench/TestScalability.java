/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.bench.Database.Measurement;

/**
 * Used to compare scalability between the old engine and the new MVStore
 * engine. Mostly it runs BenchB with various numbers of threads.
 */
public class TestScalability implements Database.DatabaseTest {

    /**
     * Whether data should be collected.
     */
    boolean collect;

    /**
     * The flag used to enable or disable trace messages.
     */
    boolean trace;

    /**
     * This method is called when executing this sample application.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new TestScalability().test(args);
    }

    private static Connection getResultConnection() throws SQLException {
        org.h2.Driver.load();
        return DriverManager.getConnection("jdbc:h2:./data/results");
    }

    private static void openResults() throws SQLException {
        try (Connection conn = getResultConnection();
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "CREATE TABLE IF NOT EXISTS RESULTS(TESTID INT, " +
                            "TEST VARCHAR, UNIT VARCHAR, DBID INT, " +
                            "DB VARCHAR, TCNT INT, RESULT VARCHAR)");
        }
    }

    private void test(String... args) throws Exception {
        int dbId = -1;
        boolean exit = false;
        String out = "scalability.html";
        int size = 400;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("-db".equals(arg)) {
                dbId = Integer.parseInt(args[++i]);
            } else if ("-init".equals(arg)) {
                FileUtils.deleteRecursive("data", true);
            } else if ("-out".equals(arg)) {
                out = args[++i];
            } else if ("-trace".equals(arg)) {
                trace = true;
            } else if ("-exit".equals(arg)) {
                exit = true;
            } else if ("-size".equals(arg)) {
                size = Integer.parseInt(args[++i]);
            }
        }

        Properties prop = loadProperties();

        ArrayList<RunSequence> dbs = new ArrayList<>();
        for (int id = 0; id < 100; id++) {
            if (dbId != -1 && id != dbId) {
                continue;
            }
            String dbString = prop.getProperty("db" + id);
            if (dbString != null) {
                Database db = Database.parse(this, id, dbString, prop);
                if (db != null) {
                    int runCount = 8;
                    String valueStr = prop.getProperty("runCount" + id);
                    if (valueStr != null) {
                        runCount = Integer.parseInt(valueStr);
                    }
                    dbs.add(new RunSequence(db, runCount));
                }
            }
        }

        BenchB test = new BenchB() {
            // Since we focus on scalability here, lets emphasize multi-threaded
            // part of the test (transactions) and minimize impact of the init.
            @Override
            protected int getTransactionsPerClient(int size) {
                return size * 8;
            }
        };
        testAll(dbs, test, size);

        List<Measurement> results = dbs.get(0).results.get(0);
        try (Connection conn = getResultConnection()) {
            openResults();
            try (PreparedStatement prep = conn.prepareStatement(
                    "INSERT INTO RESULTS(TESTID, " +
                    "TEST, UNIT, DBID, DB, TCNT, RESULT) VALUES(?, ?, ?, ?, ?, ?, ?)")) {
                for (int i = 0; i < results.size(); i++) {
                    Measurement res = results.get(i);
                    prep.setInt(1, i);
                    prep.setString(2, res.name);
                    prep.setString(3, res.unit);
                    for (RunSequence runSequence : dbs) {
                        Database db = runSequence.database;
                        int threadCount = 1;
                        for (List<Measurement> result : runSequence.results) {
                            if (result.size() > i) {
                                Measurement measurement = result.get(i);
                                prep.setInt(4, db.getId());
                                prep.setString(5, db.getName());
                                prep.setInt(6, threadCount);
                                prep.setString(7, String.valueOf(measurement.value));
                                prep.execute();
                                threadCount <<= 1;
                            }
                        }
                    }
                }
            }

            try (Statement stat = conn.createStatement();
                    PrintWriter writer = new PrintWriter(new FileWriter(out));
                    ResultSet rs = stat.executeQuery(
                        "CALL '<table border=\"1\"><tr><th rowspan=\"2\">Test Case</th>" +
                        "<th rowspan=\"2\">Unit</th>' " +
                        "|| (SELECT GROUP_CONCAT('<th colspan=\"' || COLSPAN || '\">' || TCNT || '</th>' " +
                        "ORDER BY TCNT SEPARATOR '') FROM " +
                        "(SELECT TCNT, COUNT(*) COLSPAN FROM (SELECT DISTINCT DB, TCNT FROM RESULTS) GROUP BY TCNT))" +
                        "|| '</tr>' || CHAR(10) " +
                        "|| '<tr>' || (SELECT GROUP_CONCAT('<th>' || DB || '</th>' ORDER BY TCNT, DB SEPARATOR '')" +
                        " FROM (SELECT DISTINCT DB, TCNT FROM RESULTS)) || '</tr>' || CHAR(10) " +
                        "|| (SELECT GROUP_CONCAT('<tr><td>' || TEST || '</td><td>' || UNIT || '</td>' || ( " +
                        "SELECT GROUP_CONCAT('<td>' || RESULT || '</td>' ORDER BY TCNT,DB SEPARATOR '')" +
                        " FROM RESULTS R2 WHERE R2.TESTID = R1.TESTID) || '</tr>' " +
                        "ORDER BY TESTID SEPARATOR CHAR(10)) FROM " +
                        "(SELECT DISTINCT TESTID, TEST, UNIT FROM RESULTS) R1)" +
                        "|| '</table>'")) {
                rs.next();
                String result = rs.getString(1);
                writer.println(result);
            }
        }

        if (exit) {
            System.exit(0);
        }
    }

    private void testAll(ArrayList<RunSequence> runSequences, BenchB test, int size) throws Exception {
        Database lastDb = null;
        Connection conn = null;
        for (RunSequence runSequence : runSequences) {
            Database db = runSequence.database;
            try {
                if (lastDb != null) {
                    conn.close();
                    lastDb.stopServer();
                    Thread.sleep(1000);
                    // calls garbage collection
                    TestBase.getMemoryUsed();
                }
                String dbName = db.getName();
                System.out.println("------------------");
                System.out.println("Testing the performance of " + dbName);
                db.startServer();
                // hold one connection open during the whole test to keep database up
                conn = db.openNewConnection();
                test.init(db, size);

                for (int runNo = 0, threadCount = 1; runNo < runSequence.runCount; runNo++, threadCount <<= 1) {
                    System.out.println("Testing the performance of " + dbName
                            + " (" + threadCount + " threads)");

                    DatabaseMetaData meta = conn.getMetaData();
                    System.out.println(" " + meta.getDatabaseProductName() + " " +
                            meta.getDatabaseProductVersion());
                    test.setThreadCount(threadCount);

                    test.runTest();
                    test.runTest();
                    db.reset();
                    collect = true;
                    test.runTest();

                    int executedStatements = db.getExecutedStatements();
                    int totalTime = db.getTotalTime();
                    int totalGCTime = db.getTotalGCTime();
                    db.log("Executed statements", "#", executedStatements);
                    db.log("Total time", "ms", totalTime);
                    int statPerSec = (int) (executedStatements * 1000L / totalTime);
                    db.log("Statements per second", "#/s", statPerSec);
                    collect = false;
                    System.out.println("Statements per second: " + statPerSec);
                    System.out.println("GC overhead: " + (100 * totalGCTime / totalTime) + "%");
                    ArrayList<Measurement> measurements = db.reset();
                    runSequence.results.add(measurements);
                }
            } catch (Throwable ex) {
                ex.printStackTrace();
            } finally {
                lastDb = db;
            }
        }
        if (lastDb != null) {
            conn.close();
            lastDb.stopServer();
        }
    }

    /**
     * Print a message to system out if trace is enabled.
     *
     * @param s the message
     */
    @Override
    public void trace(String s) {
        if (trace) {
            System.out.println(s);
        }
    }

    @Override
    public boolean isCollect() {
        return collect;
    }

    private static final class RunSequence
    {
        final Database database;
        final int runCount;
        final List<List<Measurement>> results = new ArrayList<>();

        public RunSequence(Database dataBase, int runCount) {
            this.database = dataBase;
            this.runCount = runCount;
        }
    }
}
