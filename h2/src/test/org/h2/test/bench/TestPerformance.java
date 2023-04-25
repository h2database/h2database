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
import java.util.Properties;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.JdbcUtils;

/**
 * The main controller class of the benchmark application.
 * To run the benchmark, call the main method of this class.
 */
public class TestPerformance implements Database.DatabaseTest {

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
        new TestPerformance().test(args);
    }

    private static Connection getResultConnection() throws SQLException {
        org.h2.Driver.load();
        return DriverManager.getConnection("jdbc:h2:./data/results");
    }

    private static void openResults() throws SQLException {
        Connection conn = null;
        Statement stat = null;
        try {
            conn = getResultConnection();
            stat = conn.createStatement();
            stat.execute(
                    "CREATE TABLE IF NOT EXISTS RESULTS(" +
                    "TESTID INT, TEST VARCHAR, " +
                    "UNIT VARCHAR, DBID INT, DB VARCHAR, RESULT VARCHAR)");
        } finally {
            JdbcUtils.closeSilently(stat);
            JdbcUtils.closeSilently(conn);
        }
    }

    private void test(String... args) throws Exception {
        int dbId = -1;
        boolean exit = false;
        String out = "benchmark.html";
        Properties prop = loadProperties();
        int size = Integer.parseInt(prop.getProperty("size"));
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
        ArrayList<Database> dbs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            if (dbId != -1 && i != dbId) {
                continue;
            }
            String dbString = prop.getProperty("db" + i);
            if (dbString != null) {
                Database db = Database.parse(this, i, dbString, prop);
                if (db != null) {
                    dbs.add(db);
                }
            }
        }
        ArrayList<Bench> tests = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String testString = prop.getProperty("test" + i);
            if (testString != null) {
                Bench bench = (Bench) Class.forName(testString).getDeclaredConstructor().newInstance();
                tests.add(bench);
            }
        }
        testAll(dbs, tests, size);
        collect = false;
        if (dbs.isEmpty()) {
            return;
        }
        ArrayList<Database.Measurement> results = dbs.get(0).getResults();
        try (Connection conn = getResultConnection()) {
            openResults();
            try (PreparedStatement prep = conn.prepareStatement(
                        "INSERT INTO RESULTS(TESTID, TEST, " +
                        "UNIT, DBID, DB, RESULT) VALUES(?, ?, ?, ?, ?, ?)")) {
                for (int i = 0; i < results.size(); i++) {
                    Database.Measurement res = results.get(i);
                    prep.setInt(1, i);
                    prep.setString(2, res.name);
                    prep.setString(3, res.unit);
                    for (Database db : dbs) {
                        prep.setInt(4, db.getId());
                        prep.setString(5, db.getName());
                        Database.Measurement measurement = db.getResults().get(i);
                        prep.setString(6, String.valueOf(measurement.value));
                        prep.execute();
                    }
                }
            }

            try (Statement stat = conn.createStatement();
                PrintWriter writer = new PrintWriter(new FileWriter(out));
                ResultSet rs = stat.executeQuery(
                    "CALL '<table><tr><th>Test Case</th><th>Unit</th>' " +
                    "|| (SELECT GROUP_CONCAT('<th>' || DB || '</th>' " +
                    "ORDER BY DBID SEPARATOR '') FROM " +
                    "(SELECT DISTINCT DBID, DB FROM RESULTS))" +
                    "|| '</tr>' || CHAR(10) " +
                    "|| (SELECT GROUP_CONCAT('<tr><td>' || TEST || " +
                    "'</td><td>' || UNIT || '</td>' || ( " +
                    "SELECT GROUP_CONCAT('<td>' || RESULT || '</td>' " +
                    "ORDER BY DBID SEPARATOR '') FROM RESULTS R2 WHERE " +
                    "R2.TESTID = R1.TESTID) || '</tr>' " +
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

    private void testAll(ArrayList<Database> dbs, ArrayList<Bench> tests,
            int size) throws Exception {
        for (int i = 0; i < dbs.size(); i++) {
            if (i > 0) {
                Thread.sleep(1000);
            }
            // calls garbage collection
            TestBase.getMemoryUsed();
            Database db = dbs.get(i);
            System.out.println();
            System.out.println("Testing the performance of " + db.getName());
            db.startServer();
            Connection conn = db.openNewConnection();
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("Database: " + meta.getDatabaseProductName() + " " + meta.getDatabaseProductVersion());
            System.out.println("Driver: " + meta.getDriverName() + " " + meta.getDriverVersion());
            runDatabase(db, tests, 1);
            runDatabase(db, tests, 1);
            db.reset();
            collect = true;
            runDatabase(db, tests, size);
            conn.close();
            db.log("Executed statements", "#", db.getExecutedStatements());
            db.log("Total time", "ms", db.getTotalTime());
            System.out.println("Total time: " + db.getTotalTime() + " ms");
            int statPerSec = (int) (db.getExecutedStatements() * 1000L / db.getTotalTime());
            db.log("Statements per second", "#/s", statPerSec);
            System.out.println("Statements per second: " + statPerSec);
            System.out.println("GC overhead: " + (100 * db.getTotalGCTime() / db.getTotalTime()) + "%");
            collect = false;
            db.stopServer();
        }
    }

    private static void runDatabase(Database db, ArrayList<Bench> tests,
            int size) throws Exception {
        for (Bench bench : tests) {
            runTest(db, bench, size);
        }
    }

    private static void runTest(Database db, Bench bench, int size)
            throws Exception {
        bench.init(db, size);
        bench.runTest();
    }

    @Override
    public void trace(String msg) {
        if (trace) {
            System.out.println(msg);
        }
    }

    @Override
    public boolean isCollect() {
        return collect;
    }

}
