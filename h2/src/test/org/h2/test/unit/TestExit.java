/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.h2.api.DatabaseEventListener;
import org.h2.test.TestBase;
import org.h2.test.utils.SelfDestructor;

/**
 * Tests the flag db_close_on_exit.
 * A new process is started.
 */
public class TestExit extends TestBase implements DatabaseEventListener {

    public static Connection conn;

    static final int OPEN_WITH_CLOSE_ON_EXIT = 1, OPEN_WITHOUT_CLOSE_ON_EXIT = 2;

    public void test() throws Exception {
        if (config.codeCoverage || config.networked) {
            return;
        }
        deleteDb("exit");
        String selfDestruct = SelfDestructor.getPropertyString(60);
        String[] procDef = { "java", selfDestruct,
                "-cp", getClassPath(),
                getClass().getName(), "" + OPEN_WITH_CLOSE_ON_EXIT };
        Process proc = Runtime.getRuntime().exec(procDef);
        while (true) {
            int ch = proc.getErrorStream().read();
            if (ch < 0) {
                break;
            }
            System.out.print((char) ch);
        }
        while (true) {
            int ch = proc.getInputStream().read();
            if (ch < 0) {
                break;
            }
            System.out.print((char) ch);
        }
        proc.waitFor();
        Thread.sleep(100);
        if (!getClosedFile().exists()) {
            fail("did not close database");
        }
        procDef = new String[] { "java",
                "-cp", getClassPath(), getClass().getName(),
                "" + OPEN_WITHOUT_CLOSE_ON_EXIT };
        proc = Runtime.getRuntime().exec(procDef);
        proc.waitFor();
        Thread.sleep(100);
        if (getClosedFile().exists()) {
            fail("closed database");
        }
        deleteDb("exit");
    }

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws SQLException {
        SelfDestructor.startCountdown(60);
        if (args.length == 0) {
            System.exit(1);
        }
        int action = Integer.parseInt(args[0]);
        TestExit app = new TestExit();
        app.execute(action);
    }

    private void execute(int action) throws SQLException {
        org.h2.Driver.load();
        String url = "";
        switch (action) {
        case OPEN_WITH_CLOSE_ON_EXIT:
            url = "jdbc:h2:" + baseDir + "/exit;database_event_listener='" + getClass().getName()
                    + "';db_close_on_exit=true";
            break;
        case OPEN_WITHOUT_CLOSE_ON_EXIT:
            url = "jdbc:h2:" + baseDir + "/exit;database_event_listener='" + getClass().getName()
                    + "';db_close_on_exit=false";
            break;
        default:
        }
        conn = open(url);
        Connection conn2 = open(url);
        conn2.close();
    }

    private static Connection open(String url) throws SQLException {
        getClosedFile().delete();
        return DriverManager.getConnection(url, "sa", "");
    }

    public void diskSpaceIsLow() {
        // nothing to do
    }

    public void exceptionThrown(SQLException e, String sql) {
        // nothing to do
    }

    public void closingDatabase() {
        try {
            getClosedFile().createNewFile();
        } catch (IOException e) {
            TestBase.logError("error", e);
        }
    }

    private static File getClosedFile() {
        return new File(baseDir + "/closed.txt");
    }

    public void setProgress(int state, String name, int x, int max) {
        // nothing to do
    }

    public void init(String url) {
        // nothing to do
    }

    public void opened() {
        // nothing to do
    }

}
