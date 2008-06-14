/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.test.unit.SelfDestructor;
import org.h2.tools.Backup;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.IOUtils;

/**
 * Tests database recovery by destroying a process that writes to the database.
 */
public abstract class TestHalt extends TestBase {

    protected static final int OP_INSERT = 1, OP_DELETE = 2, OP_UPDATE = 4, OP_SELECT = 8;
    protected static final int FLAG_NO_DELAY = 1, FLAG_LOBS = 2;
    static final String DIR = TestBase.getTestDir("halt");
    private static final String DATABASE_NAME = "halt";
    private static final String TRACE_FILE_NAME = "haltTrace.trace.db";

    protected int operations, flags, value;
    protected Connection conn;
    protected Random random = new Random();

    private SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss ");
    private int errorId;
    private int sequenceId;

    abstract void testInit() throws Exception;

    abstract void testCheckAfterCrash() throws Exception;

    abstract void testWaitAfterAppStart() throws Exception;

    abstract void appStart() throws Exception;

    abstract void appRun() throws Exception;

    public void test() throws Exception {
        for (int i = 0;; i++) {
            operations = OP_INSERT | i;
            flags = i >> 4;
            // flags |= FLAG_NO_DELAY; // | FLAG_LOBS;
            try {
                runTest();
            } catch (Throwable t) {
                System.out.println("Error: " + t);
                t.printStackTrace();
            }
        }
    }

    Connection getConnection() throws Exception {
        Class.forName("org.h2.Driver");
        return DriverManager.getConnection("jdbc:h2:" + baseDir + "/halt", "sa", "sa");
    }

    protected void start(String[] args) throws Exception {
        if (args.length == 0) {
            runTest();
        } else {
            operations = Integer.parseInt(args[0]);
            flags = Integer.parseInt(args[1]);
            value = Integer.parseInt(args[2]);
            runRandom();
        }
    }

    private void runRandom() throws Exception {
        connect();
        try {
            traceOperation("connected, operations:" + operations + " flags:" + flags + " value:" + value);
            appStart();
            System.out.println("READY");
            System.out.println("READY");
            System.out.println("READY");
            appRun();
            traceOperation("done");
        } catch (Exception e) {
            trace("run", e);
        }
        disconnect();
    }

    private void connect() throws Exception {
        try {
            traceOperation("connecting");
            conn = getConnection();
        } catch (Exception e) {
            trace("connect", e);
            e.printStackTrace();
            throw e;
        }
    }

    protected void traceOperation(String s) {
        trace(s, null);
    }

    protected void trace(String s, Exception e) {
        FileWriter writer = null;
        try {
            File f = new File(baseDir + "/" + TRACE_FILE_NAME);
            f.getParentFile().mkdirs();
            writer = new FileWriter(f, true);
            PrintWriter w = new PrintWriter(writer);
            s = dateFormat.format(new Date()) + ": " + s;
            w.println(s);
            if (e != null) {
                e.printStackTrace(w);
            }
        } catch (IOException e2) {
            e2.printStackTrace();
        } finally {
            IOUtils.closeSilently(writer);
        }
    }

    private void runTest() throws Exception {
        traceOperation("delete database -----------------------------");
        DeleteDbFiles.execute(baseDir, DATABASE_NAME, true);
        new File(baseDir + "/" + TRACE_FILE_NAME).delete();

        connect();
        testInit();
        disconnect();
        for (int i = 0; i < 10; i++) {
            traceOperation("backing up " + sequenceId);
            Backup.execute(baseDir + "/haltSeq" + sequenceId + ".zip", baseDir, null, true);
            sequenceId++;
            // int operations = OP_INSERT;
            // OP_DELETE = 1, OP_UPDATE = 2, OP_SELECT = 4;
            // int flags = FLAG_NODELAY;
            // FLAG_NO_DELAY = 1, FLAG_AUTO_COMMIT = 2, FLAG_SMALL_CACHE = 4;
            int value = random.nextInt(1000);
            // for Derby and HSQLDB
            // String classPath = "-cp
            // .;D:/data/java/hsqldb.jar;D:/data/java/derby.jar";
            String selfDestruct = SelfDestructor.getPropertyString(60);
            String classPath = "";
            String[] command = {"java", selfDestruct, classPath, getClass().getName(), "" + operations, "" + flags, "" + value};
            traceOperation("start: " + command);
            Process p = Runtime.getRuntime().exec(command);
            InputStream in = p.getInputStream();
            OutputCatcher catcher = new OutputCatcher(in);
            catcher.start();
            String s = catcher.readLine(5 * 60 * 1000);
            if (s == null) {
                throw new IOException("No reply from process, command: " + command);
            } else if (s.startsWith("READY")) {
                traceOperation("got reply: " + s);
            }
            testWaitAfterAppStart();
            p.destroy();
            try {
                traceOperation("backing up " + sequenceId);
                Backup.execute(baseDir + "/haltSeq" + sequenceId + ".zip", baseDir, null, true);
                // new File(BASE_DIR + "/haltSeq" + (sequenceId-20) +
                // ".zip").delete();
                connect();
                testCheckAfterCrash();
            } catch (Exception e) {
                File zip = new File(baseDir + "/haltSeq" + sequenceId + ".zip");
                File zipId = new File(baseDir + "/haltSeq" + sequenceId + "-" + errorId + ".zip");
                zip.renameTo(zipId);
                printTime("ERROR: " + sequenceId + " " + errorId + " " + e.toString());
                e.printStackTrace();
                errorId++;
            } finally {
                sequenceId++;
                disconnect();
            }
        }
    }

    protected void disconnect() {
        try {
            traceOperation("disconnect");
            conn.close();
        } catch (Exception e) {
            trace("disconnect", e);
        }
    }

    public Connection getConnectionHSQLDB() throws Exception {
        File lock = new File("test.lck");
        while (lock.exists()) {
            lock.delete();
            System.gc();
        }
        Class.forName("org.hsqldb.jdbcDriver");
        return DriverManager.getConnection("jdbc:hsqldb:test", "sa", "");
    }

    public Connection getConnectionDerby() throws Exception {
        File lock = new File("test3/db.lck");
        while (lock.exists()) {
            lock.delete();
            System.gc();
        }
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        try {
            return DriverManager.getConnection("jdbc:derby:test3;create=true", "sa", "sa");
        } catch (SQLException e) {
            Exception e2 = e;
            do {
                e.printStackTrace();
                e = e.getNextException();
            } while (e != null);
            throw e2;
        }
    }

    void disconnectHSQLDB() {
        try {
            conn.createStatement().execute("SHUTDOWN");
        } catch (Exception e) {
            // ignore
        }
        // super.disconnect();
    }

    void disconnectDerby() {
        // super.disconnect();
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            DriverManager.getConnection("jdbc:derby:;shutdown=true", "sa", "sa");
        } catch (Exception e) {
            // ignore
        }
    }

    protected String getRandomString(int len) {
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < len; i++) {
            buff.append('a' + random.nextInt(20));
        }
        return buff.toString();
    }

    public TestBase init(TestAll conf) throws Exception {
        super.init(conf);
        baseDir = DIR;
        return this;
    }

}
