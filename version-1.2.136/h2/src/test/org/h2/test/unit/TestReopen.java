/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import org.h2.constant.ErrorCode;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.store.fs.FileSystem;
import org.h2.test.TestBase;
import org.h2.test.utils.Recorder;
import org.h2.test.utils.RecordingFileSystem;
import org.h2.util.IOUtils;
import org.h2.util.New;
import org.h2.util.Profiler;

/**
 * A test that calls another test, and after each write operation to the
 * database file, it copies the file, and tries to reopen it.
 */
public class TestReopen extends TestBase implements Recorder {

    private static final int MAX_FILE_SIZE = 8 * 1024 * 1024;
    private String testDatabase = "memFS:" + TestBase.BASE_TEST_DIR + "/reopen";
    private long lastCheck;
    private int writeCount = Integer.parseInt(System.getProperty("reopenOffset", "0"));
    private int testEvery = 1 << Integer.parseInt(System.getProperty("reopenShift", "8"));
    private int verifyCount;
    private HashSet<String> knownErrors = New.hashSet();
    private volatile boolean testing;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        System.setProperty("h2.delayWrongPasswordMin", "0");
        RecordingFileSystem.register();
        RecordingFileSystem.setRecorder(this);
        config.record = true;

        long time = System.currentTimeMillis();
        Profiler p = new Profiler();
        p.startCollecting();
        new TestPageStoreCoverage().init(config).test();
        System.out.println(p.getTop(3));
        System.out.println(System.currentTimeMillis() - time);
        System.out.println("counter: " + writeCount);
    }

    public void log(int op, String fileName, byte[] data, long x) {
        if (op != Recorder.WRITE && op != Recorder.SET_LENGTH) {
            return;
        }
        if (!fileName.endsWith(Constants.SUFFIX_PAGE_FILE)) {
            return;
        }
        if (testing) {
            // avoid deadlocks
            return;
        }
        testing = true;
        try {
            logDb(fileName);
        } finally {
            testing = false;
        }
    }

    private synchronized void logDb(String fileName) {
        writeCount++;
        if ((writeCount & 1023) == 0) {
            long now = System.currentTimeMillis();
            if (now > lastCheck + 5000) {
                System.out.println("+ write #" + writeCount + " verify #" + verifyCount);
                lastCheck = now;
            }
        }
        if ((writeCount & (testEvery - 1)) != 0) {
            return;
        }
        if (IOUtils.length(fileName) > MAX_FILE_SIZE) {
            // System.out.println(fileName + " " + IOUtils.length(fileName));
            return;
        }
        FileSystem.getInstance(fileName).copy(fileName, testDatabase + Constants.SUFFIX_PAGE_FILE);
        try {
            verifyCount++;
            // avoid using the Engine class to avoid deadlocks
            Properties p = new Properties();
            String userName =  getUser();
            p.setProperty("user", userName);
            p.setProperty("password", getPassword());
            ConnectionInfo ci = new ConnectionInfo("jdbc:h2:" + testDatabase + ";FILE_LOCK=NO;TRACE_LEVEL_FILE=0", p);
            Database database = new Database(ci, null);
            // close the database
            Session session = database.getSystemSession();
            session.prepare("shutdown immediately").update();
            database.removeSession(null);
            // everything OK - return
            return;
        } catch (DbException e) {
            SQLException e2 = DbException.toSQLException(e);
            int errorCode = e2.getErrorCode();
            if (errorCode == ErrorCode.WRONG_USER_OR_PASSWORD) {
                return;
            } else if (errorCode == ErrorCode.FILE_ENCRYPTION_ERROR_1) {
                return;
            }
            e.printStackTrace(System.out);
        } catch (Exception e) {
            // failed
            int errorCode = 0;
            if (e instanceof SQLException) {
                errorCode = ((SQLException) e).getErrorCode();
            }
            if (errorCode == ErrorCode.WRONG_USER_OR_PASSWORD) {
                return;
            } else if (errorCode == ErrorCode.FILE_ENCRYPTION_ERROR_1) {
                return;
            }
            e.printStackTrace(System.out);
        }
        System.out.println("begin ------------------------------ " + writeCount);
        testDatabase += "X";
        FileSystem.getInstance(fileName).copy(fileName, testDatabase + Constants.SUFFIX_PAGE_FILE);
        try {
            // avoid using the Engine class to avoid deadlocks
            Properties p = new Properties();
            ConnectionInfo ci = new ConnectionInfo("jdbc:h2:" + testDatabase + ";FILE_LOCK=NO", p);
            Database database = new Database(ci, null);
            // close the database
            database.removeSession(null);
        } catch (Exception e) {
            int errorCode = 0;
            if (e instanceof SQLException) {
                errorCode = ((SQLException) e).getErrorCode();
            }
            if (errorCode == ErrorCode.WRONG_USER_OR_PASSWORD) {
                return;
            } else if (errorCode == ErrorCode.FILE_ENCRYPTION_ERROR_1) {
                return;
            }
            StringBuilder buff = new StringBuilder();
            StackTraceElement[] list = e.getStackTrace();
            for (int i = 0; i < 10 && i < list.length; i++) {
                buff.append(list[i].toString()).append('\n');
            }
            String s = buff.toString();
            if (!knownErrors.contains(s)) {
                System.out.println(writeCount + " code: " + errorCode + " " + e.toString());
                e.printStackTrace(System.out);
                knownErrors.add(s);
            } else {
                System.out.println(writeCount + " code: " + errorCode);
            }
        }
    }

}
