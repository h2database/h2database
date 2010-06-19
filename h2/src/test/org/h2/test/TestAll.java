/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test;

import java.sql.SQLException;
import java.util.Properties;
import org.h2.Driver;
import org.h2.engine.Constants;
import org.h2.store.fs.FileSystemDisk;
import org.h2.test.bench.TestPerformance;
import org.h2.test.db.TestAlter;
import org.h2.test.db.TestAutoRecompile;
import org.h2.test.db.TestBackup;
import org.h2.test.db.TestBigDb;
import org.h2.test.db.TestBigResult;
import org.h2.test.db.TestCases;
import org.h2.test.db.TestCheckpoint;
import org.h2.test.db.TestCluster;
import org.h2.test.db.TestCompatibility;
import org.h2.test.db.TestCsv;
import org.h2.test.db.TestDeadlock;
import org.h2.test.db.TestEncryptedDb;
import org.h2.test.db.TestExclusive;
import org.h2.test.db.TestFullText;
import org.h2.test.db.TestFunctionOverload;
import org.h2.test.db.TestFunctions;
import org.h2.test.db.TestIndex;
import org.h2.test.db.TestLargeBlob;
import org.h2.test.db.TestLinkedTable;
import org.h2.test.db.TestListener;
import org.h2.test.db.TestLob;
import org.h2.test.db.TestMemoryUsage;
import org.h2.test.db.TestMultiConn;
import org.h2.test.db.TestMultiDimension;
import org.h2.test.db.TestMultiThread;
import org.h2.test.db.TestMultiThreadedKernel;
import org.h2.test.db.TestNestedJoins;
import org.h2.test.db.TestOpenClose;
import org.h2.test.db.TestOptimizations;
import org.h2.test.db.TestOutOfMemory;
import org.h2.test.db.TestPowerOff;
import org.h2.test.db.TestReadOnly;
import org.h2.test.db.TestRights;
import org.h2.test.db.TestRunscript;
import org.h2.test.db.TestSQLInjection;
import org.h2.test.db.TestScript;
import org.h2.test.db.TestScriptSimple;
import org.h2.test.db.TestSequence;
import org.h2.test.db.TestSessionsLocks;
import org.h2.test.db.TestSpaceReuse;
import org.h2.test.db.TestSpeed;
import org.h2.test.db.TestTableEngines;
import org.h2.test.db.TestTempTables;
import org.h2.test.db.TestTransaction;
import org.h2.test.db.TestTriggersConstraints;
import org.h2.test.db.TestTwoPhaseCommit;
import org.h2.test.db.TestView;
import org.h2.test.db.TestViewAlterTable;
import org.h2.test.db.TestViewDropView;
import org.h2.test.jaqu.AliasMapTest;
import org.h2.test.jaqu.SamplesTest;
import org.h2.test.jaqu.UpdateTest;
import org.h2.test.jdbc.TestBatchUpdates;
import org.h2.test.jdbc.TestCallableStatement;
import org.h2.test.jdbc.TestCancel;
import org.h2.test.jdbc.TestDatabaseEventListener;
import org.h2.test.jdbc.TestDriver;
import org.h2.test.jdbc.TestManyJdbcObjects;
import org.h2.test.jdbc.TestMetaData;
import org.h2.test.jdbc.TestNativeSQL;
import org.h2.test.jdbc.TestPreparedStatement;
import org.h2.test.jdbc.TestResultSet;
import org.h2.test.jdbc.TestStatement;
import org.h2.test.jdbc.TestTransactionIsolation;
import org.h2.test.jdbc.TestUpdatableResultSet;
import org.h2.test.jdbc.TestZloty;
import org.h2.test.jdbcx.TestConnectionPool;
import org.h2.test.jdbcx.TestDataSource;
import org.h2.test.jdbcx.TestXA;
import org.h2.test.jdbcx.TestXASimple;
import org.h2.test.mvcc.TestMvcc1;
import org.h2.test.mvcc.TestMvcc2;
import org.h2.test.mvcc.TestMvcc3;
import org.h2.test.mvcc.TestMvccMultiThreaded;
import org.h2.test.rowlock.TestRowLocks;
import org.h2.test.server.TestAutoServer;
import org.h2.test.server.TestNestedLoop;
import org.h2.test.server.TestWeb;
import org.h2.test.server.TestInit;
import org.h2.test.synth.TestBtreeIndex;
import org.h2.test.synth.TestCrashAPI;
import org.h2.test.synth.TestFuzzOptimizations;
import org.h2.test.synth.TestHaltApp;
import org.h2.test.synth.TestJoin;
import org.h2.test.synth.TestKill;
import org.h2.test.synth.TestKillRestart;
import org.h2.test.synth.TestKillRestartMulti;
import org.h2.test.synth.TestMultiThreaded;
import org.h2.test.synth.TestRandomSQL;
import org.h2.test.synth.TestTimer;
import org.h2.test.synth.sql.TestSynth;
import org.h2.test.synth.thread.TestMulti;
import org.h2.test.unit.TestAutoReconnect;
import org.h2.test.unit.TestCache;
import org.h2.test.unit.TestClearReferences;
import org.h2.test.unit.TestCompress;
import org.h2.test.unit.TestDataPage;
import org.h2.test.unit.TestDate;
import org.h2.test.unit.TestDateIso8601;
import org.h2.test.unit.TestExit;
import org.h2.test.unit.TestFile;
import org.h2.test.unit.TestFileLock;
import org.h2.test.unit.TestFileLockSerialized;
import org.h2.test.unit.TestFileSystem;
import org.h2.test.unit.TestFtp;
import org.h2.test.unit.TestIntArray;
import org.h2.test.unit.TestIntIntHashMap;
import org.h2.test.unit.TestMathUtils;
import org.h2.test.unit.TestNetUtils;
import org.h2.test.unit.TestOldVersion;
import org.h2.test.unit.TestOverflow;
import org.h2.test.unit.TestPageStore;
import org.h2.test.unit.TestPageStoreCoverage;
import org.h2.test.unit.TestPattern;
import org.h2.test.unit.TestPgServer;
import org.h2.test.unit.TestReader;
import org.h2.test.unit.TestRecovery;
import org.h2.test.unit.TestReopen;
import org.h2.test.unit.TestSampleApps;
import org.h2.test.unit.TestScriptReader;
import org.h2.test.unit.TestSecurity;
import org.h2.test.unit.TestShell;
import org.h2.test.unit.TestStreams;
import org.h2.test.unit.TestStringCache;
import org.h2.test.unit.TestStringUtils;
import org.h2.test.unit.TestTools;
import org.h2.test.unit.TestValue;
import org.h2.test.unit.TestValueHashMap;
import org.h2.test.unit.TestValueMemory;
import org.h2.test.utils.OutputCatcher;
import org.h2.test.utils.RecordingFileSystem;
import org.h2.test.utils.SelfDestructor;
import org.h2.test.db.TestConnectionInfo;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2.util.Profiler;
import org.h2.util.Utils;
import org.h2.util.StringUtils;

/**
 * The main test application. JUnit is not used because loops are easier to
 * write in regular java applications (most tests are ran multiple times using
 * different settings).
 */
public class TestAll {

/*

Random test:

java15
cd h2database/h2/bin
del *.db
start cmd /k "java -cp .;%H2DRIVERS% org.h2.test.TestAll join >testJoin.txt"
start cmd /k "java -cp . org.h2.test.TestAll synth >testSynth.txt"
start cmd /k "java -cp . org.h2.test.TestAll all >testAll.txt"
start cmd /k "java -cp . org.h2.test.TestAll random >testRandom.txt"
start cmd /k "java -cp . org.h2.test.TestAll btree >testBtree.txt"
start cmd /k "java -cp . org.h2.test.TestAll halt >testHalt.txt"
java -cp . org.h2.test.TestAll crash >testCrash.txt

java org.h2.test.TestAll timer

*/

    /**
     * If the test should run with many rows.
     */
    public boolean big;

    /**
     * If remote database connections should be used.
     */
    public boolean networked;

    /**
     * If in-memory databases should be used.
     */
    public boolean memory;

    /**
     * If code coverage is enabled.
     */
    public boolean codeCoverage;

    /**
     * If the multi version concurrency control mode should be used.
     */
    public boolean mvcc;

    /**
     * The cipher to use (null for unencrypted).
     */
    public String cipher;

    /**
     * If only JDK 1.4 methods should be tested.
     */
    public boolean jdk14 = true;

    /**
     * The file trace level value to use.
     */
    public int traceLevelFile;

    /**
     * If test trace information should be written (for debugging only).
     */
    public boolean traceTest;

    /**
     * If testing on Google App Engine.
     */
    public boolean googleAppEngine;

    /**
     * If a small cache and a low number for MAX_MEMORY_ROWS should be used.
     */
    public boolean diskResult;

    /**
     * Test using the recording file system.
     */
    public boolean record;

    /**
     * If the transaction log should be kept small (that is, the log should be
     * switched early).
     */
    boolean smallLog;

    /**
     * If SSL should be used for remote connections.
     */
    boolean ssl;

    /**
     * If MAX_MEMORY_UNDO=3 should be used.
     */
    boolean diskUndo;

    /**
     * If TRACE_LEVEL_SYSTEM_OUT should be set to 2 (for debugging only).
     */
    boolean traceSystemOut;

    /**
     * If the tests should run forever.
     */
    boolean endless;

    /**
     * The THROTTLE value to use.
     */
    int throttle;

    /**
     * If the test should stop when the first error occurs.
     */
    boolean stopOnError;

    /**
     * The cache type.
     */
    String cacheType;

    private Server server;

    /**
     * Run all tests.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws Exception {
        OutputCatcher catcher = OutputCatcher.start();
        run(args);
        catcher.stop();
        catcher.writeTo("Test Output", "docs/html/testOutput.html");
    }

    private static void run(String... args) throws Exception {
        SelfDestructor.startCountdown(6 * 60);
        long time = System.currentTimeMillis();
        TestAll test = new TestAll();
        test.printSystem();
        System.setProperty("h2.maxMemoryRowsDistinct", "128");
        System.setProperty("h2.check2", "true");

        int initialTest;
        // System.setProperty("h2.largeTransactions", "true");
        // System.setProperty("h2.lobInDatabase", "true");
        // System.setProperty("h2.analyzeAuto", "100");
        // System.setProperty("h2.nestedJoins", "true");

        int speedup;
        // System.setProperty("h2.syncMethod", "");

/*

recovery tests with small freeList pages, page size 64

reopen org.h2.test.unit.TestPageStore
-Xmx1500m -D reopenOffset=3 -D reopenShift=1

power failure test
power failure test: MULTI_THREADED=TRUE
power failure test: larger binaries and additional index.
power failure test with randomly generating / dropping indexes and tables.

drop table test;
create table test(id identity, name varchar(100) default space(100));
@LOOP 10 insert into test select null, null from system_range(1, 100000);
delete from test;

documentation: review package and class level javadocs
documentation: rolling review at jaqu.html

-------------

remove old TODO, move to roadmap

kill a test:
kill -9 `jps -l | grep "org.h2.test." | cut -d " " -f 1`

*/
        if (args.length > 0) {
            if ("reopen".equals(args[0])) {
                System.setProperty("h2.delayWrongPasswordMin", "0");
                System.setProperty("h2.check2", "false");
                System.setProperty("h2.lobInDatabase", "true");
                System.setProperty("h2.analyzeAuto", "100");
                // System.setProperty("h2.pageSize", "64");
                RecordingFileSystem.register();
                test.record = true;
                TestReopen reopen = new TestReopen();
                RecordingFileSystem.setRecorder(reopen);
                test.runTests();
            } else if ("crash".equals(args[0])) {
                test.endless = true;
                new TestCrashAPI().runTest(test);
            } else if ("synth".equals(args[0])) {
                new TestSynth().runTest(test);
            } else if ("kill".equals(args[0])) {
                new TestKill().runTest(test);
            } else if ("random".equals(args[0])) {
                test.endless = true;
                new TestRandomSQL().runTest(test);
            } else if ("join".equals(args[0])) {
                new TestJoin().runTest(test);
                test.endless = true;
            } else if ("btree".equals(args[0])) {
                new TestBtreeIndex().runTest(test);
            } else if ("all".equals(args[0])) {
                test.testEverything();
            } else if ("codeCoverage".equals(args[0])) {
                test.codeCoverage = true;
                test.runTests();
            } else if ("multiThread".equals(args[0])) {
                new TestMulti().runTest(test);
            } else if ("halt".equals(args[0])) {
                new TestHaltApp().runTest(test);
            } else if ("timer".equals(args[0])) {
                new TestTimer().runTest(test);
            }
        } else {
            test.runTests();
            Profiler prof = new Profiler();
            prof.depth = 4;
            prof.interval = 1;
            prof.startCollecting();
            TestPerformance.main("-init", "-db", "1");
            prof.stopCollecting();
            System.out.println(prof.getTop(3));
//            Recover.execute("data", null);
//            RunScript.execute("jdbc:h2:data/test2",
//                 "sa1", "sa1", "data/test.h2.sql", null, false);
//            Recover.execute("data", null);
        }
        System.out.println(TestBase.formatTime(System.currentTimeMillis() - time) + " total");
    }

    /**
     * Run all tests in all possible combinations.
     */
    private void testEverything() throws SQLException {
        for (int c = 0; c < 3; c++) {
            if (c == 0) {
                cipher = null;
            } else if (c == 1) {
                cipher = "XTEA";
            } else {
                cipher = "AES";
            }
            for (int a = 0; a < 64; a++) {
                smallLog = (a & 1) != 0;
                big = (a & 2) != 0;
                networked = (a & 4) != 0;
                memory = (a & 8) != 0;
                ssl = (a & 16) != 0;
                diskResult = (a & 32) != 0;
                for (int trace = 0; trace < 3; trace++) {
                    traceLevelFile = trace;
                    test();
                }
            }
        }
    }

    /**
     * Run the tests with a number of different settings.
     */
    private void runTests() throws SQLException {
        jdk14 = true;
        smallLog = big = networked = memory = ssl = false;
        diskResult = traceSystemOut = diskUndo = false;
        mvcc = traceTest = stopOnError = false;
        traceLevelFile = throttle = 0;
        cipher = null;
        test();
        testUnit();

        networked = true;
        memory = true;
        test();

        memory = false;
        networked = false;
        diskUndo = true;
        diskResult = true;
        traceLevelFile = 3;
        throttle = 1;
        cacheType = "SOFT_LRU";
        cipher = "XTEA";
        test();

        diskUndo = false;
        diskResult = false;
        traceLevelFile = 1;
        throttle = 0;
        cacheType = null;
        cipher = null;
        test();

        traceLevelFile = 2;
        big = true;
        smallLog = true;
        networked = true;
        ssl = true;
        test();

        smallLog = false;
        networked = false;
        ssl = false;
        traceLevelFile = 0;
        test();

        big = false;
        cipher = "AES";
        test();

        mvcc = true;
        cipher = null;
        test();

        memory = true;
        test();
    }

    /**
     * Run all tests with the current settings.
     */
    private void test() throws SQLException {
        System.out.println();
        System.out.println("Test " + toString() + " (" + Utils.getMemoryUsed() + " KB used)");
        beforeTest();

        // db
        new TestScriptSimple().runTest(this);
        new TestScript().runTest(this);
        new TestAlter().runTest(this);
        new TestAutoRecompile().runTest(this);
        new TestBackup().runTest(this);
        new TestBigDb().runTest(this);
        new TestBigResult().runTest(this);
        new TestCases().runTest(this);
        new TestCheckpoint().runTest(this);
        new TestCluster().runTest(this);
        new TestConnectionInfo().runTest(this);
        new TestCompatibility().runTest(this);
        new TestCsv().runTest(this);
        new TestDeadlock().runTest(this);
        new TestEncryptedDb().runTest(this);
        new TestExclusive().runTest(this);
        new TestFullText().runTest(this);
        new TestFunctionOverload().runTest(this);
        new TestFunctions().runTest(this);
        new TestInit().runTest(this);
        new TestIndex().runTest(this);
        new TestLargeBlob().runTest(this);
        new TestLinkedTable().runTest(this);
        new TestListener().runTest(this);
        new TestLob().runTest(this);
        new TestMemoryUsage().runTest(this);
        new TestMultiConn().runTest(this);
        new TestMultiDimension().runTest(this);
        new TestMultiThread().runTest(this);
        new TestMultiThreadedKernel().runTest(this);
        new TestNestedJoins().runTest(this);
        new TestOpenClose().runTest(this);
        new TestOptimizations().runTest(this);
        new TestOutOfMemory().runTest(this);
        new TestPowerOff().runTest(this);
        new TestReadOnly().runTest(this);
        new TestRights().runTest(this);
        new TestRunscript().runTest(this);
        new TestSQLInjection().runTest(this);
        new TestSessionsLocks().runTest(this);
        new TestSequence().runTest(this);
        new TestSpaceReuse().runTest(this);
        new TestSpeed().runTest(this);
        new TestTableEngines().runTest(this);
        new TestTempTables().runTest(this);
        new TestTransaction().runTest(this);
        new TestTriggersConstraints().runTest(this);
        new TestTwoPhaseCommit().runTest(this);
        new TestView().runTest(this);
        new TestViewAlterTable().runTest(this);
        new TestViewDropView().runTest(this);

        // jaqu
        new AliasMapTest().runTest(this);
        new SamplesTest().runTest(this);
        new UpdateTest().runTest(this);

        // jdbc
        new TestBatchUpdates().runTest(this);
        new TestCallableStatement().runTest(this);
        new TestCancel().runTest(this);
        new TestDatabaseEventListener().runTest(this);
        new TestDriver().runTest(this);
        new TestManyJdbcObjects().runTest(this);
        new TestMetaData().runTest(this);
        new TestNativeSQL().runTest(this);
        new TestPreparedStatement().runTest(this);
        new TestResultSet().runTest(this);
        new TestStatement().runTest(this);
        new TestTransactionIsolation().runTest(this);
        new TestUpdatableResultSet().runTest(this);
        new TestZloty().runTest(this);

        // jdbcx
        new TestConnectionPool().runTest(this);
        new TestDataSource().runTest(this);
        new TestXA().runTest(this);
        new TestXASimple().runTest(this);

        // server
        new TestAutoServer().runTest(this);
        new TestNestedLoop().runTest(this);
        new TestWeb().runTest(this);

        // mvcc & row level locking
        new TestMvcc1().runTest(this);
        new TestMvcc2().runTest(this);
        new TestMvcc3().runTest(this);
        new TestMvccMultiThreaded().runTest(this);
        new TestRowLocks().runTest(this);

        // synth
        new TestBtreeIndex().runTest(this);
        new TestCrashAPI().runTest(this);
        new TestFuzzOptimizations().runTest(this);
        new TestRandomSQL().runTest(this);
        new TestKillRestart().runTest(this);
        new TestKillRestartMulti().runTest(this);
        new TestMultiThreaded().runTest(this);

        afterTest();
    }

    private void testUnit() {
        new TestAutoReconnect().runTest(this);
        new TestCache().runTest(this);
        new TestClearReferences().runTest(this);
        new TestCompress().runTest(this);
        new TestDataPage().runTest(this);
        new TestDate().runTest(this);
        new TestDateIso8601().runTest(this);
        new TestExit().runTest(this);
        new TestFile().runTest(this);
        new TestFileLock().runTest(this);
        new TestFileLockSerialized().runTest(this);
        new TestFtp().runTest(this);
        new TestFileSystem().runTest(this);
        new TestIntArray().runTest(this);
        new TestIntIntHashMap().runTest(this);
        new TestMathUtils().runTest(this);
        new TestOldVersion().runTest(this);
        new TestNetUtils().runTest(this);
        new TestMultiThreadedKernel().runTest(this);
        new TestOverflow().runTest(this);
        new TestPageStore().runTest(this);
        new TestPageStoreCoverage().runTest(this);
        new TestPattern().runTest(this);
        new TestPgServer().runTest(this);
        new TestReader().runTest(this);
        new TestRecovery().runTest(this);
        new TestSampleApps().runTest(this);
        new TestScriptReader().runTest(this);
        runTest("org.h2.test.unit.TestServlet");
        new TestSecurity().runTest(this);
        new TestShell().runTest(this);
        new TestStreams().runTest(this);
        new TestStringCache().runTest(this);
        new TestStringUtils().runTest(this);
        new TestTools().runTest(this);
        new TestValue().runTest(this);
        new TestValueHashMap().runTest(this);
        new TestValueMemory().runTest(this);
    }

    private void runTest(String className) {
        try {
            Class< ? > clazz = Class.forName(className);
            TestBase test = (TestBase) clazz.newInstance();
            test.runTest(this);
        } catch (Exception e) {
            // ignore
            TestBase.printlnWithTime(0, className + " class not found");
        } catch (NoClassDefFoundError e) {
            // ignore
            TestBase.printlnWithTime(0, className + " class not found");
        }
    }

    /**
     * This method is called before a complete set of tests is run. It deletes
     * old database files in the test directory and trace files. It also starts
     * a TCP server if the test uses remote connections.
     */
    void beforeTest() throws SQLException {
        Driver.load();
        FileSystemDisk.getInstance().deleteRecursive(TestBase.BASE_TEST_DIR, true);
        DeleteDbFiles.execute(TestBase.BASE_TEST_DIR, null, true);
        FileSystemDisk.getInstance().deleteRecursive("trace.db", false);
        if (networked) {
            String[] args = ssl ? new String[] { "-tcpSSL", "true", "-tcpPort", "9192" } : new String[] { "-tcpPort",
                    "9192" };
            server = Server.createTcpServer(args);
            try {
                server.start();
            } catch (SQLException e) {
                System.out.println("FAIL: can not start server (may already be running)");
                server = null;
            }
        }
    }

    private void afterTest() {
        FileSystemDisk.getInstance().deleteRecursive("trace.db", true);
        if (networked && server != null) {
            server.stop();
        }
        FileSystemDisk.getInstance().deleteRecursive(TestBase.BASE_TEST_DIR, true);
    }

    private void printSystem() {
        Properties prop = System.getProperties();
        System.out.println("H2 " + Constants.getFullVersion() + " @ " + new java.sql.Timestamp(System.currentTimeMillis()).toString());
        System.out.println("Java " +
                prop.getProperty("java.runtime.version") + ", " +
                prop.getProperty("java.vm.name")+", " +
                prop.getProperty("java.vendor"));
        System.out.println(
                prop.getProperty("os.name") + ", " +
                prop.getProperty("os.arch")+", "+
                prop.getProperty("os.version")+", "+
                prop.getProperty("sun.os.patch.level")+", "+
                prop.getProperty("file.separator")+" "+
                prop.getProperty("path.separator")+" "+
                StringUtils.javaEncode(prop.getProperty("line.separator")) + " " +
                prop.getProperty("user.country") + " " +
                prop.getProperty("user.language") + " " +
                prop.getProperty("user.variant")+" "+
                prop.getProperty("file.encoding"));
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        appendIf(buff, big, "big");
        appendIf(buff, networked, "net");
        appendIf(buff, memory, "memory");
        appendIf(buff, codeCoverage, "codeCoverage");
        appendIf(buff, mvcc, "mvcc");
        appendIf(buff, cipher != null, cipher);
        appendIf(buff, cacheType != null, cacheType);
        appendIf(buff, jdk14, "jdk14");
        appendIf(buff, smallLog, "smallLog");
        appendIf(buff, ssl, "ssl");
        appendIf(buff, diskUndo, "diskUndo");
        appendIf(buff, diskResult, "diskResult");
        appendIf(buff, traceSystemOut, "traceSystemOut");
        appendIf(buff, endless, "endless");
        appendIf(buff, traceLevelFile > 0, "traceLevelFile");
        appendIf(buff, throttle > 0, "throttle:" + throttle);
        appendIf(buff, traceTest, "traceTest");
        appendIf(buff, stopOnError, "stopOnError");
        return buff.toString();
    }

    private void appendIf(StringBuilder buff, boolean flag, String text) {
        if (flag) {
            buff.append(text);
            buff.append(' ');
        }
    }

}
