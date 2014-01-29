/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test;

import java.sql.SQLException;
import java.util.Properties;
import org.h2.Driver;
import org.h2.engine.Constants;
import org.h2.store.fs.FilePathRec;
import org.h2.store.fs.FileUtils;
import org.h2.test.bench.TestPerformance;
import org.h2.test.db.TestAlter;
import org.h2.test.db.TestAlterSchemaRename;
import org.h2.test.db.TestAutoRecompile;
import org.h2.test.db.TestBackup;
import org.h2.test.db.TestBigDb;
import org.h2.test.db.TestBigResult;
import org.h2.test.db.TestCases;
import org.h2.test.db.TestCheckpoint;
import org.h2.test.db.TestCluster;
import org.h2.test.db.TestCompatibility;
import org.h2.test.db.TestCsv;
import org.h2.test.db.TestDateStorage;
import org.h2.test.db.TestDeadlock;
import org.h2.test.db.TestDrop;
import org.h2.test.db.TestDuplicateKeyUpdate;
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
import org.h2.test.db.TestOpenClose;
import org.h2.test.db.TestOptimizations;
import org.h2.test.db.TestCompatibilityOracle;
import org.h2.test.db.TestOutOfMemory;
import org.h2.test.db.TestPowerOff;
import org.h2.test.db.TestQueryCache;
import org.h2.test.db.TestReadOnly;
import org.h2.test.db.TestRecursiveQueries;
import org.h2.test.db.TestRights;
import org.h2.test.db.TestRunscript;
import org.h2.test.db.TestSQLInjection;
import org.h2.test.db.TestScript;
import org.h2.test.db.TestScriptSimple;
import org.h2.test.db.TestSelectCountNonNullColumn;
import org.h2.test.db.TestSequence;
import org.h2.test.db.TestSessionsLocks;
import org.h2.test.db.TestShow;
import org.h2.test.db.TestSpaceReuse;
import org.h2.test.db.TestSpatial;
import org.h2.test.db.TestSpeed;
import org.h2.test.db.TestTableEngines;
import org.h2.test.db.TestTempTables;
import org.h2.test.db.TestTransaction;
import org.h2.test.db.TestTriggersConstraints;
import org.h2.test.db.TestTwoPhaseCommit;
import org.h2.test.db.TestUpgrade;
import org.h2.test.db.TestView;
import org.h2.test.db.TestViewAlterTable;
import org.h2.test.db.TestViewDropView;
import org.h2.test.jaqu.AliasMapTest;
import org.h2.test.jaqu.AnnotationsTest;
import org.h2.test.jaqu.ClobTest;
import org.h2.test.jaqu.ModelsTest;
import org.h2.test.jaqu.SamplesTest;
import org.h2.test.jaqu.UpdateTest;
import org.h2.test.jdbc.TestBatchUpdates;
import org.h2.test.jdbc.TestCallableStatement;
import org.h2.test.jdbc.TestCancel;
import org.h2.test.jdbc.TestDatabaseEventListener;
import org.h2.test.jdbc.TestDriver;
import org.h2.test.jdbc.TestJavaObject;
import org.h2.test.jdbc.TestJavaObjectSerializer;
import org.h2.test.jdbc.TestLimitUpdates;
import org.h2.test.jdbc.TestLobApi;
import org.h2.test.jdbc.TestManyJdbcObjects;
import org.h2.test.jdbc.TestMetaData;
import org.h2.test.jdbc.TestNativeSQL;
import org.h2.test.jdbc.TestPreparedStatement;
import org.h2.test.jdbc.TestResultSet;
import org.h2.test.jdbc.TestStatement;
import org.h2.test.jdbc.TestTransactionIsolation;
import org.h2.test.jdbc.TestUpdatableResultSet;
import org.h2.test.jdbc.TestUrlJavaObjectSerializer;
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
import org.h2.test.server.TestInit;
import org.h2.test.server.TestNestedLoop;
import org.h2.test.server.TestWeb;
import org.h2.test.store.TestCacheConcurrentLIRS;
import org.h2.test.store.TestCacheLIRS;
import org.h2.test.store.TestCacheLongKeyLIRS;
import org.h2.test.store.TestConcurrent;
import org.h2.test.store.TestDataUtils;
import org.h2.test.store.TestFreeSpace;
import org.h2.test.store.TestKillProcessWhileWriting;
import org.h2.test.store.TestMVRTree;
import org.h2.test.store.TestMVStore;
import org.h2.test.store.TestMVStoreBenchmark;
import org.h2.test.store.TestMVTableEngine;
import org.h2.test.store.TestObjectDataType;
import org.h2.test.store.TestRandomMapOps;
import org.h2.test.store.TestSpinLock;
import org.h2.test.store.TestStreamStore;
import org.h2.test.store.TestTransactionStore;
import org.h2.test.synth.TestBtreeIndex;
import org.h2.test.synth.TestCrashAPI;
import org.h2.test.synth.TestDiskFull;
import org.h2.test.synth.TestFuzzOptimizations;
import org.h2.test.synth.TestHaltApp;
import org.h2.test.synth.TestJoin;
import org.h2.test.synth.TestKill;
import org.h2.test.synth.TestKillRestart;
import org.h2.test.synth.TestKillRestartMulti;
import org.h2.test.synth.TestLimit;
import org.h2.test.synth.TestMultiThreaded;
import org.h2.test.synth.TestNestedJoins;
import org.h2.test.synth.TestOuterJoins;
import org.h2.test.synth.TestRandomCompare;
import org.h2.test.synth.TestRandomSQL;
import org.h2.test.synth.TestTimer;
import org.h2.test.synth.sql.TestSynth;
import org.h2.test.synth.thread.TestMulti;
import org.h2.test.unit.TestAutoReconnect;
import org.h2.test.unit.TestBitField;
import org.h2.test.unit.TestBnf;
import org.h2.test.unit.TestCache;
import org.h2.test.unit.TestClearReferences;
import org.h2.test.unit.TestCollation;
import org.h2.test.unit.TestCompress;
import org.h2.test.unit.TestConnectionInfo;
import org.h2.test.unit.TestDataPage;
import org.h2.test.unit.TestDate;
import org.h2.test.unit.TestDateIso8601;
import org.h2.test.unit.TestExit;
import org.h2.test.unit.TestFile;
import org.h2.test.unit.TestFileLock;
import org.h2.test.unit.TestFileLockProcess;
import org.h2.test.unit.TestFileLockSerialized;
import org.h2.test.unit.TestFileSystem;
import org.h2.test.unit.TestFtp;
import org.h2.test.unit.TestIntArray;
import org.h2.test.unit.TestIntIntHashMap;
import org.h2.test.unit.TestJmx;
import org.h2.test.unit.TestMathUtils;
import org.h2.test.unit.TestModifyOnWrite;
import org.h2.test.unit.TestNetUtils;
import org.h2.test.unit.TestObjectDeserialization;
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
import org.h2.test.unit.TestSort;
import org.h2.test.unit.TestStreams;
import org.h2.test.unit.TestStringCache;
import org.h2.test.unit.TestStringUtils;
import org.h2.test.unit.TestTools;
import org.h2.test.unit.TestTraceSystem;
import org.h2.test.unit.TestUtils;
import org.h2.test.unit.TestValue;
import org.h2.test.unit.TestValueHashMap;
import org.h2.test.unit.TestValueMemory;
import org.h2.test.utils.OutputCatcher;
import org.h2.test.utils.SelfDestructor;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2.util.Profiler;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * The main test application. JUnit is not used because loops are easier to
 * write in regular java applications (most tests are ran multiple times using
 * different settings).
 */
public class TestAll {

/*

PIT test:
java org.pitest.mutationtest.MutationCoverageReport
--reportDir data --targetClasses org.h2.dev.store.btree.StreamStore*
--targetTests org.h2.test.store.TestStreamStore
--sourceDirs src/test,src/tools

Dump heap on out of memory:
-XX:+HeapDumpOnOutOfMemoryError

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

    ;
    private static final boolean MV_STORE = true;

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
     * Whether to use the MVStore.
     */
    public boolean mvStore;

    /**
     * Whether the test is running with code coverage.
     */
    public boolean coverage;

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
    public boolean reopen;

    /**
     * Test the split file system.
     */
    public boolean splitFileSystem;

    /**
     * Support nested joins.
     */
    public boolean nestedJoins;

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
     * The THROTTLE value to use by default.
     */
    int throttleDefault = Integer.parseInt(System.getProperty("throttle", "0"));

    /**
     * If the test should stop when the first error occurs.
     */
    boolean stopOnError;

    /**
     * If the database should always be defragmented when closing.
     */
    boolean defrag;

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
        SelfDestructor.startCountdown(4 * 60);
        long time = System.currentTimeMillis();
        printSystemInfo();
        System.setProperty("h2.maxMemoryRowsDistinct", "128");
        System.setProperty("h2.check2", "true");
        System.setProperty("h2.delayWrongPasswordMin", "0");
        System.setProperty("h2.delayWrongPasswordMax", "0");
        System.setProperty("h2.useThreadContextClassLoader", "true");

        // System.setProperty("h2.modifyOnWrite", "true");

        // System.setProperty("h2.storeLocalTime", "true");

        // speedup
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
documentation: rolling review at main.html

-------------

remove old TODO, move to roadmap

kill a test:
kill -9 `jps -l | grep "org.h2.test." | cut -d " " -f 1`

*/
        TestAll test = new TestAll();
        if (args.length > 0) {
            if ("reopen".equals(args[0])) {
                System.setProperty("h2.delayWrongPasswordMin", "0");
                System.setProperty("h2.check2", "false");
                System.setProperty("h2.analyzeAuto", "100");
                System.setProperty("h2.pageSize", "64");
                System.setProperty("h2.reopenShift", "5");
                FilePathRec.register();
                test.reopen = true;
                TestReopen reopen = new TestReopen();
                reopen.init();
                reopen.config.mvStore = MV_STORE;
                FilePathRec.setRecorder(reopen);
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
            prof.depth = 16;
            prof.interval = 1;
            prof.startCollecting();
            if (test.mvStore) {
                TestPerformance.main("-init", "-db", "9", "-size", "1000");
            } else {
                TestPerformance.main("-init", "-db", "1");
            }
            prof.stopCollecting();
            System.out.println(prof.getTop(30));
            if (test.mvStore) {
                prof = new Profiler();
                prof.depth = 16;
                prof.interval = 1;
                prof.startCollecting();
                TestPerformance.main("-init", "-db", "1", "-size", "1000");
                prof.stopCollecting();
                System.out.println(prof.getTop(3));
                TestPerformance.main("-init", "-db", "1", "-size", "1000");
                TestPerformance.main("-init", "-db", "9", "-size", "1000");
            }
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

        coverage = isCoverage();

        mvStore = MV_STORE;

        smallLog = big = networked = memory = ssl = false;
        diskResult = traceSystemOut = diskUndo = false;
        mvcc = traceTest = stopOnError = false;
        defrag = false;
        traceLevelFile = throttle = 0;
        cipher = null;
        // splitFileSystem = true;
        test();
        testUnit();

        networked = true;
        memory = true;
        splitFileSystem = false;
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
        defrag = true;
        test();

        traceLevelFile = 0;
        smallLog = true;
        networked = true;
        ssl = true;
        defrag = false;
        test();

        big = true;
        smallLog = false;
        networked = false;
        ssl = false;
        traceLevelFile = 0;
        test();
        testUnit();

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
     * Check whether this method is running with "Emma" code coverage turned on.
     *
     * @return true if the stack trace contains ".emma."
     */
    private static boolean isCoverage() {
        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
            if (e.toString().indexOf(".emma.") >= 0) {
                return true;
            }
        }
        return false;
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
        new TestAlterSchemaRename().runTest(this);
        new TestAutoRecompile().runTest(this);
        new TestBitField().runTest(this);
        new TestBnf().runTest(this);
        new TestBackup().runTest(this);
        new TestBigDb().runTest(this);
        new TestBigResult().runTest(this);
        new TestCases().runTest(this);
        new TestCheckpoint().runTest(this);
        new TestCluster().runTest(this);
        new TestCompatibility().runTest(this);
        new TestCompatibilityOracle().runTest(this);
        new TestCsv().runTest(this);
        new TestDateStorage().runTest(this);
        new TestDeadlock().runTest(this);
        new TestDrop().runTest(this);
        new TestDuplicateKeyUpdate().runTest(this);
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
        new TestOpenClose().runTest(this);
        new TestOptimizations().runTest(this);
        new TestOutOfMemory().runTest(this);
        new TestPowerOff().runTest(this);
        new TestQueryCache().runTest(this);
        new TestReadOnly().runTest(this);
        new TestRecursiveQueries().runTest(this);
        new TestRights().runTest(this);
        new TestRunscript().runTest(this);
        new TestSQLInjection().runTest(this);
        new TestSessionsLocks().runTest(this);
        new TestSelectCountNonNullColumn().runTest(this);
        new TestSequence().runTest(this);
        new TestShow().runTest(this);
        new TestSpaceReuse().runTest(this);
        new TestSpatial().runTest(this);
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
        new AnnotationsTest().runTest(this);
        new ClobTest().runTest(this);
        new ModelsTest().runTest(this);
        new SamplesTest().runTest(this);
        new UpdateTest().runTest(this);

        // jdbc
        new TestBatchUpdates().runTest(this);
        new TestCallableStatement().runTest(this);
        new TestCancel().runTest(this);
        new TestDatabaseEventListener().runTest(this);
        new TestDriver().runTest(this);
        new TestJavaObject().runTest(this);
        new TestJavaObjectSerializer().runTest(this);
        new TestUrlJavaObjectSerializer().runTest(this);
        new TestLimitUpdates().runTest(this);
        new TestLobApi().runTest(this);
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
        new TestDiskFull().runTest(this);
        new TestCrashAPI().runTest(this);
        new TestFuzzOptimizations().runTest(this);
        new TestLimit().runTest(this);
        new TestRandomSQL().runTest(this);
        new TestRandomCompare().runTest(this);
        new TestKillRestart().runTest(this);
        new TestKillRestartMulti().runTest(this);
        new TestMultiThreaded().runTest(this);
        new TestOuterJoins().runTest(this);
        new TestNestedJoins().runTest(this);

        afterTest();
    }

    private void testUnit() {
        // mv store
        new TestCacheConcurrentLIRS().runTest(this);
        new TestCacheLIRS().runTest(this);
        new TestCacheLongKeyLIRS().runTest(this);
        new TestConcurrent().runTest(this);
        new TestDataUtils().runTest(this);
        new TestFreeSpace().runTest(this);
        new TestKillProcessWhileWriting().runTest(this);
        new TestMVRTree().runTest(this);
        new TestMVStore().runTest(this);
        new TestMVStoreBenchmark().runTest(this);
        new TestMVTableEngine().runTest(this);
        new TestObjectDataType().runTest(this);
        new TestRandomMapOps().runTest(this);
        new TestSpinLock().runTest(this);
        new TestStreamStore().runTest(this);
        new TestTransactionStore().runTest(this);

        // unit
        new TestAutoReconnect().runTest(this);
        new TestCache().runTest(this);
        new TestClearReferences().runTest(this);
        new TestCollation().runTest(this);
        new TestCompress().runTest(this);
        new TestConnectionInfo().runTest(this);
        new TestDataPage().runTest(this);
        new TestDate().runTest(this);
        new TestDateIso8601().runTest(this);
        new TestExit().runTest(this);
        new TestFile().runTest(this);
        new TestFileLock().runTest(this);
        new TestFileLockProcess().runTest(this);
        new TestFileLockSerialized().runTest(this);
        new TestFtp().runTest(this);
        new TestFileSystem().runTest(this);
        new TestIntArray().runTest(this);
        new TestIntIntHashMap().runTest(this);
        new TestJmx().runTest(this);
        new TestMathUtils().runTest(this);
        new TestModifyOnWrite().runTest(this);
        new TestOldVersion().runTest(this);
        new TestNetUtils().runTest(this);
        new TestObjectDeserialization().runTest(this);
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
        new TestSort().runTest(this);
        new TestStreams().runTest(this);
        new TestStringCache().runTest(this);
        new TestStringUtils().runTest(this);
        new TestTools().runTest(this);
        new TestTraceSystem().runTest(this);
        new TestUpgrade().runTest(this);
        new TestUtils().runTest(this);
        new TestValue().runTest(this);
        new TestValueHashMap().runTest(this);
        new TestValueMemory().runTest(this);
    }

    private void runTest(String className) {
        try {
            Class<?> clazz = Class.forName(className);
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
    public void beforeTest() throws SQLException {
        Driver.load();
        FileUtils.deleteRecursive(TestBase.BASE_TEST_DIR, true);
        DeleteDbFiles.execute(TestBase.BASE_TEST_DIR, null, true);
        FileUtils.deleteRecursive("trace.db", false);
        if (networked) {
            String[] args = ssl ? new String[] { "-tcpSSL", "-tcpPort", "9192" } : new String[] { "-tcpPort",
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

    /**
     * Stop the server if it was started.
     */
    public void afterTest() {
        FileUtils.deleteRecursive("trace.db", true);
        if (networked && server != null) {
            server.stop();
        }
        FileUtils.deleteRecursive(TestBase.BASE_TEST_DIR, true);
    }

    /**
     * Print system information.
     */
    public static void printSystemInfo() {
        Properties prop = System.getProperties();
        System.out.println("H2 " + Constants.getFullVersion() +
                " @ " + new java.sql.Timestamp(System.currentTimeMillis()).toString());
        System.out.println("Java " +
                prop.getProperty("java.runtime.version") + ", " +
                prop.getProperty("java.vm.name")+", " +
                prop.getProperty("java.vendor") + ", " +
                prop.getProperty("sun.arch.data.model"));
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
                prop.getProperty("user.timezone") + " " +
                prop.getProperty("user.variant")+" "+
                prop.getProperty("file.encoding"));
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        appendIf(buff, mvStore, "mvStore");
        appendIf(buff, big, "big");
        appendIf(buff, networked, "net");
        appendIf(buff, memory, "memory");
        appendIf(buff, codeCoverage, "codeCoverage");
        appendIf(buff, mvcc, "mvcc");
        appendIf(buff, cipher != null, cipher);
        appendIf(buff, cacheType != null, cacheType);
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
        appendIf(buff, defrag, "defrag");
        appendIf(buff, splitFileSystem, "split");
        return buff.toString();
    }

    private static void appendIf(StringBuilder buff, boolean flag, String text) {
        if (flag) {
            buff.append(text);
            buff.append(' ');
        }
    }

}
