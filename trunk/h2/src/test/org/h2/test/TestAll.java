/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test;

import java.sql.SQLException;
import java.util.Properties;

import org.h2.engine.Constants;
import org.h2.server.TcpServer;
import org.h2.test.jdbc.*;
import org.h2.test.db.*;
import org.h2.test.server.TestNestedLoop;
import org.h2.test.synth.TestBtreeIndex;
import org.h2.test.synth.TestCrashAPI;
import org.h2.test.synth.TestJoin;
import org.h2.test.synth.TestKill;
import org.h2.test.synth.TestMulti;
import org.h2.test.synth.TestRandomSQL;
import org.h2.test.synth.TestSynth;
import org.h2.test.unit.TestBitField;
import org.h2.test.unit.TestCache;
import org.h2.test.unit.TestCompress;
import org.h2.test.unit.TestDataPage;
import org.h2.test.unit.TestExit;
import org.h2.test.unit.TestFileLock;
import org.h2.test.unit.TestIntArray;
import org.h2.test.unit.TestIntIntHashMap;
import org.h2.test.unit.TestOverflow;
import org.h2.test.unit.TestPattern;
import org.h2.test.unit.TestReader;
import org.h2.test.unit.TestSampleApps;
import org.h2.test.unit.TestScriptReader;
import org.h2.test.unit.TestSecurity;
import org.h2.test.unit.TestStreams;
import org.h2.test.unit.TestStringCache;
import org.h2.test.unit.TestStringUtils;
import org.h2.test.unit.TestTools;
import org.h2.test.unit.TestValueHashMap;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2.util.StringUtils;

/**
 * @author Thomas
 */
public class TestAll {

// Snippets to run test code:
// java -cp .;%H2DRIVERS% org.h2.test.TestAll
// java -Xrunhprof:cpu=samples,depth=8 org.h2.test.TestAll
// java -Xrunhprof:heap=sites,depth=8 org.h2.test.TestAll
// C:\Programme\Java\jdk1.6.beta\bin\java

/*

Random test:

cd bin
del *.db
start cmd /k "java -cp .;%H2DRIVERS% org.h2.test.TestAll join >testJoin.txt"
start cmd /k "java org.h2.test.TestAll crash >testCrash.txt"
start cmd /k "java org.h2.test.TestAll synth >testSynth.txt"
start cmd /k "java org.h2.test.TestAll all >testAll.txt"
start cmd /k "java org.h2.test.TestAll random >testRandom.txt"
start cmd /k "java org.h2.test.TestAll btree >testBtree.txt"

Test for hot spots:
java -agentlib:yjpagent=sampling,noj2ee,dir=C:\temp\Snapshots org.h2.test.bench.TestPerformance -init -db 1
java -Xmx512m -Xrunhprof:cpu=samples,depth=8 org.h2.tools.RunScript -url jdbc:h2:test;TRACE_LEVEL_FILE=3;LOG=2;MAX_LOG_SIZE=1000;DATABASE_EVENT_LISTENER='org.h2.samples.ShowProgress' -user sa -script test.sql
 */

    public boolean smallLog, big, networked, memory, ssl, textStorage, diskUndo, diskResult, deleteIndex, traceSystemOut;
    public boolean codeCoverage;
    public int logMode = 1, traceLevelFile, throttle;
    public String cipher;

    public boolean traceTest, stopOnError;
    public boolean jdk14 = true;

    private Server server;
    private int oldCacheMin;

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();
        TestAll test = new TestAll();
        test.printSystem();
        
//        > Of course performance is relative. There are situations where a tractor is faster than a car.
//        > It would be good to have a test case where Derby is faster, tell me if you have one.
//        >
//
//        I gave a specific database server stress test scenario case when I mentioned TPC-B (this is just 1 particular case) - A tractor does not compete in some F1 race and that is where your analogy is flawed because Derby actually performs more than decently in that context - Like I said, embedded applications is one particular facet of today's applications but that does not represent all of the applications out there - Everything is relevant to the particular tests one is defining and running (yours in that case) but that does not represent how a database performs in some other contexts (embedded or not).
//        > Currently H2 doesn't support row locks. But I don't think that most embedded applications need it.
//        > Anyway, the trend is towards multi version concurrency control (MVCC), and that's the next big
//        > thing that will be implemented in H2 (however this will take some time).
//        >
//
//        Good to hear this - Row-lock in Derby has been implemented since the first incarnation of Cloudscape in 96' - MVCC is good but not for applications which are doing intense updates and writes - The reason is pretty obvious versus a lock concurrency scheme and that is why "some" database(s) are supporting both approaches.
//
//        > Durability: The default isolation level of Derby is read committed, right?
//        > As far as I understand it, for fully-ACID compliant the isolation level should
//        > be serialized (see Isolation in Wikipedia). I'm not sure if supporting 'full ACID'
//        > compliance by default would make sense. I have implemented and run a durability test
//        > with various databases and the file system (a simple power-off test using two computers),
//        > and things don't look good. The problem is, even if the database tries to flush to disk
//        > for each commit, the operating system and/or hard disk does not always do that.
//        > For details see ACID. If you really want to enforce flushing to disk, you need to wait
//        > at least 0.1 seconds per transaction, and even Derby doesn't do that by default.
//        > That means, even Derby does *not* guarantee that all committed transactions will survive
//        > a power failure or an application crash. If you have other results using common
//        > hardware / default settings and this test, or if you find a way that is faster,
//        > please tell me! Hopefully the next generation hard drives (with integrated flash memory)
//        > will be better... But if you need 'no single point of failure' then you anyway need
//        > clustering / mirroring. H2 support clustering, Derby does not.
//        >
//
//        The golden rule is that you should not rely on the file system for write operations unless you have some means to force-flush & check I/O completions - that is why Unix Raw Devices were made available almost 20 years ago so that one could bypass the FS and use Async I/O's at the kernel level to retrieve status on a particular I/O (completion) and made sure it made it to disk(s) - there are technics such as write through-case where you don't rely on I/O write operations to be handled at all by the FS buffer (as it is bypassed) but rather expect a write I/O to be written to disks everytime you request it - it is a binary operation, either it works or not and you'd get an I/O error if an I/O has not complete to disk. Relying on the FS and some UPS hardware device is ok _but_ that is NOT what you usually find in every embedded devices or client desktop - You can't expect everyone to have a UPS to alleviate some issues due to a database system loosing committed rtansaction and therefore not handling ACID durability as it should and it is expected. I've worked at many database companies and dealing with critical-level type of applications and if I had told the customers that could loose committed transactions due to an application or system crash, then I don't think these database companies would have been as successful as they have been. Some things such as not loosing committed transactions have to be handled at the database level and that is what durability is all about. Today, Derby will not loose transactions that have been committed whether you have some UPS or not.
//
//        > Download size: I think David was talking about size of product download (16 MB for
//        > Derby versus 3 MB for H2) not about the jar file size (2.2 MB for Derby versus 1 MB
//        > for H2). By the way, the H2 jar also contains the Console web application and web server,
//        > and other tools. And debugging info (line numbers) is switched on in H2, and switched off
//        > in Derby. But I agree the jar file size is not the most important factor.
//        >
//
//        Download size is irrelevant in today's world except for web applications and in this case, one does NOT have to download the whole product - for embedded applications, it is only 1 JAR file basically and whether it is H2 or Derby, the size is not really an issue (as I mentioned in some earlier thread)
//
//        > Community: Yes, Derby has more developers (4, according to Ohloh, not sure if
//        > this is correct), but that doesn't necessarily mean a better product.
//        > Development of Derby started in 1996 or earlier, while H2 started in 2004
//        > (it is now one year online). H2 is a very young product, and currently doesn't
//        > have professional support from a bigger company. This will be available in the
//        > future when there is demand. You could also say Derby has a liability (big, old,
//        > slow code base). Anyway, H2 also has quite a big community, given how young it is.
//        > But of course Derby has the advantage the Apache name ('branding'), but this doesn't
//        > mean it's better (there are many failed Apache projects).
//        >
//
//        Derby has more than 30+ contributors - what you saw in Ohloh are the top committers for 2007 (new year eh) and this is why it is 4 - last year 23 committers checked-in code, so I'll let you do the stats as far as how many contributors there could be - not every contributor is a committer to the project - that's how Apache works and a lot of other open source projects. Derby has developers from Sun (Java DB), IBM (Cloudscape) as well as other independent contributors or companies. Derby is _not_ big - The footprint is not big (2MB) for the engine compared to some other databases out there and is more than adequate for a lot of today's embedded applications. Apache is not just about branding - it has and continue to be a set of communities for many successful projects with defined rules and guidelines. At the end of the day, it is all about Open Source projects and quite a few of them have made lots of noise in the past many years and still continue to do so.
//
//        > But only time can tell which database is more successful.
//        >
//
//
//        Again, I was not bashing H2 if this is the way you felt - I clearly mentioned that one has to know what type of database(s) one is dealing with before claiming it is faster for *all* use case scenarios out there.
//        
        
//        test big:false net:false cipher:null memory:false log:0 diskResult:false
//        ERROR: query was too quick; result: 0 time:1532 java.lang.Exception: query was too quick; result: 0 time:1532 ------------------------------
//        test big:false net:false cipher:null memory:false log:1 diskResult:false
//        ERROR: query was too quick; result: 0 time:1853 java.lang.Exception: query was too quick; result: 0 time:1853 ------------------------------
//        test big:false net:false cipher:null memory:false log:2 diskResult:false
//        ERROR: query was too quick; result: 0 time:1152 java.lang.Exception: query was too quick; result: 0 time:1152 ------------------------------
//        test big:false net:false cipher:null memory:false log:0 diskResult:false
//        ERROR: query was too quick; result: 0 time:1462 java.lang.Exception: query was too quick; result: 0 time:1462 ------------------------------
//        test big:false net:false cipher:null memory:false log:2 diskResult:false
//        ERROR: query was too quick; result: 0 time:1692 java.lang.Exception: query was too quick; result: 0 time:1692 ------------------------------
//        test big:true net:false cipher:null memory:false log:0 diskResult:false
//        ERROR: query was too quick; result: 0 time:952 java.lang.Exception: query was too quick; result: 0 time:952 ------------------------------
//        test big:true net:false cipher:null memory:false log:1 diskResult:false
//        ERROR: query was too quick; result: 0 time:1171 java.lang.Exception: query was too quick; result: 0 time:1171 ------------------------------
//        test big:true net:false cipher:null memory:false log:2 diskResult:false
//        ERROR: query was too quick; result: 0 time:1502 java.lang.Exception: query was too quick; result: 0 time:1502 ------------------------------
//        test big:true net:false cipher:null memory:false log:0 diskResult:false
//        ERROR: query was too quick; result: 0 time:1372 java.lang.Exception: query was too quick; result: 0 time:1372 ------------------------------

//      Hot backup (incremental backup, online backup): backup data, log, index? files

        // delay reading the row if data is not required
        // document compensations
        // eliminate undo log records if stored on disk (just one pointer per block, not per record)

//        release checklist:
//            add to freshmeat
//            add to http://code.google.com/p/h2database/downloads/list

//        SELECT ... FROM TA, TB, TC WHERE TC.COL3 = TA.COL1 AND TC.COL3=TB.COL2 AND TC.COL4 = 1
//        ...
//        The query implies TA.COL1 = TB.COL2 but does not explicitly set this condition.
        
        //        analyze hibernate read committed tests that fail
        
        // when? server only? special test with TestAll (only this)
//    java.lang.Exception: query was too quick; result: 0 time:1002
//        at org.h2.test.TestBase.logError(TestBase.java:219)
//        at org.h2.test.db.TestCases$1.run(TestCases.java:158)
//        at java.lang.Thread.run(Unknown Source)
        
//        DROP TABLE TEST;
//        CREATE TABLE TEST(C CHAR(10));
//        INSERT INTO TEST VALUES('1');
//        SELECT COUNT(*) FROM TEST WHERE C='1 ';
//        -- PostgreSQL, HSQLDB, MySQL, Derby, MS SQL Server, Oracle: 1
//        -- H2: 0
//        SELECT LENGTH(C), LENGTH(C || 'x') FROM TEST;
//        -- MySQL: 1, 1 (??)
//        -- MS SQL Server: 1, 11 (SELECT LEN(C), LEN(C + 'x') FROM TEST)
//        -- Oracle, Derby: 10, 11
//        -- PostgreSQL, H2, HSQLDB: 1, 2
        
        // maybe use system property for base directory (h2.baseDir)
        
        // feature request: user defined aggregate functions
        
        // auto-upgrade application:
        // check if new version is available 
        // (option: digital signature)
        // if yes download new version
        // (option: http, https, ftp, network)
        // backup database to SQL script
        // (option: list of databases, use recovery mechanism)
        // install new version
        
        // ftp client
        // task to download new version from another HTTP / HTTPS / FTP server
        // multi-task

        // write a test that calls Runtime.halt at more or less random places (extend TestLob)
        
        // OSGi Bundle (see Forum)
        
        // test with PostgreSQL  Version 8.2

        // http://dev.helma.org/Wiki/RhinoLoader
        
        // Test Hibernate / read committed transaction isolation:
        // Data records retrieved by a query are not prevented from modification by some other transaction.
        // Non-repeatable reads may occur, meaning data retrieved in a SELECT statement may be modified
        // by some other transaction when it commits. In this isolation level, read locks are not acquired on selected data.
        
        // test with garbage at the end of the log file (must be consistently detected as such)
        // test LIKE: compare against other databases
        // TestRandomSQL is too random; most statements fails
        // extend the random join test that compared the result against PostgreSQL
        // long running test with the same database
        // repeatable test with a very big database (making backups of the database files)
        
        // the conversion is done automatically when the new engine connects.  
        
        if(args.length>0) {
            if("crash".equals(args[0])) {
                new TestCrashAPI().runTest(test);
            } else if("synth".equals(args[0])) {
                new TestSynth().runTest(test);
            } else if("kill".equals(args[0])) {
                new TestKill().runTest(test);
            } else if("random".equals(args[0])) {
                new TestRandomSQL().runTest(test);
            } else if("join".equals(args[0])) {
                new TestJoin().runTest(test);
            } else if("btree".equals(args[0])) {
                new TestBtreeIndex().runTest(test);
            } else if("all".equals(args[0])) {
                test.testEverything();
            } else if("codeCoverage".equals(args[0])) {
                test.testCodeCoverage();
            } else if("multiThread".equals(args[0])) {
                new TestMulti().runTest(test);
            }
        } else {
            test.runTests();
        }
        System.out.println("done ("+(System.currentTimeMillis()-time)+" ms)");
    }

    void runTests() throws Exception {
        
//        TODO test set lock_mode=0, 1; max_trace_file_size; modes; collation; assert
//        TODO test shutdown immediately

//        smallLog = big = networked = memory = ssl = textStorage = diskResult = deleteIndex = traceSystemOut = false;
//        logMode = 1; traceLevelFile = throttle = 0;
//        deleteIndex = textStorage = true;
//        cipher = null;

//        codeCoverage = true;

//        memory = true;
//        new TestSpeed().runTest(this);
//        new TestSpeed().runTest(this);
//        new TestSpeed().runTest(this);
//        new TestSpeed().runTest(this);



//        smallLog = big = networked = memory = ssl = textStorage = diskResult = deleteIndex = traceSystemOut = diskUndo = false;
//        traceLevelFile = throttle = 0;
//        big = true;
//        memory = false;
//
        
        testQuick();
        testCombination();

    }

    void testCodeCoverage() throws Exception {
        this.codeCoverage = true;
        runTests();
    }

    void testQuick() throws Exception {
        smallLog = big = networked = memory = ssl = textStorage = diskResult = deleteIndex = traceSystemOut = diskUndo = false;
        traceLevelFile = throttle = 0;
        logMode = 1;
        cipher = null;
        testAll();
    }

    void testEverything() throws Exception {
        for(int c = 0; c < 3; c++) {
            if(c == 0) {
                cipher = null;
            } else if(c==1) {
                cipher = "XTEA";
            } else {
                cipher = "AES";
            }
            for(int a = 0; a < 256; a++) {
                smallLog = (a & 1) != 0;
                big = (a & 2) != 0;
                networked = (a & 4) != 0;
                memory = (a & 8) != 0;
                ssl = (a & 16) != 0;
                textStorage = (a & 32) != 0;
                diskResult = (a & 64) != 0;
                deleteIndex = (a & 128) != 0;
                for(logMode = 0; logMode < 3; logMode++) {
                    traceLevelFile = logMode;
                    TestBase.printTime("cipher:" + cipher +" a:" +a+" logMode:"+logMode);
                    testAll();
                }
            }
        }
    }

    void testCombination() throws Exception {
        smallLog = big = networked = memory = ssl = textStorage = diskResult = deleteIndex = traceSystemOut = false;
        traceLevelFile = throttle = 0;
        logMode = 1;
        cipher = null;
        testAll();

        diskUndo = false;
        smallLog = false;
        big = false;
        networked = true;
        memory = true;
        ssl = false;
        textStorage = true;
        diskResult = deleteIndex = traceSystemOut = false;
        traceLevelFile = throttle = 0;
        logMode = 1;
        cipher = null;
        testAll();
        
        big = false;
        smallLog = false;
        networked = false;
        memory = false;
        ssl = false;
        textStorage = false;
        diskResult = false;
        deleteIndex = false;
        traceSystemOut = false;
        logMode = 2;
        traceLevelFile = 0;
        throttle = 0;
        cipher = null;
        testAll();        

        diskUndo = true;
        smallLog = false;
        big = networked = memory = ssl = false;
        textStorage = true;
        diskResult = true;
        deleteIndex = true;
        traceSystemOut = false;
        logMode = 1;
        traceLevelFile = 3;
        throttle = 1;
        cipher = "XTEA";
        testAll();

        diskUndo = false;
        big = true;
        smallLog = false;
        networked = false;
        memory = false;
        ssl = false;
        textStorage = false;
        diskResult = false;
        deleteIndex = false;
        traceSystemOut = false;
        logMode = 1;
        traceLevelFile = 1;
        throttle = 0;
        cipher = null;
        testAll();

        big = true;
        smallLog = true;
        networked = true;
        memory = false;
        ssl = true;
        textStorage = false;
        diskResult = false;
        deleteIndex = false;
        traceSystemOut = false;
        logMode = 2;
        traceLevelFile = 2;
        throttle = 0;
        cipher = null;
        testAll();
        
        big = true;
        smallLog = false;
        networked = true;
        memory = false;
        ssl = false;
        textStorage = false;
        diskResult = false;
        deleteIndex = false;
        traceSystemOut = false;
        logMode = 0;
        traceLevelFile = 0;
        throttle = 0;
        cipher = "AES";
        testAll();
        
    }

    void testAll() throws Exception {
        DeleteDbFiles.execute(TestBase.BASE_DIR, null, true);
        testDatabase();
        testUnit();
        DeleteDbFiles.execute(TestBase.BASE_DIR, null, true);
    }

    void testUnit() {
        new TestBitField().runTest(this);
        new TestCompress().runTest(this);
        new TestDataPage().runTest(this);
        new TestExit().runTest(this);
        new TestFileLock().runTest(this);
        new TestIntArray().runTest(this);
        new TestIntIntHashMap().runTest(this);
        new TestOverflow().runTest(this);
        new TestPattern().runTest(this);
        new TestReader().runTest(this);
        new TestSampleApps().runTest(this);
        new TestScriptReader().runTest(this);
        new TestSecurity().runTest(this);
        new TestStreams().runTest(this);
        new TestStringCache().runTest(this);
        new TestStringUtils().runTest(this);
        new TestTools().runTest(this);
        new TestValueHashMap().runTest(this);
    }

    void testDatabase() throws Exception {
        System.out.println("test big:"+big+" net:"+networked+" cipher:"+cipher+" memory:"+memory+" log:"+logMode+" diskResult:"+diskResult);
        beforeTest();

        // db
        new TestScriptSimple().runTest(this);
        new TestScript().runTest(this);
        new TestAutoRecompile().runTest(this);
        new TestBatchUpdates().runTest(this);
        new TestBigDb().runTest(this);
        new TestBigResult().runTest(this);
        new TestCache().runTest(this);
        new TestCases().runTest(this);
        new TestCheckpoint().runTest(this);
        new TestCluster().runTest(this);
        new TestCompatibility().runTest(this);
        new TestCsv().runTest(this);
        new TestFunctions().runTest(this);
        new TestIndex().runTest(this);
        new TestLinkedTable().runTest(this);
        new TestListener().runTest(this);
        new TestLob().runTest(this);
        new TestLogFile().runTest(this);
        new TestMemoryUsage().runTest(this);
        new TestMultiConn().runTest(this);
        new TestMultiDimension().runTest(this);
        new TestMultiThread().runTest(this);
        new TestOpenClose().runTest(this);
        new TestOptimizations().runTest(this);
        new TestPowerOff().runTest(this);
        new TestReadOnly().runTest(this);
        new TestRights().runTest(this);
        new TestRunscript().runTest(this);
        new TestSQLInjection().runTest(this);
        new TestSequence().runTest(this);
        new TestSpaceReuse().runTest(this);
        new TestSpeed().runTest(this);
        new TestTempTables().runTest(this);
        new TestTransaction().runTest(this);
        new TestTriggersConstraints().runTest(this);
        new TestTwoPhaseCommit().runTest(this);

        // server
        new TestNestedLoop().runTest(this);

        // jdbc
        new TestCancel().runTest(this);
        new TestDataSource().runTest(this);
        new TestManyJdbcObjects().runTest(this);
        new TestMetaData().runTest(this);
        new TestNativeSQL().runTest(this);
        new TestPreparedStatement().runTest(this);
        new TestResultSet().runTest(this);
        new TestStatement().runTest(this);
        new TestTransactionIsolation().runTest(this);
        new TestUpdatableResultSet().runTest(this);
        new TestZloty().runTest(this);

        afterTest();
    }

    public void beforeTest() throws SQLException {
        if(networked) {
            TcpServer.LOG_INTERNAL_ERRORS = true;
            String[] args = ssl ? new String[]{"-tcpSSL", "true"} : new String[0];
            server = Server.createTcpServer(args);
            try {
                server.start();
            } catch(SQLException e) {
                System.out.println("FAIL: can not start server (may already be running)");
                server = null;
            }
        }
        if(this.diskResult) {
            oldCacheMin = Constants.CACHE_MIN_RECORDS;
            Constants.CACHE_MIN_RECORDS = 0;
        }
    }

    public void afterTest() {
        if(networked && server != null) {
            server.stop();
        }
        if(this.diskResult) {
            Constants.CACHE_MIN_RECORDS = oldCacheMin;
        }
    }

    private void printSystem() {
        Properties prop = System.getProperties();
        System.out.println("Java: " +
                prop.getProperty("java.runtime.version") + ", " +
                prop.getProperty("java.vm.name")+", " +
                prop.getProperty("java.vendor"));
        System.out.println("Env: " +
                prop.getProperty("os.name") + ", " +
                prop.getProperty("os.arch")+", "+
                prop.getProperty("os.version")+", "+
                prop.getProperty("sun.os.patch.level")+", "+
                prop.getProperty("file.separator")+" "+
                prop.getProperty("path.separator")+" "+
                StringUtils.javaEncode( prop.getProperty("line.separator"))+" "+
                prop.getProperty("user.country") + " " +
                prop.getProperty("user.language") + " " +
                prop.getProperty("user.variant")+" "+
                prop.getProperty("file.encoding"));
    }
}
