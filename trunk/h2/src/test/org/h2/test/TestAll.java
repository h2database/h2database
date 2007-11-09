/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test;

import java.sql.SQLException;
import java.util.Properties;

import org.h2.server.TcpServer;
import org.h2.store.fs.FileSystemDisk;
import org.h2.test.db.TestAutoRecompile;
import org.h2.test.db.TestBackup;
import org.h2.test.db.TestBatchUpdates;
import org.h2.test.db.TestBigDb;
import org.h2.test.db.TestBigResult;
import org.h2.test.db.TestCases;
import org.h2.test.db.TestCheckpoint;
import org.h2.test.db.TestCluster;
import org.h2.test.db.TestCompatibility;
import org.h2.test.db.TestCsv;
import org.h2.test.db.TestFullText;
import org.h2.test.db.TestFunctions;
import org.h2.test.db.TestIndex;
import org.h2.test.db.TestLinkedTable;
import org.h2.test.db.TestListener;
import org.h2.test.db.TestLob;
import org.h2.test.db.TestLogFile;
import org.h2.test.db.TestMemoryUsage;
import org.h2.test.db.TestMultiConn;
import org.h2.test.db.TestMultiDimension;
import org.h2.test.db.TestMultiThread;
import org.h2.test.db.TestOpenClose;
import org.h2.test.db.TestOptimizations;
import org.h2.test.db.TestPowerOff;
import org.h2.test.db.TestReadOnly;
import org.h2.test.db.TestRights;
import org.h2.test.db.TestRunscript;
import org.h2.test.db.TestSQLInjection;
import org.h2.test.db.TestScript;
import org.h2.test.db.TestScriptSimple;
import org.h2.test.db.TestSequence;
import org.h2.test.db.TestSpaceReuse;
import org.h2.test.db.TestSpeed;
import org.h2.test.db.TestTempTables;
import org.h2.test.db.TestTransaction;
import org.h2.test.db.TestTriggersConstraints;
import org.h2.test.db.TestTwoPhaseCommit;
import org.h2.test.db.TestView;
import org.h2.test.jdbc.TestCallableStatement;
import org.h2.test.jdbc.TestCancel;
import org.h2.test.jdbc.TestDataSource;
import org.h2.test.jdbc.TestDatabaseEventListener;
import org.h2.test.jdbc.TestManyJdbcObjects;
import org.h2.test.jdbc.TestMetaData;
import org.h2.test.jdbc.TestNativeSQL;
import org.h2.test.jdbc.TestPreparedStatement;
import org.h2.test.jdbc.TestResultSet;
import org.h2.test.jdbc.TestStatement;
import org.h2.test.jdbc.TestTransactionIsolation;
import org.h2.test.jdbc.TestUpdatableResultSet;
import org.h2.test.jdbc.TestZloty;
import org.h2.test.jdbc.xa.TestXA;
import org.h2.test.mvcc.TestMVCC;
import org.h2.test.server.TestNestedLoop;
import org.h2.test.server.TestPgServer;
import org.h2.test.server.TestWeb;
import org.h2.test.synth.TestBtreeIndex;
import org.h2.test.synth.TestKillRestart;
import org.h2.test.synth.TestCrashAPI;
import org.h2.test.synth.TestHaltApp;
import org.h2.test.synth.TestJoin;
import org.h2.test.synth.TestKill;
import org.h2.test.synth.TestMulti;
import org.h2.test.synth.TestRandomSQL;
import org.h2.test.synth.TestSynth;
import org.h2.test.synth.TestTimer;
import org.h2.test.unit.TestBitField;
import org.h2.test.unit.TestCache;
import org.h2.test.unit.TestCompress;
import org.h2.test.unit.TestDataPage;
import org.h2.test.unit.TestExit;
import org.h2.test.unit.TestFile;
import org.h2.test.unit.TestFileLock;
import org.h2.test.unit.TestFileSystem;
import org.h2.test.unit.TestFtp;
import org.h2.test.unit.TestIntArray;
import org.h2.test.unit.TestIntIntHashMap;
import org.h2.test.unit.TestMultiThreadedKernel;
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
 * The main test application. JUnit is not used because loops are easier to write in 
 * regular java applications (most tests are ran multiple times using different settings).
 */
public class TestAll {

// Snippets to run test code:
// java -cp .;%H2DRIVERS% org.h2.test.TestAll
// java -Xrunhprof:cpu=samples,depth=8 org.h2.test.TestAll
// java -Xrunhprof:heap=sites,depth=8 org.h2.test.TestAll

/*

Random test:
 
cd bin
del *.db
start cmd /k "java -cp .;%H2DRIVERS% org.h2.test.TestAll join >testJoin.txt"
start cmd /k "java -cp . org.h2.test.TestAll crash >testCrash.txt"
start cmd /k "java -cp . org.h2.test.TestAll synth >testSynth.txt"
start cmd /k "java -cp . org.h2.test.TestAll all >testAll.txt"
start cmd /k "java -cp . org.h2.test.TestAll random >testRandom.txt"
start cmd /k "java -cp . org.h2.test.TestAll btree >testBtree.txt"
start cmd /k "java -cp . org.h2.test.TestAll halt >testHalt.txt"

java org.h2.test.TestAll timer

*/

    public boolean smallLog, big, networked, memory, ssl, textStorage, diskUndo, diskResult, deleteIndex, traceSystemOut;
    public boolean codeCoverage, mvcc, endless;
    public int logMode = 1, traceLevelFile, throttle;
    public String cipher;

    public boolean traceTest, stopOnError;
    public boolean jdk14 = true;

    private Server server;
    public boolean cache2Q;
    
    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();
        TestAll test = new TestAll();
        test.printSystem();      

/*

implement & test: checkpoint commits running transactions

start writing javadocs for jdbcx package

toString() method to print something useful

Feature request: file system that writes to two file systems (for replication)
Feature request: file system with background thread writing file system (all writes)

test DbStarter

TestFileSystem

create table test(id int, name varchar);
insert into test select x, '' from system_range(1, 10000);
-- fast
update test set name = 'y' where cast(id as varchar) like '1%';
-- slow
update test set name = 'x' where id in (select x from system_range(1, 10000) where cast(x as varchar) like '1%');
drop table test;

----
A file is sent although the Japanese translation has not been completed yet.
----

At startup, when corrupted, say if LOG=0 was used before

add more MVCC tests

slow:
select ta.attname, ia.attnum, ic.relname 
from pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class ic, pg_catalog.pg_index i, pg_catalog.pg_namespace n 
where ic.relname = 'dummy_pkey' 
AND n.nspname = '' 
AND ic.oid = i.indexrelid 
AND n.oid = ic.relnamespace 
AND ia.attrelid = i.indexrelid 
AND ta.attrelid = i.indrelid 
AND ta.attnum = i.indkey[ia.attnum-1] 
AND (NOT ta.attisdropped) 
AND (NOT ia.attisdropped) 
order by ia.attnum;

DROP TABLE IF EXISTS TEST;
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR);
@LOOP 1000000 INSERT INTO TEST VALUES(?, SPACE(100000));
<stop>
<reconnect>
out of memory?

shrink newsletter list (migrate to google groups)

don't create @~ of not translated

test performance and document fulltext search

clustered tables: test, document

extend tests that simulate power off

HSQLDB compatibility:
Openfire server uses this script to setup a user permissions
on the fresh-installed server. The database is [current] HSQLDB :
CREATE SCHEMA PUBLIC AUTHORIZATION DBA
CREATE USER SA PASSWORD ""
GRANT DBA TO SA
SET SCHEMA PUBLIC
Unfortunately, this does not work in H2
Wrong user name or password [08004-55]

rename Performance > Comparison [/Compatibility]
move  Comparison to Other Database Engines > Comparison
move Products that Work with H2 > Comparison
move Performance Tuning > Advanced Topics

testHalt
java org.h2.test.TestAll halt 

timer test

java.lang.Exception: query was too quick; result: 0 time:968
        at org.h2.test.TestBase.logError(TestBase.java:220)
        at org.h2.test.db.TestCases$1.run(TestCases.java:170)
        at java.lang.Thread.run(Thread.java:595)
        
ftp server: problem with multithreading?

h2\src\docsrc\html\images\SQLInjection.txt

send http://thecodist.com/fiche/thecodist/article/sql-injections-how-not-to-get-stuck to JavaWorld, TheServerSide, 
Send SQL Injection solution proposal to PostgreSQL, MySQL, Derby, HSQLDB,...
Convert SQL-injection-2.txt to html document, include SQLInjection.java sample
MySQL, PostgreSQL

READ_TEXT(fileName String) returning a CLOB. 
I am not sure if this will read the CLOB in memory however. 

Improve LOB in directories performance

Automate real power off tests

http://fastutil.dsi.unimi.it/
http://javolution.org/
http://joda-time.sourceforge.net/
http://ibatis.apache.org/

http://www.igniterealtime.org/projects/openfire/index.jsp

translation:
src/org.h2.res/help.csv (using ${.} as in .jsp?)
javadocs (using generated ${.} ?)
glossary
spell check / word list per language
translated .pdf

write tests using the PostgreSQL JDBC driver

*/        
        
// run  TestHalt

// GroovyServlet

// Cluster: hot deploy (adding a node on runtime)

// test with PostgreSQL  Version 8.2

// http://dev.helma.org/Wiki/RhinoLoader

// test with garbage at the end of the log file (must be consistently detected as such)
// test LIKE: compare against other databases

// TestRandomSQL is too random; most statements fails

// extend the random join test that compared the result against PostgreSQL

// long running test with the same database

// repeatable test with a very big database (making backups of the database files)
        
/*
Features of H2
- Case insensitive string data type
- GROUP_CONCAT aggregate, User defined aggregates
*/        
        
        if (args.length > 0) {
            if ("crash".equals(args[0])) {
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
            } else if ("btree".equals(args[0])) {
                new TestBtreeIndex().runTest(test);
            } else if ("all".equals(args[0])) {
                test.testEverything();
            } else if ("codeCoverage".equals(args[0])) {
                test.testCodeCoverage();
            } else if ("multiThread".equals(args[0])) {
                new TestMulti().runTest(test);
            } else if ("halt".equals(args[0])) {
                new TestHaltApp().runTest(test);
            } else if ("timer".equals(args[0])) {
                new TestTimer().runTest(test);
            }
        } else {
            test.runTests();
        }
        System.out.println("done (" + (System.currentTimeMillis() - time) + " ms)");
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
        for (int c = 0; c < 3; c++) {
            if (c == 0) {
                cipher = null;
            } else if (c == 1) {
                cipher = "XTEA";
            } else {
                cipher = "AES";
            }
            for (int a = 0; a < 256; a++) {
                smallLog = (a & 1) != 0;
                big = (a & 2) != 0;
                networked = (a & 4) != 0;
                memory = (a & 8) != 0;
                ssl = (a & 16) != 0;
                textStorage = (a & 32) != 0;
                diskResult = (a & 64) != 0;
                deleteIndex = (a & 128) != 0;
                for (logMode = 0; logMode < 3; logMode++) {
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
        mvcc = false;
        cache2Q = false;        
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
        mvcc = false;
        cache2Q = false;        
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
        mvcc = false;
        cache2Q = false;        
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
        mvcc = false;
        cache2Q = false;        
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
        mvcc = false;
        cache2Q = false;        
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
        mvcc = false;
        cache2Q = true;        
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
        mvcc = false;
        cache2Q = false;        
        testAll();
        
        smallLog = big = networked = memory = ssl = textStorage = diskResult = deleteIndex = traceSystemOut = false;
        traceLevelFile = throttle = 0;
        logMode = 1;
        cipher = null;
        mvcc = true;
        cache2Q = false;        
        testAll();
        
    }
    
    void testAll() throws Exception {
        DeleteDbFiles.execute(TestBase.baseDir, null, true);
        testDatabase();
        testUnit();
        DeleteDbFiles.execute(TestBase.baseDir, null, true);
    }

    void testUnit() {
        new TestBitField().runTest(this);
        new TestCompress().runTest(this);
        new TestDataPage().runTest(this);
        new TestExit().runTest(this);
        new TestFile().runTest(this);
        new TestFileLock().runTest(this);
        new TestFtp().runTest(this);
        new TestFileSystem().runTest(this);
        new TestIntArray().runTest(this);
        new TestIntIntHashMap().runTest(this);
        new TestMultiThreadedKernel().runTest(this);
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
        System.out.println("test big:"+big+" net:"+networked+" cipher:"+cipher+" memory:"+memory+" log:"+logMode+" diskResult:"+diskResult + " mvcc:" + mvcc);
        beforeTest();
        
//         int testMvcc;
//         mvcc = true;

        // db
        new TestScriptSimple().runTest(this);
        new TestScript().runTest(this);
        new TestAutoRecompile().runTest(this);
        new TestBackup().runTest(this);
        new TestBatchUpdates().runTest(this);
        new TestBigDb().runTest(this);
        new TestBigResult().runTest(this);
        new TestCache().runTest(this);
        new TestCases().runTest(this);
        new TestCheckpoint().runTest(this);
        new TestCluster().runTest(this);
        new TestCompatibility().runTest(this);
        new TestCsv().runTest(this);
        new TestFullText().runTest(this);
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
        new TestView().runTest(this);

        // server
        new TestNestedLoop().runTest(this);
        new TestWeb().runTest(this);
        new TestPgServer().runTest(this);

        // jdbc
        new TestCallableStatement().runTest(this);
        new TestCancel().runTest(this);
        new TestDatabaseEventListener().runTest(this);
        new TestDataSource().runTest(this);
        new TestManyJdbcObjects().runTest(this);
        new TestMetaData().runTest(this);
        new TestNativeSQL().runTest(this);
        new TestPreparedStatement().runTest(this);
        new TestResultSet().runTest(this);
        new TestStatement().runTest(this);
        new TestTransactionIsolation().runTest(this);
        new TestUpdatableResultSet().runTest(this);
        new TestXA().runTest(this);
        new TestZloty().runTest(this);

        // mvcc
        new TestMVCC().runTest(this);

        // synthetic
        new TestRandomSQL().runTest(this);
        new TestKillRestart().runTest(this);

        afterTest();
    }

    public void beforeTest() throws SQLException {
        FileSystemDisk.getInstance().deleteRecursive("trace.db");
        if (networked) {
            TcpServer.logInternalErrors = true;
            String[] args = ssl ? new String[] { "-tcpSSL", "true" } : new String[0];
            server = Server.createTcpServer(args);
            try {
                server.start();
            } catch (SQLException e) {
                System.out.println("FAIL: can not start server (may already be running)");
                server = null;
            }
        }
    }

    public void afterTest() throws SQLException {
        FileSystemDisk.getInstance().deleteRecursive("trace.db");
        if (networked && server != null) {
            server.stop();
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
                StringUtils.javaEncode(prop.getProperty("line.separator")) + " " +
                prop.getProperty("user.country") + " " +
                prop.getProperty("user.language") + " " +
                prop.getProperty("user.variant")+" "+
                prop.getProperty("file.encoding"));
    }
}
