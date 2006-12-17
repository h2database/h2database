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
        
        // codeswitch_jdk13 doesnt work? (in FAQ)
        
        // Check if new Hibernate dialect for H2 is ok
        // http://opensource.atlassian.com/projects/hibernate/browse/HHH-2300
        
        // open JPA test - submit patch
//        d:\data\h2test\openjpa\openjpa-persistence-jdbc\src\test\resources\META-INF\persistence.xml
//        <!--            <property name="openjpa.ConnectionProperties"
//            value="DriverClassName=org.apache.derby.jdbc.EmbeddedDriver,Url=jdbc:derby:target/database/openjpa-test-database;create=true,MaxActive=100,MaxWait=10000,TestOnBorrow=true"/>
//-->                
//        <property name="openjpa.ConnectionProperties"
//            value="DriverClassName=org.h2.Driver,Url=jdbc:h2:c:/temp/openjpa,MaxActive=100,MaxWait=10000,TestOnBorrow=true"/>
//        D:\data\h2test\openjpa\openjpa-persistence-jdbc>mvn test        

        // add H2 to maven
        // OSGi Bundle (see Forum)
        // Test and document JDK 1.6 QueryObjectFactory 
        
        // test with PostgreSQL  Version 8.2
        
//        create table testoe(id int primary key, name varchar(255))
//        create user oe identified by merlin 
//        CREATE SCHEMA AUTHORIZATION oe
//           CREATE TABLE new_product 
//              (color VARCHAR2(10)  PRIMARY KEY, quantity NUMBER) 
//           CREATE VIEW new_product_view 
//              AS SELECT color, quantity FROM new_product WHERE color = 'RED' 
//           GRANT select ON new_product_view TO hr; 

        // http://dev.helma.org/Wiki/RhinoLoader
        // use version numbers 1.0.4 and so on
        // Fulltext search: Use reader for CLOB data, special reader tokenizer
        // Checkstyle to verify HTML docs
        // test multithreading access to blobs (one thread reads from the inputstream, the other deletes the row using the same connection)
        // Dezign for Databases (http://www.datanamic.com
        // SET LOCK_MODE 3 not persistent, document how to set it in the database URL
        // http://eclipse-cs.sourceforge.net/
        //  http://andrei.gmxhome.de/anyedit/links.html
        // Constants.ALLOW_EMTPY_BTREE_PAGES = true

        // Test Hibernate / read committed transaction isolation:
        // Data records retrieved by a query are not prevented from modification by some other transaction.
        // Non-repeatable reads may occur, meaning data retrieved in a SELECT statement may be modified
        // by some other transaction when it commits. In this isolation level, read locks are not acquired on selected data.
        // test with garbage at the end of the log file (must be consistently detected as such)
        // describe differences between databases (Migration Guide; HSQLDB most important)
        // Maven
        // http://maven.apache.org/guides/mini/guide-ibiblio-upload.html
        // test LIKE: compare against other databases
        // autocomplete: if I type the name of a table that does not exist (should say: syntax not supported)
        // autocomplete: schema support: "Other Grammar","Table Expression","{[schemaName.]tableName | (select)} [[AS] newTableAlias]
        // TestRandomSQL is too random; most statements fails
        // extend the random join test that compared the result against PostgreSQL
        //  Donate a translation: I am looking for people who would help translating the H2 Console into other languages. Please tell me if you think you can help
        // extend TestJoin
        // long running test with the same database
        // repeatable test with a very big database (making backups of the database files)
        // check performance monitor Avg. Disk Write Queue Length, disk writes
        // test case where the GCJ problem can be repeated

        // OpenOffice docs:
        // - Stop OpenOffice, including the autostart
        // - Copy h2.jar into the directory <OpenOffice>\program\classes
        // - Start OpenOffice Base
        // - Connect to an existing database, select JDBC, [Next]
        // - Datasource URL: jdbc:h2:c:/temp/test;TRACE_LEVEL_FILE=3
        // - JDBC driver class: org.h2.Driver [Test class]

        
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
        new TestScript().runTest(this);
        new TestScriptSimple().runTest(this);
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
