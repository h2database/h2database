/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.util.ArrayList;

import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.util.RandomUtils;

// TODO hsqldb: call 1||null should return 1 but returns null
// TODO hsqldb: call mod(1) should return invalid parameter count but returns null
public class TestSynth extends TestBase {

    static final int H2 = 0, H2_MEM = 1, HSQLDB = 2, MYSQL = 3, POSTGRESQL = 4;
    
    private DbState db = new DbState(this);
    private ArrayList databases;
    private ArrayList commands;
    private RandomGen random = new RandomGen(this);
    private boolean showError, showLog;
    private boolean stopImmediately;
    private int mode;
    private String DIR = "synth";    
    
    public boolean is(int isType) {
        return mode == isType;
    }
    
    public TestSynth() {
    }
    
    public RandomGen random() {
        return random;
    }
    
    public String randomIdentifier() {
        int len = random.getLog(8)+2;
        while(true) {
            return random.randomString(len);
        }
    }
    
    private void add(Command command) throws Exception {
        command.run(db);
        commands.add(command);
    }
    
    private void addRandomCommands() throws Exception {
        switch(random.getInt(20)) {
        case 0: {
            add(Command.getDisconnect(this));
            add(Command.getConnect(this));
            break;
        }
        case 1: {
            Table table = Table.newRandomTable(this);
            add(Command.getCreateTable(this, table));
            break;
        }
        case 2: {
            Table table = randomTable();
            add(Command.getCreateIndex(this, table.newRandomIndex()));
            break;
        }
        case 3:
        case 4:
        case 5: {
            Table table = randomTable();
            add(Command.getRandomInsert(this, table));
            break;
        }
        case 6:
        case 7:
        case 8: {
            Table table = randomTable();
            add(Command.getRandomUpdate(this, table));
            break;
        }
        case 9:
        case 10: {
            Table table = randomTable();
            add(Command.getRandomDelete(this, table));
            break;
        }        
        default: {
            Table table = randomTable();
            add(Command.getRandomSelect(this, table));
        }
        }
    }

    private void testRun(int seed) throws Exception {
        random.setSeed(seed);
        commands = new ArrayList();
        add(Command.getConnect(this));
        add(Command.getReset(this));
        
        for(int i=0; i<1; i++) {
            Table table = Table.newRandomTable(this);
            add(Command.getCreateTable(this, table));
            add(Command.getCreateIndex(this, table.newRandomIndex()));
        }
        for(int i=0; i<400; i++) {
            addRandomCommands();
        }
//          for (int i = 0; i < 20; i++) {
//            Table table = randomTable();
//            add(Command.getRandomInsert(this, table));
//        }
//        for (int i = 0; i < 100; i++) {
//            Table table = randomTable();
//            add(Command.getRandomSelect(this, table));
//        }
//        for (int i = 0; i < 10; i++) {
//            Table table = randomTable();
//            add(Command.getRandomUpdate(this, table));
//        }
//        for (int i = 0; i < 30; i++) {
//            Table table = randomTable();
//            add(Command.getRandomSelect(this, table));
//        }                
//        for (int i = 0; i < 50; i++) {
//            Table table = randomTable();
//            add(Command.getRandomDelete(this, table));
//        }
//        for (int i = 0; i < 10; i++) {
//            Table table = randomTable();
//            add(Command.getRandomSelect(this, table));
//        }                
//        while(true) {
//            Table table = randomTable();
//            if(table == null) {
//                break;
//            }
//            add(Command.getDropTable(this, table));
//        }
        add(Command.getDisconnect(this));
        add(Command.getEnd(this));
        
        for(int i=0; i<commands.size(); i++) {
            Command command = (Command) commands.get(i);
            boolean stop = process(seed, i, command);
            if(stop) {
                break;
            }
        }
    }
    
    private boolean process(int seed, int id, Command command) throws Exception {
        try {
            
            ArrayList results = new ArrayList();
            for(int i=0; i<databases.size(); i++) {
                DbInterface db = (DbInterface)databases.get(i);
                Result result = command.run(db);
                results.add(result);
                if(showError && i==0) {
//                    result.log();
                }
            }
            compareResults(results);
        
        } catch(Error e) {
            if(showError) {
                e.printStackTrace();
            }
            System.out.println("new TestSynth().init(test).testCase(" + seed+"); // id="+id +" " + e.toString());
            if(stopImmediately) {
                System.exit(0);
            }
            return true;
        }
        return false;
    }

    private void compareResults(ArrayList results) {
        Result original = (Result) results.get(0);
        for (int i = 1; i < results.size(); i++) {
            Result copy = (Result) results.get(i);
            if (original.compareTo(copy) != 0) {
                if (showError) {
                    throw new Error("Results don't match: original (0): \r\n" + original + "\r\nother:\r\n" + copy);
                } else {
                    throw new Error("Results don't match");
                }
            }
        }
    }
    
    public Table randomTable() {
        return db.randomTable();
    }

    public void log(int id, String s) {
        if(showLog && id==0) {
            System.out.println(s);
        }
    }
    
    public int getMode() {
        return mode;
    }

    private void addDatabase(String className, String url, String user, String password, boolean useSentinel) {
        DbConnection db = new DbConnection(this, className, url, user, password, databases.size(), useSentinel);
        databases.add(db);
    }
    
    // java -cp .;..\..\java\mysql.jar;..\..\java\ldbc.jar;..\..\java\postgresql-8.0-311.jdbc3.jar org.h2.test.TestAll

    public TestBase init(TestAll conf) throws Exception {
        super.init(conf);
        BASE_DIR = "dataSynth";
        deleteDb("synth");
        databases = new ArrayList();

//        mode = HSQLDB;
//        addDatabase("org.hsqldb.jdbcDriver", "jdbc:hsqldb:test", "sa", "" );
//        addDatabase("org.h2.Driver", "jdbc:h2:synth;mode=hsqldb", "sa", "");
        
//        mode = POSTGRESQL;
//        addDatabase("org.postgresql.Driver", "jdbc:postgresql:test", "sa", "sa");
//        addDatabase("org.h2.Driver", "jdbc:h2:synth;mode=postgresql", "sa", "");

        mode = H2_MEM;
        Class.forName("org.h2.Driver");
        addDatabase("org.h2.Driver", "jdbc:h2:mem:synth", "sa", "", true);
        addDatabase("org.h2.Driver", "jdbc:h2:"+BASE_DIR+"/"+DIR+"/synth", "sa", "", false);
        
//        addDatabase("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/test", "sa", "");
//        addDatabase("org.h2.Driver", "jdbc:h2:synth;mode=mysql", "sa", "");
        
//        addDatabase("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/test", "sa", "");
//        addDatabase("org.ldbc.jdbc.jdbcDriver", "jdbc:ldbc:mysql://localhost/test", "sa", "");
//        addDatabase("org.h2.Driver", "jdbc:h2:inmemory:synth", "sa", "");
        
        // MySQL: NOT is bound to column: NOT ID = 1 means (NOT ID) = 1 instead of NOT (ID=1)
        for (int i = 0; i < databases.size(); i++) {
            DbConnection conn = (DbConnection) databases.get(i);
            System.out.println(i + " = " + conn.toString());
        }
        showError = true;
        showLog = false;

//        stopImmediately = true;
//        showLog = true;
//        testRun(110600); // id=27 java.lang.Error: Results don't match: original (0): 
//       System.exit(0);

        
        BASE_DIR = "data";        
        return this;
    }
    
    public void testCase(int i) throws Exception {
        BASE_DIR = "dataCrash";
        deleteDb(BASE_DIR, DIR+"/synth");
        try {
            printTime("TestSynth " + i);
            testRun(i);
        } catch (Error e) {
            System.out.println(e.toString());
            e.printStackTrace();
            System.exit(0);
        }
        BASE_DIR = "data";
    }
    
    public void test() throws Exception {
        while(true) {
            int seed = RandomUtils.nextInt(Integer.MAX_VALUE);
            testCase(seed);
        }
    }

}
