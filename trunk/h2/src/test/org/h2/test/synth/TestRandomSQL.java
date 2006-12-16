/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2.bnf.Bnf;
import org.h2.bnf.RuleHead;
import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.util.RandomUtils;

public class TestRandomSQL extends TestBase {
    
    private int dbId;
    private boolean showSQL;
    private ArrayList statements;
    private int seed;
    private boolean exitOnError = true;
    private Bnf bnf;

    private void processException(String sql, SQLException e) {
        if(e.getSQLState().equals("HY000")) {
            System.out.println("new TestRandomSQL().init(test).testCase("+seed+");  // FAIL: " + e.toString());
            e.printStackTrace();
            if(exitOnError) {
                new Error(sql, e).printStackTrace();
                System.exit(0);
            }
        }
    }
    
    private String getDatabaseName() {
//        return "dataSynth/randomsql" + dbId+";TRACE_LEVEL_FILE=3";
        return "dataSynth/randomsql" + dbId;
    }
    
    private Connection connect() throws Exception {
        while(true) {
            try {
                return getConnection(getDatabaseName());
            } catch(SQLException e) {
                dbId--;
                try {
                    deleteDb(getDatabaseName());
                } catch(Exception e2) {
                    // ignore
                }
                dbId++;
                try {
                    deleteDb(getDatabaseName());
                } catch(Exception e2) {
                    // ignore
                }
                dbId++;
                try {
                    deleteDb(getDatabaseName());
                } catch(SQLException e2) {
                    dbId++;
                    deleteDb(getDatabaseName());
                }
            }
        }
        
    }
    
    public TestBase init(TestAll conf) throws Exception {
        super.init(conf);
        bnf = Bnf.getInstance(null);
        bnf.linkStatements();
        statements = bnf.getStatements();
        
        // go backwards so we can append at the end
        for(int i=statements.size() - 1; i>=0; i--) {
            RuleHead r = (RuleHead) statements.get(i);
            String topic = r.getTopic();
            int weight = 0;
            if(topic.equals("select")) {
                weight = 50;
            } else if(topic.equals("createtable")) {
                weight = 20;
            } else if(topic.equals("insert")) {
                weight = 20;
            } else if(topic.startsWith("update")) {
                weight = 10;
            } else if(topic.startsWith("delete")) {
                weight = 5;
            } else if(topic.startsWith("drop")) {
                weight = 5;
            }
            if(showSQL) {
                System.out.println(r.getTopic());
            }
            for(int j=0; j<weight; j++) {
                statements.add(r);
            }
        }
        return this;
    }
    
    private void testWithSeed(Bnf config) throws Exception {
        config.getRandom().setSeed(seed);
        Connection conn = null;
        try {
            conn = connect();
        } catch(SQLException e) {
            processException("connect", e);
            conn = connect();
        }
        Statement stat = conn.createStatement();
        
        for(int i=0; i<statements.size(); i++) {
            int sid = config.getRandom().nextInt(statements.size());
            RuleHead r = (RuleHead) statements.get(sid);
            String rand = r.getRule().random(config, 0);
            if(rand.length() > 0) {
                try {
                    if(showSQL) {
                        System.out.println(i+"  "+rand);
                    }
                    Thread.yield();
                    if(rand.indexOf("TRACE_LEVEL_SYSTEM_OUT") < 0) {
                        stat.execute(rand);
                    }
                } catch(SQLException e) {
                    processException(rand, e);
                }
            }
        }
        try {
            conn.close();
        } catch(SQLException e) {
            processException("conn.close", e);
        }
    }
    
    public void testCase(int i) throws Exception {
        seed = i;
        printTime("TestRandomSQL " + seed);
        try {
            deleteDb(getDatabaseName());
        } catch(SQLException e) {
            processException("deleteDb", e);
        }
        testWithSeed(bnf);
    }

    public void test() throws Exception {
        exitOnError = false;
        showSQL = false;
        for(int a=0; ; a++) {
            int seed = RandomUtils.nextInt(Integer.MAX_VALUE);
            testCase(seed);
        }
    }

}
