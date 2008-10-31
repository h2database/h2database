/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;

/**
 * Tests the multi-threaded mode.
 */
public class TestMultiThreaded extends TestBase {

    /**
     * Run just this test.
     * 
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }
    
    /**
     * Processes random operations.
     */
    private static class Processor extends Thread {
        private int id;
        private Statement stat;
        private Random random;
        private volatile Throwable exception;
        Processor(Connection conn, int id) throws SQLException {
            this.id = id;
            stat = conn.createStatement();
            random = new Random(id);
        }
        public Throwable getException() {
            return exception;
        }
        public void run() {
            int count = 0;
            try {
int test;                
                while (!isInterrupted()) {
                    switch(random.nextInt(3)) {
                    case 0:
//System.out.println("insert " + id + " count: " + count);                              
                        stat.execute("INSERT INTO TEST(NAME) VALUES('"+ id +"')");
                        count++;
                        break;
                    case 1:
                        if (count > 0) {
//System.out.println("delete " + id + " count: " + count);                         
                            int updateCount = stat.executeUpdate(
                                    "DELETE FROM TEST WHERE NAME = '"+ id +"' AND ROWNUM()<2");
                            if (updateCount != 1) {
                                throw new Error("Expected: 1 Deleted: " + updateCount);
                            }
                            count--;
                        }
                        break;
                    case 2:
//System.out.println("select " + id + " count: " + count);                            
                        ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM TEST WHERE NAME = '"+ id +"'");
                        rs.next();
                        int got = rs.getInt(1);
                        if (got != count) {
                            throw new Error("Expected: " + count + " got: " + got);
                        }
                        break;
                    }
                }
            } catch (Throwable e) {
                exception = e;
            }
        }
    }

    public void test() throws Exception {
        if (config.mvcc) {
int test;
            return;
        }
        deleteDb("multiThreaded");
        int size = getSize(2, 4);
//int size = 10;
        Connection[] connList = new Connection[size];
        for (int i = 0; i < size; i++) {
            connList[i] = getConnection("multiThreaded;MULTI_THREADED=1");
        }
        Connection conn = connList[0];
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        Processor[] processors = new Processor[size];
        for (int i = 0; i < size; i++) {
            processors[i] = new Processor(conn, i);
            processors[i].start();
        }
        for (int t = 0; t < 2; t++) {
            Thread.sleep(1000);
            for (int i = 0; i < size; i++) {
                Processor p = processors[i];
                if (p.getException() != null) {
                    throw new Exception(p.getException());
                }
            }
        }
        for (int i = 0; i < size; i++) {
            Processor p = processors[i];
            p.interrupt();
            p.join(100);
            if (p.getException() != null) {
                throw new Exception(p.getException());
            }
        }
        for (int i = 0; i < size; i++) {
            connList[i].close();
        }
    }

}
