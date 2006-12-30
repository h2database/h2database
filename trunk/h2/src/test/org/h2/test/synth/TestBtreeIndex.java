/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;

public class TestBtreeIndex extends TestBase {
    
    public void test() throws Exception {
        Random random = new Random();
        while(true) {
            int seed = random.nextInt();
            testCase(seed);
        }
    }
    
    public void testCase(int seed) throws Exception {
        BASE_DIR = "dataIndex";
        testOne(seed);
        BASE_DIR = "data";
    }    

    private void testOne(int seed) throws Exception {
        Class.forName("org.h2.Driver");
        printTime("testIndex " + seed);
        Random random = new Random(seed);
        int distinct, prefixLength;
        if(random.nextBoolean()) {
            distinct=random.nextInt(8000)+1;
            prefixLength=random.nextInt(8000)+1;
        } else if(random.nextBoolean()) {
            distinct=random.nextInt(16000)+1;
            prefixLength=random.nextInt(100)+1;
        } else {
            distinct=random.nextInt(10)+1;
            prefixLength=random.nextInt(10)+1;
        }
        boolean delete = random.nextBoolean();
        StringBuffer buff = new StringBuffer();
        for(int j=0; j<prefixLength; j++) {
            buff.append("x");
        }
        String prefix = buff.toString();
        DeleteDbFiles.execute(BASE_DIR, null, true);
        Connection conn = DriverManager.getConnection("jdbc:h2:" +BASE_DIR + "/index", "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE a(text VARCHAR PRIMARY KEY)");
        PreparedStatement prepInsert = conn.prepareStatement("INSERT INTO a VALUES(?)");
        PreparedStatement prepDelete = conn.prepareStatement("DELETE FROM a WHERE text=?");
        PreparedStatement prepDeleteAllButOne = conn.prepareStatement("DELETE FROM a WHERE text <> ?");
        int count=0;
        for(int i=0; i<1000; i++) {
            int y = random.nextInt(distinct);
            try {
                prepInsert.setString(1, prefix + y);
                prepInsert.executeUpdate();
                count ++;
            } catch(SQLException e) {
                if(e.getSQLState().equals("23001")) {
                    // ignore
                } else {
                    TestBase.logError("error", e);
                    break;
                }
            }
            if(delete && random.nextInt(10) == 1) {
                if(random.nextInt(4) == 1) {
                    try {
                        prepDeleteAllButOne.setString(1, prefix + y);
                        int deleted = prepDeleteAllButOne.executeUpdate();
                        if(deleted < count-1) {
                            System.out.println("ERROR deleted:"+deleted);
                            System.out.println("new TestBtreeIndex().");
                        }
                        count -= deleted;
                    } catch(SQLException e) {
                        TestBase.logError("error", e);
                        break;
                    }
                } else {
                    try {
                        prepDelete.setString(1, prefix + y);
                        int deleted = prepDelete.executeUpdate();
                        if(deleted > 1) {
                            System.out.println("ERROR deleted:"+deleted);
                            System.out.println("new TestIndex().");
                        }
                        count -= deleted;
                    } catch(SQLException e) {
                        TestBase.logError("error", e);
                        break;
                    }
                }
            }
        }
        ResultSet rs = conn.createStatement().executeQuery("SELECT text FROM a ORDER BY text");
        int testCount = 0;
        while(rs.next()) {
            testCount++;
        }
        if(testCount != count) {
            System.out.println("ERROR count:"+count+" testCount:"+testCount);
            System.out.println("new TestIndex().");
        }
        rs = conn.createStatement().executeQuery("SELECT text, count(*) FROM a GROUP BY text HAVING COUNT(*)>1");
        if(rs.next()) {
            System.out.println("ERROR");
            System.out.println("new TestIndex().");
        }
        conn.close();
    }
    
}
