/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.util.MemoryUtils;

public class TestBigDb extends TestBase {

    public void test() throws Exception {
        if(config.memory) {
            return;
        }
        if(config.networked && config.big) {
            return;
        }        
        testLargeTable();
        testInsert();
        testLeftSummary();
    }
    
    private void testLargeTable() throws Exception {
        deleteDb("bigDb");
        Connection conn = getConnection("bigDb");
        Statement stat = conn.createStatement();
        stat.execute(
                "CREATE CACHED TABLE PAB_ARTLEV("+
                "MUTATIECODE CHAR(1) DEFAULT CAST(RAND()*9 AS INT),"+ 
                "PRDCODE CHAR(20) DEFAULT SECURE_RAND(10),"+ 
                "ORGCODESUPPLIER CHAR(13) DEFAULT SECURE_RAND(6),"+ 
                "PRDCODEGTIN CHAR(14) DEFAULT SECURE_RAND(7),"+ 
                "PRDCODEMF CHAR(20)  DEFAULT SECURE_RAND(10),"+ 
                "ORGCODEMF CHAR(13)  DEFAULT SECURE_RAND(6),"+  
                "SUBSTITUTEDBY CHAR(20) DEFAULT SECURE_RAND(10),"+ 
                "SUBSTITUTEDBYGTIN CHAR(14) DEFAULT SECURE_RAND(7),"+ 
                "SUBSTITUTIONFOR CHAR(20) DEFAULT SECURE_RAND(10),"+ 
                "SUBSTITUTIONFORGTIN CHAR(14) DEFAULT SECURE_RAND(7),"+ 
                "VERWERKBAAR CHAR(2) DEFAULT SECURE_RAND(1),"+ 
                "BESTELBAAR CHAR(2) DEFAULT SECURE_RAND(1),"+ 
                "AANTALGEBRUIKSEENHEDEN DECIMAL(7,2) DEFAULT RAND(),"+ 
                "PRIMARYUNITCODE CHAR(3) DEFAULT SECURE_RAND(1),"+ 
                "RATEPRICEORDERUNIT DECIMAL(9,3) DEFAULT RAND(),"+ 
                "ORDERUNITCODE CHAR(3) DEFAULT SECURE_RAND(1),"+ 
                "ORDERQTYMIN DECIMAL(6,1) DEFAULT RAND(),"+ 
                "ORDERQTYLOTSIZE DECIMAL(6,1) DEFAULT RAND(),"+
                "ORDERUNITCODE2 CHAR(3) DEFAULT SECURE_RAND(1),"+ 
                "PRICEGROUP CHAR(20) DEFAULT SECURE_RAND(10),"+ 
                "LEADTIME INTEGER DEFAULT RAND(),"+ 
                "LEADTIMEUNITCODE CHAR(3) DEFAULT SECURE_RAND(1),"+ 
                "PRDGROUP CHAR(10) DEFAULT SECURE_RAND(5),"+ 
                "WEIGHTGROSS DECIMAL(7,3) DEFAULT RAND(),"+
                "WEIGHTUNITCODE CHAR(3) DEFAULT SECURE_RAND(1),"+ 
                "PACKUNITCODE CHAR(3) DEFAULT SECURE_RAND(1),"+ 
                "PACKLENGTH DECIMAL(7,3) DEFAULT RAND(),"+
                "PACKWIDTH DECIMAL(7,3) DEFAULT RAND(),"+
                "PACKHEIGHT DECIMAL(7,3) DEFAULT RAND(),"+
                "SIZEUNITCODE CHAR(3) DEFAULT SECURE_RAND(1),"+ 
                "STATUCCODE CHAR(3) DEFAULT SECURE_RAND(1),"+ 
                "INTRASTATCODE CHAR(12) DEFAULT SECURE_RAND(6),"+
                "PRDTITLE CHAR(50) DEFAULT SECURE_RAND(25),"+ 
                "VALIDFROM DATE DEFAULT NOW(),"+
                "MUTATIEDATUM DATE DEFAULT NOW())");                
        int len = getSize(10, 50000);
        try {
            PreparedStatement prep = conn.prepareStatement("INSERT INTO PAB_ARTLEV(PRDCODE) VALUES('abc' || ?)");
            long time = System.currentTimeMillis();
            for(int i=0; i<len; i++) {
                if((i % 1000) == 0) {
                    long t = System.currentTimeMillis();
                    if(t-time > 1000) {
                        time = t;
                        int free = MemoryUtils.getMemoryFree();
                        System.out.println("i: " + i + " free: " + free + " used: " + MemoryUtils.getMemoryUsed());
                    }
                }
                prep.setInt(1, i);
                prep.execute();
            }
            stat.execute("CREATE INDEX IDX_PAB_ARTLEV_PRDCODE ON PAB_ARTLEV(PRDCODE)");
            ResultSet rs = stat.executeQuery("SELECT * FROM PAB_ARTLEV");
            int columns = rs.getMetaData().getColumnCount();
            while(rs.next()) {
                for(int i=0; i<columns; i++) {
                    rs.getString(i+1);
                }
            }
        } catch(OutOfMemoryError e) {
            TestBase.logError("memory", e);
            conn.close();
            throw e;
        }
        conn.close();
    }

    private void testLeftSummary() throws Exception {
        deleteDb("bigDb");
        Connection conn = getConnection("bigDb");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT, NEG INT AS -ID, NAME VARCHAR, PRIMARY KEY(ID, NAME))");
        stat.execute("CREATE INDEX IDXNEG ON TEST(NEG, NAME)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(ID, NAME) VALUES(?, '1234567890')");
        int len = getSize(10, 1000);
        int block = getSize(3, 10);
        int left, x = 0;
        for(int i=0; i<len; i++) {
            left = x+block/2;
            for(int j=0; j<block; j++) {
                prep.setInt(1, x++);
                prep.execute();
            }            
            stat.execute("DELETE FROM TEST WHERE ID>" + left);
            ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
            rs.next();
            int count = rs.getInt(1);
            trace("count: " + count);
        }
        conn.close();
    }
    
    private void testInsert() throws Exception {
        deleteDb("bigDb");
        Connection conn = getConnection("bigDb");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(NAME) VALUES('Hello World')");
        int len = getSize(1000, 10000);
        long time = System.currentTimeMillis();
        for(int i=0; i<len; i++) {
            if(i % 1000 == 0) {
                long t = System.currentTimeMillis();
                time = t;
                trace("rows:" + i + " time:" + (t-time));
                Thread.yield();
            }
            prep.execute();
        }
        conn.close();
    }
    

}
