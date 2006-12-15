/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.*;
import java.util.Random;

import org.h2.test.TestBase;

public class TestIndex extends TestBase {

    Connection conn;
    Statement stat;
    Random random = new Random();
    
    private void reconnect() throws Exception {
        if(conn != null) {
            conn.close();
            conn = null;
        }
        conn = getConnection("index");
        stat = conn.createStatement();
    }
    
    public void test() throws Exception {
        if(config.networked && config.big) {
            return;
        }
        
        random.setSeed(100);
        
        deleteDb("index");
        testWideIndex(147);
        testWideIndex(313);
        testWideIndex(979);
        testWideIndex(1200);
        testWideIndex(2400);
        if(config.big && config.logMode == 2) {
            for(int i=0; i<2000; i++) {
                if((i%100)==0) {
                    System.out.println("width: " + i);
                }
                testWideIndex(i);
            }
        }
        
        testLike();
        reconnect();
        testConstraint(); 
        testLargeIndex();
        testMultiColumnIndex();
//long time;
//time = System.currentTimeMillis();
        testHashIndex(true, false);
        testHashIndex(false, false);
//System.out.println("btree="+(System.currentTimeMillis()-time));            
//time = System.currentTimeMillis();
        testHashIndex(true, true);
        testHashIndex(false, true);
//System.out.println("hash="+(System.currentTimeMillis()-time));        
        testMultiColumnHashIndex();
        
        conn.close();
    }
    
    String getRandomString(int len) {
        StringBuffer buff = new StringBuffer();
        for(int i = 0; i<len; i++) {
            buff.append((char)('a' + random.nextInt(26)));
        }
        return buff.toString();
    }
    
    void testWideIndex(int length) throws Exception {
        reconnect();
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");
        for(int i=0; i<100; i++) {
            stat.execute("INSERT INTO TEST VALUES("+i+", SPACE("+length+") || "+i+" )");
        }
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY NAME");
        while(rs.next()) {
            int id = rs.getInt("ID");
            String name = rs.getString("NAME");
            check(""+id, name.trim());
        }
        if(!config.memory) {
            reconnect();
            rs = stat.executeQuery("SELECT * FROM TEST ORDER BY NAME");
            while(rs.next()) {
                int id = rs.getInt("ID");
                String name = rs.getString("NAME");
                check(""+id, name.trim());
            }
        }
        stat.execute("DROP TABLE TEST");
    }
    
    void testLike() throws Exception {
        reconnect();        
        stat.execute("CREATE TABLE ABC(ID INT, NAME VARCHAR)");
        stat.execute("INSERT INTO ABC VALUES(1, 'Hello')");
        PreparedStatement prep = conn.prepareStatement("SELECT * FROM ABC WHERE NAME LIKE CAST(? AS VARCHAR)");
        prep.setString(1, "Hi%");
        prep.execute();
        stat.execute("DROP TABLE ABC");
    }
    
    void testConstraint() throws Exception {
        if(config.memory) {
            return;
        }
        stat.execute("CREATE TABLE PARENT(ID INT PRIMARY KEY)");
        stat.execute("CREATE TABLE CHILD(ID INT PRIMARY KEY, PID INT, FOREIGN KEY(PID) REFERENCES PARENT(ID))");
        reconnect();
        stat.execute("DROP TABLE PARENT");        
        stat.execute("DROP TABLE CHILD");        
    }
    
    void testLargeIndex() throws Exception {
        random.setSeed(10);
        for(int i=1; i<100; i += getSize(1000, 3)) {
            stat.execute("DROP TABLE IF EXISTS TEST");
            stat.execute("CREATE TABLE TEST(NAME VARCHAR("+i+"))");
            stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");
            PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?)"); 
            for(int j=0; j<getSize(2, 5); j++) {
                prep.setString(1, getRandomString(i));
                prep.execute();
            }
            if(!config.memory) {
                conn.close();
                conn = getConnection("index");
                stat = conn.createStatement();
            }
            ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM TEST WHERE NAME > 'mdd'");
            rs.next();
            int count = rs.getInt(1);
            trace(i+" count="+count);
        }
        
        stat.execute("DROP TABLE IF EXISTS TEST");
    }
    
    void testHashIndex(boolean primaryKey, boolean hash) throws Exception {
        if(config.memory) {
            return;
        }
        
        reconnect();

        stat.execute("DROP TABLE IF EXISTS TEST");
        if(primaryKey) {
            stat.execute("CREATE TABLE TEST(A INT PRIMARY KEY "+(hash?"HASH":"")+", B INT)");
        } else {
            stat.execute("CREATE TABLE TEST(A INT, B INT)");
            stat.execute("CREATE UNIQUE "+(hash?"HASH":"")+" INDEX ON TEST(A)");
        }
        PreparedStatement prep;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        int len = getSize(5, 1000);
        for(int a=0; a<len; a++) {
            prep.setInt(1, a);
            prep.setInt(2, a);
            prep.execute();
            check(1, getValue(stat, "SELECT COUNT(*) FROM TEST WHERE A=" + a));
            check(0, getValue(stat, "SELECT COUNT(*) FROM TEST WHERE A=-1-" + a));
        }        
        
        reconnect();
        
        prep = conn.prepareStatement("DELETE FROM TEST WHERE A=?");
        for(int a=0; a<len; a++) {
            if(getValue(stat, "SELECT COUNT(*) FROM TEST WHERE A=" + a) != 1) {
                check(1, getValue(stat, "SELECT COUNT(*) FROM TEST WHERE A=" + a));
            }
            prep.setInt(1, a);
            check(1, prep.executeUpdate());
        }
        check(0, getValue(stat, "SELECT COUNT(*) FROM TEST"));
    }
    
    void testMultiColumnIndex() throws Exception {
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(A INT, B INT)");
        PreparedStatement prep;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        int len = getSize(3, 260);
        for(int a=0; a<len; a++) {
            prep.setInt(1, a);
            prep.setInt(2, a);
            prep.execute();
        }
        stat.execute("INSERT INTO TEST SELECT A, B FROM TEST");
        stat.execute("CREATE INDEX ON TEST(A, B)");
        prep = conn.prepareStatement("DELETE FROM TEST WHERE A=?");
        for(int a=0; a<len; a++) {
            log(stat, "SELECT * FROM TEST");
            check(2, getValue(stat, "SELECT COUNT(*) FROM TEST WHERE A=" + (len-a-1)));
            check((len-a)*2, getValue(stat, "SELECT COUNT(*) FROM TEST"));
            prep.setInt(1, (len-a-1));
            prep.execute();
        }
        check(0, getValue(stat, "SELECT COUNT(*) FROM TEST"));
    }

    void testMultiColumnHashIndex() throws Exception {
        if(config.memory) {
            return;
        }
        
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(A INT, B INT, DATA VARCHAR(255))");
        stat.execute("CREATE UNIQUE HASH INDEX IDXAB ON TEST(A, B)");
        PreparedStatement prep;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");
        // speed is quadratic (len*len)
        int len = getSize(2, 14);
        for(int a=0; a<len; a++) {
            for(int b=0; b<len; b+=2) {
                prep.setInt(1, a);
                prep.setInt(2, b);
                prep.setString(3, "i("+a+","+b+")");
                prep.execute();
            }
        }
        
        reconnect();
        
        prep = conn.prepareStatement("UPDATE TEST SET DATA=DATA||? WHERE A=? AND B=?");
        for(int a=0; a<len; a++) {
            for(int b=0; b<len; b+=2) {
                prep.setString(1, "u("+a+","+b+")");
                prep.setInt(2, a);
                prep.setInt(3, b);
                prep.execute();
            }
        }
        
        reconnect();
        
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST WHERE DATA <> 'i('||a||','||b||')u('||a||','||b||')'");
        checkFalse(rs.next());
        check(len*(len/2), getValue(stat, "SELECT COUNT(*) FROM TEST"));
        stat.execute("DROP TABLE TEST");
    }
    
    int getValue(Statement stat, String sql) throws Exception {
        ResultSet rs = stat.executeQuery(sql);
        rs.next();
        return rs.getInt(1);
    }
    
    void log(Statement stat, String sql) throws Exception {
        trace(sql);
        ResultSet rs = stat.executeQuery(sql);
        int cols = rs.getMetaData().getColumnCount();
        while(rs.next()) {
            for(int i=0; i<cols; i++) {
                trace("["+i+"]="+rs.getString(i+1));
            }
        }
        trace("---done---");
    }

}
