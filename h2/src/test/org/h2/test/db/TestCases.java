/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Random;

import org.h2.test.TestBase;

public class TestCases extends TestBase {

    
    public void test() throws Exception {
        testDisconnect();
        testExecuteTrace();
        if(config.memory || config.logMode == 0) {
            return;
        }    
        testSpecialSQL();
        testUpperCaseLowerCaseDatabase();
        testManualCommitSet();
        testSchemaIdentityReconnect();
        testAlterTableReconnect();
        testPersistentSettings();
        testInsertSelectUnion();
        testViewReconnect();
        testDefaultQueryReconnect();
        testBigString();
        testRenameReconnect();
        testAllSizes();
        testCreateDrop();
        testPolePos();
        testQuick();
        testMutableObjects();
        testSelectForUpdate();          
        testDoubleRecovery();
        testConstraintReconnect();
        testCollation();
    }

    private void testSpecialSQL() throws Exception {
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("SET AUTOCOMMIT OFF; \n//create sequence if not exists object_id;\n");
        stat.execute("SET AUTOCOMMIT OFF;\n//create sequence if not exists object_id;\n");
        stat.execute("SET AUTOCOMMIT OFF; //create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF;//create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF \n//create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF\n//create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF //create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF//create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF; \n///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF;\n///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF; ///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF;///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF \n///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF\n///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF ///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF///create sequence if not exists object_id;");
        conn.close();
    }
    
    private void testUpperCaseLowerCaseDatabase() throws Exception {
        if(File.separatorChar != '\\') {
            return;
        }
        deleteDb("cases");
        deleteDb("CaSeS");
        Connection conn, conn2;
        ResultSet rs;
        conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("CHECKPOINT");
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1)");
        stat.execute("CHECKPOINT");

        conn2=getConnection("CaSeS");
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        check(rs.next());
        conn2.close();
        
        conn.close();

        conn=getConnection("cases");
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        check(rs.next());
        conn.close();

        conn=getConnection("CaSeS");
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        check(rs.next());
        conn.close();
        
        deleteDb("cases");        
        deleteDb("CaSeS");        
        
    }

    private void testManualCommitSet() throws Exception {
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Connection conn2=getConnection("cases");
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);
        conn.createStatement().execute("SET MODE REGULAR");
        conn2.createStatement().execute("SET MODE REGULAR");
        conn.close();
        conn2.close();
    }
    
    private void testSchemaIdentityReconnect() throws Exception {
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create schema s authorization sa");
        stat.execute("create table s.test(id identity)");
        conn.close();
        conn=getConnection("cases");
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM S.TEST");
        while(rs.next()) {
            // ignore
        }
        conn.close();
    }
    
    private void testDisconnect() throws Exception {
        if(config.networked) {
            return;
        }
        deleteDb("cases");
        Connection conn=getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY)");
        for(int i=0; i<1000; i++) {
            stat.execute("INSERT INTO TEST() VALUES()");
        }
        final boolean[] stopped = new boolean[1];
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    ResultSet rs = stat.executeQuery("SELECT MAX(T.ID) FROM TEST T, TEST, TEST, TEST, TEST, TEST, TEST, TEST, TEST, TEST, TEST");
                    rs.next();
                    new Error("query was too quick; result: " + rs.getInt(1)).printStackTrace();
                } catch(SQLException e) {
                    // ok
                }
                stopped[0] = true;
            }
        });
        t.start();
        Thread.sleep(500);
        long time = System.currentTimeMillis();
        conn.close();
        Thread.sleep(500);
        if(!stopped[0]) {
            error("query still running");
        }
        time = System.currentTimeMillis() - time;
        if(time > 1000) {
            error("closing took " + time);
        }
        deleteDb("cases");
    }
    
    private void testExecuteTrace() throws Exception {
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT ? FROM DUAL {1: 'Hello'}");
        rs.next();
        check("Hello", rs.getString(1));
        checkFalse(rs.next());
        rs = stat.executeQuery("SELECT ? FROM DUAL UNION ALL SELECT ? FROM DUAL {1: 'Hello', 2:'World' }");
        rs.next();
        check("Hello", rs.getString(1));
        rs.next();
        check("World", rs.getString(1));
        checkFalse(rs.next());
        conn.close();
    }
    
    private void testAlterTableReconnect() throws Exception {
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity);");
        stat.execute("insert into test values(1);");
        try {
            stat.execute("alter table test add column name varchar not null;");
            error("shouldn't work");
        } catch(SQLException e) {
            checkNotGeneralException(e);
        }
        conn.close();
        conn=getConnection("cases");
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        rs.next();
        check(rs.getString(1), "1");
        checkFalse(rs.next());
        stat = conn.createStatement();
        stat.execute("drop table test");
        stat.execute("create table test(id identity)");
        stat.execute("insert into test values(1)");
        stat.execute("alter table test alter column id set default 'x'");
        conn.close();
        conn=getConnection("cases");
        stat = conn.createStatement();
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        rs.next();
        check(rs.getString(1), "1");
        checkFalse(rs.next());
        stat.execute("drop table test");
        stat.execute("create table test(id identity)");
        stat.execute("insert into test values(1)");
        try {
            stat.execute("alter table test alter column id date");
            error("shouldn't work");
        } catch(SQLException e) {
            checkNotGeneralException(e);
        }
        conn.close();
        conn=getConnection("cases");
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        rs.next();
        check(rs.getString(1), "1");
        checkFalse(rs.next());
        conn.close();
    }
    
    private void testCollation() throws Exception {
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("SET COLLATION ENGLISH STRENGTH PRIMARY");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World'), (3, 'WORLD'), (4, 'HELLO')");
        stat.execute("create index idxname on test(name)");
        ResultSet rs;
        rs = stat.executeQuery("select name from test order by name");
        rs.next();
        check(rs.getString(1), "Hello");
        rs.next();
        check(rs.getString(1), "HELLO");
        rs.next();
        check(rs.getString(1), "World");
        rs.next();
        check(rs.getString(1), "WORLD");
        rs = stat.executeQuery("select name from test where name like 'He%'");
        rs.next();
        check(rs.getString(1), "Hello");
        rs.next();
        check(rs.getString(1), "HELLO");
        conn.close();
    }
    
    private void testPersistentSettings() throws Exception {
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("SET COLLATION de_DE");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");
        // \u00f6 = oe
        stat.execute("INSERT INTO TEST VALUES(1, 'B\u00f6hlen'), (2, 'Bach'), (3, 'Bucher')");
        conn.close();
        conn=getConnection("cases");
        ResultSet rs = conn.createStatement().executeQuery("SELECT NAME FROM TEST ORDER BY NAME");
        rs.next();
        check(rs.getString(1), "Bach");
        rs.next();
        check(rs.getString(1), "B\u00f6hlen");
        rs.next();
        check(rs.getString(1), "Bucher");
        conn.close();
    }
    
    private void testInsertSelectUnion() throws Exception {
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ORDER_ID INT PRIMARY KEY, ORDER_DATE DATETIME, USER_ID INT ,"
                +"DESCRIPTION VARCHAR, STATE VARCHAR, TRACKING_ID VARCHAR)");
        Timestamp orderDate = Timestamp.valueOf("2005-05-21 17:46:00");
        String sql = "insert into TEST (ORDER_ID,ORDER_DATE,USER_ID,DESCRIPTION,STATE,TRACKING_ID) "
            +"select cast(? as int),cast(? as date),cast(? as int),cast(? as varchar),cast(? as varchar),cast(? as varchar) union all select ?,?,?,?,?,?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, 5555);
        ps.setTimestamp(2, orderDate);
        ps.setInt(3, 2222);
        ps.setString(4, "test desc");
        ps.setString(5, "teststate");
        ps.setString(6, "testid");
        ps.setInt(7, 5556);
        ps.setTimestamp(8, orderDate);
        ps.setInt(9, 2222);
        ps.setString(10, "test desc");
        ps.setString(11, "teststate");
        ps.setString(12, "testid");
        check(ps.executeUpdate(), 2);
        ps.close();
        conn.close();
    }
    
    private void testViewReconnect() throws Exception {
        trace("testViewReconnect");
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("create view abc as select * from test");
        stat.execute("drop table test");
        conn.close();
        conn = getConnection("cases");        
        stat = conn.createStatement();
        try {
            stat.execute("select * from abc");
            error("abc should be deleted");
        } catch(SQLException e) {
            checkNotGeneralException(e);
        }
        conn.close();
    }
    
    private void testDefaultQueryReconnect() throws Exception {
        trace("testDefaultQueryReconnect");
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table parent(id int)");
        stat.execute("insert into parent values(1)");
        stat.execute("create table test(id int default (select max(id) from parent), name varchar)");
        
        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        conn.setAutoCommit(false);
        stat.execute("insert into parent values(2)");
        stat.execute("insert into test(name) values('test')");
        ResultSet rs = stat.executeQuery("select * from test");
        rs.next();
        check(rs.getInt(1), 2);
        checkFalse(rs.next());
        conn.close();
    }
    
    private void testBigString() throws Exception {
        trace("testBigString");
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT, TEXT VARCHAR, TEXT_C CLOB)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");
        int len = getSize(1000, 66000);
        char[] buff = new char[len];
        
        // The UCS code values 0xd800-0xdfff (UTF-16 surrogates) 
        // as well as 0xfffe and 0xffff (UCS non-characters) 
        // should not appear in conforming UTF-8 streams.  
        // (String.getBytes("UTF-8") only returns 1 byte for 0xd800-0xdfff)
        
        Random random = new Random();
        random.setSeed(1);
        for(int i=0; i<len; i++) {
            char c;
            do {
                c = (char)random.nextInt();
            } while(c >= 0xd800 && c <= 0xdfff);
            buff[i] = c;
        }
        String big = new String(buff);
        prep.setInt(1, 1);
        prep.setString(2, big);
        prep.setString(3, big);
        prep.execute();
        prep.setInt(1, 2);
        prep.setCharacterStream(2, new StringReader(big), 0);
        prep.setCharacterStream(3, new StringReader(big), 0);
        prep.execute();
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getString(2), big);
        check(readString(rs.getCharacterStream(2)), big);
        check(rs.getString(3), big);
        check(readString(rs.getCharacterStream(3)), big);
        rs.next();
        check(rs.getInt(1), 2);
        check(rs.getString(2), big);
        check(readString(rs.getCharacterStream(2)), big);
        check(rs.getString(3), big);
        check(readString(rs.getCharacterStream(3)), big);
        rs.next();
        checkFalse(rs.next());
        conn.close();
    }
    
    private void testConstraintReconnect() throws Exception {
        trace("testConstraintReconnect");
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists parent");
        stat.execute("drop table if exists child");
        stat.execute("create table parent(id int)");
        stat.execute("create table child(c_id int, p_id int, foreign key(p_id) references parent(id))");
        stat.execute("insert into parent values(1), (2)");
        stat.execute("insert into child values(1, 1)");
        stat.execute("insert into child values(2, 2)");
        stat.execute("insert into child values(3, 2)");
        stat.execute("delete from child");
        conn.close();
        conn=getConnection("cases");
        conn.close();
    }
    
    private void testDoubleRecovery() throws Exception {
        if(config.networked) {
            return;
        }
        trace("testDoubleRecovery");
        deleteDb("cases");
        Connection conn=getConnection("cases");
        deleteDb("twoPhaseCommit");
        Statement stat = conn.createStatement();
        stat.execute("SET WRITE_DELAY 0");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.setAutoCommit(false);
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        crash(conn);
        
        conn=getConnection("cases");
        stat = conn.createStatement();
        stat.execute("SET WRITE_DELAY 0");        
        stat.execute("INSERT INTO TEST VALUES(3, 'Break')");
        crash(conn);

        conn=getConnection("cases");
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getString(2), "Hello");
        rs.next();
        check(rs.getInt(1), 3);
        check(rs.getString(2), "Break");
        conn.close();
    }
    
    private void testRenameReconnect() throws Exception {
        trace("testRenameReconnect");
        deleteDb("cases");
        Connection conn=getConnection("cases");
        conn.createStatement().execute("CREATE TABLE TEST_SEQ(ID INT IDENTITY, NAME VARCHAR(255))");
        conn.createStatement().execute("CREATE TABLE TEST(ID INT PRIMARY KEY)");
        conn.createStatement().execute("ALTER TABLE TEST RENAME TO TEST2");
        conn.createStatement().execute("CREATE TABLE TEST_B(ID INT PRIMARY KEY, NAME VARCHAR, UNIQUE(NAME));");
        conn.close();    
        conn=getConnection("cases");
        conn.createStatement().execute("INSERT INTO TEST_SEQ(NAME) VALUES('Hi')");
        ResultSet rs = conn.createStatement().executeQuery("CALL IDENTITY()");
        rs.next();
        check(rs.getInt(1), 1);
        conn.createStatement().execute("SELECT * FROM TEST2");
        conn.createStatement().execute("SELECT * FROM TEST_B");
        conn.createStatement().execute("ALTER TABLE TEST_B RENAME TO TEST_B2");
        conn.close();    
        conn=getConnection("cases");
        conn.createStatement().execute("SELECT * FROM TEST_B2");
        conn.createStatement().execute("INSERT INTO TEST_SEQ(NAME) VALUES('World')");
        rs = conn.createStatement().executeQuery("CALL IDENTITY()");
        rs.next();
        check(rs.getInt(1), 2);
        conn.close();    
    }
    
    private void testAllSizes() throws Exception {
        trace("testAllSizes");
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(A INT, B INT, C INT, DATA VARCHAR)");
        int increment = getSize(100, 1);
        for(int i=1; i<500; i+=increment) {
            StringBuffer buff = new StringBuffer();
            buff.append("CREATE TABLE TEST");
            for(int j=0; j<i; j++) {
                buff.append('a');
            }
            buff.append("(ID INT)");
            String sql = buff.toString();
            stat.execute(sql);
            stat.execute("INSERT INTO TEST VALUES("+i+", 0, 0, '"+sql+"')");
        }
        conn.close();
        conn=getConnection("cases");
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        while(rs.next()) {
            int id=rs.getInt(1);
            String s = rs.getString("DATA");
            if(!s.endsWith(")")) {
                error("id="+id);
            }
        }
        conn.close();
    }
    
    private void testSelectForUpdate() throws Exception {
        trace("testSelectForUpdate");
        deleteDb("cases");
        Connection conn1=getConnection("cases");
        Statement stat1 = conn1.createStatement();
        stat1.execute("CREATE TABLE TEST(ID INT)");
        stat1.execute("INSERT INTO TEST VALUES(1)");
        conn1.setAutoCommit(false);
        stat1.execute("SELECT * FROM TEST FOR UPDATE");
        Connection conn2=getConnection("cases");
        Statement stat2 = conn2.createStatement();
        try {
            stat2.execute("UPDATE TEST SET ID=2");
            error("must fail");
        } catch(SQLException e) {
            checkNotGeneralException(e);
        }
        conn1.commit();
        stat2.execute("UPDATE TEST SET ID=2");
        conn1.close();
        conn2.close();
    }
    
    private void testMutableObjects() throws Exception {
        trace("testMutableObjects");
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT, D DATE, T TIME, TS TIMESTAMP)");
        stat.execute("INSERT INTO TEST VALUES(1, '2001-01-01', '20:00:00', '2002-02-02 22:22:22.2')");
        stat.execute("INSERT INTO TEST VALUES(1, '2001-01-01', '20:00:00', '2002-02-02 22:22:22.2')");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        Date d1 = rs.getDate("D");
        Time t1 = rs.getTime("T");
        Timestamp ts1 = rs.getTimestamp("TS");
        rs.next();
        Date d2 = rs.getDate("D");
        Time t2 = rs.getTime("T");
        Timestamp ts2 = rs.getTimestamp("TS");
        check(ts1 != ts2);
        check(d1 != d2);
        check(t1 != t2);
        check(t2 != rs.getObject("T"));
        check(d2 != rs.getObject("D"));
        check(ts2 != rs.getObject("TS"));
        checkFalse(rs.next());
        conn.close();
    }
    
    private void testCreateDrop() throws Exception {
        trace("testCreateDrop");
        deleteDb("cases");
        Connection conn=getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute(
                "create table employee(id int, "
                +"firstname VARCHAR(50), "
                +"salary decimal(10, 2), "
                +"superior_id int, "
                +"CONSTRAINT PK_employee PRIMARY KEY (id), "
                +"CONSTRAINT FK_superior FOREIGN KEY (superior_id) "
                +"REFERENCES employee(ID))");
        stat.execute("DROP TABLE employee");
        conn.close();
        conn=getConnection("cases");
        conn.close();
    }
    
    private void testPolePos() throws Exception {
        trace("testPolePos");
        // poleposition-0.20
        
        Connection c0=getConnection("cases");
        c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        c0.createStatement().executeUpdate("create table australia (ID  INTEGER NOT NULL, Name VARCHAR(100), FirstName VARCHAR(100), Points INTEGER, LicenseID INTEGER, PRIMARY KEY(ID))");
        c0.createStatement().executeUpdate("COMMIT");
        c0.close();
        
        c0=getConnection("cases");
        c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        PreparedStatement p15=c0.prepareStatement("insert into australia (id,Name,FirstName,Points,LicenseID) values (?,?,?,?,?)");
        int len = getSize(1, 1000);
        for(int i=0; i<len; i++) {
            p15.setInt(1, i); p15.setString(2, "Pilot_"+i); p15.setString(3, "Herkules"); p15.setInt(4, i); p15.setInt(5, i); p15.executeUpdate();
        }
        c0.createStatement().executeUpdate("COMMIT");
        c0.close();
        
//      c0=getConnection("cases");
//      c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
//      c0.createStatement().executeQuery("select * from australia");
//      c0.createStatement().executeQuery("select * from australia");
//      c0.close();
        
//      c0=getConnection("cases");
//      c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
//      c0.createStatement().executeUpdate("COMMIT");
//      c0.createStatement().executeUpdate("delete from australia");
//      c0.createStatement().executeUpdate("COMMIT");
//      c0.close();
        
        c0=getConnection("cases");
        c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        c0.createStatement().executeUpdate("drop table australia");
        c0.createStatement().executeUpdate("create table australia (ID  INTEGER NOT NULL, Name VARCHAR(100), FirstName VARCHAR(100), Points INTEGER, LicenseID INTEGER, PRIMARY KEY(ID))");
        c0.createStatement().executeUpdate("COMMIT");
        c0.close();
        
        c0=getConnection("cases");
        c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        PreparedStatement p65=c0.prepareStatement("insert into australia (id,Name,FirstName,Points,LicenseID) values (?,?,?,?,?)");
        len = getSize(1, 1000);
        for(int i=0; i<len; i++) {
            p65.setInt(1, i); p65.setString(2, "Pilot_"+i); p65.setString(3, "Herkules"); p65.setInt(4, i); p65.setInt(5, i); p65.executeUpdate();
        }
        c0.createStatement().executeUpdate("COMMIT");
        c0.createStatement().executeUpdate("COMMIT");
        c0.createStatement().executeUpdate("COMMIT");
        c0.close();
        
        c0=getConnection("cases");
        c0.close();
    }
    
    
    private void testQuick() throws Exception {
        trace("testQuick");
        deleteDb("cases");

        Connection c0=getConnection("cases");
        c0.createStatement().executeUpdate("create table test (ID  int PRIMARY KEY)");
        c0.createStatement().executeUpdate("insert into test values(1)");
        c0.createStatement().executeUpdate("drop table test");
        c0.createStatement().executeUpdate("create table test (ID  int PRIMARY KEY)");
        c0.close();
        
        c0=getConnection("cases");
        c0.createStatement().executeUpdate("insert into test values(1)");
        c0.close();
        
        c0=getConnection("cases");
        c0.close();
    }    
    

}
