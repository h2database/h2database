/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
 * Initial Developer: H2 Group 
 */
package org.h2.test.jdbc;

import java.sql.*;

import org.h2.test.TestBase;

/**
 * @author Thomas
 */

public class TestNativeSQL extends TestBase {

    public void test() throws Exception {
        deleteDb("nativeSql");
        Connection conn = getConnection("nativeSql");

        for(int i=0;i<PAIRS.length;i+=2) {
            test(conn, PAIRS[i], PAIRS[i+1]);
        }
        conn.nativeSQL("TEST");
        conn.nativeSQL("TEST--testing");
        conn.nativeSQL("TEST--testing{oj }");
        conn.nativeSQL("TEST/*{fn }*/");
        conn.nativeSQL("TEST//{fn }");        
        conn.nativeSQL("TEST-TEST/TEST/*TEST*/TEST--\rTEST--{fn }");
        conn.nativeSQL("TEST-TEST//TEST");
        conn.nativeSQL("'{}' '' \"1\" \"\"\"\"");
        conn.nativeSQL("{?= call HELLO{t '10'}}");
        conn.nativeSQL("TEST 'test'{OJ OUTER JOIN}'test'{oj OUTER JOIN}");
        conn.nativeSQL("{call {ts '2001-01-10'}}");
        conn.nativeSQL("call ? { 1: '}' };");
        conn.nativeSQL("TEST TEST TEST TEST TEST 'TEST' TEST \"TEST\"");
        conn.nativeSQL("TEST TEST TEST  'TEST' TEST \"TEST\"");
        Statement stat = conn.createStatement();
        stat.setEscapeProcessing(true);
        stat.execute("CALL {d '2001-01-01'}");
        stat.setEscapeProcessing(false);
        try {
            stat.execute("CALL {d '2001-01-01'} // this is a test");
            error("expected error if setEscapeProcessing=false");
        } catch(SQLException e) {
            checkNotGeneralException(e);
        }
        checkFalse(conn.isClosed());
        conn.close();
        check(conn.isClosed());
    }
    

    static final String[] PAIRS=new String[]{
        "CREATE TABLE TEST(ID INT PRIMARY KEY)",
        "CREATE TABLE TEST(ID INT PRIMARY KEY)",
        
        "INSERT INTO TEST VALUES(1)",
        "INSERT INTO TEST VALUES(1)",
        
        "SELECT '{nothing}' FROM TEST",
        "SELECT '{nothing}' FROM TEST",
        
        "SELECT '{fn ABS(1)}' FROM TEST",
        "SELECT '{fn ABS(1)}' FROM TEST",
        
        "SELECT {d '2001-01-01'} FROM TEST",
        "SELECT    '2001-01-01'  FROM TEST",
        
        "SELECT {t '20:00:00'} FROM TEST",
        "SELECT    '20:00:00'  FROM TEST",
        
        "SELECT {ts '2001-01-01 20:00:00'} FROM TEST",
        "SELECT     '2001-01-01 20:00:00'  FROM TEST",
        
        "SELECT {fn CONCAT('{fn x}','{oj}')} FROM TEST",
        "SELECT     CONCAT('{fn x}','{oj}')  FROM TEST",
        
        "SELECT * FROM {oj TEST T1 LEFT OUTER JOIN TEST T2 ON T1.ID=T2.ID}",
        "SELECT * FROM     TEST T1 LEFT OUTER JOIN TEST T2 ON T1.ID=T2.ID ",
        
        "SELECT * FROM TEST WHERE '{' LIKE '{{' {escape '{'}",
        "SELECT * FROM TEST WHERE '{' LIKE '{{'  escape '{' ",
                        
        "SELECT * FROM TEST WHERE '}' LIKE '}}' {escape '}'}",
        "SELECT * FROM TEST WHERE '}' LIKE '}}'  escape '}' ",
        
        "{call TEST('}')}",
        " call TEST('}') ",

        "{?= call TEST('}')}",
        "    call TEST('}') ",

        "{? = call TEST('}')}",
        "     call TEST('}') ",

        "{{{{this is a bug}",
        null,
    };

    void test(Connection conn, String original,String expected) throws Exception {
        trace("original: <"+original+">");
        trace("expected: <"+expected+">");
        try {
            String result=conn.nativeSQL(original);
            trace("result: <"+result+">");
            check(expected, result);
        } catch(SQLException e) {
            check(expected, null);
            checkNotGeneralException(e);
            trace("got exception, good");
        }
    }    

}
