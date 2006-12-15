/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.*;

import org.h2.test.TestBase;

public class TestRights extends TestBase {
    
    Statement stat;

    public void test() throws Exception {
        if(config.memory) {
            return;
        }
        
        deleteDb("rights");
        Connection conn = getConnection("rights");
        stat = conn.createStatement();
        // default table type
        testTableType(conn, "MEMORY");
        testTableType(conn, "CACHED");

        // rights on tables and views
        executeSuccess("CREATE USER PASSREADER PASSWORD 'abc'");
        executeSuccess("CREATE TABLE PASS(ID INT PRIMARY KEY, NAME VARCHAR, PASSWORD VARCHAR)");
        executeSuccess("CREATE VIEW PASS_NAME AS SELECT ID, NAME FROM PASS");
        executeSuccess("GRANT SELECT ON PASS_NAME TO PASSREADER");
        conn.close();
        
        conn = getConnection("rights", "PASSREADER", "abc");
        stat = conn.createStatement();
        executeSuccess("SELECT * FROM PASS_NAME");
        executeError("SELECT * FROM PASS");
        conn.close();
        
        conn = getConnection("rights");
        stat = conn.createStatement();
        
        executeSuccess("CREATE USER TEST PASSWORD 'abc'");
        executeSuccess("ALTER USER TEST ADMIN TRUE");
        executeSuccess("CREATE TABLE TEST(ID INT)");
        executeSuccess("CREATE SCHEMA SCHEMA_A AUTHORIZATION SA");        
        executeSuccess("CREATE TABLE SCHEMA_A.TABLE_B(ID INT)");
        executeSuccess("GRANT ALL ON SCHEMA_A.TABLE_B TO TEST");
        executeSuccess("CREATE TABLE HIDDEN(ID INT)");
        executeSuccess("CREATE TABLE PUBTABLE(ID INT)");
        executeSuccess("CREATE TABLE ROLETABLE(ID INT)");
        executeSuccess("CREATE ROLE TESTROLE");
        executeSuccess("GRANT SELECT ON ROLETABLE TO TESTROLE");
        executeSuccess("GRANT UPDATE ON ROLETABLE TO TESTROLE");
        executeSuccess("REVOKE UPDATE ON ROLETABLE FROM TESTROLE");
        executeError("REVOKE SELECT, SUB1 ON ROLETABLE FROM TESTROLE");
        executeSuccess("GRANT TESTROLE TO TEST");
        executeSuccess("GRANT SELECT ON PUBTABLE TO PUBLIC");
        executeSuccess("GRANT SELECT ON TEST TO TEST");
        executeSuccess("CREATE ROLE SUB1");
        executeSuccess("CREATE ROLE SUB2");
        executeSuccess("CREATE TABLE SUBTABLE(ID INT)");
        executeSuccess("GRANT ALL ON SUBTABLE TO SUB2");
        executeSuccess("REVOKE UPDATE, DELETE ON SUBTABLE FROM SUB2");
        executeSuccess("GRANT SUB2 TO SUB1");
        executeSuccess("GRANT SUB1 TO TEST");
        
        executeSuccess("ALTER USER TEST SET PASSWORD 'def'");        
        executeSuccess("CREATE USER TEST2 PASSWORD 'def' ADMIN");
        executeSuccess("ALTER USER TEST ADMIN FALSE");
        executeSuccess("SCRIPT TO '"+BASE_DIR+"/rights.sql' CIPHER XTEA PASSWORD 'test'");
        conn.close();
        
        try {
            conn = getConnection("rights", "Test", "abc");
            error("unexpected success (mixed case user name)");
        } catch(SQLException e) { 
            checkNotGeneralException(e);
        }
        try {
            conn = getConnection("rights", "TEST", "abc");
            error("unexpected success (wrong password)");
        } catch(SQLException e) { 
            checkNotGeneralException(e);
        }
        try {
            conn = getConnection("rights", "TEST", null);
            error("unexpected success (wrong password)");
        } catch(SQLException e) { 
            checkNotGeneralException(e);
        }
        conn = getConnection("rights", "TEST", "def");
        stat = conn.createStatement();
        
        executeError("SET DEFAULT_TABLE_TYPE MEMORY");

        executeSuccess("SELECT * FROM TEST");
        executeSuccess("SELECT * FROM SYSTEM_RANGE(1,2)");
        executeSuccess("SELECT * FROM SCHEMA_A.TABLE_B");
        executeSuccess("SELECT * FROM PUBTABLE");
        executeSuccess("SELECT * FROM ROLETABLE");
        executeError("UPDATE ROLETABLE SET ID=0");
        executeError("DELETE FROM ROLETABLE");
        executeError("SELECT * FROM HIDDEN");
        executeError("UPDATE TEST SET ID=0");
        executeSuccess("SELECT * FROM SUBTABLE");
        executeSuccess("INSERT INTO SUBTABLE VALUES(1)");
        executeError("DELETE FROM SUBTABLE");
        executeError("UPDATE FROM SUBTABLE");
        
        executeError("CREATE USER TEST3 PASSWORD 'def'");
        executeError("ALTER USER TEST2 ADMIN FALSE");
        executeError("ALTER USER TEST2 SET PASSWORD 'ghi'");
        executeError("ALTER USER TEST2 RENAME TO TEST_X");
        executeError("ALTER USER TEST RENAME TO TEST_X");
        executeSuccess("ALTER USER TEST SET PASSWORD 'ghi'");
        executeError("DROP USER TEST2");
        
        conn.close();
        conn = getConnection("rights");
        stat = conn.createStatement();
        executeSuccess("DROP ROLE SUB1");
        executeSuccess("DROP TABLE ROLETABLE");
        executeSuccess("DROP USER TEST");
        
        conn.close();
        conn = getConnection("rights");
        stat = conn.createStatement();

        executeSuccess("DROP TABLE IF EXISTS TEST");
        executeSuccess("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        executeSuccess("CREATE USER GUEST PASSWORD 'abc'");
        executeSuccess("GRANT SELECT ON TEST TO GUEST");
        executeSuccess("ALTER USER GUEST RENAME TO GAST");
        conn.close();
        conn = getConnection("rights");
        conn.close();
    }
    
    private void testTableType(Connection conn, String type) throws Exception {
        executeSuccess("SET DEFAULT_TABLE_TYPE " + type);
        executeSuccess("CREATE TABLE TEST(ID INT)");
        ResultSet rs = conn.createStatement().executeQuery("SELECT STORAGE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='TEST'");
        rs.next();
        check(rs.getString(1), type);
        executeSuccess("DROP TABLE TEST");
    }

    public void executeError(String sql) throws Exception {
        try {
            stat.execute(sql);
            error("unexpected success (not admin)");
        } catch(SQLException e) { 
            checkNotGeneralException(e);
        }
    }

    public void executeSuccess(String sql) throws Exception {
        if(stat.execute(sql)) {
            ResultSet rs = stat.getResultSet();
            
            // this will check if the resultset is updatable
            rs.getConcurrency();
            
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            for(int i=0; i<columnCount; i++) {
                meta.getCatalogName(i+1);
                meta.getColumnClassName(i+1);
                meta.getColumnDisplaySize(i+1);
                meta.getColumnLabel(i+1);
                meta.getColumnName(i+1);
                meta.getColumnType(i+1);
                meta.getColumnTypeName(i+1);
                meta.getPrecision(i+1);
                meta.getScale(i+1);
                meta.getSchemaName(i+1);
                meta.getTableName(i+1);
            }
            while(rs.next()) {
                for(int i=0; i<columnCount; i++) {
                    rs.getObject(i+1);
                }
            }
        }
    }
    
}
