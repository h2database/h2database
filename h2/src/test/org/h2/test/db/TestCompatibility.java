/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Tests the compatibility with other databases.
 */
public class TestCompatibility extends TestBase {

    private Connection conn;

    public void test() throws Exception {
        deleteDb("compatibility");
        conn = getConnection("compatibility");

        testUniqueIndexSingleNull();
        testUniqueIndexOracle();
        testHsqlDb();
        testMySQL();

        conn.close();

    }
    
    private void testUniqueIndexSingleNull() throws Exception {
        Statement stat = conn.createStatement();
        String[] modes = new String[] { "PostgreSQL", "MySQL", "HSQLDB", "MSSQLServer", "Derby", "Oracle", "Regular" };
        String multiNull = "PostgreSQL,MySQL,Oracle,Regular";
        for (int i = 0; i < modes.length; i++) {
            String mode = modes[i];
            stat.execute("SET MODE " + mode);
            stat.execute("CREATE TABLE TEST(ID INT)");
            stat.execute("CREATE UNIQUE INDEX IDX_ID_U ON TEST(ID)");
            try {
                stat.execute("INSERT INTO TEST VALUES(1), (2), (NULL), (NULL)");
                assertTrue(mode + " mode should not support multiple NULL", multiNull.indexOf(mode) >= 0);
            } catch (SQLException e) {
                assertTrue(mode + " mode should support multiple NULL", multiNull.indexOf(mode) < 0);
            }
            stat.execute("DROP TABLE TEST");
        }
    }

    private void testUniqueIndexOracle() throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("SET MODE ORACLE");
        stat.execute("create table t2(c1 int, c2 int)");
        stat.execute("create unique index i2 on t2(c1, c2)");
        stat.execute("insert into t2 values (null, 1)");
        try {
            stat.execute("insert into t2 values (null, 1)");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        stat.execute("insert into t2 values (null, null)");
        stat.execute("insert into t2 values (null, null)");
        stat.execute("insert into t2 values (1, null)");
        try {
            stat.execute("insert into t2 values (1, null)");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        stat.execute("DROP TABLE T2");
    }

    private void testHsqlDb() throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE TEST IF EXISTS; CREATE TABLE TEST(ID INT PRIMARY KEY); ");
        stat.execute("CALL CURRENT_TIME");
        stat.execute("CALL CURRENT_TIMESTAMP");
        stat.execute("CALL CURRENT_DATE");
        stat.execute("CALL SYSDATE");
        stat.execute("CALL TODAY");

        stat.execute("DROP TABLE TEST IF EXISTS");
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1)");
        PreparedStatement prep = conn.prepareStatement("SELECT LIMIT ? 1 ID FROM TEST");
        prep.setInt(1, 2);
        prep.executeQuery();
        stat.execute("DROP TABLE TEST IF EXISTS");

    }

    private void testMySQL() throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("SELECT 1");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World')");
    }

}
