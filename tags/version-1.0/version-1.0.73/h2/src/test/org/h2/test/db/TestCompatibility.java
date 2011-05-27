/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
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

        testHsqlDb();
        testMySQL();

        conn.close();

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
