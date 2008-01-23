/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Tests if prepared statements are re-compiled when required.
 */
public class TestAutoRecompile extends TestBase {

    public void test() throws Exception {
        deleteDb("autoRecompile");
        Connection conn = getConnection("autoRecompile");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY)");
        PreparedStatement prep = conn.prepareStatement("SELECT * FROM TEST");
        check(prep.executeQuery().getMetaData().getColumnCount(), 1);
        stat.execute("ALTER TABLE TEST ADD COLUMN NAME VARCHAR(255)");
        check(prep.executeQuery().getMetaData().getColumnCount(), 2);
        stat.execute("DROP TABLE TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, X INT, Y INT)");
        check(prep.executeQuery().getMetaData().getColumnCount(), 3);
        // TODO test auto-recompile with insert..select, views and so on

        prep = conn.prepareStatement("INSERT INTO TEST VALUES(1, 2, 3)");
        stat.execute("ALTER TABLE TEST ADD COLUMN Z INT");
        try {
            prep.execute();
            error("must fail");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        try {
            prep.execute();
            error("must fail");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        conn.close();
    }

}
