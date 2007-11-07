/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.test.TestBase;

public class TestFullText extends TestBase {

    public void test() throws Exception {
        if (config.memory) {
            return;
        }
        
        deleteDb("fullText");
        Connection conn = getConnection("fullText");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS IF NOT EXISTS FT_INIT FOR \"org.h2.fulltext.FullText.init\"");
        stat.execute("CALL FT_INIT()");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello World')");
        stat.execute("CALL FT_CREATE_INDEX('PUBLIC', 'TEST', NULL)");
        ResultSet rs;
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('Hello', 0, 0)");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=1");
        checkFalse(rs.next());
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('Hallo', 0, 0)");
        checkFalse(rs.next());
        stat.execute("INSERT INTO TEST VALUES(2, 'Hallo Welt')");
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('Hello', 0, 0)");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=1");
        checkFalse(rs.next());
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('Hallo', 0, 0)");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=2");
        checkFalse(rs.next());
        
        stat.execute("CALL FT_REINDEX()");
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('Hello', 0, 0)");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=1");
        checkFalse(rs.next());
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('Hallo', 0, 0)");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=2");
        checkFalse(rs.next());
        
        stat.execute("INSERT INTO TEST VALUES(3, 'Hello World')");
        stat.execute("INSERT INTO TEST VALUES(4, 'Hello World')");
        stat.execute("INSERT INTO TEST VALUES(5, 'Hello World')");
        
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('World', 0, 0)");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=4");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=1");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=5");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=3");
        checkFalse(rs.next());
        
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('World', 1, 0)");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=4");
        checkFalse(rs.next());
        
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('World', 0, 2)");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=5");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=3");
        checkFalse(rs.next());
        
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('World', 2, 1)");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=1");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=5");
        checkFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('1', 0, 0)");
        rs.next();
        check(rs.getString(1), "\"PUBLIC\".\"TEST\" WHERE \"ID\"=1");
        checkFalse(rs.next());
        
        stat.execute("CALL FT_DROP_ALL()");
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('World', 2, 1)");
        stat.execute("CALL FT_DROP_ALL()");
        
        conn.close();
    }
}
