/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestLuceneCustomAnalyzer extends TestDb {

    private Connection conn;

    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    private void setup() throws SQLException {
        System.setProperty("h2.luceneAnalyzer", "org.apache.lucene.analysis.core.WhitespaceAnalyzer");
        conn = getConnection(getTestName());
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE t(path VARCHAR(100), PRIMARY KEY(path))");
            stmt.executeUpdate("INSERT INTO t VALUES('c:\\hello-world\\random-path-1.txt')");
            stmt.executeUpdate("INSERT INTO t VALUES('c:\\hello-world\\random-path-2.txt')");
            stmt.executeUpdate("INSERT INTO t VALUES('c:\\hello-world\\random-path-3.txt')");
            stmt.executeUpdate("INSERT INTO t VALUES('d:\\hello-world\\random-path-3.txt')");
            stmt.executeUpdate("INSERT INTO t VALUES('d:\\hello-world\\c-random-path-3.txt')");
            stmt.executeUpdate("INSERT INTO t VALUES('d:\\copy\\c-random-path-3.txt')");
            stmt.executeUpdate("INSERT INTO t VALUES('d:\\brown-cow\\c-random-path-3.txt')");

            stmt.execute("CREATE ALIAS IF NOT EXISTS FTL_INIT FOR \"org.h2.fulltext.FullTextLucene.init\"");
            stmt.execute("CALL FTL_INIT()");
            stmt.execute("CALL FTL_CREATE_INDEX('PUBLIC', 'T', 'PATH')");
            stmt.execute("CALL FTL_REINDEX()");
        }
    }


    private void searchWithoutDelimiter() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM FTL_SEARCH('C*',0,0)")) {
            rs.next();
            assertEquals(3, rs.getInt(1));
        }
    }

    private void searchWithDelimiter() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM FTL_SEARCH('C\\:*',0,0)")) {
            rs.next();
            assertEquals(3, rs.getInt(1));
        }
    }

    @Override
    public void test() throws Exception {
        deleteDb(getTestName());
        setup();
        searchWithDelimiter();
        searchWithoutDelimiter();
        conn.close();
        deleteDb(getTestName());
    }
}
