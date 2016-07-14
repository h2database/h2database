/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Tests INIT command within embedded/server mode using two connections. This test verifies that the INIT
 * script is not re-run when the second connection to the database is opened.
 */
public class TestInitWithTwoConnections extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {

        String sqlFileName = getBaseDir() + "/test-init-1.sql";

        // Create the script that we will run via "INIT"
        FileUtils.createDirectories(FileUtils.getParent(sqlFileName));

        Writer w = new OutputStreamWriter(FileUtils.newOutputStream(sqlFileName, false));

        PrintWriter writer = new PrintWriter(w);
        writer.println("create table test(id int identity, name varchar);");
        writer.println("insert into test(name) values('cat');");
        writer.close();

        // Make the database connection, and run the two scripts
        deleteDb("initDb");

        String dbUrl = "initDb;INIT=RUNSCRIPT FROM '" + sqlFileName + "'";

        Connection conn = getConnection(dbUrl);

        String sqlStatement = "select name from test";
        Statement stat = conn.createStatement();

        // Confirm our scripts have run by loading the data they inserted
        ResultSet rs = stat.executeQuery(sqlStatement);

        assertTrue(rs.next());
        assertEquals("cat", rs.getString(1));
        assertFalse(rs.next());

        // Before the first connection is closed, create a second.

        Connection conn2 = getConnection(dbUrl);

        Statement stat2 = conn2.createStatement();

        // Confirm our scripts have run by loading the data they inserted
        ResultSet rs2 = stat2.executeQuery(sqlStatement);

        assertTrue(rs2.next());
        assertEquals("cat", rs2.getString(1));

        assertFalse(rs2.next());

        conn2.close();
        conn.close();
        deleteDb("initDb");

        FileUtils.delete(sqlFileName);
    }

}
