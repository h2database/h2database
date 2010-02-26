/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.h2.test.TestBase;
import org.h2.util.IOUtils;

/**
 * Tests INIT command within embedded/server mode.
 */
public class TestInit extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {

        // Create two scripts that we will run via "INIT"
        IOUtils.createDirs(baseDir + "/test-init-1.sql");
        FileWriter fw = new FileWriter(baseDir + "/test-init-1.sql");

        PrintWriter writer = new PrintWriter(fw);
        writer.println("create table test(id int identity, name varchar);");
        writer.println("insert into test(name) values('cat');");
        writer.close();

        fw = new FileWriter(baseDir + "/test-init-2.sql");
        writer = new PrintWriter(fw);
        writer.println("insert into test(name) values('dog');");
        writer.close();

        // Make the database connection, and run the two scripts
        deleteDb("initDb");
        Connection conn = getConnection("initDb;" +
                "INIT=" +
                "RUNSCRIPT FROM '" + baseDir + "/test-init-1.sql'\\;" +
                "RUNSCRIPT FROM '" + baseDir + "/test-init-2.sql'");

        Statement stat = conn.createStatement();

        // Confirm our scripts have run by loading the data they inserted
        ResultSet rs = stat.executeQuery("select name from test order by name");

        assertTrue(rs.next());
        assertEquals("cat", rs.getString(1));

        assertTrue(rs.next());
        assertEquals("dog", rs.getString(1));

        assertFalse(rs.next());

        conn.close();
        deleteDb("initDb");

        IOUtils.delete(baseDir + "/test-init-1.sql");
        IOUtils.delete(baseDir + "/test-init-2.sql");
    }

}
