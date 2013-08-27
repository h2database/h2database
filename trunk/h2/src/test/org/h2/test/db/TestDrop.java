/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;

/**
 * Test DROP statement
 */
public class TestDrop extends TestBase {

    private Connection conn;
    private Statement stat;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        deleteDb("drop");
        conn = getConnection("drop");
        stat = conn.createStatement();

        testComputedColumnDependency();

        conn.close();
        deleteDb("drop");
    }

    private void testComputedColumnDependency() throws SQLException {
        stat.execute("DROP ALL OBJECTS");
        stat.execute("CREATE TABLE A (A INT);");
        stat.execute("CREATE TABLE B (B INT AS SELECT A FROM A);");
        stat.execute("DROP ALL OBJECTS");
        stat.execute("CREATE SCHEMA TEST_SCHEMA");
        stat.execute("CREATE TABLE TEST_SCHEMA.A (A INT);");
        stat.execute("CREATE TABLE TEST_SCHEMA.B (B INT AS SELECT A FROM TEST_SCHEMA.A);");
        stat.execute("DROP SCHEMA TEST_SCHEMA");
    }
}
