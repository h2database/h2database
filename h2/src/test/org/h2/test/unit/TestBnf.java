/*
 * Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.h2.bnf.Bnf;
import org.h2.bnf.context.DbContents;
import org.h2.bnf.context.DbContextRule;
import org.h2.bnf.context.DbProcedure;
import org.h2.bnf.context.DbSchema;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Test Bnf Sql parser
 * @author Nicolas Fortin
 */
public class TestBnf extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        deleteDb("bnf");
        try (Connection conn = getConnection("bnf")) {
            testModes(conn);
            testProcedures(conn, false);
        }
        deleteDb("bnf");
        try (Connection conn = getConnection("bnf;mode=mysql;database_to_lower=true")) {
            testProcedures(conn, true);
        }
    }

    private void testModes(Connection conn) throws Exception {
        DbContents dbContents;
        dbContents = new DbContents();
        dbContents.readContents("jdbc:h2:./test", conn);
        assertTrue(dbContents.isH2());
        dbContents = new DbContents();
        dbContents.readContents("jdbc:derby:test", conn);
        assertTrue(dbContents.isDerby());
        dbContents = new DbContents();
        dbContents.readContents("jdbc:firebirdsql:test", conn);
        assertTrue(dbContents.isFirebird());
        dbContents = new DbContents();
        dbContents.readContents("jdbc:sqlserver:test", conn);
        assertTrue(dbContents.isMSSQLServer());
        dbContents = new DbContents();
        dbContents.readContents("jdbc:mysql:test", conn);
        assertTrue(dbContents.isMySQL());
        dbContents = new DbContents();
        dbContents.readContents("jdbc:oracle:test", conn);
        assertTrue(dbContents.isOracle());
        dbContents = new DbContents();
        dbContents.readContents("jdbc:postgresql:test", conn);
        assertTrue(dbContents.isPostgreSQL());
        dbContents = new DbContents();
        dbContents.readContents("jdbc:sqlite:test", conn);
        assertTrue(dbContents.isSQLite());
    }

    private void testProcedures(Connection conn, boolean isMySQLMode)
            throws Exception {
        // Register a procedure and check if it is present in DbContents
        conn.createStatement().execute(
                "DROP ALIAS IF EXISTS CUSTOM_PRINT");
        conn.createStatement().execute(
                "CREATE ALIAS CUSTOM_PRINT " +
                "AS $$ void print(String s) { System.out.println(s); } $$");
        conn.createStatement().execute(
                "DROP TABLE IF EXISTS " +
                "TABLE_WITH_STRING_FIELD");
        conn.createStatement().execute(
                "CREATE TABLE " +
                "TABLE_WITH_STRING_FIELD (STRING_FIELD VARCHAR(50), INT_FIELD integer)");
        DbContents dbContents = new DbContents();
        dbContents.readContents("jdbc:h2:./test", conn);
        assertTrue(dbContents.isH2());
        assertFalse(dbContents.isDerby());
        assertFalse(dbContents.isFirebird());
        assertEquals(null, dbContents.quoteIdentifier(null));
        if (isMySQLMode) {
            assertEquals("\"TEST\"", dbContents.quoteIdentifier("TEST"));
            assertEquals("\"Test\"", dbContents.quoteIdentifier("Test"));
            assertEquals("test", dbContents.quoteIdentifier("test"));
        } else {
            assertEquals("TEST", dbContents.quoteIdentifier("TEST"));
            assertEquals("\"Test\"", dbContents.quoteIdentifier("Test"));
            assertEquals("\"test\"", dbContents.quoteIdentifier("test"));
        }
        assertFalse(dbContents.isMSSQLServer());
        assertFalse(dbContents.isMySQL());
        assertFalse(dbContents.isOracle());
        assertFalse(dbContents.isPostgreSQL());
        assertFalse(dbContents.isSQLite());
        DbSchema defaultSchema = dbContents.getDefaultSchema();
        DbProcedure[] procedures = defaultSchema.getProcedures();
        Set<String> procedureName = new HashSet<>(procedures.length);
        for (DbProcedure procedure : procedures) {
            assertTrue(defaultSchema == procedure.getSchema());
            procedureName.add(procedure.getName());
        }
        if (isMySQLMode) {
            assertTrue(procedureName.contains("custom_print"));
        } else {
            assertTrue(procedureName.contains("CUSTOM_PRINT"));
        }

        if (isMySQLMode) {
            return;
        }

        // Test completion
        Bnf bnf = Bnf.getInstance(null);
        DbContextRule columnRule = new
                DbContextRule(dbContents, DbContextRule.COLUMN);
        bnf.updateTopic("column_name", columnRule);
        bnf.updateTopic("user_defined_function_name", new
                DbContextRule(dbContents, DbContextRule.PROCEDURE));
        bnf.linkStatements();
        // Test partial
        Map<String, String> tokens;
        tokens = bnf.getNextTokenList("SELECT CUSTOM_PR");
        assertTrue(tokens.values().contains("INT"));

        // Test identifiers are working
        tokens = bnf.getNextTokenList("create table \"test\" as (s" + "el");
        assertTrue(tokens.values().contains("E" + "CT"));

        tokens = bnf.getNextTokenList("create table test as (s" + "el");
        assertTrue(tokens.values().contains("E" + "CT"));

        // Test || with and without spaces
        tokens = bnf.getNextTokenList("select 1||f");
        assertFalse(tokens.values().contains("R" + "OM"));
        tokens = bnf.getNextTokenList("select 1 || f");
        assertFalse(tokens.values().contains("R" + "OM"));
        tokens = bnf.getNextTokenList("select 1 || 2 ");
        assertTrue(tokens.values().contains("FROM"));
        tokens = bnf.getNextTokenList("select 1||2");
        assertTrue(tokens.values().contains("FROM"));
        tokens = bnf.getNextTokenList("select 1 || 2");
        assertTrue(tokens.values().contains("FROM"));

        // Test keyword
        tokens = bnf.getNextTokenList("SELECT LE" + "AS");
        assertTrue(tokens.values().contains("T"));

        // Test parameters
        tokens = bnf.getNextTokenList("SELECT CUSTOM_PRINT(");
        assertTrue(tokens.values().contains("STRING_FIELD"));
        assertFalse(tokens.values().contains("INT_FIELD"));

        // Test parameters with spaces
        tokens = bnf.getNextTokenList("SELECT CUSTOM_PRINT ( ");
        assertTrue(tokens.values().contains("STRING_FIELD"));
        assertFalse(tokens.values().contains("INT_FIELD"));

        // Test parameters with close bracket
        tokens = bnf.getNextTokenList("SELECT CUSTOM_PRINT ( STRING_FIELD");
        assertTrue(tokens.values().contains(")"));

        testWithClause(dbContents);
    }

    private void testWithClause(DbContents dbContents) throws Exception {
        // Mirror the rules of WebSession.loadBnf()
        Bnf bnf = Bnf.getInstance(null);
        bnf.updateTopic("procedure", new DbContextRule(dbContents, DbContextRule.PROCEDURE));
        bnf.updateTopic("column_name", new DbContextRule(dbContents, DbContextRule.COLUMN));
        bnf.updateTopic("new_table_alias", new DbContextRule(dbContents, DbContextRule.NEW_TABLE_ALIAS));
        bnf.updateTopic("table_alias", new DbContextRule(dbContents, DbContextRule.TABLE_ALIAS));
        bnf.updateTopic("column_alias", new DbContextRule(dbContents, DbContextRule.COLUMN_ALIAS));
        bnf.updateTopic("table_name", new DbContextRule(dbContents, DbContextRule.TABLE));
        bnf.updateTopic("query_name", new DbContextRule(dbContents, DbContextRule.QUERY_NAME));
        bnf.updateTopic("schema_name", new DbContextRule(dbContents, DbContextRule.SCHEMA));
        bnf.linkStatements();

        Map<String, String> tokens;
        tokens = bnf.getNextTokenList("WITH ");
        assertTrue(tokens.values().contains("RECURSIVE"));
        tokens = bnf.getNextTokenList("WITH X AS ");
        assertTrue(tokens.values().contains("("));
        tokens = bnf.getNextTokenList("WITH X AS (");
        assertTrue(tokens.values().contains("SELECT"));
        tokens = bnf.getNextTokenList("WITH X AS (SELECT 1) ");
        assertTrue(tokens.values().contains("SELECT"));
        tokens = bnf.getNextTokenList("WITH X AS (SELECT 1) SE" + "L");
        assertTrue(tokens.values().contains("E" + "CT"));
        tokens = bnf.getNextTokenList("WITH X AS (SELECT 1) SELECT * ");
        assertTrue(tokens.values().contains("FROM"));
        // the table of the schema and the name of the
        // common table expression are completed
        tokens = bnf.getNextTokenList("WITH X AS (SELECT 1) SELECT * FROM ");
        assertTrue(tokens.values().contains("TABLE_WITH_STRING_FIELD"));
        assertTrue(tokens.values().contains("X"));
        // multiple common table expressions
        tokens = bnf.getNextTokenList("WITH X AS (SELECT 1), Y AS (");
        assertTrue(tokens.values().contains("SELECT"));
        tokens = bnf.getNextTokenList(
                "WITH X AS (SELECT 1), Y AS (SELECT 2) SELECT * FR");
        assertTrue(tokens.values().contains("OM"));
        // recursive query referencing its own name
        tokens = bnf.getNextTokenList("WITH RECURSIVE CTE(N) AS (" +
                "SELECT 1 UNION ALL SELECT N+1 FROM CTE WHERE N<9) SELECT * FROM ");
        assertTrue(tokens.values().contains("CTE"));
        // WITH clause in a subquery
        tokens = bnf.getNextTokenList("SELECT * FROM TABLE_WITH_STRING_FIELD " +
                "WHERE STRING_FIELD IN (WITH C AS (SELECT 'A') SELECT * FROM C) ");
        assertTrue(tokens.values().contains("ORDER"));
    }
}