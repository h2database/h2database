/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Tests for MySQL 8.19+ style INSERT ... VALUES ... AS alias syntax.
 */
public class TestValuesAlias extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws SQLException {
        deleteDb("valuesAlias");
        Connection conn = getConnection("valuesAlias;MODE=MySQL");
        testBasicValuesAlias(conn);
        testValuesAliasWithMultipleColumns(conn);
        testValuesAliasWithComplexExpressions(conn);
        testBackwardCompatibility(conn);
        testMixedSyntax(conn);
        conn.close();
        deleteDb("valuesAlias");
    }

    private void testBasicValuesAlias(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        
        // Create test table
        stat.execute("CREATE TABLE test_basic (id INT PRIMARY KEY, name VARCHAR(50), value INT)");
        
        // Insert initial data
        stat.execute("INSERT INTO test_basic VALUES (1, 'initial', 100)");
        
        // Test VALUES alias syntax
        stat.execute("INSERT INTO test_basic (id, name, value) VALUES (1, 'updated', 200) AS new_values " +
                    "ON DUPLICATE KEY UPDATE name = new_values.name, value = new_values.value");
        
        // Verify the update
        ResultSet rs = stat.executeQuery("SELECT * FROM test_basic WHERE id = 1");
        assertTrue(rs.next());
        assertEquals("updated", rs.getString("name"));
        assertEquals(200, rs.getInt("value"));
        rs.close();
        
        stat.execute("DROP TABLE test_basic");
        stat.close();
    }

    private void testValuesAliasWithMultipleColumns(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        
        // Create test table
        stat.execute("CREATE TABLE test_multi (id INT PRIMARY KEY, col1 VARCHAR(50), col2 INT, col3 DECIMAL(10,2))");
        
        // Insert initial data
        stat.execute("INSERT INTO test_multi VALUES (1, 'old1', 10, 1.5)");
        
        // Test with multiple columns
        stat.execute("INSERT INTO test_multi (id, col1, col2, col3) VALUES (1, 'new1', 20, 2.5) AS x " +
                    "ON DUPLICATE KEY UPDATE col1 = x.col1, col2 = x.col2, col3 = x.col3");
        
        // Verify the update
        ResultSet rs = stat.executeQuery("SELECT * FROM test_multi WHERE id = 1");
        assertTrue(rs.next());
        assertEquals("new1", rs.getString("col1"));
        assertEquals(20, rs.getInt("col2"));
        assertEquals(2.5, rs.getDouble("col3"));
        rs.close();
        
        stat.execute("DROP TABLE test_multi");
        stat.close();
    }

    private void testValuesAliasWithComplexExpressions(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        
        // Create test table
        stat.execute("CREATE TABLE test_complex (id INT PRIMARY KEY, counter INT, total DECIMAL(10,2))");
        
        // Insert initial data
        stat.execute("INSERT INTO test_complex VALUES (1, 5, 100.0)");
        
        // Test with complex expressions
        stat.execute("INSERT INTO test_complex (id, counter, total) VALUES (1, 3, 50.0) AS new_data " +
                    "ON DUPLICATE KEY UPDATE counter = counter + new_data.counter, total = total + new_data.total");
        
        // Verify the update
        ResultSet rs = stat.executeQuery("SELECT * FROM test_complex WHERE id = 1");
        assertTrue(rs.next());
        assertEquals(8, rs.getInt("counter")); // 5 + 3
        assertEquals(150.0, rs.getDouble("total")); // 100.0 + 50.0
        rs.close();
        
        stat.execute("DROP TABLE test_complex");
        stat.close();
    }

    private void testBackwardCompatibility(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        
        // Create test table
        stat.execute("CREATE TABLE test_compat (id INT PRIMARY KEY, name VARCHAR(50), value INT)");
        
        // Insert initial data
        stat.execute("INSERT INTO test_compat VALUES (1, 'initial', 100)");
        
        // Test old VALUES() function still works
        stat.execute("INSERT INTO test_compat (id, name, value) VALUES (1, 'updated_old', 200) " +
                    "ON DUPLICATE KEY UPDATE name = VALUES(name), value = VALUES(value)");
        
        // Verify the update
        ResultSet rs = stat.executeQuery("SELECT * FROM test_compat WHERE id = 1");
        assertTrue(rs.next());
        assertEquals("updated_old", rs.getString("name"));
        assertEquals(200, rs.getInt("value"));
        rs.close();
        
        stat.execute("DROP TABLE test_compat");
        stat.close();
    }

    private void testMixedSyntax(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        
        // Create test table
        stat.execute("CREATE TABLE test_mixed (id INT PRIMARY KEY, name VARCHAR(50), value INT, extra VARCHAR(50))");
        
        // Insert initial data
        stat.execute("INSERT INTO test_mixed VALUES (1, 'initial', 100, 'old')");
        
        // Test mixing old VALUES() and new alias syntax
        stat.execute("INSERT INTO test_mixed (id, name, value, extra) VALUES (1, 'updated_mixed', 200, 'new') AS x " +
                    "ON DUPLICATE KEY UPDATE name = x.name, value = VALUES(value), extra = x.extra");
        
        // Verify the update
        ResultSet rs = stat.executeQuery("SELECT * FROM test_mixed WHERE id = 1");
        assertTrue(rs.next());
        assertEquals("updated_mixed", rs.getString("name"));
        assertEquals(200, rs.getInt("value"));
        assertEquals("new", rs.getString("extra"));
        rs.close();
        
        stat.execute("DROP TABLE test_mixed");
        stat.close();
    }

}
