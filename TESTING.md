# H2 Database - Testing Guide

This guide explains how to write and run tests for the H2 Database Engine.

## Table of Contents

- [Overview](#overview)
- [Test Structure](#test-structure)
- [Running Tests](#running-tests)
- [Writing Tests](#writing-tests)
- [Test Categories](#test-categories)
- [Best Practices](#best-practices)
- [Debugging Tests](#debugging-tests)
- [Continuous Integration](#continuous-integration)

## Overview

H2 has a comprehensive test suite that ensures database correctness, performance, and compatibility. The tests are crucial for:

- **Verifying functionality**: Ensuring features work as expected
- **Preventing regressions**: Catching bugs before they reach users
- **Documentation**: Tests serve as usage examples
- **Compatibility**: Testing various database modes and JDBC compliance

### Test Philosophy

- **Comprehensive coverage**: Test all code paths
- **Fast execution**: Keep tests quick when possible
- **Isolation**: Tests should be independent
- **Readability**: Tests should be easy to understand
- **Repeatability**: Same results every run

## Test Structure

### Test Organization

All tests are in `h2/src/test/org/h2/test/`:

```
test/
├── TestAll.java          - Main test suite runner
├── TestBase.java         - Base class for all tests
├── TestDb.java           - Database test category runner
├── db/                   - Database functionality tests
│   ├── TestAlter.java
│   ├── TestBackup.java
│   ├── TestIndex.java
│   ├── TestTransaction.java
│   └── ...
├── jdbc/                 - JDBC driver tests
│   ├── TestConnection.java
│   ├── TestPreparedStatement.java
│   ├── TestResultSet.java
│   └── ...
├── mvcc/                 - MVCC and concurrency tests
├── server/               - Server mode tests
├── store/                - Storage engine tests
├── unit/                 - Unit tests
│   ├── TestValue.java
│   ├── TestExpression.java
│   └── ...
└── scripts/              - SQL script-based tests
    ├── testSimple.sql
    └── ...
```

### Test Base Classes

**`TestAll`**: Main test runner
- Coordinates all test execution
- Provides command-line options
- Reports results

**`TestBase`**: Base class for tests
- Provides utility methods
- Database connection helpers
- Assertion helpers
- Cleanup logic

**`TestDb`**: Database test category
- Groups database-specific tests
- Runs in various modes

## Running Tests

### Run All Tests

```bash
cd h2
./build.sh test
```

This runs the complete test suite. It may take 30-60 minutes.

### Run CI Tests (Faster)

```bash
./build.sh testCI
```

Runs a subset of tests suitable for continuous integration (~5-10 minutes).

### Run Specific Test Classes

```bash
# Compile first
./build.sh compile

# Run a specific test class
java -cp "temp:bin/h2-*.jar" org.h2.test.db.TestIndex

# Run with the TestAll framework
java -cp "temp:bin/h2-*.jar" org.h2.test.TestAll db.TestIndex
```

### Test Options

Run `TestAll` with options:

```bash
java -cp "temp:bin/h2-*.jar" org.h2.test.TestAll [options] [test]
```

**Common options**:
- `quick` - Run only quick tests
- `tiny` - Run minimal test set
- `small` - Run small test set
- `big` - Run extended test set
- `stopOnError` - Stop on first error
- `networked` - Include network tests
- `mvStore` - Test MVStore mode
- `memory` - Test in-memory mode

**Examples**:
```bash
# Run quick tests only
java -cp "temp:bin/h2-*.jar" org.h2.test.TestAll quick

# Run specific test with stop on error
java -cp "temp:bin/h2-*.jar" org.h2.test.TestAll stopOnError db.TestIndex

# Run small set of MVStore tests
java -cp "temp:bin/h2-*.jar" org.h2.test.TestAll small mvStore
```

### Run from IDE

**IntelliJ IDEA**:
1. Navigate to test class (e.g., `TestIndex.java`)
2. Right-click on the class or test method
3. Select "Run 'TestIndex'" or "Debug 'TestIndex'"

**Eclipse**:
1. Navigate to test class
2. Right-click → Run As → JUnit Test

### Run JUnit Tests

H2 also supports JUnit (though most tests use the custom framework):

```bash
# Using Maven
cd h2
./mvnw test

# Run specific JUnit test
./mvnw test -Dtest=TestClassName
```

## Writing Tests

### Basic Test Structure

Create a test class extending `TestBase`:

```java
package org.h2.test.db;

import org.h2.test.TestBase;
import java.sql.*;

public class TestMyFeature extends TestBase {

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
        testBasicOperation();
        testEdgeCases();
        testErrors();
    }

    private void testBasicOperation() throws SQLException {
        deleteDb("myTest");
        Connection conn = getConnection("myTest");
        Statement stat = conn.createStatement();
        
        // Test setup
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        
        // Test execution
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST WHERE ID=1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        rs.close();
        
        // Cleanup
        stat.execute("DROP TABLE TEST");
        conn.close();
        deleteDb("myTest");
    }

    private void testEdgeCases() throws SQLException {
        // Test edge cases, null values, empty sets, etc.
    }

    private void testErrors() throws SQLException {
        // Test error conditions
        Connection conn = getConnection("myTest");
        Statement stat = conn.createStatement();
        
        // Should throw exception
        assertThrows(() -> {
            stat.execute("SELECT * FROM NONEXISTENT_TABLE");
        });
        
        conn.close();
    }
}
```

### Using TestBase Utilities

**Database Connection**:
```java
// In-memory database
Connection conn = getConnection("testDb");

// File-based database
Connection conn = getConnection("testDb;FILE_LOCK=NO");

// With specific mode
Connection conn = getConnection("testDb;MODE=PostgreSQL");
```

**Assertions**:
```java
// Boolean assertions
assertTrue(condition);
assertFalse(condition);

// Equality
assertEquals(expected, actual);
assertEquals(message, expected, actual);

// Exceptions
assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND, () -> {
    stat.execute("SELECT * FROM NONEXISTENT");
});

// Result set assertions
assertResult(resultSet, new String[][] {
    {"1", "Alice"},
    {"2", "Bob"}
});
```

**Cleanup**:
```java
// Delete database files
deleteDb("testDb");

// Delete and recreate
deleteDb("testDb");
Connection conn = getConnection("testDb");
// ... use connection ...
conn.close();
deleteDb("testDb");
```

### Testing Queries

```java
private void testQuery() throws SQLException {
    Connection conn = getConnection("testQuery");
    Statement stat = conn.createStatement();
    
    stat.execute("CREATE TABLE USERS(ID INT, NAME VARCHAR, AGE INT)");
    stat.execute("INSERT INTO USERS VALUES(1, 'Alice', 30), (2, 'Bob', 25)");
    
    // Test query results
    ResultSet rs = stat.executeQuery("SELECT NAME FROM USERS WHERE AGE > 26 ORDER BY NAME");
    assertTrue(rs.next());
    assertEquals("Alice", rs.getString(1));
    assertFalse(rs.next());
    rs.close();
    
    // Test with PreparedStatement
    PreparedStatement prep = conn.prepareStatement("SELECT * FROM USERS WHERE ID = ?");
    prep.setInt(1, 2);
    rs = prep.executeQuery();
    assertTrue(rs.next());
    assertEquals("Bob", rs.getString("NAME"));
    assertEquals(25, rs.getInt("AGE"));
    rs.close();
    prep.close();
    
    conn.close();
    deleteDb("testQuery");
}
```

### Testing Transactions

```java
private void testTransaction() throws SQLException {
    deleteDb("testTx");
    Connection conn = getConnection("testTx");
    Statement stat = conn.createStatement();
    
    stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY)");
    
    // Test commit
    conn.setAutoCommit(false);
    stat.execute("INSERT INTO TEST VALUES(1)");
    conn.commit();
    
    ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
    rs.next();
    assertEquals(1, rs.getInt(1));
    rs.close();
    
    // Test rollback
    stat.execute("INSERT INTO TEST VALUES(2)");
    conn.rollback();
    
    rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
    rs.next();
    assertEquals(1, rs.getInt(1)); // Still only 1 row
    rs.close();
    
    conn.close();
    deleteDb("testTx");
}
```

### Testing Concurrency

```java
private void testConcurrency() throws Exception {
    deleteDb("testConcurrent");
    Connection conn1 = getConnection("testConcurrent");
    Connection conn2 = getConnection("testConcurrent");
    
    Statement stat1 = conn1.createStatement();
    stat1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE INT)");
    stat1.execute("INSERT INTO TEST VALUES(1, 0)");
    
    // Two connections updating same row
    conn1.setAutoCommit(false);
    conn2.setAutoCommit(false);
    
    stat1.execute("UPDATE TEST SET VALUE = 1 WHERE ID = 1");
    
    // Second connection should wait or see old value (depending on isolation)
    Statement stat2 = conn2.createStatement();
    ResultSet rs = stat2.executeQuery("SELECT VALUE FROM TEST WHERE ID = 1");
    rs.next();
    int value = rs.getInt(1);
    // Check value based on isolation level
    rs.close();
    
    conn1.commit();
    conn2.commit();
    
    conn1.close();
    conn2.close();
    deleteDb("testConcurrent");
}
```

### Testing Error Conditions

```java
private void testErrors() throws SQLException {
    Connection conn = getConnection("testErrors");
    Statement stat = conn.createStatement();
    
    // Syntax error
    assertThrows(ErrorCode.SYNTAX_ERROR_1, () -> {
        stat.execute("SSELECT * FROM DUAL");
    });
    
    // Table not found
    assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, () -> {
        stat.execute("SELECT * FROM NONEXISTENT_TABLE");
    });
    
    // Duplicate key
    stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY)");
    stat.execute("INSERT INTO TEST VALUES(1)");
    assertThrows(ErrorCode.DUPLICATE_KEY_1, () -> {
        stat.execute("INSERT INTO TEST VALUES(1)");
    });
    
    conn.close();
    deleteDb("testErrors");
}
```

## Test Categories

### 1. Unit Tests (`test/unit/`)

Test individual classes and methods in isolation.

**Example**: `TestValue.java` tests the Value class hierarchy.

```java
public class TestMyUnit extends TestBase {
    @Override
    public void test() {
        testValueCreation();
        testValueConversion();
        testValueComparison();
    }
}
```

### 2. Database Tests (`test/db/`)

Test database functionality: tables, indexes, constraints, transactions.

**Examples**:
- `TestIndex.java` - Index operations
- `TestTransaction.java` - Transaction behavior
- `TestAlter.java` - ALTER TABLE operations

### 3. JDBC Tests (`test/jdbc/`)

Test JDBC driver compliance and functionality.

**Examples**:
- `TestConnection.java` - Connection handling
- `TestPreparedStatement.java` - Prepared statements
- `TestResultSet.java` - Result set operations

### 4. Server Tests (`test/server/`)

Test server modes (TCP, Web, PG protocol).

```java
public class TestMyServer extends TestBase {
    @Override
    public void test() throws Exception {
        // Start server
        Server server = Server.createTcpServer().start();
        
        // Connect remotely
        Connection conn = DriverManager.getConnection(
            "jdbc:h2:tcp://localhost/mem:test");
        
        // Test operations
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT)");
        
        // Cleanup
        conn.close();
        server.stop();
    }
}
```

### 5. MVCC Tests (`test/mvcc/`)

Test multi-version concurrency control.

### 6. Script Tests (`test/scripts/`)

SQL script-based tests for regression testing.

**Example**: `testSimple.sql`
```sql
-- Test basic operations
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

SELECT * FROM TEST;
> ID NAME
> -- -----
> 1  Hello
> rows: 1
```

## Best Practices

### 1. Test Isolation

Each test should be independent:
```java
@Override
public void test() throws Exception {
    deleteDb("test1"); // Clean start
    testFeatureA();
    deleteDb("test1"); // Clean up
    
    deleteDb("test2");
    testFeatureB();
    deleteDb("test2");
}
```

### 2. Descriptive Test Names

```java
// Good
private void testInsertWithDuplicateKeyThrowsException() { }
private void testSelectWithNullValues() { }

// Less clear
private void testInsert() { }
private void testSelect() { }
```

### 3. Test Edge Cases

```java
private void testEdgeCases() throws SQLException {
    Connection conn = getConnection("test");
    Statement stat = conn.createStatement();
    
    // Empty table
    stat.execute("CREATE TABLE TEST(ID INT)");
    ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
    assertFalse(rs.next());
    rs.close();
    
    // NULL values
    stat.execute("INSERT INTO TEST VALUES(NULL)");
    rs = stat.executeQuery("SELECT * FROM TEST");
    assertTrue(rs.next());
    assertEquals(0, rs.getInt(1));
    assertTrue(rs.wasNull());
    rs.close();
    
    // Large values
    stat.execute("INSERT INTO TEST VALUES(2147483647)"); // MAX_INT
    
    // Boundary conditions
    // Empty strings, zero, negative numbers, etc.
    
    conn.close();
}
```

### 4. Clean Resource Management

```java
// Always close resources
Connection conn = null;
try {
    conn = getConnection("test");
    // ... test code ...
} finally {
    if (conn != null) {
        conn.close();
    }
    deleteDb("test");
}

// Or use try-with-resources (Java 7+)
try (Connection conn = getConnection("test")) {
    // ... test code ...
} finally {
    deleteDb("test");
}
```

### 5. Meaningful Assertions

```java
// Good - explains what's being tested
assertEquals("Expected one row after insert", 1, count);

// Less clear
assertEquals(1, count);

// Very clear with helper methods
assertSingleRow(rs, "Expected inserted row to be returned");
```

### 6. Test Performance Separately

```java
// Separate performance tests from correctness tests
private void testPerformance() {
    // Only run in full test mode, not in quick mode
    if (!config.big) {
        return;
    }
    
    long start = System.currentTimeMillis();
    // ... performance test ...
    long elapsed = System.currentTimeMillis() - start;
    assertTrue("Operation too slow", elapsed < 1000);
}
```

## Debugging Tests

### Enable Trace Output

```java
// In test setup
conn = getConnection("test;TRACE_LEVEL_FILE=4");

// Check test.trace.db file for details
```

### Print Debug Information

```java
private void testWithDebug() throws SQLException {
    Connection conn = getConnection("test");
    Statement stat = conn.createStatement();
    
    ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
    ResultSetMetaData meta = rs.getMetaData();
    
    // Debug output
    System.out.println("Columns: " + meta.getColumnCount());
    for (int i = 1; i <= meta.getColumnCount(); i++) {
        System.out.println(i + ": " + meta.getColumnName(i));
    }
    
    while (rs.next()) {
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            System.out.println(meta.getColumnName(i) + " = " + rs.getObject(i));
        }
    }
    
    conn.close();
}
```

### Run Single Test

```bash
# Run just one test class
java -cp "temp:bin/h2-*.jar" org.h2.test.db.TestIndex

# Or from main method in the test class
```

### Use IDE Debugger

1. Set breakpoints in test code
2. Run test in debug mode
3. Step through execution
4. Inspect variables and state

## Continuous Integration

### GitHub Actions

H2 uses GitHub Actions for CI (see `.github/workflows/ci.yml`):

```yaml
- name: Test
  run: |
    cd h2
    ./build.sh jar testCI
```

### Local CI Simulation

```bash
# Run the same tests as CI
cd h2
./build.sh clean
./build.sh jar testCI
```

### Test Matrices

CI tests against multiple Java versions:
- Java 11 (minimum supported)
- Java 17 (recommended)

## Code Coverage

### Generate Coverage Report

```bash
cd h2
./build.sh coverage
```

Opens coverage report in `coverage/index.html`.

### Coverage Goals

- **Statement coverage**: >80%
- **Branch coverage**: >70%
- **Critical paths**: 100%

### Viewing Coverage

Open `coverage/index.html` in a browser to see:
- Overall coverage percentages
- Per-package coverage
- Per-class coverage
- Line-by-line coverage

## Summary

Good testing practices:
1. Write tests for all new features
2. Test both success and failure cases
3. Keep tests isolated and repeatable
4. Use descriptive names and comments
5. Clean up resources properly
6. Run tests frequently during development
7. Aim for high code coverage

Remember: Tests are documentation. Write them clearly so others can understand what your code does and how to use it.
