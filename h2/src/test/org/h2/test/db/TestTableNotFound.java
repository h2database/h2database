package org.h2.test.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

public class TestTableNotFound extends TestDb {

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
    testWithoutAnyCandidate();
    testWithOneCandidate();
    testWithTwoCandidates();
    testWithSchema();
    testWithSchemaSearchPath();
  }

  private void testWithoutAnyCandidate() throws SQLException {
    deleteDb(getTestName());
    final Connection conn = getJdbcConnection();
    final Statement stat = conn.createStatement();
    stat.execute("CREATE TABLE T2 ( ID INT IDENTITY )");
    try {
      stat.executeQuery("SELECT 1 FROM t1");
      fail("Table `t1` was accessible but should not have been.");
    } catch (SQLException e) {
      final String message = e.getMessage();
      assertContains(message, "Table \"t1\" not found;");
    }

    conn.close();
    deleteDb(getTestName());
  }

  private void testWithOneCandidate() throws SQLException {
    deleteDb(getTestName());
    final Connection conn = getJdbcConnection();
    final Statement stat = conn.createStatement();
    stat.execute("CREATE TABLE T1 ( ID INT IDENTITY )");
    try {
      stat.executeQuery("SELECT 1 FROM t1");
      fail("Table `t1` was accessible but should not have been.");
    } catch (SQLException e) {
      final String message = e.getMessage();
      assertContains(message, "Table \"t1\" not found, candidates are: \"`T1`\"");
    }

    conn.close();
    deleteDb(getTestName());
  }

  private void testWithTwoCandidates() throws SQLException {
    deleteDb(getTestName());
    final Connection conn = getJdbcConnection();
    final Statement stat = conn.createStatement();
    stat.execute("CREATE TABLE Toast ( ID INT IDENTITY )");
    stat.execute("CREATE TABLE TOAST ( ID INT IDENTITY )");
    try {
      stat.executeQuery("SELECT 1 FROM toast");
      fail("Table `toast` was accessible but should not have been.");
    } catch (SQLException e) {
      final String message = e.getMessage();
      assertContains(message, "Table \"toast\" not found, candidates are: \"`TOAST`, `Toast`");
    }

    conn.close();
    deleteDb(getTestName());
  }

  private void testWithSchema() throws SQLException {
    deleteDb(getTestName());
    final Connection conn = getJdbcConnection();
    final Statement stat = conn.createStatement();
    stat.execute("CREATE TABLE T1 ( ID INT IDENTITY )");
    try {
      stat.executeQuery("SELECT 1 FROM PUBLIC.t1");
      fail("Table `t1` was accessible but should not have been.");
    } catch (SQLException e) {
      final String message = e.getMessage();
      assertContains(message, "Table \"t1\" not found, candidates are: \"`T1`\"");
    }

    conn.close();
    deleteDb(getTestName());
  }

  private void testWithSchemaSearchPath() throws SQLException {
    deleteDb(getTestName());
    final JdbcConnection conn = getJdbcConnection();
    final Session session = (Session) conn.getSession();
    session.setSchemaSearchPath(new String[]{ "PUBLIC" });

    final Statement stat = conn.createStatement();
    stat.execute("CREATE TABLE T1 ( ID INT IDENTITY )");
    try {
      stat.executeQuery("SELECT 1 FROM t1");
      fail("Table `t1` was accessible but should not have been.");
    } catch (SQLException e) {
      final String message = e.getMessage();
      assertContains(message, "Table \"t1\" not found, candidates are: \"`T1`\"");
    }

    conn.close();
    deleteDb(getTestName());
  }

  private JdbcConnection getJdbcConnection() throws SQLException {
    return (JdbcConnection) getConnection(getTestName() + ";DATABASE_TO_LOWER=false;DATABASE_TO_UPPER=false");
  }
}
