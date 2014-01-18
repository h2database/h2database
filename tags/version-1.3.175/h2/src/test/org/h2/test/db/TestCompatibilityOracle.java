/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;

/**
 * Test Oracle compatibility mode.
 */
public class TestCompatibilityOracle extends TestBase {

    /**
     * Run just this test.
     *
     * @param s ignored
     */
    public static void main(String... s) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.test();
    }

    @Override
    public void test() throws Exception {
        testTreatEmptyStringsAsNull();
        testDecimalScale();
    }

    private void testTreatEmptyStringsAsNull() throws SQLException {
        deleteDb("oracle");
        Connection conn = getConnection("oracle;MODE=Oracle");
        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE A (ID NUMBER, X VARCHAR2(1))");
        stat.execute("INSERT INTO A VALUES (1, 'a')");
        stat.execute("INSERT INTO A VALUES (2, '')");
        stat.execute("INSERT INTO A VALUES (3, ' ')");
        assertResult("3", stat, "SELECT COUNT(*) FROM A");
        assertResult("1", stat, "SELECT COUNT(*) FROM A WHERE X IS NULL");
        assertResult("2", stat, "SELECT COUNT(*) FROM A WHERE TRIM(X) IS NULL");
        assertResult("0", stat, "SELECT COUNT(*) FROM A WHERE X = ''");
        assertResult(new Object[][] { { 1, "a" }, { 2, null }, { 3, " " } }, stat, "SELECT * FROM A");
        assertResult(new Object[][] { { 1, "a" }, { 2, null }, { 3, null } }, stat, "SELECT ID, TRIM(X) FROM A");

        stat.execute("CREATE TABLE B (ID NUMBER, X NUMBER)");
        stat.execute("INSERT INTO B VALUES (1, '5')");
        stat.execute("INSERT INTO B VALUES (2, '')");
        assertResult("2", stat, "SELECT COUNT(*) FROM B");
        assertResult("1", stat, "SELECT COUNT(*) FROM B WHERE X IS NULL");
        assertResult("0", stat, "SELECT COUNT(*) FROM B WHERE X = ''");
        assertResult(new Object[][] { { 1, 5 }, { 2, null } }, stat, "SELECT * FROM B");

        stat.execute("CREATE TABLE C (ID NUMBER, X TIMESTAMP)");
        stat.execute("INSERT INTO C VALUES (1, '1979-11-12')");
        stat.execute("INSERT INTO C VALUES (2, '')");
        assertResult("2", stat, "SELECT COUNT(*) FROM C");
        assertResult("1", stat, "SELECT COUNT(*) FROM C WHERE X IS NULL");
        assertResult("0", stat, "SELECT COUNT(*) FROM C WHERE X = ''");
        assertResult(new Object[][] { { 1, "1979-11-12 00:00:00.0" }, { 2, null } }, stat, "SELECT * FROM C");

        stat.execute("CREATE TABLE D (ID NUMBER, X VARCHAR2(1))");
        stat.execute("INSERT INTO D VALUES (1, 'a')");
        stat.execute("SET @FOO = ''");
        stat.execute("INSERT INTO D VALUES (2, @FOO)");
        assertResult("2", stat, "SELECT COUNT(*) FROM D");
        assertResult("1", stat, "SELECT COUNT(*) FROM D WHERE X IS NULL");
        assertResult("0", stat, "SELECT COUNT(*) FROM D WHERE X = ''");
        assertResult(new Object[][] { { 1, "a" }, { 2, null } }, stat, "SELECT * FROM D");

        stat.execute("CREATE TABLE E (ID NUMBER, X RAW(1))");
        stat.execute("INSERT INTO E VALUES (1, '0A')");
        stat.execute("INSERT INTO E VALUES (2, '')");
        assertResult("2", stat, "SELECT COUNT(*) FROM E");
        assertResult("1", stat, "SELECT COUNT(*) FROM E WHERE X IS NULL");
        assertResult("0", stat, "SELECT COUNT(*) FROM E WHERE X = ''");
        assertResult(new Object[][] { { 1, new byte[] { 10 } }, { 2, null } }, stat, "SELECT * FROM E");

        conn.close();
    }

    private void testDecimalScale() throws SQLException {
        deleteDb("oracle");
        Connection conn = getConnection("oracle;MODE=Oracle");
        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE A (ID NUMBER, X DECIMAL(9,5))");
        stat.execute("INSERT INTO A VALUES (1, 2)");
        stat.execute("INSERT INTO A VALUES (2, 4.3)");
        stat.execute("INSERT INTO A VALUES (3, '6.78')");
        assertResult("3", stat, "SELECT COUNT(*) FROM A");
        assertResult(new Object[][] { { 1, 2 }, { 2, 4.3 }, { 3, 6.78 } }, stat, "SELECT * FROM A");

        conn.close();
    }

    private void assertResult(Object[][] expectedRowsOfValues, Statement stat, String sql) throws SQLException {
        assertResult(newSimpleResultSet(expectedRowsOfValues), stat, sql);
    }

    private void assertResult(ResultSet expected, Statement stat, String sql) throws SQLException {
        ResultSet actual = stat.executeQuery(sql);
        int expectedColumnCount = expected.getMetaData().getColumnCount();
        assertEquals(expectedColumnCount, actual.getMetaData().getColumnCount());
        while (true) {
            boolean expectedNext = expected.next();
            boolean actualNext = actual.next();
            if (!expectedNext && !actualNext) {
                return;
            }
            if (expectedNext != actualNext) {
                fail("number of rows in actual and expected results sets does not match");
            }
            for (int i = 0; i < expectedColumnCount; i++) {
                String expectedString = columnResultToString(expected.getObject(i + 1));
                String actualString = columnResultToString(actual.getObject(i + 1));
                assertEquals(expectedString, actualString);
            }
        }
    }

    private static String columnResultToString(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Object[]) {
            return Arrays.deepToString((Object[]) object);
        }
        if (object instanceof byte[]) {
            return Arrays.toString((byte[]) object);
        }
        return object.toString();
    }

    private static SimpleResultSet newSimpleResultSet(Object[][] rowsOfValues) {
        SimpleResultSet result = new SimpleResultSet();
        for (int i = 0; i < rowsOfValues[0].length; i++) {
            result.addColumn(i + "", Types.JAVA_OBJECT, 0, 0);
        }
        for (int i = 0; i < rowsOfValues.length; i++) {
            result.addRow(rowsOfValues[i]);
        }
        return result;
    }

}
