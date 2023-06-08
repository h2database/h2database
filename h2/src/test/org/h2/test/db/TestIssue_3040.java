/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

public class TestIssue_3040 extends TestDb {

    public static final String TABLE_TO_QUERY = "TO_QUERY";

    public static final String QUERY_STATEMENT = "WITH TMP_TO_QUERY"
            + " as (SELECT avg(SIMPLE_VALUE) AVG_SIMPLE_VALUE FROM public." + TABLE_TO_QUERY
            + ")  SELECT * FROM TMP_TO_QUERY";

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws SQLException {
        createTableTest();
    }

    public void createTableTest() throws SQLException {
        deleteDb(getTestName());
        try (Connection connection = getConnection(getTestName())) {
            createTable(connection, TABLE_TO_QUERY);

            runCTE(connection);

            // another connection to simulate parallel execution with connection
            // pools sequence used for GENERATED_ID will get the same ID as temp
            // table used for CTE query
            try (Connection conn2 = getConnection(getTestName())) {
                createTable(conn2, "WITH_MISSING_SEQUENCE");
            }
            // commit to release again already released systemIds, could be just
            // another query to trigger tx commit
            connection.commit();
            // id reused again, sequence entry from MVStore gets dropped as side
            // effect.
            runCTE(connection);

        }
        // try to reconnect to already corrupted file
        try (Connection connection = getConnection(getTestName())) {
            runCTE(connection);
        }
    }

    private static void createTable(Connection connection, String tableName) {
        try (Statement st = connection.createStatement()) {
            st.execute("CREATE TABLE public." + tableName + " (GENERATED_ID IDENTITY PRIMARY KEY, SIMPLE_VALUE INT)");
        } catch (SQLException ex) {
            System.out.println("error: " + ex);
            ex.printStackTrace();
        }
    }

    private static void runCTE(Connection connection) {
        try (PreparedStatement st = connection.prepareStatement(QUERY_STATEMENT)) {
            st.executeQuery();
        } catch (SQLException ex) {
            System.out.println("error: " + ex);
            ex.printStackTrace();
        }
    }

}
