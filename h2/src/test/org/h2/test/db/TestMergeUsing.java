package org.h2.test.db;
/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.h2.test.TestBase;
/**
 * Test merge using syntax.
 */

public class TestMergeUsing extends TestBase {
 
    private static final String GATHER_ORDERED_RESULTS_SQL = "SELECT ID, NAME FROM PARENT ORDER BY ID ASC";

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
        
        // Simple ID,NAME inserts, target table with PK initially empty
        testMergeUsing(
                "CREATE TABLE PARENT(ID INT, NAME VARCHAR, PRIMARY KEY(ID) );",
                "MERGE INTO PARENT AS P USING (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) ) AS S ON (P.ID = S.ID AND 1=1 AND S.ID = P.ID) WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME WHERE 2 = 2 WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (S.ID, S.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2)",
                2
                );
        // Simple NAME updates, target table missing PK
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );",
                "MERGE INTO PARENT AS P USING (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) ) AS S ON (P.ID = S.ID AND 1=1 AND S.ID = P.ID) WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME||S.ID WHERE 1 = 1 WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (S.ID, S.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(1,2)",
                2
                );
        // No NAME updates, WHERE clause is always false, insert clause missing
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );",
                "MERGE INTO PARENT AS P USING (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) ) AS S ON (P.ID = S.ID) WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME||S.ID WHERE 1 = 2",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2)",
                0
                );
        // No NAME updates, no WHERE clause, insert clause missing
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );",
                "MERGE INTO PARENT AS P USING (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) ) AS S ON (P.ID = S.ID) WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME||S.ID",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(1,2)",
                2
                );
        // Two delete updates done, no WHERE clause, insert clause missing
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );",
                "MERGE INTO PARENT AS P USING (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) ) AS S ON (P.ID = S.ID) WHEN MATCHED THEN DELETE",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) WHERE 1=0",
                2
                );
        // One insert, one update one delete happens, target table missing PK
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );",
                "MERGE INTO PARENT AS P USING (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3) ) AS S ON (P.ID = S.ID) WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME||S.ID WHERE P.ID = 2 DELETE WHERE P.ID = 1 WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (S.ID, S.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(2,2) UNION ALL SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(3,3)",
                3
                );
        // No updates happen: No insert defined, no update or delete happens due to ON condition failing always, target table missing PK
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );",
                "MERGE INTO PARENT AS P USING (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3) ) AS S ON (P.ID = S.ID AND 1=0) WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME||S.ID WHERE P.ID = 2 DELETE WHERE P.ID = 1",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2)",
                0
                );
        // One insert, one update one delete happens, target table missing PK
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );"+
                        "CREATE TABLE SOURCE AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)  );",
                "MERGE INTO PARENT AS P USING SOURCE AS S ON (P.ID = S.ID) WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME||S.ID WHERE P.ID = 2 DELETE WHERE P.ID = 1 WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (S.ID, S.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(2,2) UNION ALL SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(3,3)",
                3
                );
        // One insert, one update one delete happens, target table missing PK, no source alias
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );"+
                        "CREATE TABLE SOURCE AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)  );",
                "MERGE INTO PARENT AS P USING SOURCE ON (P.ID = SOURCE.ID) WHEN MATCHED THEN UPDATE SET P.NAME = SOURCE.NAME||SOURCE.ID WHERE P.ID = 2 DELETE WHERE P.ID = 1 WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SOURCE.ID, SOURCE.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(2,2) UNION ALL SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(3,3)",
                3
                );
        // One insert, one update one delete happens, target table missing PK, no source or target alias
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );"+
                        "CREATE TABLE SOURCE AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)  );",
                "MERGE INTO PARENT USING SOURCE ON (PARENT.ID = SOURCE.ID) WHEN MATCHED THEN UPDATE SET PARENT.NAME = SOURCE.NAME||SOURCE.ID WHERE PARENT.ID = 2 DELETE WHERE PARENT.ID = 1 WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SOURCE.ID, SOURCE.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(2,2) UNION ALL SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(3,3)",
                3
                );        
        
        // Duplicate source keys: SQL standard says duplicate or repeated updates in same statement should cause errors
        // One insert, one update one delete happens, target table missing PK, no source or target alias
        testMergeUsingException(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,1) );"+
                        "CREATE TABLE SOURCE AS (SELECT 1 AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2)  );",
                "MERGE INTO PARENT USING SOURCE ON (PARENT.ID = SOURCE.ID) WHEN MATCHED THEN UPDATE SET PARENT.NAME = SOURCE.NAME||SOURCE.ID WHERE PARENT.ID = 2 DELETE WHERE PARENT.ID = 1 WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SOURCE.ID, SOURCE.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT 1 AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(1,1)",
                3,
                "Unique index or primary key violation: \"Merge using ON column expression, duplicate values found:keys[ID]:values:[1]:from:PUBLIC.SOURCE:alias:SOURCE:current row number:2:conflicting row number:1"
                );
        
        // Duplicate key updated 3 rows at once, only 1 expected
        testMergeUsingException(
                "CREATE TABLE PARENT AS (SELECT 1 AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3) );"+
                        "CREATE TABLE SOURCE AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)  );",
                "MERGE INTO PARENT USING SOURCE ON (PARENT.ID = SOURCE.ID) WHEN MATCHED THEN UPDATE SET PARENT.NAME = SOURCE.NAME||SOURCE.ID WHERE PARENT.ID = 2 DELETE WHERE PARENT.ID = 1 WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SOURCE.ID, SOURCE.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(2,2) UNION ALL SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(3,3)",
                3,
                "Duplicate key updated 3 rows at once, only 1 expected"
                );        
        }

    /**
     * Run a test case of the merge using syntax
     * @param setupSQL - one or more SQL statements to setup the case
     * @param statementUnderTest - the merge statement being tested
     * @param gatherResultsSQL - a select which gathers the results of the merge from the target table
     * @param expectedResultsSQL - a select which returns the expected results in the target table
     * @param expectedRowUpdateCount - how many updates should be expected from the merge using
     * @throws Exception
     */
    private void testMergeUsing(String setupSQL, String statementUnderTest, String gatherResultsSQL,
            String expectedResultsSQL, int expectedRowUpdateCount) throws Exception {
        deleteDb("mergeUsingQueries");
        Connection conn = getConnection("mergeUsingQueries");
        Statement stat;
        PreparedStatement prep;
        ResultSet rs;
        int rowCountUpdate;

        try{
            stat = conn.createStatement();
            stat.execute(setupSQL);
    
            prep = conn.prepareStatement(statementUnderTest);
            rowCountUpdate = prep.executeUpdate();
    
            // compare actual results from SQL resultsset with expected results - by diffing (aka set MINUS operation)
            rs = stat.executeQuery("( "+gatherResultsSQL+" ) MINUS ( "+expectedResultsSQL+" )");
    
            int rowCount = 0;
            StringBuffer diffBuffer = new StringBuffer("");
            while (rs.next()) {
                rowCount++;
                diffBuffer.append("|");
                System.out.println("rs.getMetaData().getColumnCount()="+rs.getMetaData().getColumnCount());
                for(int ndx = 1; ndx <= rs.getMetaData().getColumnCount(); ndx++){
                    diffBuffer.append(rs.getObject(ndx));
                    diffBuffer.append("|\n");
                }
            }
            assertEquals("Differences between expected and actual output found:"+diffBuffer,0,rowCount);
            assertEquals("Expected update counts differ",expectedRowUpdateCount,rowCountUpdate);
        }
        finally{
            conn.close();
            deleteDb("mergeUsingQueries");
        }
    }
    /**
     * Run a test case of the merge using syntax
     * @param setupSQL - one or more SQL statements to setup the case
     * @param statementUnderTest - the merge statement being tested
     * @param gatherResultsSQL - a select which gathers the results of the merge from the target table
     * @param expectedResultsSQL - a select which returns the expected results in the target table
     * @param expectedRowUpdateCount - how many updates should be expected from the merge using
     * @throws Exception
     */
    private void testMergeUsingException(String setupSQL, String statementUnderTest, String gatherResultsSQL,
            String expectedResultsSQL, int expectedRowUpdateCount, String exceptionMessage) throws Exception {
        try{
            testMergeUsing( setupSQL,  statementUnderTest,  gatherResultsSQL,
                     expectedResultsSQL,  expectedRowUpdateCount);
        }
        catch(RuntimeException|org.h2.jdbc.JdbcSQLException e){
            assertContains(e.getMessage(),exceptionMessage);
            return;            
        }
        fail("Failed to see exception with message:"+exceptionMessage);
    }

}
