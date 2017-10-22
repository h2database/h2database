/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.h2.jdbc.JdbcSQLException;
import org.h2.test.TestBase;

/**
 * Test non-recursive queries using WITH, but more than one common table defined.
 */
public class TestGeneralCommonTableQueries extends TestBase {

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
        testSimpleSelect();
        testImpliedColumnNames();
        testChainedQuery();
        testParameterizedQuery();
        testNumberedParameterizedQuery();
        testColumnNames();

        testInsert();
        testUpdate();
        testDelete();
        testMerge();
        testCreateTable();
        testNestedSQL();
        testRecursiveTable();
    }

    private void testSimpleSelect() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        Statement stat;
        PreparedStatement prep;
        ResultSet rs;

        stat = conn.createStatement();
        final String simple_two_column_query = "with " +
            "t1(n) as (select 1 as first) " +
            ",t2(n) as (select 2 as first) " +
            "select * from t1 union all select * from t2";
        rs = stat.executeQuery(simple_two_column_query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement(simple_two_column_query);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("with " +
            "t1(n) as (select 2 as first) " +
            ",t2(n) as (select 3 as first) " +
            "select * from t1 union all select * from t2 where n<>?");
        prep.setInt(1, 0); // omit no lines since zero is not in list
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("with " +
            "t1(n) as (select 2 as first) " +
            ",t2(n) as (select 3 as first) " +
            ",t3(n) as (select 4 as first) " +
            "select * from t1 union all select * from t2 union all select * from t3 where n<>?");
        prep.setInt(1, 4); // omit 4 line (last)
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testImpliedColumnNames() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        PreparedStatement prep;
        ResultSet rs;

        prep = conn.prepareStatement("with " +
            "t1 as (select 2 as first_col) " +
            ",t2 as (select first_col+1 from t1) " +
            ",t3 as (select 4 as first_col) " +
            "select * from t1 union all select * from t2 union all select * from t3 where first_col<>?");
        prep.setInt(1, 4); // omit 4 line (last)
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("FIRST_COL"));
        assertFalse(rs.next());
        assertEquals(rs.getMetaData().getColumnCount(),1);
        assertEquals("FIRST_COL",rs.getMetaData().getColumnLabel(1));

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testChainedQuery() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        PreparedStatement prep;
        ResultSet rs;

        prep = conn.prepareStatement(
                "    WITH t1 AS (" +
                "        SELECT 1 AS FIRST_COLUMN" +
                ")," +
                "     t2 AS (" +
                "        SELECT FIRST_COLUMN+1 AS FIRST_COLUMN FROM t1 " +
                ") " +
                "SELECT sum(FIRST_COLUMN) FROM t2");

        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testParameterizedQuery() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        PreparedStatement prep;
        ResultSet rs;

        prep = conn.prepareStatement("WITH t1 AS (" +
                "     SELECT X, 'T1' FROM SYSTEM_RANGE(?,?)" +
                ")," +
                "t2 AS (" +
                "     SELECT X, 'T2' FROM SYSTEM_RANGE(?,?)" +
                ") " +
                "SELECT * FROM t1 UNION ALL SELECT * FROM t2 " +
                "UNION ALL SELECT X, 'Q' FROM SYSTEM_RANGE(?,?)");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.setInt(3, 3);
        prep.setInt(4, 4);
        prep.setInt(5, 5);
        prep.setInt(6, 6);
        rs = prep.executeQuery();

        for(int n: new int[]{1,2,3,4,5,6} ){
            assertTrue(rs.next());
            assertEquals(n, rs.getInt(1));
        }
        assertFalse(rs.next());

        // call it twice
        rs = prep.executeQuery();

        for(int n: new int[]{1,2,3,4,5,6} ){
            assertTrue(rs.next());
            assertEquals(n, rs.getInt(1));
        }
        assertFalse(rs.next());

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testNumberedParameterizedQuery() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        PreparedStatement prep;
        ResultSet rs;

        conn.setAutoCommit(false);

        prep = conn.prepareStatement("WITH t1 AS ("
            +"     SELECT R.X, 'T1' FROM SYSTEM_RANGE(?1,?2) R"
            +"),"
            +"t2 AS ("
            +"     SELECT R.X, 'T2' FROM SYSTEM_RANGE(?3,?4) R"
            +") "
            +"SELECT * FROM t1 UNION ALL SELECT * FROM t2 UNION ALL SELECT X, 'Q' FROM SYSTEM_RANGE(?5,?6)");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.setInt(3, 3);
        prep.setInt(4, 4);
        prep.setInt(5, 5);
        prep.setInt(6, 6);
        rs = prep.executeQuery();

        for (int n : new int[] { 1, 2, 3, 4, 5, 6 }) {
            assertTrue(rs.next());
            assertEquals(n, rs.getInt(1));
        }
        assertEquals("X",rs.getMetaData().getColumnLabel(1));
        assertEquals("'T1'",rs.getMetaData().getColumnLabel(2));

        assertFalse(rs.next());

        try{
            prep = conn.prepareStatement("SELECT * FROM t1 UNION ALL SELECT * FROM t2 "+
                    "UNION ALL SELECT X, 'Q' FROM SYSTEM_RANGE(5,6)");
            rs = prep.executeQuery();
            fail("Temp view T1 was accessible after previous WITH statement finished "+
                    "- but should not have been.");
        }
        catch(JdbcSQLException e){
            // ensure the T1 table has been removed even without auto commit
            assertContains(e.getMessage(),"Table \"T1\" not found;");
        }

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testInsert() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        Statement stat;
        PreparedStatement prep;
        ResultSet rs;
        int rowCount;

        stat = conn.createStatement();
        stat.execute("CREATE TABLE T1 ( ID INT IDENTITY,  X INT NULL, Y VARCHAR(100) NULL )");

        prep = conn.prepareStatement("WITH v1 AS ("
                + "     SELECT R.X, 'X1' AS Y FROM SYSTEM_RANGE(?1,?2) R"
                + ")"
                + "INSERT INTO T1 (X,Y) SELECT v1.X, v1.Y FROM v1");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        rowCount = prep.executeUpdate();

        assertEquals(2, rowCount);

        rs = stat.executeQuery("SELECT ID, X,Y FROM T1");

        for (int n : new int[]{1, 2}) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) != 0);
            assertEquals(n, rs.getInt(2));
            assertEquals("X1", rs.getString(3));
        }
        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testUpdate() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        Statement stat;
        PreparedStatement prep;
        ResultSet rs;
        int rowCount;

        stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS T1 AS SELECT R.X AS ID, R.X, 'X1' AS Y FROM SYSTEM_RANGE(1,2) R");

        prep = conn.prepareStatement("WITH v1 AS ("
                +"     SELECT R.X, 'X1' AS Y FROM SYSTEM_RANGE(?1,?2) R"
                +")"
                +"UPDATE T1 SET Y = 'Y1' WHERE X IN ( SELECT v1.X FROM v1 )");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        rowCount = prep.executeUpdate();

        assertEquals(2,rowCount);

        rs = stat.executeQuery("SELECT ID, X,Y FROM T1");

        for (int n : new int[] { 1, 2 }) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1)!=0);
            assertEquals(n, rs.getInt(2));
            assertEquals("Y1", rs.getString(3));
        }
        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testDelete() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        Statement stat;
        PreparedStatement prep;
        ResultSet rs;
        int rowCount;

        stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS T1 AS SELECT R.X AS ID, R.X, 'X1' AS Y FROM SYSTEM_RANGE(1,2) R");

        prep = conn.prepareStatement("WITH v1 AS ("
                +"     SELECT R.X, 'X1' AS Y FROM SYSTEM_RANGE(1,2) R"
                +")"
                +"DELETE FROM T1 WHERE X IN ( SELECT v1.X FROM v1 )");
        rowCount = prep.executeUpdate();

        assertEquals(2,rowCount);

        rs = stat.executeQuery("SELECT ID, X,Y FROM T1");

        assertFalse(rs.next());

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testMerge() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        Statement stat;
        PreparedStatement prep;
        ResultSet rs;
        int rowCount;

        stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS T1 AS SELECT R.X AS ID, R.X, 'X1' AS Y FROM SYSTEM_RANGE(1,2) R");

        prep = conn.prepareStatement("WITH v1 AS ("
                +"     SELECT R.X, 'X1' AS Y FROM SYSTEM_RANGE(1,3) R"
                +")"
                +"MERGE INTO T1 KEY(ID) SELECT v1.X AS ID, v1.X, v1.Y FROM v1");
        rowCount = prep.executeUpdate();

        assertEquals(3,rowCount);

        rs = stat.executeQuery("SELECT ID, X,Y FROM T1");

        for (int n : new int[] { 1, 2, 3 }) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1)!=0);
            assertEquals(n, rs.getInt(2));
            assertEquals("X1", rs.getString(3));
        }
        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testCreateTable() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        Statement stat;
        PreparedStatement prep;
        ResultSet rs;
        boolean success;

        stat = conn.createStatement();
        prep = conn.prepareStatement("WITH v1 AS ("
                +"     SELECT R.X, 'X1' AS Y FROM SYSTEM_RANGE(1,3) R"
                +")"
                +"CREATE TABLE IF NOT EXISTS T1 AS SELECT v1.X AS ID, v1.X, v1.Y FROM v1");
        success = prep.execute();

        assertEquals(false,success);

        rs = stat.executeQuery("SELECT ID, X,Y FROM T1");

        for (int n : new int[] { 1, 2, 3 }) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1)!=0);
            assertEquals(n, rs.getInt(2));
            assertEquals("X1", rs.getString(3));
        }
        conn.close();
        deleteDb("commonTableExpressionQueries");
    }
    
    private void testNestedSQL() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        PreparedStatement prep;
        ResultSet rs;

        prep = conn.prepareStatement(
            "WITH T1 AS (                        "+
            "        SELECT *                    "+
            "        FROM TABLE (                "+
            "            K VARCHAR = ('a', 'b'), "+
            "            V INTEGER = (1, 2)      "+
            "    )                               "+
            "),                                  "+
            "                                    "+
            "                                    "+
            "T2 AS (                             "+
            "        SELECT *                    "+
            "        FROM TABLE (                "+
            "            K VARCHAR = ('a', 'b'), "+
            "            V INTEGER = (3, 4)      "+
            "    )                               "+
            "),                                  "+
            "                                    "+
            "                                    "+
            "JOIN_CTE AS (                       "+
            "    SELECT T1.*                     "+
            "                                    "+
            "    FROM                            "+
            "        T1                          "+
            "        JOIN T2 ON (                "+
            "            T1.K = T2.K             "+
            "        )                           "+
            ")                                   "+
            "                                    "+
            "SELECT * FROM JOIN_CTE");

        rs = prep.executeQuery();

        for (String keyLetter : new String[] { "a", "b" }) {
            assertTrue(rs.next());
            assertContains("ab",rs.getString(1));
            assertEquals(rs.getString(1),keyLetter);
            assertTrue(rs.getInt(2)!=0);
        }
        conn.close();
        deleteDb("commonTableExpressionQueries");
    }    

    private void testColumnNames() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        PreparedStatement prep;
        ResultSet rs;

        conn.setAutoCommit(false);

        prep = conn.prepareStatement("WITH t1 AS ("
            +"     SELECT 1 AS ONE, R.X AS TWO, 'T1' AS THREE, X FROM SYSTEM_RANGE(1,1) R"
            +")"
            +"SELECT * FROM t1");
        rs = prep.executeQuery();

        for (int n : new int[] { 1 }) {
            assertTrue(rs.next());
            assertEquals(n, rs.getInt(1));
            assertEquals(n, rs.getInt(4));
        }
        assertEquals("ONE",rs.getMetaData().getColumnLabel(1));
        assertEquals("TWO",rs.getMetaData().getColumnLabel(2));
        assertEquals("THREE",rs.getMetaData().getColumnLabel(3));
        assertEquals("X",rs.getMetaData().getColumnLabel(4));

        assertFalse(rs.next());

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }
    
    private void testRecursiveTable() throws Exception {
        String[] expectedRowData =new String[]{"|meat|null","|fruit|3","|veg|2"};
        String[] expectedColumnNames =new String[]{"VAL",
                "SUM(SELECT\n    X\nFROM PUBLIC.\"\" BB\n    /* SELECT\n        SUM(1) AS X,\n        A\n    FROM PUBLIC.B\n        /++ PUBLIC.B.tableScan ++/\n        /++ WHERE A IS ?1\n        ++/\n        /++ scanCount: 4 ++/\n    INNER JOIN PUBLIC.C\n        /++ PUBLIC.C.tableScan ++/\n        ON 1=1\n    WHERE (A IS ?1)\n        AND (B.VAL = C.B)\n    GROUP BY A: A IS A.VAL\n     */\n    /* scanCount: 1 */\nWHERE BB.A IS A.VAL)"};
        
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        PreparedStatement prep;
        ResultSet rs;
        
        String SETUP_SQL = 
             "DROP TABLE IF EXISTS A;                           "
            +"DROP TABLE IF EXISTS B;                           "
            +"DROP TABLE IF EXISTS C;                           "
            +"CREATE TABLE A(VAL VARCHAR(255));                 "
            +"CREATE TABLE B(A VARCHAR(255), VAL VARCHAR(255)); "
            +"CREATE TABLE C(B VARCHAR(255), VAL VARCHAR(255)); "
            +"                                                  "
            +"INSERT INTO A VALUES('fruit');                    "
            +"INSERT INTO B VALUES('fruit','apple');            "
            +"INSERT INTO B VALUES('fruit','banana');           "
            +"INSERT INTO C VALUES('apple', 'golden delicious');"
            +"INSERT INTO C VALUES('apple', 'granny smith');    "
            +"INSERT INTO C VALUES('apple', 'pippin');          "
            +"INSERT INTO A VALUES('veg');                      "
            +"INSERT INTO B VALUES('veg', 'carrot');            "
            +"INSERT INTO C VALUES('carrot', 'nantes');         "
            +"INSERT INTO C VALUES('carrot', 'imperator');      "
            +"INSERT INTO C VALUES(null, 'banapple');           "
            +"INSERT INTO A VALUES('meat');                     "
            ;
        String WITH_QUERY =
            "WITH BB as (SELECT                        \n"
            +"sum(1) as X,                             \n"
            +"a                                        \n"
            +"FROM B                                   \n"
            +"JOIN C ON B.val=C.b                      \n"
            +"GROUP BY a)                              \n"
            +"SELECT                                   \n"
            +"A.val,                                   \n"
            +"sum(SELECT X FROM BB WHERE BB.a IS A.val)\n"//AS SUM_X
            +"FROM A                                   \n"
            +"GROUP BY A.val";

        for(int queryRunTries=1;queryRunTries<4;queryRunTries++){
            Statement stat = conn.createStatement();
            stat.execute(SETUP_SQL);
            stat.close();

            prep = conn.prepareStatement(WITH_QUERY);

            rs = prep.executeQuery();
            for(int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++){
                // previously the column label was null or had \n or \r in the string
                assertTrue(rs.getMetaData().getColumnLabel(columnIndex)!=null);
                assertEquals(expectedColumnNames[columnIndex-1],rs.getMetaData().getColumnLabel(columnIndex));
            }
            
            int rowNdx=0;
            while (rs.next()) {
                StringBuffer buf = new StringBuffer();
                for(int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++){
                    buf.append("|"+rs.getString(columnIndex));
                }
                assertEquals(expectedRowData[rowNdx], buf.toString());
                rowNdx++;
            }
            assertEquals(3,rowNdx);
            rs.close();
            prep.close();
        }

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }     
}