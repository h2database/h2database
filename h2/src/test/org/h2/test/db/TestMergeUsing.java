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
        testMergeUsing();
    }

    private void testMergeUsing() throws Exception {
        deleteDb("mergeUsingQueries");
        Connection conn = getConnection("mergeUsingQueries");
        Statement stat;
        PreparedStatement prep;
        ResultSet rs;
        int rowCount;

        stat = conn.createStatement();
        stat.execute("CREATE TABLE PARENT(ID INT, NAME VARCHAR, PRIMARY KEY(ID) );");

        prep = conn.prepareStatement("MERGE INTO PARENT AS P USING (SELECT 1 AS ID, 'Marcy' AS NAME) AS S ON (P.ID = S.ID AND 1=1 AND S.ID = P.ID) WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME WHERE 2 = 2 WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (S.ID, S.NAME)");
        rowCount = prep.executeUpdate();

        assertEquals(1,rowCount);

        rs = stat.executeQuery("SELECT ID, NAME FROM PARENT");

        for (int n : new int[] { 1 }) {
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
            assertEquals("Marcy", rs.getString(2));
            System.out.println("id="+rs.getInt(1)+",name="+rs.getString(2));
        }
        conn.close();
        deleteDb("mergeUsingQueries");
    }

}
