/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.*;

import org.h2.tools.DeleteDbFiles;

public class TestCollation {

    private static final int TOTAL = 100000;
    private static final int ROOT = 1000;

    private Connection conn;

    public static void main(String[] args) throws Exception {
        new TestCollation().test();
        new TestCollation().test();
    }

    public void test() throws Exception {
        DeleteDbFiles.execute(null, "test", true);
        Class.forName("org.h2.Driver");
        Class.forName("org.postgresql.Driver");
        Class.forName("com.mysql.jdbc.Driver");
        Class.forName("org.hsqldb.jdbcDriver");
//        
        conn = DriverManager.getConnection("jdbc:h2:testColl", "sa", "sa");
//        conn = DriverManager.getConnection("jdbc:hsqldb:testColl", "sa", "");
//        conn = DriverManager.getConnection("jdbc:postgresql:jpox2", "sa", "sa");
//        conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "sa", "sa");
        
        try {
            conn.createStatement().execute("DROP TABLE TEST");
        } catch(Exception e) {
            // ignore
        }
        conn.createStatement().execute("CREATE TABLE TEST(ID INT)");
        conn.createStatement().execute("INSERT INTO TEST VALUES(1)");
        PreparedStatement prep = conn.prepareStatement("Select Case When ? >= ? Then 'Yes' Else 'No' End FROM TEST");
        prep.setObject(1, new Integer(26));
        prep.setObject(2, new Integer(26));

        ResultSet rs = prep.executeQuery();
        while(rs.next()) {
        System.out.println("a:"+rs.getObject(1));
        }

//        /=====return 'No'

//        but *****
        prep = conn.prepareStatement("Select Case When ? >= Cast(? As Int) Then 'Yes' Else 'No' End FROM TEST");
        prep.setObject(1, new Integer(26));
        prep.setObject(2, new Integer(26));

        rs = prep.executeQuery();
        while (rs.next()) {
            System.out.println("b:"+rs.getObject(1));
        }

//        /=====return 'Yes'


//        This problem also happen if there are date parameters;        

            Statement stat = conn.createStatement();
            stat.executeUpdate(
                "DROP TABLE IF EXISTS test");
            stat.executeUpdate(
                "SET COLLATION OFF");
            stat.executeUpdate(
                "CREATE TABLE test (id INT IDENTITY, code VARCHAR(20) NOT NULL, parentId INT)");
            stat.executeUpdate(
                "CREATE INDEX test_code ON test(code)");
            PreparedStatement prep2 = conn.prepareStatement(
                "INSERT INTO test (code,parentId) VALUES (?,NULL)");
            PreparedStatement prep3 = conn.prepareStatement(
                "INSERT INTO test (code,parentId) SELECT ?,id FROM test WHERE code=?");

            long time = System.currentTimeMillis();
            for (int i = 1; i < TOTAL; ++i) {
                if (i < ROOT) {
                    prep2.setString(1, Integer.toBinaryString(i));
                    prep2.executeUpdate();
                } else {
                    prep3.setString(1, Integer.toBinaryString(i));
                    prep3.setString(2, Integer.toBinaryString(i % 100 + 1));
                    prep3.executeUpdate();
                }
            }
            System.out.println("INSERT w/o Collation: " + (System.currentTimeMillis()-time));

            testWithCollation();

    }

    public void testWithCollation() throws Exception {

            Statement stat = conn.createStatement();
            stat.executeUpdate(
                "DROP TABLE IF EXISTS test");
            stat.executeUpdate(
                "SET COLLATION ENGLISH STRENGTH PRIMARY");
            stat.executeUpdate(
                "CREATE TABLE test (id INT IDENTITY, code VARCHAR(20) NOT NULL, parentId INT)");
            stat.executeUpdate(
                "CREATE INDEX test_code ON test(code)");
            PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO test (code,parentId) VALUES (?,NULL)");
            PreparedStatement prep2 = conn.prepareStatement(
                "INSERT INTO test (code,parentId) SELECT ?,id FROM test WHERE code=?");

            long time = System.currentTimeMillis();
            for (int i = 1; i < TOTAL; ++i) {
                if (i < ROOT) {
                    prep.setString(1, Integer.toBinaryString(i));
                    prep.executeUpdate();
                } else {
                    prep2.setString(1, Integer.toBinaryString(i));
                    prep2.setString(2, Integer.toBinaryString(i % 100 + 1));
                    prep2.executeUpdate();
                }
            }
            System.out.println("INSERT with Collation: " + (System.currentTimeMillis()-time));
    }

}
