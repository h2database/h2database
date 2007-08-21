/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.*;
import java.util.Random;
import org.h2.tools.DeleteDbFiles;

public class TestMVCC {
    
    Connection c1, c2;
    Statement s1, s2;
    
    public static void main(String[] args) throws Exception {
        TestMVCC app = new TestMVCC();
        app.test();
    }
    
    void test() throws Exception {
        // TODO Prio 1: document: exclusive table lock still used when altering tables, adding indexes, select ... for update; table level locks are checked
        // TODO Prio 1: make unit test work
        // TODO Prio 1: free up disk space (for deleted rows and old versions of updated rows) on commit
        // TODO Prio 1: ScanIndex: never remove uncommitted data from cache (lost sessionId)
        // TODO Prio 1: Test with Hibernate
        // TODO Prio 2: if MVCC is used, rows of transactions need to fit in memory
        // TODO Prio 2: write the log only when committed; remove restriction at Record.canRemove
        // TODO Prio 2: getRowCount: different row count for different indexes (MultiVersionIndex)
        // TODO Prio 2: getRowCount: different row count for different sessions: TableLink (use different connections?)
        // TODO Prio 2: getFirst / getLast in MultiVersionIndex
        // TODO Prio 2: snapshot isolation (currently read-committed, not repeatable read)

        // TODO test: one thread appends, the other selects new data (select * from test where id > ?) and deletes
        
        // System.setProperty("h2.mvcc", "true");
        DeleteDbFiles.execute(null, "test", true);
        Class.forName("org.h2.Driver");
        c1 = DriverManager.getConnection("jdbc:h2:test;MVCC=TRUE", "sa", "sa");
        s1 = c1.createStatement();
        c2 = DriverManager.getConnection("jdbc:h2:test;MVCC=TRUE", "sa", "sa");
        s2 = c2.createStatement();
        c1.setAutoCommit(false);
        c2.setAutoCommit(false);

        s1.execute("CREATE TABLE TEST(ID INT)");
        s1.execute("INSERT INTO TEST VALUES(1)");
        c1.commit();
        s1.execute("DELETE FROM TEST");
        test(s1, "SELECT COUNT(*) FROM TEST", "0");
        c1.commit();
        test(s1, "SELECT COUNT(*) FROM TEST", "0");
        s1.execute("INSERT INTO TEST VALUES(1)");
        s1.execute("DELETE FROM TEST");
        c1.commit();
        test(s1, "SELECT COUNT(*) FROM TEST", "0");
        s1.execute("DROP TABLE TEST");
        
        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World')");
        test(s2, "SELECT COUNT(*) FROM TEST", "0");
        c1.commit();
        test(s2, "SELECT COUNT(*) FROM TEST", "2");
        s1.execute("INSERT INTO TEST VALUES(3, '!')");
        c1.rollback();
        test(s2, "SELECT COUNT(*) FROM TEST", "2");
        s1.execute("DROP TABLE TEST");
        c1.commit();

        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        s1.execute("DELETE FROM TEST");
        test(s2, "SELECT COUNT(*) FROM TEST", "0");
        c1.commit();
        test(s2, "SELECT COUNT(*) FROM TEST", "0");
        s1.execute("DROP TABLE TEST");
        c1.commit();

        s1.execute("CREATE TABLE TEST(ID INT IDENTITY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        test(s2, "SELECT COUNT(*) FROM TEST", "0");
        test(s1, "SELECT COUNT(*) FROM TEST", "1");
        s1.execute("DROP TABLE TEST");
        c1.commit();

        s1.execute("CREATE TABLE TEST(ID INT IDENTITY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        s1.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        s1.execute("DROP TABLE TEST");
        c1.commit();
        
        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        c1.commit();
        s1.execute("DELETE FROM TEST WHERE ID=1");
        c1.rollback();
        s1.execute("DROP TABLE TEST");
        c1.commit();
        
        Random random = new Random(1);
        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        for(int i=0; i<100; i++) {
            switch(random.nextInt(3)) {
            case 0:
                s1.execute("INSERT INTO TEST VALUES("+ i + ", 'Hello')");
                break;
            case 1:
                s1.execute("UPDATE TEST SET NAME=" + i + " WHERE ID=" + random.nextInt(i));
                break;
            case 2:
                s1.execute("DELETE FROM TEST WHERE ID=" + random.nextInt(i));
                break;
            }
            s2.execute("SELECT * FROM TEST ORDER BY ID");
        }
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();
        
        s1.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        test(s2, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'", "0");
        test(s1, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'", "1");
        c1.commit();
        test(s2, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'", "1");
        test(s2, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'", "1");
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();
        
        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        test(s2, "SELECT COUNT(*) FROM TEST WHERE ID<100", "0");
        test(s1, "SELECT COUNT(*) FROM TEST WHERE ID<100", "1");
        c1.commit();
        test(s2, "SELECT COUNT(*) FROM TEST WHERE ID<100", "1");
        test(s2, "SELECT COUNT(*) FROM TEST WHERE ID<100", "1");
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();
        
        s1.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR, PRIMARY KEY(ID, NAME))");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        c1.commit();
        test(s2, "SELECT NAME FROM TEST WHERE ID=1", "Hello");
        s1.execute("UPDATE TEST SET NAME = 'Hallo' WHERE ID=1");
        test(s2, "SELECT NAME FROM TEST WHERE ID=1", "Hello");
        test(s1, "SELECT NAME FROM TEST WHERE ID=1", "Hallo");
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();
        
        c1.close();
        c2.close();
        
        
        
    }

    private void test(Statement stat, String sql, String expected) throws Exception {
        ResultSet rs = stat.executeQuery(sql);
        if(rs.next()) {
            String s = rs.getString(1);
            if(expected == null) {
                throw new Error("expected: no rows, got: " + s);
            } else if(!expected.equals(s)) {
                throw new Error("expected: " + expected + ", got: " + s);
            }
        } else {
            if(expected != null) {
                throw new Error("expected: " + expected + ", got: no rows");
            }
        }
        // TODO Auto-generated method stub
        
    }
}
