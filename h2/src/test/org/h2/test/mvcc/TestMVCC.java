package org.h2.test.mvcc;

import java.sql.*;
import org.h2.tools.DeleteDbFiles;

public class TestMVCC {
    
    Connection c1, c2;
    Statement s1, s2;
    
    public static void main(String[] args) throws Exception {
        TestMVCC app = new TestMVCC();
        app.test();
    }
    
    void test() throws Exception {
        // TODO Prio 1: don't store records before they are committed (otherwise re-reading from a different session may return the wrong value)
        // TODO Prio 1: getRowCount: different row count for different sessions: TableData
        // TODO Prio 2: getRowCount: different row count for different sessions: TableLink (use different connections?)
        
        System.setProperty("h2.mvcc", "true");
        DeleteDbFiles.execute(null, "test", true);
        Class.forName("org.h2.Driver");
        c1 = DriverManager.getConnection("jdbc:h2:test");
        s1 = c1.createStatement();
        c2 = DriverManager.getConnection("jdbc:h2:test");
        s2 = c2.createStatement();
        c1.setAutoCommit(false);
        c2.setAutoCommit(false);
        s1.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        // s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        test(s2, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'", "0");
        c1.commit();
        // TODO support snapshot isolation
        // test(s2, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'", "0");
        test(s2, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'", "1");
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
