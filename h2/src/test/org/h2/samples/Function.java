/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.h2.tools.SimpleResultSet;

public class Function {
    
    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS IS_PRIME FOR \"org.h2.samples.Function.isPrime\" ");
        ResultSet rs;
        rs = stat.executeQuery("SELECT IS_PRIME(X), X FROM SYSTEM_RANGE(1, 20) ORDER BY X");
        while(rs.next()) {
            boolean isPrime = rs.getBoolean(1);
            if(isPrime) {
                int x = rs.getInt(2);
                System.out.println(x + " is prime");
            }
        }
        conn.close();
    }

    public static boolean isPrime(int value) {
        return new BigInteger(String.valueOf(value)).isProbablePrime(100);
    }
    
    public static ResultSet query(Connection conn, String sql) throws SQLException {
        return conn.createStatement().executeQuery(sql);
    }
    
    public static ResultSet simpleResultSet() throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("ID", Types.INTEGER, 10, 0);
        rs.addColumn("NAME", Types.VARCHAR, 255, 0);
        rs.addRow(new Object[] { new Integer(0), "Hello" });
        return rs;
    }    

    public static ResultSet getMatrix(Connection conn, Integer id) throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("X", Types.INTEGER, 10, 0);
        rs.addColumn("Y", Types.INTEGER, 10, 0);
        if(id == null) {
            return rs;
        }
        for(int x = 0; x < id.intValue(); x++) {
            for(int y = 0; y < id.intValue(); y++) {
                rs.addRow(new Object[] { new Integer(x), new Integer(y) });
            }
        }
        return rs;
    }

}
