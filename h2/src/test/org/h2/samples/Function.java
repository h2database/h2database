/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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

/**
 * This sample application shows how to define and use
 * custom (user defined) functions in this database.
 */
public class Function {

    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS IS_PRIME FOR \"org.h2.samples.Function.isPrime\" ");
        ResultSet rs;
        rs = stat.executeQuery("SELECT IS_PRIME(X), X FROM SYSTEM_RANGE(1, 20) ORDER BY X");
        while (rs.next()) {
            boolean isPrime = rs.getBoolean(1);
            if (isPrime) {
                int x = rs.getInt(2);
                System.out.println(x + " is prime");
            }
        }
        conn.close();
    }

    /**
     * Check if a value is a prime number.
     *
     * @param value the value
     * @return true if it is a prime number
     */
    public static boolean isPrime(int value) {
        return new BigInteger(String.valueOf(value)).isProbablePrime(100);
    }

    /**
     * Execute a query.
     *
     * @param conn the connection
     * @param sql the SQL statement
     * @return the result set
     */
    public static ResultSet query(Connection conn, String sql) throws SQLException {
        return conn.createStatement().executeQuery(sql);
    }

    /**
     * Creates a simple result set with one row.
     *
     * @return the result set
     */
    public static ResultSet simpleResultSet() throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("ID", Types.INTEGER, 10, 0);
        rs.addColumn("NAME", Types.VARCHAR, 255, 0);
        rs.addRow(new Object[] { new Integer(0), "Hello" });
        return rs;
    }

}
