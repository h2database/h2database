/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import org.h2.test.TestBase;
import java.sql.*;

/**
 *
 */
public class TestFunctionTrunc extends TestBase {

    /**
     *
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new TestFunctionTrunc().test();
    }

    @Override
    public void test() throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:", "sa", "");
        Statement stat = conn.createStatement();

        ResultSet rs;
        rs = stat.executeQuery("SELECT TRUNC(CURRENT_DATE);");
        while (rs.next()) {
            Date d = rs.getDate(1);
            System.out.println("Date is : "+d);
        }

        rs = stat.executeQuery("SELECT TRUNC(CURRENT_DATE-2);");
        while (rs.next()) {
            Date d = rs.getDate(1);
            System.out.println("Date is : "+d);
        }

        rs = stat.executeQuery("SELECT TRUNC('2015-05-29');");
        while (rs.next()) {
            Date d = rs.getDate(1);
            System.out.println("Date is : "+d);
        }

        rs = stat.executeQuery("SELECT TRUNC('2015-05-29 15:00:00');");
        while (rs.next()) {
            Date d = rs.getDate(1);
            System.out.println("Date is : "+d);
        }

        stat.close();
        conn.close();
    }
}
