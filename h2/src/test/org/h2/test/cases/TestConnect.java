/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/*
del *.db
java -Xrunhprof:cpu=samples org.h2.test.cases.TestConnect
java org.h2.test.cases.TestConnect
 */
public class TestConnect {
    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:test";
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        PreparedStatement prep = conn.prepareStatement("SELECT CURRENT_TIMESTAMP()");
        ResultSet rs = prep.executeQuery();
        rs.next();
        System.out.println(rs.getString(1));
        Thread.sleep(50);
        rs = prep.executeQuery();
        rs.next();
        System.out.println(rs.getString(1));
        conn.close();
    }
    
}
