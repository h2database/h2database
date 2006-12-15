/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestLimitOffset {

    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:test", "sa", "sa");
        Statement stat = conn.createStatement();
        try {
            stat.execute("DROP TABLE file");
        } catch (SQLException e) {
            // 
        }
        stat.execute("create table IF NOT EXISTS file (id IDENTITY primary key, path nvarchar(1000) not null, name nvarchar(100), date timestamp not null, size int , is_dir char(1) not null, unique (path))");
        PreparedStatement prep = conn.prepareStatement(" insert into file (path, is_dir, date)  values(?, 'N', NOW()+? )");
        for(int i=0; i<60000; i++) {
            prep.setInt(1, i);
            prep.setInt(2, i);
            prep.execute();
        }
        ResultSet rs;
        prep = conn.prepareStatement("select path from file where path like ? and is_dir <> 'Y' order by date desc LIMIT 25 OFFSET 0");
        prep.setString(1, "%");
        rs = prep.executeQuery();
        rs.next();
        System.out.println(rs.getString(1));
        prep = conn.prepareStatement("select path from file where path like ? and is_dir <> 'Y' order by date desc LIMIT 25 OFFSET 25");
        prep.setString(1, "%");
        rs = prep.executeQuery();
        rs.next();
        System.out.println(rs.getString(1));
        prep = conn.prepareStatement("select path from file where path like ? and is_dir <> 'Y' order by date desc LIMIT 25 OFFSET 50");
        prep.setString(1, "%");
        rs = prep.executeQuery();
        rs.next();
        System.out.println(rs.getString(1));
        conn.close();
    }
}
