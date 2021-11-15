package org.h2.my.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * JDBCTest
 *
 * @author longhuashen
 * @since 2021-11-16
 */
public class JDBCTest {

    static Properties prop = new Properties();
    static String url = "jdbc:h2:tcp://localhost:9092/mydb";

    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");

        Connection conn = DriverManager.getConnection(url, "sa", "");
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS my_table");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS my_table(name varchar(20), age int)");
        stmt.executeUpdate("INSERT INTO my_table(name, age) VALUES('zhh', 18)");

        stmt.executeUpdate("UPDATE my_table SET age = 20 WHERE name = 'zhh'");

        ResultSet rs = stmt.executeQuery("SELECT name FROM my_table");
        rs.next();
        System.out.println(rs.getString(1));

        stmt.executeUpdate("DELETE FROM my_table WHERE name = 'zhh'");

        stmt.close();
        conn.close();
    }
}
