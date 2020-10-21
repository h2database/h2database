package org.h2.my.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * TcpTest
 *
 * @author longhuashen
 * @since 2020-10-21
 */
public class TcpTest {

    private static final String JDBC_URL = "jdbc:h2:tcp://localhost/~/test";
    //连接数据库时使用的用户名
    private static final String USER = "sa";
    //连接数据库时使用的密码
    private static final String PASSWORD = "123";
    private static final String DRIVER_CLASS = "org.h2.Driver";

    public static void main(String[] args) throws Exception {
        Class.forName(DRIVER_CLASS);
        Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        Statement statement = conn.createStatement();

        ResultSet resultSet = statement.executeQuery("select * from USER_INF");

        while (resultSet.next()) {
            System.out.println(resultSet.getInt("id") + ", "
                    + resultSet.getString("name") + ", "
                    + resultSet.getString("sex"));
        }

        statement.close();
        conn.close();
    }
}
