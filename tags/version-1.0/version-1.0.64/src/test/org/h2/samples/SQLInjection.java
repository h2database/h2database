/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * SQL Injection is a common security vulnerability for applications that use database.
 * It is one of the most common security vulnerabilities for web applications today.
 * This sample application shows how SQL injection works, and how to protect
 * the application from it.
 */
public class SQLInjection {

    private Connection conn;
    private Statement stat;

    public static void main(String[] args) throws Exception {
        new SQLInjection().run("org.h2.Driver",
                "jdbc:h2:test", "sa", "sa");
//        new SQLInjection().run("org.postgresql.Driver",
//                "jdbc:postgresql:jpox2", "sa", "sa");
//        new SQLInjection().run("com.mysql.jdbc.Driver",
//                "jdbc:mysql://localhost/test", "sa", "sa");
//        new SQLInjection().run("org.hsqldb.jdbcDriver",
//                "jdbc:hsqldb:test", "sa", "");
//        new SQLInjection().run(
//                "org.apache.derby.jdbc.EmbeddedDriver",
//                "jdbc:derby:test3;create=true", "sa", "sa");
    }

    void run(String driver, String url, String user, String password) throws Exception {
        Class.forName(driver);
        conn = DriverManager.getConnection(url, user, password);
        stat = conn.createStatement();
        try {
            stat.execute("DROP TABLE USERS");
        } catch (SQLException e) {
            // ignore
        }
        stat.execute("CREATE TABLE USERS(ID INT PRIMARY KEY, " +
                "NAME VARCHAR(255), PASSWORD VARCHAR(255))");
        stat.execute("INSERT INTO USERS VALUES(1, 'admin', 'super')");
        stat.execute("INSERT INTO USERS VALUES(2, 'guest', '123456')");
        stat.execute("INSERT INTO USERS VALUES(3, 'test', 'abc')");

        loginByNameInsecure();

        if (url.startsWith("jdbc:h2:")) {
            loginStoredProcedureInsecure();
            limitRowAccess();
        }

        loginByNameSecure();

        if (url.startsWith("jdbc:h2:")) {
            stat.execute("SET ALLOW_LITERALS NONE");
            stat.execute("SET ALLOW_LITERALS NUMBERS");
            stat.execute("SET ALLOW_LITERALS ALL");
        }

        loginByIdInsecure();
        loginByIdSecure();

        try {
            stat.execute("DROP TABLE ITEMS");
        } catch (SQLException e) {
            // ignore
        }

        stat.execute("CREATE TABLE ITEMS(ID INT PRIMARY KEY, " +
                "NAME VARCHAR(255), ACTIVE INT)");
        stat.execute("INSERT INTO ITEMS VALUES(0, 'XBox', 0)");
        stat.execute("INSERT INTO ITEMS VALUES(1, 'XBox 360', 1)");
        stat.execute("INSERT INTO ITEMS VALUES(2, 'PlayStation 1', 0)");
        stat.execute("INSERT INTO ITEMS VALUES(3, 'PlayStation 2', 1)");
        stat.execute("INSERT INTO ITEMS VALUES(4, 'PlayStation 3', 1)");

        listActiveItems();

        if (url.startsWith("jdbc:h2:")) {
            stat.execute("DROP CONSTANT IF EXISTS TYPE_INACTIVE");
            stat.execute("DROP CONSTANT IF EXISTS TYPE_ACTIVE");
            stat.execute("CREATE CONSTANT TYPE_INACTIVE VALUE 0");
            stat.execute("CREATE CONSTANT TYPE_ACTIVE VALUE 1");
            listActiveItemsUsingConstants();
        }

        listItemsSortedInsecure();
        listItemsSortedSecure();

        if (url.startsWith("jdbc:h2:")) {
            listItemsSortedSecureParam();
            storePasswordHashWithSalt();
        }

        conn.close();
    }

    void loginByNameInsecure() throws Exception {
        System.out.println("Insecure Systems Inc. - login");
        String name = input("Name?");
        String password = input("Password?");
        ResultSet rs = stat.executeQuery("SELECT * FROM USERS WHERE " +
                "NAME='" + name + "' AND PASSWORD='" + password + "'");
        if (rs.next()) {
            System.out.println("Welcome!");
        } else {
            System.out.println("Access denied!");
        }
    }

    public static ResultSet getUser(Connection conn, String userName, String password) throws Exception {
        PreparedStatement prep = conn.prepareStatement(
                "SELECT * FROM USERS WHERE NAME=? AND PASSWORD=?");
        prep.setString(1, userName);
        prep.setString(2, password);
        return prep.executeQuery();
    }

    public static String changePassword(Connection conn, String userName, String password) throws Exception {
        PreparedStatement prep = conn.prepareStatement(
                "UPDATE USERS SET PASSWORD=? WHERE NAME=?");
        prep.setString(1, password);
        prep.setString(2, userName);
        prep.executeUpdate();
        return password;
    }

    void loginStoredProcedureInsecure() throws Exception {
        System.out.println("Insecure Systems Inc. - login using a stored procedure");
        stat.execute("CREATE ALIAS IF NOT EXISTS " +
                "GET_USER FOR \"org.h2.samples.SQLInjection.getUser\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS " +
                "CHANGE_PASSWORD FOR \"org.h2.samples.SQLInjection.changePassword\"");
        String name = input("Name?");
        String password = input("Password?");
        ResultSet rs = stat.executeQuery(
                "CALL GET_USER('" + name + "', '" + password + "')");
        if (rs.next()) {
            System.out.println("Welcome!");
        } else {
            System.out.println("Access denied!");
        }
    }

    void loginByNameSecure() throws Exception {
        System.out.println("Secure Systems Inc. - login using placeholders");
        String name = input("Name?");
        String password = input("Password?");
        PreparedStatement prep = conn.prepareStatement(
                "SELECT * FROM USERS WHERE " +
                "NAME=? AND PASSWORD=?");
        prep.setString(1, name);
        prep.setString(2, password);
        ResultSet rs = prep.executeQuery();
        if (rs.next()) {
            System.out.println("Welcome!");
        } else {
            System.out.println("Access denied!");
        }
    }

    void limitRowAccess() throws Exception {
        System.out.println("Secure Systems Inc. - limit row access");
        stat.execute("DROP TABLE IF EXISTS SESSION_USER");
        stat.execute("CREATE TABLE SESSION_USER(ID INT, USER INT)");
        stat.execute("DROP VIEW IF EXISTS MY_USER");
        stat.execute("CREATE VIEW MY_USER AS " +
                "SELECT U.* FROM SESSION_USER S, USERS U " +
                "WHERE S.ID=SESSION_ID() AND S.USER=U.ID");
        stat.execute("INSERT INTO SESSION_USER VALUES(SESSION_ID(), 1)");
        ResultSet rs = stat.executeQuery("SELECT ID, NAME FROM MY_USER");
        while (rs.next()) {
            System.out.println(rs.getString(1) + ": " + rs.getString(2));
        }
    }

    void loginByIdInsecure() throws Exception {
        System.out.println("Half Secure Systems Inc. - login by id");
        String id = input("User ID?");
        String password = input("Password?");
        try {
            PreparedStatement prep = conn.prepareStatement(
                    "SELECT * FROM USERS WHERE " +
                    "ID=" + id + " AND PASSWORD=?");
            prep.setString(1, password);
            ResultSet rs = prep.executeQuery();
            if (rs.next()) {
                System.out.println("Welcome!");
            } else {
                System.out.println("Access denied!");
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    void loginByIdSecure() throws Exception {
        System.out.println("Secure Systems Inc. - login by id");
        String id = input("User ID?");
        String password = input("Password?");
        try {
            PreparedStatement prep = conn.prepareStatement(
                    "SELECT * FROM USERS WHERE " +
                    "ID=? AND PASSWORD=?");
            prep.setInt(1, Integer.parseInt(id));
            prep.setString(2, password);
            ResultSet rs = prep.executeQuery();
            if (rs.next()) {
                System.out.println("Welcome!");
            } else {
                System.out.println("Access denied!");
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    void listActiveItems() throws Exception {
        System.out.println("Half Secure Systems Inc. - list active items");
        ResultSet rs = stat.executeQuery(
                "SELECT NAME FROM ITEMS WHERE ACTIVE=1");
        while (rs.next()) {
            System.out.println("Name: " + rs.getString(1));
        }
    }

    void listActiveItemsUsingConstants() throws Exception {
        System.out.println("Secure Systems Inc. - list active items");
        ResultSet rs = stat.executeQuery(
                "SELECT NAME FROM ITEMS WHERE ACTIVE=TYPE_ACTIVE");
        while (rs.next()) {
            System.out.println("Name: " + rs.getString(1));
        }
    }

    void listItemsSortedInsecure() throws Exception {
        System.out.println("Insecure Systems Inc. - list items");
        String order = input("order (id, name)?");
        try {
            ResultSet rs = stat.executeQuery(
                    "SELECT ID, NAME FROM ITEMS ORDER BY " + order);
            while (rs.next()) {
                System.out.println(rs.getString(1) + ": " + rs.getString(2));
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    void listItemsSortedSecure() throws Exception {
        System.out.println("Secure Systems Inc. - list items");
        String order = input("order (id, name)?");
        if (!order.matches("[a-zA-Z0-9_]*")) {
            order = "id";
        }
        try {
            ResultSet rs = stat.executeQuery(
                    "SELECT ID, NAME FROM ITEMS ORDER BY " + order);
            while (rs.next()) {
                System.out.println(rs.getString(1) + ": " + rs.getString(2));
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    void listItemsSortedSecureParam() throws Exception {
        System.out.println("Secure Systems Inc. - list items");
        String order = input("order (1, 2, -1, -2)?");
        PreparedStatement prep = conn.prepareStatement(
                "SELECT ID, NAME FROM ITEMS ORDER BY ?");
        try {
            prep.setInt(1, Integer.parseInt(order));
            ResultSet rs = prep.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1) + ": " + rs.getString(2));
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    void storePasswordHashWithSalt() throws Exception {
        System.out.println("Very Secure Systems Inc. - login");
        stat.execute("DROP TABLE IF EXISTS USERS2");
        stat.execute("CREATE TABLE USERS2(ID INT PRIMARY KEY, " +
            "NAME VARCHAR, SALT BINARY, HASH BINARY)");
        stat.execute("INSERT INTO USERS2 VALUES" +
                "(1, 'admin', SECURE_RAND(16), NULL)");
        stat.execute("DROP CONSTANT IF EXISTS HASH_ITERATIONS");
        stat.execute("DROP CONSTANT IF EXISTS HASH_ALGORITHM");
        stat.execute("CREATE CONSTANT HASH_ITERATIONS VALUE 100");
        stat.execute("CREATE CONSTANT HASH_ALGORITHM VALUE 'SHA256'");
        stat.execute("UPDATE USERS2 SET " +
                "HASH=HASH(HASH_ALGORITHM, STRINGTOUTF8('abc' || SALT), HASH_ITERATIONS) " +
                "WHERE ID=1");
        String user = input("user?");
        String password = input("password?");
        stat.execute("SET ALLOW_LITERALS NONE");
        PreparedStatement prep = conn.prepareStatement(
                "SELECT * FROM USERS2 WHERE NAME=? AND " +
                "HASH=HASH(HASH_ALGORITHM, STRINGTOUTF8(? || SALT), HASH_ITERATIONS)");
        prep.setString(1, user);
        prep.setString(2, password);
        ResultSet rs = prep.executeQuery();
        while (rs.next()) {
            System.out.println("name: " + rs.getString("NAME"));
            System.out.println("salt: " + rs.getString("SALT"));
            System.out.println("hash: " + rs.getString("HASH"));
        }
        stat.execute("SET ALLOW_LITERALS ALL");
    }

    String input(String prompt) throws Exception {
        System.out.print(prompt);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        return reader.readLine();
    }

}
