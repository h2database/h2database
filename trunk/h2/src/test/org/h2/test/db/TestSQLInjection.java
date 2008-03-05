/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Tests the ALLOW_LITERALS feature (protection against SQL injection).
 */
public class TestSQLInjection extends TestBase {

    Connection conn;
    Statement stat;

    public void test() throws Exception {
        deleteDb("sqlInjection");
        reconnect("sqlInjection");
        stat.execute("DROP TABLE IF EXISTS USERS");
        stat.execute("CREATE TABLE USERS(NAME VARCHAR PRIMARY KEY, PASSWORD VARCHAR, TYPE VARCHAR)");
        stat.execute("CREATE SCHEMA CONST");
        stat.execute("CREATE CONSTANT CONST.ACTIVE VALUE 'Active'");
        stat.execute("INSERT INTO USERS VALUES('James', '123456', CONST.ACTIVE)");
        check(checkPasswordInsecure("123456"));
        checkFalse(checkPasswordInsecure("abcdef"));
        check(checkPasswordInsecure("' OR ''='"));
        check(checkPasswordSecure("123456"));
        checkFalse(checkPasswordSecure("abcdef"));
        checkFalse(checkPasswordSecure("' OR ''='"));
        stat.execute("SET ALLOW_LITERALS NONE");

        try {
            check(checkPasswordInsecure("123456"));
            error();
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        check(checkPasswordSecure("123456"));
        checkFalse(checkPasswordSecure("' OR ''='"));
        conn.close();

        if (config.memory) {
            return;
        }

        reconnect("sqlInjection");

        try {
            check(checkPasswordInsecure("123456"));
            error("Should fail now");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        check(checkPasswordSecure("123456"));
        checkFalse(checkPasswordSecure("' OR ''='"));
        conn.close();
    }

    boolean checkPasswordInsecure(String pwd) throws SQLException {
        String sql = "SELECT * FROM USERS WHERE PASSWORD='" + pwd + "'";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        return (rs.next());
    }

    boolean checkPasswordSecure(String pwd) throws Exception {
        String sql = "SELECT * FROM USERS WHERE PASSWORD=?";
        PreparedStatement prep = conn.prepareStatement(sql);
        prep.setString(1, pwd);
        ResultSet rs = prep.executeQuery();
        return (rs.next());
    }

    private void reconnect(String name) throws Exception {
        if (!config.memory) {
            if (conn != null) {
                conn.close();
                conn = null;
            }
        }
        if (conn == null) {
            conn = getConnection(name);
            stat = conn.createStatement();
        }
    }
}
