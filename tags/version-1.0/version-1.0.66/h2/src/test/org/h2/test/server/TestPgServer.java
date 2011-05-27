/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.tools.Server;

/**
 * Tests the PostgreSQL server protocol compliant implementation.
 */
public class TestPgServer extends TestBase {

    public void test() throws Exception {
        deleteDb("test");
        Server server = Server.createPgServer(new String[]{"-baseDir", baseDir, "-ifExists", "false", "-pgAllowOthers", "false"});
        server.start();
        try {
            Class.forName("org.postgresql.Driver");
            testPgClient();
        } catch (ClassNotFoundException e) {
            println("PostgreSQL JDBC driver not found - PgServer not tested");
        } finally {
            server.stop();
        }
    }

    private void testPgClient() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5435/test", "sa", "sa");
        Statement stat = conn.createStatement();
        try {
            stat.execute("select ***");
            error("expected failure");
        } catch (SQLException e) {
            // expected
        }
        conn.close();
        conn = DriverManager.getConnection("jdbc:postgresql://localhost:5435/test", "sa", "sa");
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        PreparedStatement prep = conn.prepareStatement("insert into test values(?, ?)");
        ParameterMetaData meta = prep.getParameterMetaData();
        check(2, meta.getParameterCount());
        prep.setInt(1, 1);
        prep.setString(2, "Hello");
        prep.execute();
        ResultSet rs = stat.executeQuery("select * from test");
        rs.next();
        prep.close();
        check(rs.getInt(1), 1);
        check(rs.getString(2), "Hello");
        checkFalse(rs.next());
        prep = conn.prepareStatement("select * from test where id = ? and name = ?");
        prep.setInt(1, 1);
        prep.setString(2, "Hello");
        rs = prep.executeQuery();
        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getString(2), "Hello");
        checkFalse(rs.next());
        rs.close();
        DatabaseMetaData dbMeta = conn.getMetaData();
        rs = dbMeta.getTables(null, null, "TEST", null);
        rs.next();
        check(rs.getString("TABLE_NAME"), "TEST");
        checkFalse(rs.next());
        rs = dbMeta.getColumns(null, null, "TEST", null);
        rs.next();
        check(rs.getString("COLUMN_NAME"), "ID");
        rs.next();
        check(rs.getString("COLUMN_NAME"), "NAME");
        checkFalse(rs.next());
        rs = dbMeta.getIndexInfo(null, null, "TEST", false, false);
        rs.next();
        check(rs.getString("TABLE_NAME"), "TEST");
        rs.next();
        check(rs.getString("TABLE_NAME"), "TEST");
        checkFalse(rs.next());
        rs = stat.executeQuery("select version(), pg_postmaster_start_time(), current_schema()");
        rs.next();
        String s = rs.getString(1);
        check(s.indexOf("H2") >= 0);
        check(s.indexOf("PostgreSQL") >= 0);
        s = rs.getString(2);
        s = rs.getString(3);
        check(s, "PUBLIC");
        checkFalse(rs.next());

        conn.setAutoCommit(false);
        stat.execute("delete from test");
        conn.rollback();
        stat.execute("update test set name = 'Hallo'");
        conn.commit();
        rs = stat.executeQuery("select * from test order by id");
        rs.next();
        check(1, rs.getInt(1));
        check("Hallo", rs.getString(2));
        checkFalse(rs.next());

        rs = stat.executeQuery("select id, name, pg_get_userbyid(id) from information_schema.users order by id");
        rs.next();
        check(rs.getString(2), rs.getString(3));
        checkFalse(rs.next());


        conn.close();
    }

}
