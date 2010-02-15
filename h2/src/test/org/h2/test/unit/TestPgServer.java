/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.h2.test.TestBase;
import org.h2.tools.Server;

/**
 * Tests the PostgreSQL server protocol compliant implementation.
 */
public class TestPgServer extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        deleteDb("test");
        Server server = Server.createPgServer("-baseDir", baseDir, "-pgPort", "5535");
        server.start();
        try {
            Class.forName("org.postgresql.Driver");
            testPgClient();
        } catch (ClassNotFoundException e) {
            println("PostgreSQL JDBC driver not found - PgServer not tested");
        } finally {
            server.stop();
        }
        deleteDb("test");
    }

    private void testPgClient() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5535/test", "sa", "sa");
        Statement stat = conn.createStatement();
        try {
            stat.execute("select ***");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        conn.close();
        conn = DriverManager.getConnection("jdbc:postgresql://localhost:5535/test", "sa", "sa");
        stat = conn.createStatement();
        ResultSet rs;

        stat.execute("prepare test(int, int) as select ?1*?2");
        rs = stat.executeQuery("execute test(3, 2)");
        rs.next();
        assertEquals(6, rs.getInt(1));
        stat.execute("deallocate test");

        stat.execute("create table test(id int primary key, name varchar)");
        PreparedStatement prep = conn.prepareStatement("insert into test values(?, ?)");
        ParameterMetaData meta = prep.getParameterMetaData();
        assertEquals(2, meta.getParameterCount());
        prep.setInt(1, 1);
        prep.setString(2, "Hello");
        prep.execute();
        rs = stat.executeQuery("select * from test");
        rs.next();

        ResultSetMetaData rsMeta = rs.getMetaData();
        assertEquals(Types.INTEGER, rsMeta.getColumnType(1));
        assertEquals(Types.VARCHAR, rsMeta.getColumnType(2));

        prep.close();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        prep = conn.prepareStatement("select * from test where id = ? and name = ?");
        prep.setInt(1, 1);
        prep.setString(2, "Hello");
        rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        rs.close();
        DatabaseMetaData dbMeta = conn.getMetaData();
        rs = dbMeta.getTables(null, null, "TEST", null);
        rs.next();
        assertEquals("TEST", rs.getString("TABLE_NAME"));
        assertFalse(rs.next());
        rs = dbMeta.getColumns(null, null, "TEST", null);
        rs.next();
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        rs.next();
        assertEquals("NAME", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        rs = dbMeta.getIndexInfo(null, null, "TEST", false, false);
        rs.next();
        assertEquals("TEST", rs.getString("TABLE_NAME"));
        rs.next();
        assertEquals("TEST", rs.getString("TABLE_NAME"));
        assertFalse(rs.next());
        rs = stat.executeQuery("select version(), pg_postmaster_start_time(), current_schema()");
        rs.next();
        String s = rs.getString(1);
        assertTrue(s.indexOf("H2") >= 0);
        assertTrue(s.indexOf("PostgreSQL") >= 0);
        s = rs.getString(2);
        s = rs.getString(3);
        assertEquals(s, "PUBLIC");
        assertFalse(rs.next());

        conn.setAutoCommit(false);
        stat.execute("delete from test");
        conn.rollback();
        stat.execute("update test set name = 'Hallo'");
        conn.commit();
        rs = stat.executeQuery("select * from test order by id");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hallo", rs.getString(2));
        assertFalse(rs.next());

        rs = stat.executeQuery("select id, name, pg_get_userbyid(id) from information_schema.users order by id");
        rs.next();
        assertEquals(rs.getString(2), rs.getString(3));
        assertFalse(rs.next());


        conn.close();
    }

}
