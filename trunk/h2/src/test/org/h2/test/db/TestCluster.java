/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.tools.CreateCluster;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;

/**
 * Test for the cluster feature.
 */
public class TestCluster extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        if (config.memory || config.networked || config.cipher != null) {
            return;
        }
        deleteFiles();

        // create the master database
        Connection conn;
        org.h2.Driver.load();
        String urlNode1 = getURL("node1/test", true);
        String urlNode2 = getURL("node2/test", true);
        String user = getUser(), password = getPassword();
        conn = DriverManager.getConnection(urlNode1, user, password);
        Statement stat;
        stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        int len = getSize(10, 1000);
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.setString(2, "Data" + i);
            prep.executeUpdate();
        }
        check(conn, len, "''");
        conn.close();

        CreateCluster.main("-urlSource", urlNode1, "-urlTarget",
                urlNode2, "-user", user, "-password", password, "-serverList",
                "localhost:9191,localhost:9192");
        Server n1 = org.h2.tools.Server.createTcpServer("-tcpPort", "9191", "-baseDir", baseDir + "/node1").start();
        Server n2 = org.h2.tools.Server.createTcpServer("-tcpPort", "9192", "-baseDir", baseDir + "/node2").start();

        try {
            conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9191/test", user, password);
            fail("should not be able to connect in standalone mode");
        } catch (SQLException e) {
            assertKnownException(e);
        }

        try {
            DriverManager.getConnection("jdbc:h2:tcp://localhost:9192/test", user, password);
            fail("should not be able to connect in standalone mode");
        } catch (SQLException e) {
            assertKnownException(e);
        }

        // test regular cluster connection
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9191,localhost:9192/test", user, password);
        check(conn, len, "'localhost:9191,localhost:9192'");
        conn.close();

        // test if only one server is available at the beginning
        n2.stop();
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9191,localhost:9192/test", user, password);
        check(conn, len, "''");
        conn.close();

        // disable the cluster
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9191/test;CLUSTER=''", user, password);
        conn.close();
        n1.stop();

        // re-create the cluster
        DeleteDbFiles.main("-dir", baseDir + "/node2", "-quiet");
        CreateCluster.main("-urlSource", urlNode1, "-urlTarget",
                urlNode2, "-user", user, "-password", password, "-serverList",
                "localhost:9191,localhost:9192");
        n1 = org.h2.tools.Server.createTcpServer("-tcpPort", "9191", "-baseDir", baseDir + "/node1").start();
        n2 = org.h2.tools.Server.createTcpServer("-tcpPort", "9192", "-baseDir", baseDir + "/node2").start();

        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9191,localhost:9192/test", user, password);
        stat = conn.createStatement();
        stat.execute("CREATE TABLE BOTH(ID INT)");

        n1.stop();

        stat.execute("CREATE TABLE A(ID INT)");
        conn.close();
        n2.stop();

        n1 = org.h2.tools.Server.createTcpServer("-tcpPort", "9191", "-baseDir", baseDir + "/node1").start();
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9191/test;CLUSTER=''", user, password);
        check(conn, len, "''");
        conn.close();
        n1.stop();

        n2 = org.h2.tools.Server.createTcpServer("-tcpPort", "9192", "-baseDir", baseDir + "/node2").start();
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9192/test;CLUSTER=''", user, password);
        check(conn, len, "''");
        conn.createStatement().execute("SELECT * FROM A");
        conn.close();
        n2.stop();
        deleteFiles();
    }

    private void deleteFiles() throws SQLException {
        DeleteDbFiles.main("-dir", baseDir + "/node1", "-quiet");
        DeleteDbFiles.main("-dir", baseDir + "/node2", "-quiet");
    }

    private void check(Connection conn, int len, String expectedCluster) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT * FROM TEST WHERE ID=?");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            ResultSet rs = prep.executeQuery();
            rs.next();
            assertEquals("Data" + i, rs.getString(2));
            assertFalse(rs.next());
        }
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME='CLUSTER'");
        rs.next();
        String cluster = rs.getString(1);
        assertEquals(expectedCluster, cluster);
    }

}
