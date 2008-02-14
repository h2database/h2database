/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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

    public void test() throws Exception {
        if (config.memory || config.networked) {
            return;
        }

        DeleteDbFiles.main(new String[] { "-dir", baseDir + "/node1", "-quiet" });
        DeleteDbFiles.main(new String[] { "-dir", baseDir + "/node2", "-quiet" });

        // create the master database
        Connection conn;
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:file:" + baseDir + "/node1/test", "sa", "");
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
        check(conn, len);
        conn.close();

        CreateCluster.main(new String[] { "-urlSource", "jdbc:h2:file:" + baseDir + "/node1/test", "-urlTarget",
                "jdbc:h2:file:" + baseDir + "/node2/test", "-user", "sa", "-serverlist",
                "localhost:9191,localhost:9192" });
        Server n1 = org.h2.tools.Server.createTcpServer(
                new String[] { "-tcpPort", "9191", "-baseDir", baseDir + "/node1" }).start();
        Server n2 = org.h2.tools.Server.createTcpServer(
                new String[] { "-tcpPort", "9192", "-baseDir", baseDir + "/node2" }).start();

        try {
            conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9191/test", "sa", "");
            error("should not be able to connect in standalone mode");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }

        try {
            conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9192/test", "sa", "");
            error("should not be able to connect in standalone mode");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }

        // test regular cluster connection
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9191,localhost:9192/test", "sa", "");
        check(conn, len);
        conn.close();

        // test if only one server is available at the beginning
        n2.stop();
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9191,localhost:9192/test", "sa", "");
        stat = conn.createStatement();
        check(conn, len);
        conn.close();

        // disable the cluster
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9191/test;CLUSTER=''", "sa", "");
        conn.close();
        n1.stop();

        // re-create the cluster
        DeleteDbFiles.main(new String[] { "-dir", baseDir + "/node2", "-quiet" });
        CreateCluster.main(new String[] { "-urlSource", "jdbc:h2:file:" + baseDir + "/node1/test", "-urlTarget",
                "jdbc:h2:file:" + baseDir + "/node2/test", "-user", "sa", "-serverlist",
                "localhost:9191,localhost:9192" });
        n1 = org.h2.tools.Server.createTcpServer(
                new String[] { "-tcpPort", "9191", "-baseDir", baseDir + "/node1" }).start();
        n2 = org.h2.tools.Server.createTcpServer(
                new String[] { "-tcpPort", "9192", "-baseDir", baseDir + "/node2" }).start();

        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9191,localhost:9192/test", "sa", "");
        stat = conn.createStatement();
        stat.execute("CREATE TABLE BOTH(ID INT)");

        n1.stop();

        stat.execute("CREATE TABLE A(ID INT)");
        conn.close();
        n2.stop();

        n1 = org.h2.tools.Server.createTcpServer(new String[] { "-tcpPort", "9191", "-baseDir", baseDir + "/node1" })
                .start();
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9191/test;CLUSTER=''", "sa", "");
        check(conn, len);
        conn.close();
        n1.stop();

        n2 = org.h2.tools.Server.createTcpServer(new String[] { "-tcpPort", "9192", "-baseDir", baseDir + "/node2" })
                .start();
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9192/test;CLUSTER=''", "sa", "");
        check(conn, len);
        conn.createStatement().execute("SELECT * FROM A");
        conn.close();
        n2.stop();
    }

    void check(Connection conn, int len) throws Exception {
        PreparedStatement prep = conn.prepareStatement("SELECT * FROM TEST WHERE ID=?");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            ResultSet rs = prep.executeQuery();
            rs.next();
            check(rs.getString(2), "Data" + i);
            checkFalse(rs.next());
        }
    }

}
