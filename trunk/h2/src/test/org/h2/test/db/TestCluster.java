/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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

public class TestCluster extends TestBase {

    public void test() throws Exception {
        if(config.memory || config.networked) {
            return;
        }
        
        DeleteDbFiles.main(new String[]{"-dir", BASE_DIR + "/node1", "-quiet"});
        DeleteDbFiles.main(new String[]{"-dir", BASE_DIR + "/node2", "-quiet"});
        
        // create the master database
        Connection conn;
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:file:" + BASE_DIR + "/node1/test", "sa", "");
        Statement stat;
        stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        int len = getSize(10, 1000);
        for(int i=0; i<len; i++) {
            prep.setInt(1, i);
            prep.setString(2, "Data" + i);
            prep.executeUpdate();
        }
        conn.close();

        CreateCluster.main(new String[]{
                "-urlSource", "jdbc:h2:file:"+ BASE_DIR + "/node1/test", 
                "-urlTarget", "jdbc:h2:file:"+BASE_DIR + "/node2/test", 
                "-user", "sa", 
                "-serverlist", "localhost:9091,localhost:9092"
        });
        
        Server n1 = org.h2.tools.Server.createTcpServer(new String[]{"-tcpPort", "9091", "-baseDir", BASE_DIR + "/node1"}).start();        
        Server n2 = org.h2.tools.Server.createTcpServer(new String[]{"-tcpPort", "9092", "-baseDir", BASE_DIR + "/node2"}).start();        

        try {
            conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9091/test", "sa", "");
            error("should not be able to connect in standalone mode");
        } catch(SQLException e) {
            checkNotGeneralException(e);
        }

        try {
            conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9092/test", "sa", "");
            error("should not be able to connect in standalone mode");
        } catch(SQLException e) {
            checkNotGeneralException(e);
        }
        
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9091,localhost:9092/test", "sa", "");
        stat = conn.createStatement();
        check(conn, len);
        
        conn.close();
//        n1.stop();
//        n2.stop();

//        n1 = org.h2.tools.Server.startTcpServer(new String[]{"-tcpPort", "9091", "-baseDir", BASE_DIR + "/node1"});        
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9091/test;CLUSTER=''", "sa", "");
        check(conn, len);
        conn.close();
        n1.stop();
        
//        n2 = org.h2.tools.Server.startTcpServer(new String[]{"-tcpPort", "9092", "-baseDir", BASE_DIR + "/node2"});        
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9092/test;CLUSTER=''", "sa", "");
        check(conn, len);
        conn.close();
        n2.stop();
    }
    
    void check(Connection conn, int len) throws Exception {
        PreparedStatement prep = conn.prepareStatement("SELECT * FROM TEST WHERE ID=?");
        for(int i=0; i<len; i++) {
            prep.setInt(1, i);
            ResultSet rs = prep.executeQuery();
            rs.next();
            check(rs.getString(2), "Data"+i);
            checkFalse(rs.next());
        }
    }

}
