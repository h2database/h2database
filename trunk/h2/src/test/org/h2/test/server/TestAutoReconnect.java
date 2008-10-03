/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.tools.Server;

/**
 * Tests automatic embedded/server mode.
 */
public class TestAutoReconnect extends TestBase {

    private String url;
    private boolean autoServer;
    private Server server;
    private Connection connServer;
    
    /**
     * Run just this test.
     * 
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        new TestAutoReconnect().init().test();
    }
    
    private void restart() throws SQLException {
        if (autoServer) {
            if (connServer != null) {
                connServer.createStatement().execute("SHUTDOWN");
                connServer.close();
            }
            connServer = DriverManager.getConnection(url); 
        } else {
            server.stop();
            server.start();
        }
    }
    
    public void test() throws Exception {
        test(true);
        test(false);
    }
    
    private void test(boolean autoServer) throws Exception {
        this.autoServer = autoServer;
        deleteDb("autoReconnect");
        if (autoServer) {
            url = "jdbc:h2:" + baseDir + "/autoReconnect;" + 
                "FILE_LOCK=SOCKET;AUTO_RECONNECT=TRUE;" + 
                "AUTO_SERVER=TRUE;OPEN_NEW=TRUE";
            restart();
        } else {
            server = Server.createTcpServer(null).start();
            url = "jdbc:h2:tcp://localhost/" + baseDir + "/autoReconnect;" + 
                "FILE_LOCK=SOCKET;AUTO_RECONNECT=TRUE";
        }
        Connection conn = DriverManager.getConnection(url);
        restart();
        Statement stat = conn.createStatement();
        restart();
        stat.execute("create table test(id identity, name varchar)");
        restart();
        PreparedStatement prep = conn.prepareStatement("insert into test values(null, ?)");
        restart();
        prep.setString(1, "Hello");
        restart();
        prep.execute();
        restart();
        prep.setString(1, "World");
        restart();
        prep.execute();
        restart();
        ResultSet rs = stat.executeQuery("select * from test order by id");
        restart();
        assertTrue(rs.next());
        restart();
        assertEquals(1, rs.getInt(1));
        restart();
        assertEquals("Hello", rs.getString(2));
        restart();
        assertTrue(rs.next());
        restart();
        assertEquals(2, rs.getInt(1));
        restart();
        assertEquals("World", rs.getString(2));
        restart();
        assertFalse(rs.next());
        restart();
        stat.setFetchSize(10);
        restart();
        rs = stat.executeQuery("select * from system_range(1, 20)");
        restart();
        for (int i = 0;; i++) {
            try {
                boolean more = rs.next();
                if (!more) {
                    assertEquals(i, 20);
                    break;
                }
                restart();
                int x = rs.getInt(1);
                assertEquals(x, i + 1);
                if (i > 10) {
                    fail();
                }
            } catch (SQLException e) {
                if (i < 10) {
                    throw e;
                }
            }
        }
        restart();
        rs.close();
        
        conn.setAutoCommit(false);
        restart();
        try {
            conn.createStatement().execute("select * from test");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        
        conn.close();
        if (autoServer) {
            connServer.close();
        } else {
            server.stop();
        }
    }

}
