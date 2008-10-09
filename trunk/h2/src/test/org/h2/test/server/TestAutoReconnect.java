/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.h2.api.DatabaseEventListener;
import org.h2.test.TestBase;
import org.h2.tools.Server;

/**
 * Tests automatic embedded/server mode.
 */
public class TestAutoReconnect extends TestBase implements DatabaseEventListener {

    private String url;
    private boolean autoServer;
    private Server server;
    private Connection connServer;
    private Connection conn;
    private String state;
    
    /**
     * Run just this test.
     * 
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
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
                "FILE_LOCK=SOCKET;" + 
                "AUTO_SERVER=TRUE;OPEN_NEW=TRUE";
            restart();
        } else {
            server = Server.createTcpServer(new String[]{"-tcpPort", "8181"}).start();
            url = "jdbc:h2:tcp://localhost:8181/" + baseDir + "/autoReconnect;" + 
                "FILE_LOCK=SOCKET;AUTO_RECONNECT=TRUE";
        }
        
        // test the database event listener
        conn = DriverManager.getConnection(url + ";DATABASE_EVENT_LISTENER='" + getClass().getName() + "'");
        conn.close();
        
        // test the database event listener object
        Properties prop = new Properties();
        state = null;
        Driver postgreDriver = null;
        try {
            postgreDriver = DriverManager.getDriver("jdbc:postgresql:test");
            if (postgreDriver != null) {
                DriverManager.deregisterDriver(postgreDriver);
            }
        } catch (Exception e) {
            // ignore
        }
        prop.put("DATABASE_EVENT_LISTENER_OBJECT", this);
        conn = DriverManager.getConnection(url, prop);
        assertEquals(null, state);
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        restart();
        // the table is created in the database event listener
        stat.execute("SELECT * FROM TEST");
        assertEquals("state " + DatabaseEventListener.STATE_RECONNECTED, state);
        conn.close();
        
        if (postgreDriver != null) {
            DriverManager.registerDriver(postgreDriver);
        }
        
        conn = DriverManager.getConnection(url);
        restart();
        stat = conn.createStatement();
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
        stat.execute("SET @TEST 10");
        restart();
        rs = stat.executeQuery("CALL @TEST");
        rs.next();
        assertEquals(10, rs.getInt(1));
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

    public void closingDatabase() {
        // ignore
    }

    public void diskSpaceIsLow(long stillAvailable) throws SQLException {
        // ignore
    }

    public void exceptionThrown(SQLException e, String sql) {
        // ignore
    }

    public void init(String url) {
        state = "init";
    }

    public void opened() {
        state = "opened";
    }

    public void setProgress(int state, String name, int x, int max) {
        this.state = "state " + state;
        if (state == DatabaseEventListener.STATE_RECONNECTED) {
            try {
                conn.createStatement().execute("CREATE LOCAL TEMPORARY TABLE TEST(ID INT)");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
