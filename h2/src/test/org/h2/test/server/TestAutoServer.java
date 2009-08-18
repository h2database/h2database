/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.util.SortedProperties;

/**
 * Tests automatic embedded/server mode.
 */
public class TestAutoServer extends TestBase {

    /**
     * The number of iterations.
     */
    static final int ITERATIONS = 30;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        if (config.memory || config.networked) {
            return;
        }
        deleteDb("autoServer");
        String url = getURL("autoServer;AUTO_SERVER=TRUE", true);
        String user = getUser(), password = getPassword();
        Connection connServer = getConnection(url + ";OPEN_NEW=TRUE", user, password);

        int i = ITERATIONS;
        for (; i > 0; i--) {
            Thread.sleep(100);
            SortedProperties prop = SortedProperties.loadProperties(baseDir + "/autoServer.lock.db");
            String key = prop.getProperty("id");
            String server = prop.getProperty("server");
            if (server != null) {
                String u2 = url.substring(url.indexOf(";"));
                u2 = "jdbc:h2:tcp://" + server + "/" + key + u2;
                Connection conn = DriverManager.getConnection(u2, user, password);
                conn.close();
                break;
            }
        }
        if (i <= 0) {
            fail();
        }
        Connection conn = getConnection(url + ";OPEN_NEW=TRUE");
        Statement stat = conn.createStatement();
        if (config.big) {
            try {
                stat.execute("SHUTDOWN");
            } catch (SQLException e) {
                assertKnownException(e);
                // the connection is closed
            }
        }
        conn.close();
        connServer.close();
        deleteDb("autoServer");
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param exitValue the exit value
     */
    public static void halt(int exitValue) {
        Runtime.getRuntime().halt(exitValue);
    }

}
