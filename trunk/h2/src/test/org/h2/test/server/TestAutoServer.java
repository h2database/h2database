/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
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
    
    static final boolean SLOW = false;

    /**
     * Run just this test.
     * 
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        new TestAutoServer().init().test();
    }

    public void test() throws Exception {
        if (config.memory || config.networked) {
            return;
        }
        deleteDb("autoServer");
        String url = getURL("autoServer;AUTO_SERVER=TRUE", true);
        String user = getUser(), password = getPassword();
        Connection conn = getConnection(url, user, password);
        conn.close();
        String[] procDef = new String[] { "java", "-cp", "bin", TestAutoServer2.class.getName(), url, user, password };
        // TestAutoServer2.main(new String[]{url, user, password});
        Process proc = Runtime.getRuntime().exec(procDef);
        
        int i = SLOW ? Integer.MAX_VALUE : 30;
        for (; i > 0; i--) {
            Thread.sleep(100);
            SortedProperties prop = SortedProperties.loadProperties(baseDir + "/autoServer.lock.db");
            String server = prop.getProperty("server");
            if (server != null) {
                String u2 = url.substring(url.indexOf("autoServer"));
                u2 = "jdbc:h2:tcp://" + server + "/" + baseDir + "/" + u2;
                conn = DriverManager.getConnection(u2, user, password);
                conn.close();
                break;
            }
        }
        if (i <= 0) {
            fail();
        }
        
        conn = getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS HALT FOR \"" + getClass().getName() + ".halt\"");
        try {
            stat.execute("CALL HALT(11)");
        } catch (SQLException e) {
            assertKnownException(e);
        }
        conn.close();
        assertEquals(11, proc.exitValue());
        // proc.destroy();
    }
    
    public static void halt(int exitValue) {
        Runtime.getRuntime().halt(exitValue);
    }

}
