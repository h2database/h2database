/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;

import org.h2.test.TestBase;

/**
 * Tests automatic embedded/server mode.
 */
public class TestAutoServer2 extends TestBase {
    public static void main(String[] a) throws Exception {
        if (a.length == 3) {
            // PrintStream ps = new PrintStream(new File("test.txt"));
            PrintStream ps = new PrintStream(new ByteArrayOutputStream());
            System.setErr(ps);
            System.setOut(ps);
            ps.println(new java.sql.Timestamp(System.currentTimeMillis()).toString());
            ps.flush();
            try {
                ps.println("loading");
                ps.flush();
                org.h2.Driver.load();
                ps.println("connecting url:" + a[0] + " user:" + a[1] + " pwd:" + a[2]);
                ps.flush();
                Connection conn = DriverManager.getConnection(a[0], a[1], a[2]);
                ps.println("waiting");
                ps.flush();
                Thread.sleep(TestAutoServer.SLOW ? 60000 : 5000);
                ps.println("closing");
                ps.flush();
                conn.close();
                ps.println("closed");
                ps.flush();
            } catch (Throwable t) {
                t.printStackTrace(ps);
                t.printStackTrace();
            }
            ps.close();
            System.exit(0);
        } else {
            new TestAutoServer2().init().test();
        }
    }

    public void test() throws Exception {
        deleteDb("autoServer");
        String url = getURL("autoServer;AUTO_SERVER=TRUE", true);
        String user = getUser(), password = getPassword();
        main(new String[]{url, user, password});
    }
}
