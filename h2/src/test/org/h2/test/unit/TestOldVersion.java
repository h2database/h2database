/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;
import org.h2.tools.Server;

/**
 * Tests the compatibility with older versions
 */
public class TestOldVersion extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        URL[] urls = { new URL("file:ext/h2-1.2.127.jar") };
        ClassLoader cl = new URLClassLoader(urls, null);
        // cl = getClass().getClassLoader();
        Class< ? > driverClass = cl.loadClass("org.h2.Driver");
        Method m = driverClass.getMethod("load");
        Driver driver = (Driver) m.invoke(null);
        Connection conn = driver.connect("jdbc:h2:mem:", null);
        assertEquals("1.2.127 (2010-01-15)", conn.getMetaData().getDatabaseProductVersion());
        Server server = org.h2.tools.Server.createTcpServer("-tcpPort", "9001");
        server.start();
        try {
            conn = driver.connect("jdbc:h2:tcp://localhost:9001/mem:test", null);
        } catch (SQLException e) {
            assertEquals(ErrorCode.DRIVER_VERSION_ERROR_2, e.getErrorCode());
        }
        server.stop();

        Class< ? > serverClass = cl.loadClass("org.h2.tools.Server");
        m = serverClass.getMethod("createTcpServer", String[].class);
        Object serverOld = m.invoke(null, new Object[]{new String[]{"-tcpPort", "9001"}});
        m = serverOld.getClass().getMethod("start");
        m.invoke(serverOld);
        conn = org.h2.Driver.load().connect("jdbc:h2:mem:", null);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("call 1");
        rs.next();
        assertEquals(1, rs.getInt(1));
        conn.close();
        m = serverOld.getClass().getMethod("stop");
        m.invoke(serverOld);
    }

}
