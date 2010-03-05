/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.h2.server.web.DbStarter;
import org.h2.test.TestBase;
import org.h2.util.New;

/**
 * Tests the DbStarter servlet.
 * This test simulates a minimum servlet container environment.
 */
public class TestServlet extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    /**
     * Minimum ServletContext implementation.
     * Most methods are not implemented.
     */
    static class TestServletContext implements ServletContext {

        private Properties initParams = new Properties();
        private HashMap<String, Object> attributes = New.hashMap();

        public void setAttribute(String key, Object value) {
            attributes.put(key, value);
        }

        public Object getAttribute(String key) {
            return attributes.get(key);
        }

        /**
         * Set an initialization parameter.
         *
         * @param key the parameter key
         * @param value the value
         */
        void setInitParameter(String key, String value) {
            initParams.setProperty(key, value);
        }

        public String getInitParameter(String key) {
            return initParams.getProperty(key);
        }

        public Enumeration<Object> getAttributeNames() {
            throw new UnsupportedOperationException();
        }

        public ServletContext getContext(String string) {
            throw new UnsupportedOperationException();
        }

        public Enumeration<Object> getInitParameterNames() {
            throw new UnsupportedOperationException();
        }

        public int getMajorVersion() {
            throw new UnsupportedOperationException();
        }

        public String getMimeType(String string) {
            throw new UnsupportedOperationException();
        }

        public int getMinorVersion() {
            throw new UnsupportedOperationException();
        }

        public RequestDispatcher getNamedDispatcher(String string) {
            throw new UnsupportedOperationException();
        }

        public String getRealPath(String string) {
            throw new UnsupportedOperationException();
        }

        public RequestDispatcher getRequestDispatcher(String string) {
            throw new UnsupportedOperationException();
        }

        public URL getResource(String string) {
            throw new UnsupportedOperationException();
        }

        public InputStream getResourceAsStream(String string) {
            throw new UnsupportedOperationException();
        }

        public Set<Object> getResourcePaths(String string) {
            throw new UnsupportedOperationException();
        }

        public String getServerInfo() {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated
         */
        public Servlet getServlet(String string) {
            throw new UnsupportedOperationException();
        }

        public String getServletContextName() {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated
         */
        public Enumeration<Object> getServletNames() {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated
         */
        public Enumeration<Object> getServlets() {
            throw new UnsupportedOperationException();
        }

        public void log(String string) {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated
         */
        public void log(Exception exception, String string) {
            throw new UnsupportedOperationException();
        }

        public void log(String string, Throwable throwable) {
            throw new UnsupportedOperationException();
        }

        public void removeAttribute(String string) {
            throw new UnsupportedOperationException();
        }

    }

    public void test() throws SQLException {
        if (config.networked || config.memory) {
            return;
        }
        DbStarter listener = new DbStarter();

        TestServletContext context = new TestServletContext();
        String url = getURL("servlet", true);
        context.setInitParameter("db.url", url);
        context.setInitParameter("db.user", getUser());
        context.setInitParameter("db.password", getPassword());
        context.setInitParameter("db.tcpServer", "-tcpPort 8888");

        ServletContextEvent event = new ServletContextEvent(context);
        listener.contextInitialized(event);

        Connection conn1 = listener.getConnection();
        Connection conn1a = (Connection) context.getAttribute("connection");
        assertTrue(conn1 == conn1a);
        Statement stat1 = conn1.createStatement();
        stat1.execute("CREATE TABLE T(ID INT)");

        String u2 = url.substring(url.indexOf("servlet"));
        u2 = "jdbc:h2:tcp://localhost:8888/" + baseDir + "/" + u2;
        Connection conn2 = DriverManager.getConnection(
                u2, getUser(), getPassword());
        Statement stat2 = conn2.createStatement();
        stat2.execute("SELECT * FROM T");
        stat2.execute("DROP TABLE T");

        try {
            stat1.execute("SELECT * FROM T");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }

        conn2.close();

        listener.contextDestroyed(event);

        // listener must be stopped
        try {
            DriverManager.getConnection("jdbc:h2:tcp://localhost:8888/" + baseDir + "/servlet", getUser(), getPassword());
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }

        // connection must be closed
        try {
            stat1.execute("SELECT * FROM DUAL");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }

    }

}
