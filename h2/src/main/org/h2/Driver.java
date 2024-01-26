/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;

/**
 * The database driver. An application should not use this class directly. The
 * only thing the application needs to do is load the driver. This can be done
 * using Class.forName. To load the driver and open a database connection, use
 * the following code:
 *
 * <pre>
 * Class.forName(&quot;org.h2.Driver&quot;);
 * Connection conn = DriverManager.getConnection(
 *      &quot;jdbc:h2:&tilde;/test&quot;, &quot;sa&quot;, &quot;sa&quot;);
 * </pre>
 */
public class Driver implements java.sql.Driver, JdbcDriverBackwardsCompat {

    private static final Driver INSTANCE = new Driver();
    private static final String DEFAULT_URL = "jdbc:default:connection";
    private static final ThreadLocal<Connection> DEFAULT_CONNECTION =
            new ThreadLocal<>();

    private static boolean registered;

    static {
        load();
    }

    /**
     * Open a database connection.
     * This method should not be called by an application.
     * Instead, the method DriverManager.getConnection should be used.
     *
     * @param url the database URL
     * @param info the connection properties
     * @return the new connection or null if the URL is not supported
     * @throws SQLException on connection exception or if URL is {@code null}
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null) {
            throw DbException.getJdbcSQLException(ErrorCode.URL_FORMAT_ERROR_2, null, Constants.URL_FORMAT, null);
        } else if (url.startsWith(Constants.START_URL)) {
            return new JdbcConnection(url, info, null, null, false);
        } else if (url.equals(DEFAULT_URL)) {
            return DEFAULT_CONNECTION.get();
        } else {
            return null;
        }
    }

    /**
     * Check if the driver understands this URL.
     * This method should not be called by an application.
     *
     * @param url the database URL
     * @return if the driver understands the URL
     * @throws SQLException if URL is {@code null}
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            throw DbException.getJdbcSQLException(ErrorCode.URL_FORMAT_ERROR_2, null, Constants.URL_FORMAT, null);
        } else if (url.startsWith(Constants.START_URL)) {
            return true;
        } else if (url.equals(DEFAULT_URL)) {
            return DEFAULT_CONNECTION.get() != null;
        } else {
            return false;
        }
    }

    /**
     * Get the major version number of the driver.
     * This method should not be called by an application.
     *
     * @return the major version number
     */
    @Override
    public int getMajorVersion() {
        return Constants.VERSION_MAJOR;
    }

    /**
     * Get the minor version number of the driver.
     * This method should not be called by an application.
     *
     * @return the minor version number
     */
    @Override
    public int getMinorVersion() {
        return Constants.VERSION_MINOR;
    }

    /**
     * Get the list of supported properties.
     * This method should not be called by an application.
     *
     * @param url the database URL
     * @param info the connection properties
     * @return a zero length array
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    /**
     * Check if this driver is compliant to the JDBC specification.
     * This method should not be called by an application.
     *
     * @return true
     */
    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    /**
     * [Not supported]
     */
    @Override
    public Logger getParentLogger() {
        return null;
    }

    /**
     * INTERNAL
     * @return instance of the driver registered with the DriverManager
     */
    public static synchronized Driver load() {
        try {
            if (!registered) {
                registered = true;
                DriverManager.registerDriver(INSTANCE);
            }
        } catch (SQLException e) {
            DbException.traceThrowable(e);
        }
        return INSTANCE;
    }

    /**
     * INTERNAL
     */
    public static synchronized void unload() {
        try {
            if (registered) {
                registered = false;
                DriverManager.deregisterDriver(INSTANCE);
            }
        } catch (SQLException e) {
            DbException.traceThrowable(e);
        }
    }

    /**
     * INTERNAL
     * Sets, on a per-thread basis, the default-connection for
     * user-defined functions.
     * @param c to set default to
     */
    public static void setDefaultConnection(Connection c) {
        if (c == null) {
            DEFAULT_CONNECTION.remove();
        } else {
            DEFAULT_CONNECTION.set(c);
        }
    }

    /**
     * INTERNAL
     * @param thread to set context class loader for
     */
    public static void setThreadContextClassLoader(Thread thread) {
        // Apache Tomcat: use the classloader of the driver to avoid the
        // following log message:
        // org.apache.catalina.loader.WebappClassLoader clearReferencesThreads
        // SEVERE: The web application appears to have started a thread named
        // ... but has failed to stop it.
        // This is very likely to create a memory leak.
        try {
            thread.setContextClassLoader(Driver.class.getClassLoader());
        } catch (Throwable t) {
            // ignore
        }
    }

}
