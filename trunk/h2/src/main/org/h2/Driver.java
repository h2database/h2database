/* * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.h2.engine.Constants;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.Message;
import org.h2.message.TraceSystem;

/**
 * The database driver. An application should not use this class directly. 
 * The only thing the application needs to do is load the driver. This can be done 
 * using Class.forName:
 * <pre>
 * Class.forName("org.h2.Driver");
 * </pre>
 */
public class Driver implements java.sql.Driver {

    private static final Driver instance = new Driver();

    static {
        try {
            DriverManager.registerDriver(instance);
        } catch (SQLException e) {
            TraceSystem.traceThrowable(e);
        }
    }

    /**
     * This method should not be called by an application.
     * 
     * @return the new connection
     */
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            if (info == null) {
                info = new Properties();
            }
            if (!acceptsURL(url)) {
                return null;
            }
            synchronized (this) {
                return new JdbcConnection(url, info);
            }
        } catch (Throwable e) {
            throw Message.convert(e);
        }
    }

    /**
     * This method should not be called by an application.
     * 
     * @return if the driver understands the URL
     */
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(Constants.START_URL);
    }

    /**
     * This method should not be called by an application.
     * 
     * @return the major version number
     */
    public int getMajorVersion() {
        return Constants.VERSION_MAJOR;
    }

    /**
     * This method should not be called by an application.
     * 
     * @return the minor version number
     */
    public int getMinorVersion() {
        return Constants.VERSION_MINOR;
    }

    /**
     * This method should not be called by an application.
     * 
     * @return a zero length array
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    /**
     * This method should not be called by an application.
     * 
     * @return true
     */
    public boolean jdbcCompliant() {
        return true;
    }
    
    /**
     * INTERNAL
     */
    public static void load() {
    }

}
