/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: Christian d'Heureuse, www.source-code.biz
 * 
 * This class is dual-licensed LGPL and under the H2 License.
 * 
 * This module is free software: you can redistribute it and/or 
 * modify it under the terms of the GNU Lesser General Public 
 * License as published by the Free Software Foundation, either 
 * version 3 of the License, or (at your option) any later version. 
 * See http://www.gnu.org/licenses/lgpl.html
 * 
 * This program is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied 
 * warranty of MERCHANTABILITY or FITNESS FOR A 
 * PARTICULAR PURPOSE. See the GNU General Public 
 * License for more details.
 */
package org.h2.jdbcx;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Stack;

import javax.sql.ConnectionEvent;
import javax.sql.DataSource;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

/*## Java 1.6 begin ##
import org.h2.message.Message;
## Java 1.6 end ##*/

/**
 * A simple standalone JDBC connection pool.
 * It is based on the 
 * <a href="http://www.source-code.biz/snippets/java/8.htm">
 *  MiniConnectionPoolManager written by Christian d'Heureuse (Java 1.5)
 * </a>. It is used as follows:
 * <pre>
 * // init
 * import org.h2.jdbcx.*;
 * ...
 * JdbcDataSource ds = new JdbcDataSource();
 * ds.setURL("jdbc:h2:~/test");
 * ds.setUser("sa");
 * ds.setPassword("sa");
 * JdbcConnectionPool cp = JdbcConnectionPool.create(ds);
 * // use
 * Connection conn = cp.getConnection();
 * ...
 * conn.close();
 * // dispose
 * cp.dispose();
 * </pre>
 * 
 * @author Christian d'Heureuse 
 *      (<a href="http://www.source-code.biz">www.source-code.biz</a>)
 * @author Thomas Mueller (ported to Java 1.4, some changes)
 */
public class JdbcConnectionPool implements DataSource {

    private final ConnectionPoolDataSource dataSource;
    private final Stack recycledConnections = new Stack();
    private final PoolConnectionEventListener poolConnectionEventListener = new PoolConnectionEventListener();
    private PrintWriter logWriter;
    private int maxConnections = 10;
    private int timeout = 60;
    private int activeConnections;
    private boolean isDisposed;

    private JdbcConnectionPool(ConnectionPoolDataSource dataSource) {
        this.dataSource = dataSource;
        try {
            logWriter = dataSource.getLogWriter();
        } catch (SQLException e) {
            // ignore
        }
    }
    
    /**
     * Constructs a new connection pool.
     * 
     * @param dataSource the data source to create connections
     * @return the connection pool
     */
    public static JdbcConnectionPool create(ConnectionPoolDataSource dataSource) {
        return new JdbcConnectionPool(dataSource);
    }
    
    /**
     * Sets the maximum number of connections to use from now on.
     * The default value is 10 connections.
     * 
     * @param max the maximum number of connections
     */
    public synchronized void setMaxConnections(int max) {
        if (maxConnections < 1) {
            throw new IllegalArgumentException("Invalid maxConnections value.");
        }
        this.maxConnections = max;
        // notify waiting threads if the value was increased
        notifyAll();
    }
    
    /**
     * Gets the maximum number of connections to use.
     * 
     * @return the max the maximum number of connections
     */
    public synchronized int getMaxConnections() {
        return maxConnections;
    }
    
    /**
     * Gets the maximum time in seconds to wait for a free connection.
     * 
     * @return the timeout in seconds
     */    
    public synchronized int getLoginTimeout() {
        return timeout;
    }
    
    /**
     * Sets the maximum time in seconds to wait for a free connection.
     * The default timeout is 60 seconds.
     * 
     * @param seconds the maximum timeout
     */
    public synchronized void setLoginTimeout(int seconds) {
        this.timeout = seconds;
    }
    
    /**
     * Closes all unused pooled connections.
     */
    public synchronized void dispose() throws SQLException {
        if (isDisposed) {
            return;
        }
        isDisposed = true;
        SQLException e = null;
        while (!recycledConnections.isEmpty()) {
            PooledConnection pc = (PooledConnection) recycledConnections.pop();
            try {
                pc.close();
            } catch (SQLException e2) {
                if (e == null) {
                    e = e2;
                }
            }
        }
        if (e != null) {
            throw e;
        }
    }

    /**
     * Retrieves a connection from the connection pool. If
     * <code>maxConnections</code> connections are already in use, the method
     * waits until a connection becomes available or <code>timeout</code>
     * seconds elapsed. When the application is finished using the connection,
     * it must close it in order to return it to the pool.
     * If no connection becomes available within the given timeout, an exception
     * with SQL state 08001 and vendor code 8001 is thrown.
     * 
     * @return a new Connection object.
     * @throws SQLException when a new connection could not be established, 
     *      or a timeout occured
     */
    public Connection getConnection() throws SQLException {
        for (int i = 0;; i++) {
            synchronized (this) {
                if (activeConnections < maxConnections) {
                    return getConnectionNow();
                }
                if (i >= timeout) {
                    throw new SQLException("Login timeout", "08001", 8001);
                }
                try {
                    wait(1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }

    private Connection getConnectionNow() throws SQLException {
        if (isDisposed) {
            throw new IllegalStateException("Connection pool has been disposed.");
        }
        PooledConnection pc;
        if (!recycledConnections.empty()) {
            pc = (PooledConnection) recycledConnections.pop();
        } else {
            pc = dataSource.getPooledConnection();
        }
        Connection conn = pc.getConnection();
        activeConnections++;
        pc.addConnectionEventListener(poolConnectionEventListener);
        return conn;
    }

    synchronized void recycleConnection(PooledConnection pc) {
        if (isDisposed) {
            disposeConnection(pc);
            return;
        }
        if (activeConnections <= 0) {
            throw new AssertionError();
        }
        activeConnections--;
        if (activeConnections < maxConnections) {
            recycledConnections.push(pc);
        } else {
            closeConnection(pc);
        }
        notifyAll();
    }
    
    private void closeConnection(PooledConnection pc) {
        try {
            pc.close();
        } catch (SQLException e) {
            log("Error while closing database connection: " + e.toString());
        }
    }

    synchronized void disposeConnection(PooledConnection pc) {
        if (activeConnections <= 0) {
            throw new AssertionError();
        }
        activeConnections--;
        notifyAll();
        closeConnection(pc);
    }

    private void log(String msg) {
        String s = getClass().getName() + ": " + msg;
        try {
            if (logWriter == null) {
                System.err.println(s);
            } else {
                logWriter.println(s);
            }
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * This event listener informs the connection pool that about closed and
     * broken connections.
     */
    class PoolConnectionEventListener implements ConnectionEventListener {
        public void connectionClosed(ConnectionEvent event) {
            PooledConnection pc = (PooledConnection) event.getSource();
            pc.removeConnectionEventListener(this);
            recycleConnection(pc);
        }

        public void connectionErrorOccurred(ConnectionEvent event) {
            PooledConnection pc = (PooledConnection) event.getSource();
            pc.removeConnectionEventListener(this);
            disposeConnection(pc);
        }
    }

    /**
     * Returns the number of active (open) connections of this pool. This is the
     * number of <code>Connection</code> objects that have been issued by
     * getConnection() for which <code>Connection.close()</code> has
     * not yet been called.
     * 
     * @return the number of active connections.
     */
    public synchronized int getActiveConnections() {
        return activeConnections;
    }

    /**
     * INTERNAL
     */
    public Connection getConnection(String username, String password) {
        throw new UnsupportedOperationException();
    }

    /**
     * INTERNAL
     */
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    /**
     * INTERNAL
     */    
    public void setLogWriter(PrintWriter logWriter) {
        this.logWriter = logWriter;
    }

    /**
     * [Not supported] Return an object of this class if possible.
     *
     * @param iface the class
     */
/*## Java 1.6 begin ##
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw Message.getUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     */
/*## Java 1.6 begin ##
    public boolean isWrapperFor(Class< ? > iface) throws SQLException {
        throw Message.getUnsupportedException();
    }
## Java 1.6 end ##*/

}
