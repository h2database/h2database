/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
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

import java.util.Stack;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;

/**
 * A simple standalone JDBC connection pool manager.
 * It is based on the 
 * <a href="http://www.source-code.biz/snippets/java/8.htm">
 *  MiniConnectionPoolManager written by Christian d'Heureuse (JDK 1.5)
 * </a>.
 * 
 * @author Christian d'Heureuse 
 *      (<a href="http://www.source-code.biz">www.source-code.biz</a>)
 * @author Thomas Mueller (ported to JDK 1.4)
 */
public class JdbcConnectionPoolManager {

    private ConnectionPoolDataSource dataSource;
    private int maxConnections;
    private int timeout;
    private PrintWriter logWriter;
    private Stack recycledConnections;
    private int activeConnections;
    private PoolConnectionEventListener poolConnectionEventListener;
    private boolean isDisposed;

    /**
     * This inThrown in {@link #getConnection()} when no free connection becomes
     * available within <code>timeout</code> seconds.
     */
    public static class TimeoutException extends RuntimeException {
        private static final long serialVersionUID = 1;

        public TimeoutException() {
            super("Timeout while waiting for a free database connection.");
        }
    }

    /**
     * Constructs a JdbcConnectionPoolManager object with a timeout of 60
     * seconds.
     * 
     * @param dataSource the data source for the connections.
     * @param maxConnections the maximum number of connections.
     */
    public JdbcConnectionPoolManager(ConnectionPoolDataSource dataSource, int maxConnections) {
        this(dataSource, maxConnections, 60);
    }

    /**
     * Constructs a JdbcConnectionPoolManager object.
     * 
     * @param dataSource the data source for the connections.
     * @param maxConnections the maximum number of connections.
     * @param timeout the maximum time in seconds to wait for a free connection.
     */
    public JdbcConnectionPoolManager(ConnectionPoolDataSource dataSource, int maxConnections, int timeout) {
        this.dataSource = dataSource;
        this.maxConnections = maxConnections;
        this.timeout = timeout;
        try {
            logWriter = dataSource.getLogWriter();
        } catch (SQLException e) {
        }
        if (maxConnections < 1) {
            throw new IllegalArgumentException("Invalid maxConnections value.");
        }
        recycledConnections = new Stack();
        poolConnectionEventListener = new PoolConnectionEventListener();
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
     * 
     * @return a new Connection object.
     * @throws TimeoutException when no connection becomes available within
     *             <code>timeout</code> seconds.
     */
    public Connection getConnection() throws SQLException {
        for (int i = 0;; i++) {
            synchronized (this) {
                if (activeConnections < maxConnections) {
                    return getConnectionNow();
                }
                if (i >= timeout) {
                    throw new TimeoutException();
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
        assertInnerState();
        return conn;
    }

    private synchronized void recycleConnection(PooledConnection pc) {
        if (isDisposed) {
            disposeConnection(pc);
            return;
        }
        if (activeConnections <= 0) {
            throw new AssertionError();
        }
        activeConnections--;
        notifyAll();
        recycledConnections.push(pc);
        assertInnerState();
    }

    private synchronized void disposeConnection(PooledConnection pc) {
        if (activeConnections <= 0) {
            throw new AssertionError();
        }
        activeConnections--;
        notifyAll();
        try {
            pc.close();
        } catch (SQLException e) {
            log("Error while closing database connection: " + e.toString());
        }
        assertInnerState();
    }

    private void log(String msg) {
        String s = "JdbcConnectionPoolManager: " + msg;
        try {
            if (logWriter == null) {
                System.err.println(s);
            } else {
                logWriter.println(s);
            }
        } catch (Exception e) {
        }
    }

    private void assertInnerState() {
        if (activeConnections < 0) {
            throw new AssertionError();
        }
        if (activeConnections + recycledConnections.size() > maxConnections) {
            throw new AssertionError();
        }
    }

    private class PoolConnectionEventListener implements ConnectionEventListener {
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

}
