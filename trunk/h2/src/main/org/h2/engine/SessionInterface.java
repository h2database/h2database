/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

import org.h2.command.CommandInterface;
import org.h2.message.Trace;
import org.h2.store.DataHandler;

/**
 * A local or remote session. A session represents a database connection.
 */
public interface SessionInterface {

    /**
     * Parse a command and prepare it for execution.
     *
     * @param sql the SQL statement
     * @param fetchSize the number of rows to fetch in one step
     * @return the prepared command
     */
    CommandInterface prepareCommand(String sql, int fetchSize) throws SQLException;

    /**
     * Roll back pending transactions and close the session.
     */
    void close() throws SQLException;

    /**
     * Get the trace object
     *
     * @return the trace object
     */
    Trace getTrace();

    /**
     * Check if close was called.
     *
     * @return if the session has been closed
     */
    boolean isClosed();

    /**
     * Get the number of disk operations before power failure is simulated.
     * This is used for testing. If not set, 0 is returned
     *
     * @return the number of operations, or 0
     */
    int getPowerOffCount();

    /**
     * Set the number of disk operations before power failure is simulated.
     * To disable the countdown, use 0.
     *
     * @param i the number of operations
     */
    void setPowerOffCount(int i) throws SQLException;

    /**
     * Get the data handler object.
     *
     * @return the data handler
     */
    DataHandler getDataHandler();

    /**
     * Cancel the current or next command (called when closing a connection).
     */
    void cancel();

    /**
     * Check if the database changed and therefore reconnecting is required.
     *
     * @param write if the next operation may be writing
     * @return true if reconnecting is required
     */
    boolean isReconnectNeeded(boolean write);

    /**
     * Close the connection and open a new connection.
     *
     * @return the new connection
     */
    SessionInterface reconnect() throws SQLException;
}
