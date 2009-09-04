/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.h2.command.CommandInterface;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.SessionInterface;
import org.h2.message.Message;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.util.ObjectArray;

/**
 * Represents a statement.
 */
public class JdbcStatement extends TraceObject implements Statement {

    protected JdbcConnection conn;
    protected SessionInterface session;
    protected JdbcResultSet resultSet;
    protected int maxRows;
    protected int fetchSize = SysProperties.SERVER_RESULT_SET_FETCH_SIZE;
    protected int updateCount;
    protected final int resultSetType;
    protected final int resultSetConcurrency;
    protected boolean closedByResultSet;
    private CommandInterface executingCommand;
    private ObjectArray<String> batchCommands;
    private boolean escapeProcessing = true;

    JdbcStatement(JdbcConnection conn, int id, int resultSetType, int resultSetConcurrency, boolean closeWithResultSet) {
        this.conn = conn;
        this.session = conn.getSession();
        setTrace(session.getTrace(), TraceObject.STATEMENT, id);
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.closedByResultSet = closeWithResultSet;
    }

    /**
     * Executes a query (select statement) and returns the result set.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * @param sql the SQL statement to execute
     * @return the result set
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id, "executeQuery(" + quote(sql) + ")");
            }
            checkClosed();
            closeOldResultSet();
            sql = conn.translateSQL(sql, escapeProcessing);
            synchronized (session) {
                CommandInterface command = conn.prepareCommand(sql, fetchSize);
                ResultInterface result;
                boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
                boolean updatable = resultSetConcurrency == ResultSet.CONCUR_UPDATABLE;
                setExecutingStatement(command);
                try {
                    result = command.executeQuery(maxRows, scrollable);
                } finally {
                    setExecutingStatement(null);
                }
                command.close();
                resultSet = new JdbcResultSet(conn, this, result, id, closedByResultSet, scrollable, updatable);
            }
            return resultSet;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement (insert, update, delete, create, drop)
     * and returns the update count.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * If the statement is a create or drop and does not throw an exception,
     * the current transaction (if any) is committed after executing the statement.
     * If auto commit is on, this statement will be committed.
     *
     * @param sql the SQL statement
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    public int executeUpdate(String sql) throws SQLException {
        try {
            debugCodeCall("executeUpdate", sql);
            checkClosedForWrite();
            closeOldResultSet();
            sql = conn.translateSQL(sql, escapeProcessing);
            CommandInterface command = conn.prepareCommand(sql, fetchSize);
            synchronized (session) {
                setExecutingStatement(command);
                try {
                    updateCount = command.executeUpdate();
                } finally {
                    setExecutingStatement(null);
                }
            }
            command.close();
            return updateCount;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes an arbitrary statement. If another result set exists for this
     * statement, this will be closed (even if this statement fails).
     *
     * If the statement is a create or drop and does not throw an exception, the
     * current transaction (if any) is committed after executing the statement.
     * If auto commit is on, and the statement is not a select, this statement
     * will be committed.
     *
     * @param sql the SQL statement to execute
     * @return true if a result set is available, false if not
     */
    public boolean execute(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET);
            if (isDebugEnabled()) {
                debugCodeCall("execute", sql);
            }
            checkClosedForWrite();
            closeOldResultSet();
            sql = conn.translateSQL(sql, escapeProcessing);
            CommandInterface command = conn.prepareCommand(sql, fetchSize);
            boolean returnsResultSet;
            synchronized (session) {
                setExecutingStatement(command);
                try {
                    if (command.isQuery()) {
                        returnsResultSet = true;
                        boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
                        boolean updatable = resultSetConcurrency == ResultSet.CONCUR_UPDATABLE;
                        ResultInterface result = command.executeQuery(maxRows, scrollable);
                        resultSet = new JdbcResultSet(conn, this, result, id, closedByResultSet, scrollable, updatable);
                    } else {
                        returnsResultSet = false;
                        updateCount = command.executeUpdate();
                    }
                } finally {
                    setExecutingStatement(null);
                }
            }
            command.close();
            return returnsResultSet;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the last result set produces by this statement.
     *
     * @return the result set
     */
    public ResultSet getResultSet() throws SQLException {
        try {
            checkClosed();
            if (resultSet != null) {
                int id = resultSet.getTraceId();
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id, "getResultSet()");
            } else {
                debugCodeCall("getResultSet");
            }
            return resultSet;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the last update count of this statement.
     *
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback; -1 if the statement was a select).
     * @throws SQLException if this object is closed or invalid
     */
    public int getUpdateCount() throws SQLException {
        try {
            debugCodeCall("getUpdateCount");
            checkClosed();
            return updateCount;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Closes this statement.
     * All result sets that where created by this statement
     * become invalid after calling this method.
     */
    public void close() throws SQLException {
        try {
            debugCodeCall("close");
            closeOldResultSet();
            if (conn != null) {
                conn = null;
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the connection that created this object.
     *
     * @return the connection
     */
    public Connection getConnection() throws SQLException {
        try {
            debugCodeCall("getConnection");
            checkClosed();
            return conn;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the first warning reported by calls on this object.
     * This driver does not support warnings, and will always return null.
     *
     * @return null
     */
    public SQLWarning getWarnings() throws SQLException {
        try {
            debugCodeCall("getWarnings");
            checkClosed();
            return null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears all warnings. As this driver does not support warnings,
     * this call is ignored.
     */
    public void clearWarnings() throws SQLException {
        try {
            debugCodeCall("clearWarnings");
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves to the next result set - however there is always only one result
     * set. This call also closes the current result set (if there is one).
     * Returns true if there is a next result set (that means - it always
     * returns false).
     *
     * @return false
     * @throws SQLException if this object is closed.
     */
    public boolean getMoreResults() throws SQLException {
        try {
            debugCodeCall("getMoreResults");
            checkClosed();
            closeOldResultSet();
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the name of the cursor. This call is ignored.
     *
     * @param name ignored
     * @throws SQLException if this object is closed
     */
    public void setCursorName(String name) throws SQLException {
        try {
            debugCodeCall("setCursorName", name);
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the fetch direction.
     * This call is ignored by this driver.
     *
     * @param direction ignored
     * @throws SQLException if this object is closed
     */
    public void setFetchDirection(int direction) throws SQLException {
        try {
            debugCodeCall("setFetchDirection", direction);
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the fetch direction.
     *
     * @return FETCH_FORWARD
     * @throws SQLException if this object is closed
     */
    public int getFetchDirection() throws SQLException {
        try {
            debugCodeCall("getFetchDirection");
            checkClosed();
            return ResultSet.FETCH_FORWARD;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the maximum number of rows for a ResultSet.
     *
     * @return the number of rows where 0 means no limit
     * @throws SQLException if this object is closed
     */
    public int getMaxRows() throws SQLException {
        try {
            debugCodeCall("getMaxRows");
            checkClosed();
            return maxRows;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the maximum number of rows for a ResultSet.
     *
     * @param maxRows the number of rows where 0 means no limit
     * @throws SQLException if this object is closed
     */
    public void setMaxRows(int maxRows) throws SQLException {
        try {
            debugCodeCall("setMaxRows", maxRows);
            checkClosed();
            if (maxRows < 0) {
                throw Message.getInvalidValueException(""+maxRows, "maxRows");
            }
            this.maxRows = maxRows;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the number of rows suggested to read in one step.
     * This value cannot be higher than the maximum rows (setMaxRows)
     * set by the statement or prepared statement, otherwise an exception
     * is throws. Setting the value to 0 will set the default value.
     * The default value can be changed using the system property
     * h2.serverResultSetFetchSize.
     *
     * @param rows the number of rows
     * @throws SQLException if this object is closed
     */
    public void setFetchSize(int rows) throws SQLException {
        try {
            debugCodeCall("setFetchSize", rows);
            checkClosed();
            if (rows < 0 || (rows > 0 && maxRows > 0 && rows > maxRows)) {
                throw Message.getInvalidValueException("" + rows, "rows");
            }
            if (rows == 0) {
                rows = SysProperties.SERVER_RESULT_SET_FETCH_SIZE;
            }
            fetchSize = rows;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the number of rows suggested to read in one step.
     *
     * @return the current fetch size
     * @throws SQLException if this object is closed
     */
    public int getFetchSize() throws SQLException {
        try {
            debugCodeCall("getFetchSize");
            checkClosed();
            return fetchSize;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set concurrency created by this object.
     *
     * @return the concurrency
     */
    public int getResultSetConcurrency() throws SQLException {
        try {
            debugCodeCall("getResultSetConcurrency");
            checkClosed();
            return resultSetConcurrency;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set type.
     *
     * @return the type
     * @throws SQLException if this object is closed
     */
    public int getResultSetType()  throws SQLException {
        try {
            debugCodeCall("getResultSetType");
            checkClosed();
            return resultSetType;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the maximum number of bytes for a result set column.
     *
     * @return always 0 for no limit
     * @throws SQLException if this object is closed
     */
    public int getMaxFieldSize() throws SQLException {
        try {
            debugCodeCall("getMaxFieldSize");
            checkClosed();
            return 0;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the maximum number of bytes for a result set column.
     * This method does currently do nothing for this driver.
     *
     * @param max the maximum size - ignored
     * @throws SQLException if this object is closed
     */
    public void setMaxFieldSize(int max) throws SQLException {
        try {
            debugCodeCall("setMaxFieldSize", max);
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Enables or disables processing or JDBC escape syntax.
     * See also Connection.nativeSQL.
     *
     * @param enable - true (default) or false (no conversion is attempted)
     * @throws SQLException if this object is closed
     */
    public void setEscapeProcessing(boolean enable) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setEscapeProcessing("+enable+");");
            }
            checkClosed();
            escapeProcessing = enable;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Cancels a currently running statement.
     * This method must be called from within another
     * thread than the execute method.
     *
     * @throws SQLException if this object is closed
     */
    public void cancel() throws SQLException {
        try {
            debugCodeCall("cancel");
            checkClosed();
            // executingCommand can be reset  by another thread
            CommandInterface c = executingCommand;
            try {
                if (c != null) {
                    c.cancel();
                }
            } finally {
                setExecutingStatement(null);
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current query timeout in seconds.
     * This method will return 0 if no query timeout is set.
     * The result is rounded to the next second.
     *
     * @return the timeout in seconds
     * @throws SQLException if this object is closed
     */
    public int getQueryTimeout() throws SQLException {
        try {
            debugCodeCall("getQueryTimeout");
            checkClosed();
            return conn.getQueryTimeout();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the current query timeout in seconds. Calling this method will
     * commit an open transaction, even if the value is the same as before.
     * Changing the value will affect all statements of this connection. This
     * method does not commit a transaction, and rolling back a transaction does
     * not affect this setting.
     *
     * @param seconds the timeout in seconds - 0 means no timeout, values
     *            smaller 0 will throw an exception
     * @throws SQLException if this object is closed
     */
    public void setQueryTimeout(int seconds) throws SQLException {
        try {
            debugCodeCall("setQueryTimeout", seconds);
            checkClosed();
            if (seconds < 0) {
                throw Message.getInvalidValueException("" + seconds, "seconds");
            }
            conn.setQueryTimeout(seconds);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Adds a statement to the batch.
     *
     * @param sql the SQL statement
     */
    public void addBatch(String sql) throws SQLException {
        try {
            debugCodeCall("addBatch", sql);
            checkClosed();
            sql = conn.translateSQL(sql, escapeProcessing);
            if (batchCommands == null) {
                batchCommands = ObjectArray.newInstance();
            }
            batchCommands.add(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears the batch.
     */
    public void clearBatch() throws SQLException {
        try {
            debugCodeCall("clearBatch");
            checkClosed();
            batchCommands = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes the batch.
     *
     * @return the array of update counts
     */
    public int[] executeBatch() throws SQLException {
        try {
            debugCodeCall("executeBatch");
            checkClosedForWrite();
            if (batchCommands == null) {
                // TODO batch: check what other database do if no commands are set
                batchCommands = ObjectArray.newInstance();
            }
            int[] result = new int[batchCommands.size()];
            boolean error = false;
            for (int i = 0; i < batchCommands.size(); i++) {
                String sql = batchCommands.get(i);
                try {
                    result[i] = executeUpdate(sql);
                } catch (SQLException e) {
                    logAndConvert(e);
                    //## Java 1.4 begin ##
                    result[i] = Statement.EXECUTE_FAILED;
                    //## Java 1.4 end ##
                    error = true;
                }
            }
            batchCommands = null;
            if (error) {
                throw new BatchUpdateException(result);
            }
            return result;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Return a result set that contains the last generated autoincrement key
     * for this connection.
     *
     * @return the result set with one row and one column containing the key
     * @throws SQLException if this object is closed
     */
    public ResultSet getGeneratedKeys() throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id, "getGeneratedKeys()");
            }
            checkClosed();
            ResultInterface result = conn.getGeneratedKeys();
            ResultSet rs = new JdbcResultSet(conn, this, result, id, false, true, false);
            return rs;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Move to the next result set.
     * This method always returns false.
     *
     * @param current Statement.CLOSE_CURRENT_RESULT,
     *          Statement.KEEP_CURRENT_RESULT,
     *          or Statement.CLOSE_ALL_RESULTS
     * @return false
     */
    public boolean getMoreResults(int current) throws SQLException {
        try {
            debugCodeCall("getMoreResults", current);
            switch (current) {
            case Statement.CLOSE_CURRENT_RESULT:
            case Statement.CLOSE_ALL_RESULTS:
                if (resultSet != null) {
                    resultSet.close();
                }
                break;
            case Statement.KEEP_CURRENT_RESULT:
                // nothing to do
                break;
            default:
                throw Message.getInvalidValueException(""+current, "current");
            }
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     * This method just calls executeUpdate(String sql).
     *
     * @param sql the SQL statement
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate("+quote(sql)+", "+autoGeneratedKeys+");");
            }
            return executeUpdate(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     * This method just calls executeUpdate(String sql).
     *
     * @param sql the SQL statement
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate("+quote(sql)+", "+quoteIntArray(columnIndexes)+");");
            }
            return executeUpdate(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     * This method just calls executeUpdate(String sql).
     *
     * @param sql the SQL statement
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate("+quote(sql)+", "+quoteArray(columnNames)+");");
            }
            return executeUpdate(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     * This method just calls execute(String sql).
     *
     * @param sql the SQL statement
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("execute("+quote(sql)+", "+autoGeneratedKeys+");");
            }
            return execute(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     * This method just calls execute(String sql).
     *
     * @param sql the SQL statement
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("execute("+quote(sql)+", "+quoteIntArray(columnIndexes)+");");
            }
            return execute(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     * This method just calls execute(String sql).
     *
     * @param sql the SQL statement
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("execute("+quote(sql)+", "+quoteArray(columnNames)+");");
            }
            return execute(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set holdability.
     *
     * @return the holdability
     */
//## Java 1.4 begin ##
    public int getResultSetHoldability() throws SQLException {
        try {
            debugCodeCall("getResultSetHoldability");
            checkClosed();
            return ResultSet.HOLD_CURSORS_OVER_COMMIT;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
//## Java 1.4 end ##

    // =============================================================

    /**
     * Check if this connection is closed.
     * The next operation is a read request.
     *
     * @return true if the session was re-connected
     * @throws SQLException if the connection or session is closed
     */
    boolean checkClosed() throws SQLException {
        return checkClosed(false);
    }

    /**
     * Check if this connection is closed.
     * The next operation may be a write request.
     *
     * @return true if the session was re-connected
     * @throws SQLException if the connection or session is closed
     */
    boolean checkClosedForWrite() throws SQLException {
        return checkClosed(true);
    }

    /**
     * INTERNAL.
     * Check if the statement is closed.
     *
     * @param write if the next operation is possibly writing
     * @return true if a reconnect was required
     * @throws SQLException if it is closed
     */
    protected boolean checkClosed(boolean write) throws SQLException {
        if (conn == null) {
            throw Message.getSQLException(ErrorCode.OBJECT_CLOSED);
        }
        conn.checkClosed(write);
        SessionInterface s = conn.getSession();
        if (s != session) {
            session = s;
            setTrace(session.getTrace());
            return true;
        }
        return false;
    }

    /**
     * INTERNAL.
     * Close and old result set if there is still one open.
     */
    protected void closeOldResultSet() throws SQLException {
        try {
            if (!closedByResultSet) {
                if (resultSet != null) {
                    resultSet.closeInternal();
                }
            }
        } finally {
            resultSet = null;
            updateCount = -1;
        }
    }

    /**
     * INTERNAL.
     * Set the statement that is currently running.
     *
     * @param c the command
     */
    protected void setExecutingStatement(CommandInterface c) {
        conn.setExecutingStatement(c == null ? null : this);
        executingCommand = c;
    }

    /**
     * Returns whether this statement is closed.
     *
     * @return true if the statement is closed
     */
    public boolean isClosed() throws SQLException {
        try {
            debugCodeCall("isClosed");
            return conn == null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Return an object of this class if possible.
     */
/*## Java 1.6 begin ##
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw Message.getUnsupportedException("unwrap");
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Checks if unwrap can return an object of this class.
     */
/*## Java 1.6 begin ##
    public boolean isWrapperFor(Class< ? > iface) throws SQLException {
        throw Message.getUnsupportedException("isWrapperFor");
    }
## Java 1.6 end ##*/

    /**
     * Returns whether this object is poolable.
     * @return false
     */
    public boolean isPoolable() {
        debugCodeCall("isPoolable");
        return false;
    }

    /**
     * Requests that this object should be pooled or not.
     * This call is ignored.
     *
     * @param poolable the requested value
     */
    public void setPoolable(boolean poolable) {
        if (isDebugEnabled()) {
            debugCode("setPoolable("+poolable+");");
        }
    }

    /**
     * INTERNAL
     */
    public String toString() {
        return getTraceObjectName();
    }

}

