/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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
    protected boolean escapeProcessing=true;
    protected int queryTimeout;
    protected boolean queryTimeoutSet;
    protected int fetchSize;
    protected boolean fetchSizeSet;
    protected int updateCount;
    private CommandInterface executingCommand;
    private ObjectArray batchCommands;
    protected int resultSetType;
    protected boolean closedByResultSet;

    /**
     * Executes a query (select statement) and returns the result set.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * @return the result set
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET);
            if(debug()) {
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id);
                debugCodeCall("executeQuery", sql);
            }
            checkClosed();
            closeOld();
            if(escapeProcessing) {
                sql=conn.translateSQL(sql);
            }
            CommandInterface command=conn.prepareCommand(sql);
            ResultInterface result;
            synchronized(session) {
                setExecutingStatement(command);
                try {
                    boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
                    result = command.executeQuery(maxRows, scrollable);
                } finally {
                    setExecutingStatement(null);
                }
            }
            command.close();
            resultSet = new JdbcResultSet(session, conn, this, result, id, closedByResultSet);
            return resultSet;
        } catch(Throwable e) {
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
     * If autocommit is on, this statement will be committed.
     *
     * @param sql the SQL statement
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occured or a
     *         select statement was executed
     */
    public int executeUpdate(String sql) throws SQLException {
        try {
            debugCodeCall("executeUpdate", sql);
            checkClosed();
            closeOld();
            if(escapeProcessing) {
                sql = conn.translateSQL(sql);
            }
            CommandInterface command = conn.prepareCommand(sql);
            synchronized(session) {
                setExecutingStatement(command);
                try {
                    updateCount = command.executeUpdate();
                } finally {
                    setExecutingStatement(null);
                }
            }
            command.close();
            return updateCount;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes an arbitrary statement.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * If the statement is a create or drop and does not throw an exception,
     * the current transaction (if any) is committed after executing the statement.
     * If autocommit is on, and the statement is not a select, this statement will be committed.
     *
     * @return true if a result set is available, false if not
     */
    public boolean execute(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET);
            if(debug()) {
                debugCodeCall("execute", sql);
            }
            checkClosed();
            closeOld();
            if(escapeProcessing) {
                sql = conn.translateSQL(sql);
            }
            CommandInterface command=conn.prepareCommand(sql);
            boolean returnsResultSet;
            synchronized(session) {
                setExecutingStatement(command);
                try {
                    if(command.isQuery()) {
                        returnsResultSet = true;
                        boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
                        ResultInterface result = command.executeQuery(maxRows, scrollable);
                        resultSet = new JdbcResultSet(session, conn, this, result, id, closedByResultSet);
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
        } catch(Throwable e) {
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
            if(resultSet != null) {
                int id = resultSet.getTraceId();
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id);
            }
            debugCodeCall("getResultSet");
            return resultSet;
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
            closeOld();
            if(conn != null) {
                conn = null;
            }
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves to the next result set - however there is always only one result set.
     * This call also closes the current result set (if there is one).
     * Returns true if there is a next result set (that means - it always returns false).
     *
     * @return false
     * @throws SQLException if this object is closed.
     */
    public boolean getMoreResults() throws SQLException {
        try {
            debugCodeCall("getMoreResults");
            checkClosed();
            closeOld();
            return false;
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
            if(maxRows < 0) {
                throw Message.getInvalidValueException(""+maxRows, "maxRows");
            }
            this.maxRows = maxRows;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the number of rows suggested to read in one step.
     * This value cannot be higher than the maximum rows (setMaxRows)
     * set by the statement or prepared statement, otherwise an exception
     * is throws.
     *
     * @param rows the number of rows
     * @throws SQLException if this object is closed
     */
    public void setFetchSize(int rows) throws SQLException {
        try {
            debugCodeCall("setFetchSize", rows);
            checkClosed();
            if(rows<0 || (rows>0 && maxRows>0 && rows>maxRows)) {
                throw Message.getInvalidValueException(""+rows, "rows");
            }
            fetchSize=rows;
            fetchSizeSet=true;
        } catch(Throwable e) {
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set concurrency created by this object.
     *
     * @return ResultSet.CONCUR_UPDATABLE
     * @throws SQLException if this object is closed
     */
    public int getResultSetConcurrency() throws SQLException {
        try {
            debugCodeCall("getResultSetConcurrency");
            checkClosed();
            return ResultSet.CONCUR_UPDATABLE;
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("setEscapeProcessing("+enable+");");
            }
            checkClosed();
            escapeProcessing=enable;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Cancels a currently running statement.
     * This method must be called from within another
     * thread than the execute method.
     * This method is not supported in the server mode.
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
                if(c != null) {
                    c.cancel();
                }
            } finally {
                setExecutingStatement(null);
            }
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current query timeout in seconds.
     *
     * @return the timeout in seconds
     * @throws SQLException if this object is closed
     */
    public int getQueryTimeout() throws SQLException {
        try {
            debugCodeCall("getQueryTimeout");
            checkClosed();
            return queryTimeout;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the current query timeout in seconds.
     * This method will succeed, even if the functionality is not supported by the database.
     *
     * @param seconds the timeout in seconds -
     *                0 means no timeout, values smaller 0 will throw an exception
     * @throws SQLException if this object is closed
     */
    public void setQueryTimeout(int seconds) throws SQLException {
        try {
            debugCodeCall("setQueryTimeout", seconds);
            checkClosed();
            if(seconds<0) {
                throw Message.getInvalidValueException(""+seconds, "seconds");
            }
            queryTimeout=seconds;
            queryTimeoutSet=true;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Adds a statement to the batch.
     */
    public void addBatch(String sql) throws SQLException {
        try {
            debugCodeCall("addBatch", sql);
            checkClosed();
            if(escapeProcessing) {
                sql = conn.translateSQL(sql);
            }
            if(batchCommands == null) {
                batchCommands = new ObjectArray();
            }
            batchCommands.add(sql);
        } catch(Throwable e) {
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes the batch.
     *
     * @return the array of updatecounts
     */
    public int[] executeBatch() throws SQLException {
        try {
            debugCodeCall("executeBatch");
            checkClosed();
            if(batchCommands == null) {
                // TODO batch: check what other database do if no commands are set
                batchCommands = new ObjectArray();
            }
            int[] result = new int[batchCommands.size()];
            boolean error = false;
            for(int i=0; i<batchCommands.size(); i++) {
                String sql = (String) batchCommands.get(i);
                try {
                    result[i] = executeUpdate(sql);
                } catch(SQLException e) {
                    logAndConvert(e);
                    result[i] = Statement.EXECUTE_FAILED;
                    error = true;
                }
            }
            batchCommands = null;
            if(error) {
                throw new BatchUpdateException(result);
            }
            return result;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Return a result set that contains the last generated autoincrement key for this connection.
     *
     * @return the result set with one row and one column containing the key
     * @throws SQLException if this object is closed
     */
    public ResultSet getGeneratedKeys() throws SQLException {
        try {
            debugCodeCall("getGeneratedKeys");
            checkClosed();
            return conn.getGeneratedKeys(this);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public boolean getMoreResults(int current) throws SQLException {
        try {
            debugCodeCall("getMoreResults");
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
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
     * @throws SQLException if a database error occured or a
     *         select statement was executed
     */
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if(debug()) {
                debugCode("executeUpdate("+quote(sql)+", "+autoGeneratedKeys+");");
            }
            return executeUpdate(sql);
        } catch(Throwable e) {
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
     * @throws SQLException if a database error occured or a
     *         select statement was executed
     */
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            if(debug()) {
                debugCode("executeUpdate("+quote(sql)+", "+quoteIntArray(columnIndexes)+");");
            }
            return executeUpdate(sql);
        } catch(Throwable e) {
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
     * @throws SQLException if a database error occured or a
     *         select statement was executed
     */
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            if(debug()) {
                debugCode("executeUpdate("+quote(sql)+", "+quoteArray(columnNames)+");");
            }
            return executeUpdate(sql);
        } catch(Throwable e) {
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
     * @throws SQLException if a database error occured or a
     *         select statement was executed
     */
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if(debug()) {
                debugCode("execute("+quote(sql)+", "+autoGeneratedKeys+");");
            }
            return execute(sql);
        } catch(Throwable e) {
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
     * @throws SQLException if a database error occured or a
     *         select statement was executed
     */
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        try {
            if(debug()) {
                debugCode("execute("+quote(sql)+", "+quoteIntArray(columnIndexes)+");");
            }
            return execute(sql);
        } catch(Throwable e) {
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
     * @throws SQLException if a database error occured or a
     *         select statement was executed
     */
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        try {
            if(debug()) {
                debugCode("execute("+quote(sql)+", "+quoteArray(columnNames)+");");
            }
            return execute(sql);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set holdability.
     *
     * @return the holdability
     */
    public int getResultSetHoldability() throws SQLException {
        try {
            debugCodeCall("getResultSetHoldability");
            checkClosed();
            return ResultSet.HOLD_CURSORS_OVER_COMMIT;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    JdbcStatement(SessionInterface session, JdbcConnection conn, int resultSetType, int id, boolean closeWithResultSet) {
        setTrace(session.getTrace(), TraceObject.STATEMENT, id);
        this.session = session;
        this.conn = conn;
        this.resultSetType = resultSetType;
        this.closedByResultSet = closeWithResultSet;
    }

    void checkClosed() throws SQLException {
        if(conn == null) {
            throw Message.getSQLException(Message.OBJECT_CLOSED);
        }
        conn.checkClosed();
    }

    void closeOld() throws SQLException {
        try {
            if(!closedByResultSet) {
                if(resultSet != null) {
                    resultSet.closeInternal();
                }
            }
        } finally {
            resultSet = null;
            updateCount=-1;
        }
    }

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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Return an object of this class if possible.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public Object unwrap(Class<?> iface) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

    /**
     * Checks if unwrap can return an object of this class.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

//    public void finalize() {
//        if(!Database.RUN_FINALIZERS) {
//            return;
//        }
//        closeOld();
//        if(conn != null) {
//            conn = null;
//        }
//    }

}

