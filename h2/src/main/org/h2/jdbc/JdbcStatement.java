/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import org.h2.result.SimpleResult;
import org.h2.util.ParserUtil;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * Represents a statement.
 * <p>
 * Thread safety: the statement is not thread-safe. If the same statement is
 * used by multiple threads access to it must be synchronized. The single
 * synchronized block must include execution of the command and all operations
 * with its result.
 * </p>
 * <pre>
 * synchronized (stat) {
 *     try (ResultSet rs = stat.executeQuery(queryString)) {
 *         while (rs.next) {
 *             // Do something
 *         }
 *     }
 * }
 * synchronized (stat) {
 *     updateCount = stat.executeUpdate(commandString);
 * }
 * </pre>
 */
public class JdbcStatement extends TraceObject implements Statement, JdbcStatementBackwardsCompat {

    protected JdbcConnection conn;
    protected Session session;
    protected JdbcResultSet resultSet;
    protected long maxRows;
    protected int fetchSize = SysProperties.SERVER_RESULT_SET_FETCH_SIZE;
    protected long updateCount;
    protected JdbcResultSet generatedKeys;
    protected final int resultSetType;
    protected final int resultSetConcurrency;
    private volatile CommandInterface executingCommand;
    private ArrayList<String> batchCommands;
    private boolean escapeProcessing = true;
    private volatile boolean cancelled;
    private boolean closeOnCompletion;

    JdbcStatement(JdbcConnection conn, int id, int resultSetType, int resultSetConcurrency) {
        this.conn = conn;
        this.session = conn.getSession();
        setTrace(session.getTrace(), TraceObject.STATEMENT, id);
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
    }

    /**
     * Executes a query (select statement) and returns the result set.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * @param sql the SQL statement to execute
     * @return the result set
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id, "executeQuery(" + quote(sql) + ')');
            }
            final Session session = this.session;
            session.lock();
            try {
                checkClosed();
                closeOldResultSet();
                sql = JdbcConnection.translateSQL(sql, escapeProcessing);
                CommandInterface command = conn.prepareCommand(sql, fetchSize);
                ResultInterface result;
                boolean lazy = false;
                boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
                boolean updatable = resultSetConcurrency == ResultSet.CONCUR_UPDATABLE;
                setExecutingStatement(command);
                try {
                    result = command.executeQuery(maxRows, scrollable);
                    lazy = result.isLazy();
                } finally {
                    if (!lazy) {
                        setExecutingStatement(null);
                    }
                }
                if (!lazy) {
                    command.close();
                }
                resultSet = new JdbcResultSet(conn, this, command, result, id, scrollable, updatable, false);
            } finally {
                session.unlock();
            }
            return resultSet;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement (insert, update, delete, create, drop)
     * and returns the update count. This method is not
     * allowed for prepared statements.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * If auto commit is on, this statement will be committed.
     * If the statement is a DDL statement (create, drop, alter) and does not
     * throw an exception, the current transaction (if any) is committed after
     * executing the statement.
     *
     * @param sql the SQL statement
     * @return the update count (number of affected rows by a DML statement or
     *         other statement able to return number of rows, or 0 if no rows
     *         were affected or the statement returned nothing, or
     *         {@link #SUCCESS_NO_INFO} if number of rows is too large for the
     *         {@code int} data type)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     * @see #executeLargeUpdate(String)
     */
    @Override
    public final int executeUpdate(String sql) throws SQLException {
        try {
            debugCodeCall("executeUpdate", sql);
            long updateCount = executeUpdateInternal(sql, null);
            return updateCount <= Integer.MAX_VALUE ? (int) updateCount : SUCCESS_NO_INFO;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement (insert, update, delete, create, drop)
     * and returns the update count. This method is not
     * allowed for prepared statements.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * If auto commit is on, this statement will be committed.
     * If the statement is a DDL statement (create, drop, alter) and does not
     * throw an exception, the current transaction (if any) is committed after
     * executing the statement.
     *
     * @param sql the SQL statement
     * @return the update count (number of affected rows by a DML statement or
     *         other statement able to return number of rows, or 0 if no rows
     *         were affected or the statement returned nothing)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public final long executeLargeUpdate(String sql) throws SQLException {
        try {
            debugCodeCall("executeLargeUpdate", sql);
            return executeUpdateInternal(sql, null);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private long executeUpdateInternal(String sql, Object generatedKeysRequest) {
        if (getClass() != JdbcStatement.class) {
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        }
        checkClosed();
        closeOldResultSet();
        sql = JdbcConnection.translateSQL(sql, escapeProcessing);
        CommandInterface command = conn.prepareCommand(sql, fetchSize);
        final Session session = this.session;
        session.lock();
        try {
            setExecutingStatement(command);
            try {
                ResultWithGeneratedKeys result = command.executeUpdate(generatedKeysRequest);
                updateCount = result.getUpdateCount();
                ResultInterface gk = result.getGeneratedKeys();
                if (gk != null) {
                    int id = getNextId(TraceObject.RESULT_SET);
                    generatedKeys = new JdbcResultSet(conn, this, command, gk, id, true, false, false);
                }
            } finally {
                setExecutingStatement(null);
            }
        } finally {
            session.unlock();
        }
        command.close();
        return updateCount;
    }

    /**
     * Executes a statement and returns type of its result. This method is not
     * allowed for prepared statements.
     * If another result set exists for this
     * statement, this will be closed (even if this statement fails).
     *
     * If the statement is a create or drop and does not throw an exception, the
     * current transaction (if any) is committed after executing the statement.
     * If auto commit is on, and the statement is not a select, this statement
     * will be committed.
     *
     * @param sql the SQL statement to execute
     * @return true if result is a result set, false otherwise
     */
    @Override
    public final boolean execute(String sql) throws SQLException {
        try {
            debugCodeCall("execute", sql);
            return executeInternal(sql, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private boolean executeInternal(String sql, Object generatedKeysRequest) {
        if (getClass() != JdbcStatement.class) {
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        }
        int id = getNextId(TraceObject.RESULT_SET);
        checkClosed();
        closeOldResultSet();
        sql = JdbcConnection.translateSQL(sql, escapeProcessing);
        CommandInterface command = conn.prepareCommand(sql, fetchSize);
        boolean lazy = false;
        boolean returnsResultSet;
        final Session session = this.session;
        session.lock();
        try {
            setExecutingStatement(command);
            try {
                if (command.isQuery()) {
                    returnsResultSet = true;
                    boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
                    boolean updatable = resultSetConcurrency == ResultSet.CONCUR_UPDATABLE;
                    ResultInterface result = command.executeQuery(maxRows, scrollable);
                    lazy = result.isLazy();
                    resultSet = new JdbcResultSet(conn, this, command, result, id, scrollable, updatable, false);
                } else {
                    returnsResultSet = false;
                    ResultWithGeneratedKeys result = command.executeUpdate(generatedKeysRequest);
                    updateCount = result.getUpdateCount();
                    ResultInterface gk = result.getGeneratedKeys();
                    if (gk != null) {
                        generatedKeys = new JdbcResultSet(conn, this, command, gk, id, true, false, false);
                    }
                }
            } finally {
                if (!lazy) {
                    setExecutingStatement(null);
                }
            }
        } finally {
            session.unlock();
        }
        if (!lazy) {
            command.close();
        }
        return returnsResultSet;
    }

    /**
     * Returns the last result set produces by this statement.
     *
     * @return the result set
     */
    @Override
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
     * @return the update count (number of affected rows by a DML statement or
     *         other statement able to return number of rows, or 0 if no rows
     *         were affected or the statement returned nothing, or -1 if
     *         statement was a query, or {@link #SUCCESS_NO_INFO} if number of
     *         rows is too large for the {@code int} data type)
     * @throws SQLException if this object is closed or invalid
     * @see #getLargeUpdateCount()
     */
    @Override
    public final int getUpdateCount() throws SQLException {
        try {
            debugCodeCall("getUpdateCount");
            checkClosed();
            return updateCount <= Integer.MAX_VALUE ? (int) updateCount : SUCCESS_NO_INFO;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the last update count of this statement.
     *
     * @return the update count (number of affected rows by a DML statement or
     *         other statement able to return number of rows, or 0 if no rows
     *         were affected or the statement returned nothing, or -1 if
     *         statement was a query)
     * @throws SQLException if this object is closed or invalid
     */
    @Override
    public final long getLargeUpdateCount() throws SQLException {
        try {
            debugCodeCall("getLargeUpdateCount");
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
    @Override
    public void close() throws SQLException {
        try {
            debugCodeCall("close");
            closeInternal();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void closeInternal() {
        final Session session = this.session;
        session.lock();
        try {
            closeOldResultSet();
            if (conn != null) {
                conn = null;
            }
        } finally {
            session.unlock();
        }
    }

    /**
     * Returns the connection that created this object.
     *
     * @return the connection
     */
    @Override
    public Connection getConnection() {
        debugCodeCall("getConnection");
        return conn;
    }

    /**
     * Gets the first warning reported by calls on this object.
     * This driver does not support warnings, and will always return null.
     *
     * @return null
     */
    @Override
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
    @Override
    public void clearWarnings() throws SQLException {
        try {
            debugCodeCall("clearWarnings");
            checkClosed();
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
    @Override
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
    @Override
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
    @Override
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
    @Override
    public int getMaxRows() throws SQLException {
        try {
            debugCodeCall("getMaxRows");
            checkClosed();
            return maxRows <= Integer.MAX_VALUE ? (int) maxRows : 0;
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
    @Override
    public long getLargeMaxRows() throws SQLException {
        try {
            debugCodeCall("getLargeMaxRows");
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
    @Override
    public void setMaxRows(int maxRows) throws SQLException {
        try {
            debugCodeCall("setMaxRows", maxRows);
            checkClosed();
            if (maxRows < 0) {
                throw DbException.getInvalidValueException("maxRows", maxRows);
            }
            this.maxRows = maxRows;
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
    @Override
    public void setLargeMaxRows(long maxRows) throws SQLException {
        try {
            debugCodeCall("setLargeMaxRows", maxRows);
            checkClosed();
            if (maxRows < 0) {
                throw DbException.getInvalidValueException("maxRows", maxRows);
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
    @Override
    public void setFetchSize(int rows) throws SQLException {
        try {
            debugCodeCall("setFetchSize", rows);
            checkClosed();
            if (rows < 0 || (rows > 0 && maxRows > 0 && rows > maxRows)) {
                throw DbException.getInvalidValueException("rows", rows);
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
    @Override
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
    @Override
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
    @Override
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
    @Override
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
    @Override
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
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setEscapeProcessing(" + enable + ')');
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
     * Operations on large objects are not interrupted,
     * only operations that process many rows.
     *
     * @throws SQLException if this object is closed
     */
    @Override
    public void cancel() throws SQLException {
        try {
            debugCodeCall("cancel");
            checkClosed();
            // executingCommand can be reset  by another thread
            CommandInterface c = executingCommand;
            try {
                if (c != null) {
                    c.cancel();
                    cancelled = true;
                }
            } finally {
                setExecutingStatement(null);
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Check whether the statement was cancelled.
     *
     * @return true if yes
     */
    public boolean isCancelled() {
        return cancelled;
    }

    /**
     * Gets the current query timeout in seconds.
     * This method will return 0 if no query timeout is set.
     * The result is rounded to the next second.
     * For performance reasons, only the first call to this method
     * will query the database. If the query timeout was changed in another
     * way than calling setQueryTimeout, this method will always return
     * the last value.
     *
     * @return the timeout in seconds
     * @throws SQLException if this object is closed
     */
    @Override
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
     * Sets the current query timeout in seconds.
     * Changing the value will affect all statements of this connection.
     * This method does not commit a transaction,
     * and rolling back a transaction does not affect this setting.
     *
     * @param seconds the timeout in seconds - 0 means no timeout, values
     *        smaller 0 will throw an exception
     * @throws SQLException if this object is closed
     */
    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        try {
            debugCodeCall("setQueryTimeout", seconds);
            checkClosed();
            if (seconds < 0) {
                throw DbException.getInvalidValueException("seconds", seconds);
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
    @Override
    public void addBatch(String sql) throws SQLException {
        try {
            debugCodeCall("addBatch", sql);
            checkClosed();
            sql = JdbcConnection.translateSQL(sql, escapeProcessing);
            if (batchCommands == null) {
                batchCommands = Utils.newSmallArrayList();
            }
            batchCommands.add(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears the batch.
     */
    @Override
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
     * If one of the batched statements fails, this database will continue.
     *
     * @return the array of update counts
     * @see #executeLargeBatch()
     */
    @Override
    public int[] executeBatch() throws SQLException {
        try {
            debugCodeCall("executeBatch");
            checkClosed();
            if (batchCommands == null) {
                batchCommands = new ArrayList<>();
            }
            int size = batchCommands.size();
            int[] result = new int[size];
            SQLException exception = new SQLException();
            for (int i = 0; i < size; i++) {
                long updateCount = executeBatchElement(batchCommands.get(i), exception);
                result[i] = updateCount <= Integer.MAX_VALUE ? (int) updateCount : SUCCESS_NO_INFO;
            }
            batchCommands = null;
            exception = exception.getNextException();
            if (exception != null) {
                throw new JdbcBatchUpdateException(exception, result);
            }
            return result;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes the batch.
     * If one of the batched statements fails, this database will continue.
     *
     * @return the array of update counts
     */
    @Override
    public long[] executeLargeBatch() throws SQLException {
        try {
            debugCodeCall("executeLargeBatch");
            checkClosed();
            if (batchCommands == null) {
                batchCommands = new ArrayList<>();
            }
            int size = batchCommands.size();
            long[] result = new long[size];
            SQLException exception = new SQLException();
            for (int i = 0; i < size; i++) {
                result[i] = executeBatchElement(batchCommands.get(i), exception);
            }
            batchCommands = null;
            exception = exception.getNextException();
            if (exception != null) {
                throw new JdbcBatchUpdateException(exception, result);
            }
            return result;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private long executeBatchElement(String sql, SQLException exception) {
        long updateCount;
        try {
            updateCount = executeUpdateInternal(sql, null);
        } catch (Exception e) {
            exception.setNextException(logAndConvert(e));
            updateCount = Statement.EXECUTE_FAILED;
        }
        return updateCount;
    }

    /**
     * Return a result set with generated keys from the latest executed command
     * or an empty result set if keys were not generated or were not requested
     * with {@link Statement#RETURN_GENERATED_KEYS}, column indexes, or column
     * names.
     * <p>
     * Generated keys are only returned from from {@code INSERT},
     * {@code UPDATE}, {@code MERGE INTO}, and {@code MERGE INTO ... USING}
     * commands.
     * </p>
     * <p>
     * If SQL command inserts or updates multiple rows with generated keys each
     * such inserted or updated row is returned. Batch methods are also
     * supported.
     * </p>
     * <p>
     * When {@link Statement#RETURN_GENERATED_KEYS} is used H2 chooses columns
     * to return automatically. The following columns are chosen:
     * </p>
     * <ul>
     * <li>Columns with sequences including {@code IDENTITY} columns and columns
     * with {@code AUTO_INCREMENT}.</li>
     * <li>Columns with other default values that are not evaluated into
     * constant expressions (like {@code DEFAULT RANDOM_UUID()}).</li>
     * <li>Columns that are included into the PRIMARY KEY constraint.</li>
     * </ul>
     * <p>
     * Exact required columns for the returning result set may be specified on
     * execution of command with names or indexes of columns.
     * </p>
     *
     * @return the possibly empty result set with generated keys
     * @throws SQLException if this object is closed
     */
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        try {
            int id = generatedKeys != null ? generatedKeys.getTraceId() : getNextId(TraceObject.RESULT_SET);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id, "getGeneratedKeys()");
            }
            checkClosed();
            if (generatedKeys == null) {
                generatedKeys = new JdbcResultSet(conn, this, null, new SimpleResult(), id, true, false, false);
            }
            return generatedKeys;
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
    @Override
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
     * Move to the next result set.
     * This method always returns false.
     *
     * @param current Statement.CLOSE_CURRENT_RESULT,
     *          Statement.KEEP_CURRENT_RESULT,
     *          or Statement.CLOSE_ALL_RESULTS
     * @return false
     */
    @Override
    public boolean getMoreResults(int current) throws SQLException {
        try {
            debugCodeCall("getMoreResults", current);
            switch (current) {
            case Statement.CLOSE_CURRENT_RESULT:
            case Statement.CLOSE_ALL_RESULTS:
                checkClosed();
                closeOldResultSet();
                break;
            case Statement.KEEP_CURRENT_RESULT:
                // nothing to do
                break;
            default:
                throw DbException.getInvalidValueException("current", current);
            }
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count. This method is not
     * allowed for prepared statements.
     *
     * @param sql the SQL statement
     * @param autoGeneratedKeys
     *            {@link Statement#RETURN_GENERATED_KEYS} if generated keys should
     *            be available for retrieval, {@link Statement#NO_GENERATED_KEYS} if
     *            generated keys should not be available
     * @return the update count (number of affected rows by a DML statement or
     *         other statement able to return number of rows, or 0 if no rows
     *         were affected or the statement returned nothing, or
     *         {@link #SUCCESS_NO_INFO} if number of rows is too large for the
     *         {@code int} data type)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     * @see #executeLargeUpdate(String, int)
     */
    @Override
    public final int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate(" + quote(sql) + ", " + autoGeneratedKeys + ')');
            }
            long updateCount = executeUpdateInternal(sql, autoGeneratedKeys == RETURN_GENERATED_KEYS);
            return updateCount <= Integer.MAX_VALUE ? (int) updateCount : SUCCESS_NO_INFO;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count. This method is not
     * allowed for prepared statements.
     *
     * @param sql the SQL statement
     * @param autoGeneratedKeys
     *            {@link Statement#RETURN_GENERATED_KEYS} if generated keys should
     *            be available for retrieval, {@link Statement#NO_GENERATED_KEYS} if
     *            generated keys should not be available
     * @return the update count (number of affected rows by a DML statement or
     *         other statement able to return number of rows, or 0 if no rows
     *         were affected or the statement returned nothing)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public final long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeLargeUpdate(" + quote(sql) + ", " + autoGeneratedKeys + ')');
            }
            return executeUpdateInternal(sql, autoGeneratedKeys == RETURN_GENERATED_KEYS);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count. This method is not
     * allowed for prepared statements.
     *
     * @param sql the SQL statement
     * @param columnIndexes
     *            an array of column indexes indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return the update count (number of affected rows by a DML statement or
     *         other statement able to return number of rows, or 0 if no rows
     *         were affected or the statement returned nothing, or
     *         {@link #SUCCESS_NO_INFO} if number of rows is too large for the
     *         {@code int} data type)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     * @see #executeLargeUpdate(String, int[])
     */
    @Override
    public final int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ')');
            }
            long updateCount = executeUpdateInternal(sql, columnIndexes);
            return updateCount <= Integer.MAX_VALUE ? (int) updateCount : SUCCESS_NO_INFO;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count. This method is not
     * allowed for prepared statements.
     *
     * @param sql the SQL statement
     * @param columnIndexes
     *            an array of column indexes indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return the update count (number of affected rows by a DML statement or
     *         other statement able to return number of rows, or 0 if no rows
     *         were affected or the statement returned nothing)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public final long executeLargeUpdate(String sql, int columnIndexes[]) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeLargeUpdate(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ')');
            }
            return executeUpdateInternal(sql, columnIndexes);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count. This method is not
     * allowed for prepared statements.
     *
     * @param sql the SQL statement
     * @param columnNames
     *            an array of column names indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return the update count (number of affected rows by a DML statement or
     *         other statement able to return number of rows, or 0 if no rows
     *         were affected or the statement returned nothing, or
     *         {@link #SUCCESS_NO_INFO} if number of rows is too large for the
     *         {@code int} data type)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     * @see #executeLargeUpdate(String, String[])
     */
    @Override
    public final int executeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate(" + quote(sql) + ", " + quoteArray(columnNames) + ')');
            }
            long updateCount = executeUpdateInternal(sql, columnNames);
            return updateCount <= Integer.MAX_VALUE ? (int) updateCount : SUCCESS_NO_INFO;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count. This method is not
     * allowed for prepared statements.
     *
     * @param sql the SQL statement
     * @param columnNames
     *            an array of column names indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public final long executeLargeUpdate(String sql, String columnNames[]) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeLargeUpdate(" + quote(sql) + ", " + quoteArray(columnNames) + ')');
            }
            return executeUpdateInternal(sql, columnNames);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns type of its result. This method is not
     * allowed for prepared statements.
     *
     * @param sql the SQL statement
     * @param autoGeneratedKeys
     *            {@link Statement#RETURN_GENERATED_KEYS} if generated keys should
     *            be available for retrieval, {@link Statement#NO_GENERATED_KEYS} if
     *            generated keys should not be available
     * @return true if result is a result set, false otherwise
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public final boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("execute(" + quote(sql) + ", " + autoGeneratedKeys + ')');
            }
            return executeInternal(sql, autoGeneratedKeys == RETURN_GENERATED_KEYS);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns type of its result. This method is not
     * allowed for prepared statements.
     *
     * @param sql the SQL statement
     * @param columnIndexes
     *            an array of column indexes indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return true if result is a result set, false otherwise
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public final boolean execute(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("execute(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ')');
            }
            return executeInternal(sql, columnIndexes);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns type of its result. This method is not
     * allowed for prepared statements.
     *
     * @param sql the SQL statement
     * @param columnNames
     *            an array of column names indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return true if result is a result set, false otherwise
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public final boolean execute(String sql, String[] columnNames) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("execute(" + quote(sql) + ", " + quoteArray(columnNames) + ')');
            }
            return executeInternal(sql, columnNames);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set holdability.
     *
     * @return the holdability
     */
    @Override
    public int getResultSetHoldability() throws SQLException {
        try {
            debugCodeCall("getResultSetHoldability");
            checkClosed();
            return ResultSet.HOLD_CURSORS_OVER_COMMIT;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Specifies that this statement will be closed when its dependent result
     * set is closed.
     *
     * @throws SQLException
     *             if this statement is closed
     */
    @Override
    public void closeOnCompletion() throws SQLException {
        try {
            debugCodeCall("closeOnCompletion");
            checkClosed();
            closeOnCompletion = true;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns whether this statement will be closed when its dependent result
     * set is closed.
     *
     * @return {@code true} if this statement will be closed when its dependent
     *         result set is closed
     * @throws SQLException
     *             if this statement is closed
     */
    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        try {
            debugCodeCall("isCloseOnCompletion");
            checkClosed();
            return closeOnCompletion;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    void closeIfCloseOnCompletion() {
        if (closeOnCompletion) {
            try {
                closeInternal();
            } catch (Exception e) {
                // Don't re-throw
                logAndConvert(e);
            }
        }
    }

    // =============================================================

    /**
     * Check if this connection is closed.
     *
     * @throws DbException if the connection or session is closed
     */
    void checkClosed() {
        if (conn == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        conn.checkClosed();
    }

    /**
     * INTERNAL.
     * Close and old result set if there is still one open.
     */
    protected void closeOldResultSet() {
        try {
            if (resultSet != null) {
                resultSet.closeInternal(true);
            }
            if (generatedKeys != null) {
                generatedKeys.closeInternal(true);
            }
        } finally {
            cancelled = false;
            resultSet = null;
            updateCount = -1;
            generatedKeys = null;
        }
    }

    /**
     * INTERNAL.
     * Set the statement that is currently running.
     *
     * @param c the command
     */
    void setExecutingStatement(CommandInterface c) {
        if (c == null) {
            conn.setExecutingStatement(null);
        } else {
            conn.setExecutingStatement(this);
        }
        executingCommand = c;
    }

    /**
     * Called when the result set is closed.
     *
     * @param command the command
     * @param closeCommand whether to close the command
     */
    void onLazyResultSetClose(CommandInterface command, boolean closeCommand) {
        setExecutingStatement(null);
        command.stop();
        if (closeCommand) {
            command.close();
        }
    }

    /**
     * Returns whether this statement is closed.
     *
     * @return true if the statement is closed
     */
    @Override
    public boolean isClosed() throws SQLException {
        try {
            debugCodeCall("isClosed");
            return conn == null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Return an object of this class if possible.
     *
     * @param iface the class
     * @return this
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return (T) this;
            }
            throw DbException.getInvalidValueException("iface", iface);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     * @return whether or not the interface is assignable from this class
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    /**
     * Returns whether this object is poolable.
     * @return false
     */
    @Override
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
    @Override
    public void setPoolable(boolean poolable) {
        if (isDebugEnabled()) {
            debugCode("setPoolable(" + poolable + ')');
        }
    }

    /**
     * @param identifier
     *            identifier to quote if required, may be quoted or unquoted
     * @param alwaysQuote
     *            if {@code true} identifier will be quoted unconditionally
     * @return specified identifier quoted if required, explicitly requested, or
     *         if it was already quoted
     * @throws NullPointerException
     *             if identifier is {@code null}
     * @throws SQLException
     *             if identifier is not a valid identifier
     */
    @Override
    public String enquoteIdentifier(String identifier, boolean alwaysQuote) throws SQLException {
        if (isSimpleIdentifier(identifier)) {
            return alwaysQuote ? '"' + identifier + '"': identifier;
        }
        try {
            int length = identifier.length();
            if (length > 0) {
                if (identifier.charAt(0) == '"') {
                    checkQuotes(identifier, 1, length);
                    return identifier;
                } else if (identifier.startsWith("U&\"") || identifier.startsWith("u&\"")) {
                    // Check validity of double quotes
                    checkQuotes(identifier, 3, length);
                    // Check validity of escape sequences
                    StringUtils.decodeUnicodeStringSQL(identifier, '\\');
                    return identifier;
                }
            }
            return StringUtils.quoteIdentifier(identifier);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private static void checkQuotes(String identifier, int offset, int length) {
        boolean quoted = true;
        for (int i = offset; i < length; i++) {
            if (identifier.charAt(i) == '"') {
                quoted = !quoted;
            } else if (!quoted) {
                throw DbException.get(ErrorCode.INVALID_NAME_1, identifier);
            }
        }
        if (quoted) {
            throw DbException.get(ErrorCode.INVALID_NAME_1, identifier);
        }
    }

    /**
     * @param identifier
     *            identifier to check
     * @return is specified identifier may be used without quotes
     * @throws NullPointerException
     *             if identifier is {@code null}
     */
    @Override
    public boolean isSimpleIdentifier(String identifier) throws SQLException {
        Session.StaticSettings settings;
        try {
            checkClosed();
            settings = conn.getStaticSettings();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
        return ParserUtil.isSimpleIdentifier(identifier, settings.databaseToUpper, settings.databaseToLower);
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName();
    }

}

