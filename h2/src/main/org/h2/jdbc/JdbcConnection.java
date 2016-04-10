/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.SessionInterface;
import org.h2.engine.SessionRemote;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.util.CloseWatcher;
import org.h2.util.JdbcUtils;
import org.h2.util.Utils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/*## Java 1.7 ##
import java.util.concurrent.Executor;
//*/

/**
 * <p>
 * Represents a connection (session) to a database.
 * </p>
 * <p>
 * Thread safety: the connection is thread-safe, because access
 * is synchronized. However, for compatibility with other databases, a
 * connection should only be used in one thread at any time.
 * </p>
 */
public class JdbcConnection extends TraceObject implements Connection {

    private static boolean keepOpenStackTrace;

    private final String url;
    private final String user;

    // ResultSet.HOLD_CURSORS_OVER_COMMIT
    private int holdability = 1;

    private SessionInterface session;
    private CommandInterface commit, rollback;
    private CommandInterface getReadOnly, getGeneratedKeys;
    private CommandInterface setLockMode, getLockMode;
    private CommandInterface setQueryTimeout, getQueryTimeout;

    private int savepointId;
    private String catalog;
    private Statement executingStatement;
    private final CloseWatcher watcher;
    private int queryTimeoutCache = -1;
    private volatile JdbcDatabaseMetaData metaData;

    /**
     * INTERNAL
     */
    public JdbcConnection(String url, Properties info) throws SQLException {
        this(new ConnectionInfo(url, info), true);
    }

    /**
     * INTERNAL
     */
    public JdbcConnection(ConnectionInfo ci, boolean useBaseDir)
            throws SQLException {
        try {
            if (useBaseDir) {
                String baseDir = SysProperties.getBaseDir();
                if (baseDir != null) {
                    ci.setBaseDir(baseDir);
                }
            }
            // this will return an embedded or server connection
            session = new SessionRemote(ci).connectEmbeddedOrServer(false);
            trace = session.getTrace();
            int id = getNextId(TraceObject.CONNECTION);
            setTrace(trace, TraceObject.CONNECTION, id);
            this.user = ci.getUserName();
            if (isInfoEnabled()) {
                trace.infoCode("Connection " + getTraceObjectName()
                        + " = DriverManager.getConnection(" + quote(ci.getOriginalURL())
                        + ", " + quote(user) + ", \"\");");
            }
            this.url = ci.getURL();
            closeOld();
            watcher = CloseWatcher.register(this, session, keepOpenStackTrace);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public JdbcConnection(JdbcConnection clone) {
        this.session = clone.session;
        trace = session.getTrace();
        int id = getNextId(TraceObject.CONNECTION);
        setTrace(trace, TraceObject.CONNECTION, id);
        this.user = clone.user;
        this.url = clone.url;
        this.catalog = clone.catalog;
        this.commit = clone.commit;
        this.getGeneratedKeys = clone.getGeneratedKeys;
        this.getLockMode = clone.getLockMode;
        this.getQueryTimeout = clone.getQueryTimeout;
        this.getReadOnly = clone.getReadOnly;
        this.rollback = clone.rollback;
        this.watcher = null;
    }

    /**
     * INTERNAL
     */
    public JdbcConnection(SessionInterface session, String user, String url) {
        this.session = session;
        trace = session.getTrace();
        int id = getNextId(TraceObject.CONNECTION);
        setTrace(trace, TraceObject.CONNECTION, id);
        this.user = user;
        this.url = url;
        this.watcher = null;
    }

    private void closeOld() {
        while (true) {
            CloseWatcher w = CloseWatcher.pollUnclosed();
            if (w == null) {
                break;
            }
            try {
                w.getCloseable().close();
            } catch (Exception e) {
                trace.error(e, "closing session");
            }
            // there was an unclosed object -
            // keep the stack trace from now on
            keepOpenStackTrace = true;
            String s = w.getOpenStackTrace();
            Exception ex = DbException.get(ErrorCode.TRACE_CONNECTION_NOT_CLOSED);
            trace.error(ex, s);
        }
    }

    /**
     * Creates a new statement.
     *
     * @return the new statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public Statement createStatement() throws SQLException {
        try {
            int id = getNextId(TraceObject.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id, "createStatement()");
            }
            checkClosed();
            return new JdbcStatement(this, id,
                    ResultSet.TYPE_FORWARD_ONLY,
                    Constants.DEFAULT_RESULT_SET_CONCURRENCY, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a statement with the specified result set type and concurrency.
     *
     * @param resultSetType the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @return the statement
     * @throws SQLException
     *             if the connection is closed or the result set type or
     *             concurrency are not supported
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        try {
            int id = getNextId(TraceObject.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id,
                        "createStatement(" + resultSetType + ", " + resultSetConcurrency + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkClosed();
            return new JdbcStatement(this, id, resultSetType, resultSetConcurrency, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a statement with the specified result set type, concurrency, and
     * holdability.
     *
     * @param resultSetType the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @param resultSetHoldability the holdability (ResultSet.HOLD* / CLOSE*)
     * @return the statement
     * @throws SQLException if the connection is closed or the result set type,
     *             concurrency, or holdability are not supported
     */
    @Override
    public Statement createStatement(int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        try {
            int id = getNextId(TraceObject.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id,
                        "createStatement(" + resultSetType + ", " +
                        resultSetConcurrency + ", " + resultSetHoldability + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            checkClosed();
            return new JdbcStatement(this, id, resultSetType, resultSetConcurrency, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement",
                        TraceObject.PREPARED_STATEMENT, id,
                        "prepareStatement(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id,
                    ResultSet.TYPE_FORWARD_ONLY,
                    Constants.DEFAULT_RESULT_SET_CONCURRENCY, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Prepare a statement that will automatically close when the result set is
     * closed. This method is used to retrieve database meta data.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    PreparedStatement prepareAutoCloseStatement(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement",
                        TraceObject.PREPARED_STATEMENT, id,
                        "prepareStatement(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id,
                    ResultSet.TYPE_FORWARD_ONLY,
                    Constants.DEFAULT_RESULT_SET_CONCURRENCY, true);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the database meta data for this database.
     *
     * @return the database meta data
     * @throws SQLException if the connection is closed
     */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        JdbcDatabaseMetaData result = this.metaData;
        if (result == null) {
            synchronized (this) {
                result = this.metaData;
                if (result == null) {
                    this.metaData = result = createMetaData();
                }
            }
        }
        return result;
    }

    private JdbcDatabaseMetaData createMetaData() throws SQLException {
        try {
            int id = getNextId(TraceObject.DATABASE_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign("DatabaseMetaData",
                        TraceObject.DATABASE_META_DATA, id, "getMetaData()");
            }
            checkClosed();
            return new JdbcDatabaseMetaData(this, trace, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public SessionInterface getSession() {
        return session;
    }

    /**
     * Closes this connection. All open statements, prepared statements and
     * result sets that where created by this connection become invalid after
     * calling this method. If there is an uncommitted transaction, it will be
     * rolled back.
     */
    @Override
    public synchronized void close() throws SQLException {
        try {
            debugCodeCall("close");
            if (session == null) {
                return;
            }
            CloseWatcher.unregister(watcher);
            session.cancel();
            if (executingStatement != null) {
                try {
                    executingStatement.cancel();
                } catch (NullPointerException e) {
                    // ignore
                }
            }
            synchronized (session) {
                try {
                    if (!session.isClosed()) {
                        try {
                            if (session.hasPendingTransaction()) {
                                // roll back unless that would require to
                                // re-connect (the transaction can't be rolled
                                // back after re-connecting)
                                if (!session.isReconnectNeeded(true)) {
                                    try {
                                        rollbackInternal();
                                    } catch (DbException e) {
                                        // ignore if the connection is broken
                                        // right now
                                        if (e.getErrorCode() !=
                                                ErrorCode.CONNECTION_BROKEN_1) {
                                            throw e;
                                        }
                                    }
                                }
                                session.afterWriting();
                            }
                            closePreparedCommands();
                        } finally {
                            session.close();
                        }
                    }
                } finally {
                    session = null;
                }
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void closePreparedCommands() {
        commit = closeAndSetNull(commit);
        rollback = closeAndSetNull(rollback);
        getReadOnly = closeAndSetNull(getReadOnly);
        getGeneratedKeys = closeAndSetNull(getGeneratedKeys);
        getLockMode = closeAndSetNull(getLockMode);
        setLockMode = closeAndSetNull(setLockMode);
        getQueryTimeout = closeAndSetNull(getQueryTimeout);
        setQueryTimeout = closeAndSetNull(setQueryTimeout);
    }

    private static CommandInterface closeAndSetNull(CommandInterface command) {
        if (command != null) {
            command.close();
        }
        return null;
    }

    /**
     * Switches auto commit on or off. Enabling it commits an uncommitted
     * transaction, if there is one.
     *
     * @param autoCommit true for auto commit on, false for off
     * @throws SQLException if the connection is closed
     */
    @Override
    public synchronized void setAutoCommit(boolean autoCommit)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setAutoCommit(" + autoCommit + ");");
            }
            checkClosed();
            if (autoCommit && !session.getAutoCommit()) {
                commit();
            }
            session.setAutoCommit(autoCommit);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current setting for auto commit.
     *
     * @return true for on, false for off
     * @throws SQLException if the connection is closed
     */
    @Override
    public synchronized boolean getAutoCommit() throws SQLException {
        try {
            checkClosed();
            debugCodeCall("getAutoCommit");
            return session.getAutoCommit();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Commits the current transaction. This call has only an effect if auto
     * commit is switched off.
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public synchronized void commit() throws SQLException {
        try {
            debugCodeCall("commit");
            checkClosedForWrite();
            try {
                commit = prepareCommand("COMMIT", commit);
                commit.executeUpdate();
            } finally {
                afterWriting();
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Rolls back the current transaction. This call has only an effect if auto
     * commit is switched off.
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public synchronized void rollback() throws SQLException {
        try {
            debugCodeCall("rollback");
            checkClosedForWrite();
            try {
                rollbackInternal();
            } finally {
                afterWriting();
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns true if this connection has been closed.
     *
     * @return true if close was called
     */
    @Override
    public boolean isClosed() throws SQLException {
        try {
            debugCodeCall("isClosed");
            return session == null || session.isClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Translates a SQL statement into the database grammar.
     *
     * @param sql the SQL statement with or without JDBC escape sequences
     * @return the translated statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public String nativeSQL(String sql) throws SQLException {
        try {
            debugCodeCall("nativeSQL", sql);
            checkClosed();
            return translateSQL(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * According to the JDBC specs, this setting is only a hint to the database
     * to enable optimizations - it does not cause writes to be prohibited.
     *
     * @param readOnly ignored
     * @throws SQLException if the connection is closed
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setReadOnly(" + readOnly + ");");
            }
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns true if the database is read-only.
     *
     * @return if the database is read-only
     * @throws SQLException if the connection is closed
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        try {
            debugCodeCall("isReadOnly");
            checkClosed();
            getReadOnly = prepareCommand("CALL READONLY()", getReadOnly);
            ResultInterface result = getReadOnly.executeQuery(0, false);
            result.next();
            boolean readOnly = result.currentRow()[0].getBoolean().booleanValue();
            return readOnly;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Set the default catalog name. This call is ignored.
     *
     * @param catalog ignored
     * @throws SQLException if the connection is closed
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        try {
            debugCodeCall("setCatalog", catalog);
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current catalog name.
     *
     * @return the catalog name
     * @throws SQLException if the connection is closed
     */
    @Override
    public String getCatalog() throws SQLException {
        try {
            debugCodeCall("getCatalog");
            checkClosed();
            if (catalog == null) {
                CommandInterface cat = prepareCommand("CALL DATABASE()", Integer.MAX_VALUE);
                ResultInterface result = cat.executeQuery(0, false);
                result.next();
                catalog = result.currentRow()[0].getString();
                cat.close();
            }
            return catalog;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the first warning reported by calls on this object.
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
     * Clears all warnings.
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
     * Creates a prepared statement with the specified result set type and
     * concurrency.
     *
     * @param sql the SQL statement
     * @param resultSetType the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed or the result set type or
     *             concurrency are not supported
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id,
                        "prepareStatement(" + quote(sql) + ", " +
                                resultSetType + ", " + resultSetConcurrency +
                                ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, resultSetType,
                    resultSetConcurrency, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Changes the current transaction isolation level. Calling this method will
     * commit an open transaction, even if the new level is the same as the old
     * one, except if the level is not supported. Internally, this method calls
     * SET LOCK_MODE, which affects all connections.
     * The following isolation levels are supported:
     * <ul>
     * <li> Connection.TRANSACTION_READ_UNCOMMITTED = SET LOCK_MODE 0: no
     * locking (should only be used for testing). </li>
     * <li>Connection.TRANSACTION_SERIALIZABLE = SET LOCK_MODE 1: table level
     * locking. </li>
     * <li>Connection.TRANSACTION_READ_COMMITTED = SET LOCK_MODE 3: table
     * level locking, but read locks are released immediately (default). </li>
     * </ul>
     * This setting is not persistent. Please note that using
     * TRANSACTION_READ_UNCOMMITTED while at the same time using multiple
     * connections may result in inconsistent transactions.
     *
     * @param level the new transaction isolation level:
     *            Connection.TRANSACTION_READ_UNCOMMITTED,
     *            Connection.TRANSACTION_READ_COMMITTED, or
     *            Connection.TRANSACTION_SERIALIZABLE
     * @throws SQLException if the connection is closed or the isolation level
     *             is not supported
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        try {
            debugCodeCall("setTransactionIsolation", level);
            checkClosed();
            int lockMode;
            switch (level) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                lockMode = Constants.LOCK_MODE_OFF;
                break;
            case Connection.TRANSACTION_READ_COMMITTED:
                lockMode = Constants.LOCK_MODE_READ_COMMITTED;
                break;
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                lockMode = Constants.LOCK_MODE_TABLE;
                break;
            default:
                throw DbException.getInvalidValueException("level", level);
            }
            commit();
            setLockMode = prepareCommand("SET LOCK_MODE ?", setLockMode);
            setLockMode.getParameters().get(0).setValue(ValueInt.get(lockMode), false);
            setLockMode.executeUpdate();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public void setQueryTimeout(int seconds) throws SQLException {
        try {
            debugCodeCall("setQueryTimeout", seconds);
            checkClosed();
            setQueryTimeout = prepareCommand("SET QUERY_TIMEOUT ?", setQueryTimeout);
            setQueryTimeout.getParameters().get(0).
                    setValue(ValueInt.get(seconds * 1000), false);
            setQueryTimeout.executeUpdate();
            queryTimeoutCache = seconds;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    int getQueryTimeout() throws SQLException {
        try {
            if (queryTimeoutCache == -1) {
                checkClosed();
                getQueryTimeout = prepareCommand(
                        "SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS " +
                        "WHERE NAME=?", getQueryTimeout);
                getQueryTimeout.getParameters().get(0).
                        setValue(ValueString.get("QUERY_TIMEOUT"), false);
                ResultInterface result = getQueryTimeout.executeQuery(0, false);
                result.next();
                int queryTimeout = result.currentRow()[0].getInt();
                result.close();
                if (queryTimeout != 0) {
                    // round to the next second, otherwise 999 millis would
                    // return 0 seconds
                    queryTimeout = (queryTimeout + 999) / 1000;
                }
                queryTimeoutCache = queryTimeout;
            }
            return queryTimeoutCache;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the current transaction isolation level.
     *
     * @return the isolation level.
     * @throws SQLException if the connection is closed
     */
    @Override
    public int getTransactionIsolation() throws SQLException {
        try {
            debugCodeCall("getTransactionIsolation");
            checkClosed();
            getLockMode = prepareCommand("CALL LOCK_MODE()", getLockMode);
            ResultInterface result = getLockMode.executeQuery(0, false);
            result.next();
            int lockMode = result.currentRow()[0].getInt();
            result.close();
            int transactionIsolationLevel;
            switch (lockMode) {
            case Constants.LOCK_MODE_OFF:
                transactionIsolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
                break;
            case Constants.LOCK_MODE_READ_COMMITTED:
                transactionIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;
                break;
            case Constants.LOCK_MODE_TABLE:
            case Constants.LOCK_MODE_TABLE_GC:
                transactionIsolationLevel = Connection.TRANSACTION_SERIALIZABLE;
                break;
            default:
                throw DbException.throwInternalError("lockMode:" + lockMode);
            }
            return transactionIsolationLevel;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Changes the current result set holdability.
     *
     * @param holdability
     *            ResultSet.HOLD_CURSORS_OVER_COMMIT or
     *            ResultSet.CLOSE_CURSORS_AT_COMMIT;
     * @throws SQLException
     *            if the connection is closed or the holdability is not
     *            supported
     */
    @Override
    public void setHoldability(int holdability) throws SQLException {
        try {
            debugCodeCall("setHoldability", holdability);
            checkClosed();
            checkHoldability(holdability);
            this.holdability = holdability;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the current result set holdability.
     *
     * @return the holdability
     * @throws SQLException if the connection is closed
     */
    @Override
    public int getHoldability() throws SQLException {
        try {
            debugCodeCall("getHoldability");
            checkClosed();
            return holdability;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the type map.
     *
     * @return null
     * @throws SQLException if the connection is closed
     */
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        try {
            debugCodeCall("getTypeMap");
            checkClosed();
            return null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Partially supported] Sets the type map. This is only supported if the
     * map is empty or null.
     */
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        try {
            debugCode("setTypeMap(" + quoteMap(map) + ");");
            checkMap(map);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new callable statement.
     *
     * @param sql the SQL statement
     * @return the callable statement
     * @throws SQLException
     *             if the connection is closed or the statement is not valid
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("CallableStatement",
                        TraceObject.CALLABLE_STATEMENT, id, "prepareCall(" +
                                quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcCallableStatement(this, sql, id,
                    ResultSet.TYPE_FORWARD_ONLY,
                    Constants.DEFAULT_RESULT_SET_CONCURRENCY);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a callable statement with the specified result set type and
     * concurrency.
     *
     * @param sql the SQL statement
     * @param resultSetType the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @return the callable statement
     * @throws SQLException
     *             if the connection is closed or the result set type or
     *             concurrency are not supported
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        try {
            int id = getNextId(TraceObject.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("CallableStatement",
                        TraceObject.CALLABLE_STATEMENT, id, "prepareCall(" +
                                quote(sql) + ", " + resultSetType + ", " +
                                resultSetConcurrency + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcCallableStatement(this, sql, id, resultSetType,
                    resultSetConcurrency);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a callable statement with the specified result set type,
     * concurrency, and holdability.
     *
     * @param sql the SQL statement
     * @param resultSetType the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @param resultSetHoldability the holdability (ResultSet.HOLD* / CLOSE*)
     * @return the callable statement
     * @throws SQLException
     *             if the connection is closed or the result set type,
     *             concurrency, or holdability are not supported
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        try {
            int id = getNextId(TraceObject.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("CallableStatement",
                        TraceObject.CALLABLE_STATEMENT, id, "prepareCall(" +
                                quote(sql) + ", " + resultSetType + ", " +
                                resultSetConcurrency + ", " +
                                resultSetHoldability + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcCallableStatement(this, sql, id, resultSetType,
                    resultSetConcurrency);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new unnamed savepoint.
     *
     * @return the new savepoint
     */
    @Override
    public Savepoint setSavepoint() throws SQLException {
        try {
            int id = getNextId(TraceObject.SAVEPOINT);
            if (isDebugEnabled()) {
                debugCodeAssign("Savepoint", TraceObject.SAVEPOINT, id, "setSavepoint()");
            }
            checkClosed();
            CommandInterface set = prepareCommand(
                    "SAVEPOINT " + JdbcSavepoint.getName(null, savepointId),
                    Integer.MAX_VALUE);
            set.executeUpdate();
            JdbcSavepoint savepoint = new JdbcSavepoint(this, savepointId, null, trace, id);
            savepointId++;
            return savepoint;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new named savepoint.
     *
     * @param name the savepoint name
     * @return the new savepoint
     */
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        try {
            int id = getNextId(TraceObject.SAVEPOINT);
            if (isDebugEnabled()) {
                debugCodeAssign("Savepoint",
                        TraceObject.SAVEPOINT, id, "setSavepoint(" + quote(name) + ")");
            }
            checkClosed();
            CommandInterface set = prepareCommand(
                    "SAVEPOINT " + JdbcSavepoint.getName(name, 0), Integer.MAX_VALUE);
            set.executeUpdate();
            JdbcSavepoint savepoint = new JdbcSavepoint(this, 0, name, trace, id);
            return savepoint;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Rolls back to a savepoint.
     *
     * @param savepoint the savepoint
     */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        try {
            JdbcSavepoint sp = convertSavepoint(savepoint);
            debugCode("rollback(" + sp.getTraceObjectName() + ");");
            checkClosedForWrite();
            try {
                sp.rollback();
            } finally {
                afterWriting();
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Releases a savepoint.
     *
     * @param savepoint the savepoint to release
     */
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        try {
            debugCode("releaseSavepoint(savepoint);");
            checkClosed();
            convertSavepoint(savepoint).release();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private static JdbcSavepoint convertSavepoint(Savepoint savepoint) {
        if (!(savepoint instanceof JdbcSavepoint)) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, "" + savepoint);
        }
        return (JdbcSavepoint) savepoint;
    }

    /**
     * Creates a prepared statement with the specified result set type,
     * concurrency, and holdability.
     *
     * @param sql the SQL statement
     * @param resultSetType the result set type (ResultSet.TYPE_*)
     * @param resultSetConcurrency the concurrency (ResultSet.CONCUR_*)
     * @param resultSetHoldability the holdability (ResultSet.HOLD* / CLOSE*)
     * @return the prepared statement
     * @throws SQLException if the connection is closed or the result set type,
     *             concurrency, or holdability are not supported
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id,
                        "prepareStatement(" + quote(sql) + ", " +
                        resultSetType + ", " + resultSetConcurrency + ", " +
                        resultSetHoldability + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id,
                    resultSetType, resultSetConcurrency, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement.
     * This method just calls prepareStatement(String sql) internally.
     * The method getGeneratedKeys only supports one column.
     *
     * @param sql the SQL statement
     * @param autoGeneratedKeys ignored
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("prepareStatement(" + quote(sql) + ", " + autoGeneratedKeys + ");");
            }
            return prepareStatement(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement.
     * This method just calls prepareStatement(String sql) internally.
     * The method getGeneratedKeys only supports one column.
     *
     * @param sql the SQL statement
     * @param columnIndexes ignored
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("prepareStatement(" + quote(sql) + ", " +
                        quoteIntArray(columnIndexes) + ");");
            }
            return prepareStatement(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement.
     * This method just calls prepareStatement(String sql) internally.
     * The method getGeneratedKeys only supports one column.
     *
     * @param sql the SQL statement
     * @param columnNames ignored
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("prepareStatement(" + quote(sql) + ", " +
                        quoteArray(columnNames) + ");");
            }
            return prepareStatement(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    /**
     * Prepare an command. This will parse the SQL statement.
     *
     * @param sql the SQL statement
     * @param fetchSize the fetch size (used in remote connections)
     * @return the command
     */
    CommandInterface prepareCommand(String sql, int fetchSize) {
        return session.prepareCommand(sql, fetchSize);
    }

    private CommandInterface prepareCommand(String sql, CommandInterface old) {
        return old == null ? session.prepareCommand(sql, Integer.MAX_VALUE) : old;
    }

    private static int translateGetEnd(String sql, int i, char c) {
        int len = sql.length();
        switch (c) {
        case '$': {
            if (i < len - 1 && sql.charAt(i + 1) == '$' &&
                    (i == 0 || sql.charAt(i - 1) <= ' ')) {
                int j = sql.indexOf("$$", i + 2);
                if (j < 0) {
                    throw DbException.getSyntaxError(sql, i);
                }
                return j + 1;
            }
            return i;
        }
        case '\'': {
            int j = sql.indexOf('\'', i + 1);
            if (j < 0) {
                throw DbException.getSyntaxError(sql, i);
            }
            return j;
        }
        case '"': {
            int j = sql.indexOf('"', i + 1);
            if (j < 0) {
                throw DbException.getSyntaxError(sql, i);
            }
            return j;
        }
        case '/': {
            checkRunOver(i+1, len, sql);
            if (sql.charAt(i + 1) == '*') {
                // block comment
                int j = sql.indexOf("*/", i + 2);
                if (j < 0) {
                    throw DbException.getSyntaxError(sql, i);
                }
                i = j + 1;
            } else if (sql.charAt(i + 1) == '/') {
                // single line comment
                i += 2;
                while (i < len && (c = sql.charAt(i)) != '\r' && c != '\n') {
                    i++;
                }
            }
            return i;
        }
        case '-': {
            checkRunOver(i+1, len, sql);
            if (sql.charAt(i + 1) == '-') {
                // single line comment
                i += 2;
                while (i < len && (c = sql.charAt(i)) != '\r' && c != '\n') {
                    i++;
                }
            }
            return i;
        }
        default:
            throw DbException.throwInternalError("c=" + c);
        }
    }

    /**
     * Convert JDBC escape sequences in the SQL statement. This
     * method throws an exception if the SQL statement is null.
     *
     * @param sql the SQL statement with or without JDBC escape sequences
     * @return the SQL statement without JDBC escape sequences
     */
    private static String translateSQL(String sql) {
        return translateSQL(sql, true);
    }

    /**
     * Convert JDBC escape sequences in the SQL statement if required. This
     * method throws an exception if the SQL statement is null.
     *
     * @param sql the SQL statement with or without JDBC escape sequences
     * @param escapeProcessing whether escape sequences should be replaced
     * @return the SQL statement without JDBC escape sequences
     */
    static String translateSQL(String sql, boolean escapeProcessing) {
        if (sql == null) {
            throw DbException.getInvalidValueException("SQL", null);
        }
        if (!escapeProcessing) {
            return sql;
        }
        if (sql.indexOf('{') < 0) {
            return sql;
        }
        int len = sql.length();
        char[] chars = null;
        int level = 0;
        for (int i = 0; i < len; i++) {
            char c = sql.charAt(i);
            switch (c) {
            case '\'':
            case '"':
            case '/':
            case '-':
                i = translateGetEnd(sql, i, c);
                break;
            case '{':
                level++;
                if (chars == null) {
                    chars = sql.toCharArray();
                }
                chars[i] = ' ';
                while (Character.isSpaceChar(chars[i])) {
                    i++;
                    checkRunOver(i, len, sql);
                }
                int start = i;
                if (chars[i] >= '0' && chars[i] <= '9') {
                    chars[i - 1] = '{';
                    while (true) {
                        checkRunOver(i, len, sql);
                        c = chars[i];
                        if (c == '}') {
                            break;
                        }
                        switch (c) {
                        case '\'':
                        case '"':
                        case '/':
                        case '-':
                            i = translateGetEnd(sql, i, c);
                            break;
                        default:
                        }
                        i++;
                    }
                    level--;
                    break;
                } else if (chars[i] == '?') {
                    i++;
                    checkRunOver(i, len, sql);
                    while (Character.isSpaceChar(chars[i])) {
                        i++;
                        checkRunOver(i, len, sql);
                    }
                    if (sql.charAt(i) != '=') {
                        throw DbException.getSyntaxError(sql, i, "=");
                    }
                    i++;
                    checkRunOver(i, len, sql);
                    while (Character.isSpaceChar(chars[i])) {
                        i++;
                        checkRunOver(i, len, sql);
                    }
                }
                while (!Character.isSpaceChar(chars[i])) {
                    i++;
                    checkRunOver(i, len, sql);
                }
                int remove = 0;
                if (found(sql, start, "fn")) {
                    remove = 2;
                } else if (found(sql, start, "escape")) {
                    break;
                } else if (found(sql, start, "call")) {
                    break;
                } else if (found(sql, start, "oj")) {
                    remove = 2;
                } else if (found(sql, start, "ts")) {
                    break;
                } else if (found(sql, start, "t")) {
                    break;
                } else if (found(sql, start, "d")) {
                    break;
                } else if (found(sql, start, "params")) {
                    remove = "params".length();
                }
                for (i = start; remove > 0; i++, remove--) {
                    chars[i] = ' ';
                }
                break;
            case '}':
                if (--level < 0) {
                    throw DbException.getSyntaxError(sql, i);
                }
                chars[i] = ' ';
                break;
            case '$':
                i = translateGetEnd(sql, i, c);
                break;
            default:
            }
        }
        if (level != 0) {
            throw DbException.getSyntaxError(sql, sql.length() - 1);
        }
        if (chars != null) {
            sql = new String(chars);
        }
        return sql;
    }

    private static void checkRunOver(int i, int len, String sql) {
        if (i >= len) {
            throw DbException.getSyntaxError(sql, i);
        }
    }

    private static boolean found(String sql, int start, String other) {
        return sql.regionMatches(true, start, other, 0, other.length());
    }

    private static void checkTypeConcurrency(int resultSetType,
            int resultSetConcurrency) {
        switch (resultSetType) {
        case ResultSet.TYPE_FORWARD_ONLY:
        case ResultSet.TYPE_SCROLL_INSENSITIVE:
        case ResultSet.TYPE_SCROLL_SENSITIVE:
            break;
        default:
            throw DbException.getInvalidValueException("resultSetType",
                    resultSetType);
        }
        switch (resultSetConcurrency) {
        case ResultSet.CONCUR_READ_ONLY:
        case ResultSet.CONCUR_UPDATABLE:
            break;
        default:
            throw DbException.getInvalidValueException("resultSetConcurrency",
                    resultSetConcurrency);
        }
    }

    private static void checkHoldability(int resultSetHoldability) {
        // TODO compatibility / correctness: DBPool uses
        // ResultSet.HOLD_CURSORS_OVER_COMMIT
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT
                && resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw DbException.getInvalidValueException("resultSetHoldability",
                    resultSetHoldability);
        }
    }

    /**
     * INTERNAL.
     * Check if this connection is closed.
     * The next operation is a read request.
     *
     * @throws DbException if the connection or session is closed
     */
    protected void checkClosed() {
        checkClosed(false);
    }

    /**
     * Check if this connection is closed.
     * The next operation may be a write request.
     *
     * @throws DbException if the connection or session is closed
     */
    private void checkClosedForWrite() {
        checkClosed(true);
    }

    /**
     * INTERNAL.
     * Check if this connection is closed.
     *
     * @param write if the next operation is possibly writing
     * @throws DbException if the connection or session is closed
     */
    protected void checkClosed(boolean write) {
        if (session == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        if (session.isClosed()) {
            throw DbException.get(ErrorCode.DATABASE_CALLED_AT_SHUTDOWN);
        }
        if (session.isReconnectNeeded(write)) {
            trace.debug("reconnect");
            closePreparedCommands();
            session = session.reconnect(write);
            trace = session.getTrace();
        }
    }

    /**
     * INTERNAL.
     * Called after executing a command that could have written something.
     */
    protected void afterWriting() {
        if (session != null) {
            session.afterWriting();
        }
    }

    String getURL() {
        checkClosed();
        return url;
    }

    String getUser() {
        checkClosed();
        return user;
    }

    private void rollbackInternal() {
        rollback = prepareCommand("ROLLBACK", rollback);
        rollback.executeUpdate();
    }

    /**
     * INTERNAL
     */
    public int getPowerOffCount() {
        return (session == null || session.isClosed()) ?
                0 : session.getPowerOffCount();
    }

    /**
     * INTERNAL
     */
    public void setPowerOffCount(int count) {
        if (session != null) {
            session.setPowerOffCount(count);
        }
    }

    /**
     * INTERNAL
     */
    public void setExecutingStatement(Statement stat) {
        executingStatement = stat;
    }

    /**
     * INTERNAL
     */
    ResultSet getGeneratedKeys(JdbcStatement stat, int id) {
        getGeneratedKeys = prepareCommand(
                "SELECT SCOPE_IDENTITY() " +
                "WHERE SCOPE_IDENTITY() IS NOT NULL", getGeneratedKeys);
        ResultInterface result = getGeneratedKeys.executeQuery(0, false);
        ResultSet rs = new JdbcResultSet(this, stat, result, id, false, true, false);
        return rs;
    }

    /**
     * Create a new empty Clob object.
     *
     * @return the object
     */
    @Override
    public Clob createClob() throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("Clob", TraceObject.CLOB, id, "createClob()");
            checkClosedForWrite();
            try {
                Value v = session.getDataHandler().getLobStorage().createClob(
                        new InputStreamReader(
                        new ByteArrayInputStream(Utils.EMPTY_BYTES)), 0);
                session.addTemporaryLob(v);
                return new JdbcClob(this, v, id);
            } finally {
                afterWriting();
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Create a new empty Blob object.
     *
     * @return the object
     */
    @Override
    public Blob createBlob() throws SQLException {
        try {
            int id = getNextId(TraceObject.BLOB);
            debugCodeAssign("Blob", TraceObject.BLOB, id, "createClob()");
            checkClosedForWrite();
            try {
                Value v = session.getDataHandler().getLobStorage().createBlob(
                        new ByteArrayInputStream(Utils.EMPTY_BYTES), 0);
                session.addTemporaryLob(v);
                return new JdbcBlob(this, v, id);
            } finally {
                afterWriting();
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Create a new empty NClob object.
     *
     * @return the object
     */
    @Override
    public NClob createNClob() throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("NClob", TraceObject.CLOB, id, "createNClob()");
            checkClosedForWrite();
            try {
                Value v = session.getDataHandler().getLobStorage().createClob(
                        new InputStreamReader(
                        new ByteArrayInputStream(Utils.EMPTY_BYTES)), 0);
                session.addTemporaryLob(v);
                return new JdbcClob(this, v, id);
            } finally {
                afterWriting();
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Create a new empty SQLXML object.
     */
    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw unsupported("SQLXML");
    }

    /**
     * [Not supported] Create a new empty Array object.
     */
    @Override
    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        throw unsupported("createArray");
    }

    /**
     * [Not supported] Create a new empty Struct object.
     */
    @Override
    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        throw unsupported("Struct");
    }

    /**
     * Returns true if this connection is still valid.
     *
     * @param timeout the number of seconds to wait for the database to respond
     *            (ignored)
     * @return true if the connection is valid.
     */
    @Override
    public synchronized boolean isValid(int timeout) {
        try {
            debugCodeCall("isValid", timeout);
            if (session == null || session.isClosed()) {
                return false;
            }
            // force a network round trip (if networked)
            getTransactionIsolation();
            return true;
        } catch (Exception e) {
            // this method doesn't throw an exception, but it logs it
            logAndConvert(e);
            return false;
        }
    }

    /**
     * Set a client property.
     * This method always throws a SQLClientInfoException.
     *
     * @param name the name of the property (ignored)
     * @param value the value (ignored)
     */
    @Override
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClientInfo("
                        +quote(name)+", "
                        +quote(value)+");");
            }
            checkClosed();
            // we don't have any client properties, so just throw
            throw new SQLClientInfoException();
        } catch (Exception e) {
            throw convertToClientInfoException(logAndConvert(e));
        }
    }

    private static SQLClientInfoException convertToClientInfoException(
            SQLException x) {
        if (x instanceof SQLClientInfoException) {
            return (SQLClientInfoException) x;
        }
        return new SQLClientInfoException(
                x.getMessage(), x.getSQLState(), x.getErrorCode(), null, null);
    }

    /**
     * Set the client properties.
     * This method always throws a SQLClientInfoException.
     *
     * @param properties the properties (ignored)
     */
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClientInfo(properties);");
            }
            checkClosed();
            // we don't have any client properties, so just throw
            throw new SQLClientInfoException();
        } catch (Exception e) {
            throw convertToClientInfoException(logAndConvert(e));
        }
    }

    /**
     * Get the client properties.
     *
     * @return the property list
     */
    @Override
    public Properties getClientInfo() throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getClientInfo();");
            }
            checkClosed();
            ArrayList<String> serverList = session.getClusterServers();
            Properties p = new Properties();

            p.setProperty("numServers", String.valueOf(serverList.size()));
            for (int i = 0; i < serverList.size(); i++) {
                p.setProperty("server" + String.valueOf(i), serverList.get(i));
            }
            return p;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get a client property.
     *
     * @param name the client info name (ignored)
     * @return the property value
     */
    @Override
    public String getClientInfo(String name) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCodeCall("getClientInfo", name);
            }
            checkClosed();
            Properties p = getClientInfo();
            String s = p.getProperty(name);
            if (s == null) {
                throw new SQLClientInfoException();
            }
            return s;
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
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw DbException.getInvalidValueException("iface", iface);
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
     * Create a Clob value from this reader.
     *
     * @param x the reader
     * @param length the length (if smaller or equal than 0, all data until the
     *            end of file is read)
     * @return the value
     */
    public Value createClob(Reader x, long length) {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (length <= 0) {
            length = -1;
        }
        Value v = session.getDataHandler().getLobStorage().createClob(x, length);
        session.addTemporaryLob(v);
        return v;
    }

    /**
     * Create a Blob value from this input stream.
     *
     * @param x the input stream
     * @param length the length (if smaller or equal than 0, all data until the
     *            end of file is read)
     * @return the value
     */
    public Value createBlob(InputStream x, long length) {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (length <= 0) {
            length = -1;
        }
        Value v = session.getDataHandler().getLobStorage().createBlob(x, length);
        session.addTemporaryLob(v);
        return v;
    }

    /**
     * [Not supported]
     *
     * @param schema the schema
     */
/*## Java 1.7 ##
    @Override
    public void setSchema(String schema) {
        // not supported
    }
//*/

    /**
     * [Not supported]
     */
/*## Java 1.7 ##
    @Override
    public String getSchema() {
        return null;
    }
//*/

    /**
     * [Not supported]
     *
     * @param executor the executor used by this method
     */
/*## Java 1.7 ##
    @Override
    public void abort(Executor executor) {
        // not supported
    }
//*/

    /**
     * [Not supported]
     *
     * @param executor the executor used by this method
     * @param milliseconds the TCP connection timeout
     */
/*## Java 1.7 ##
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
        // not supported
    }
//*/

    /**
     * [Not supported]
     */
/*## Java 1.7 ##
    @Override
    public int getNetworkTimeout() {
        return 0;
    }
//*/

    /**
     * Check that the given type map is either null or empty.
     *
     * @param map the type map
     * @throws DbException if the map is not empty
     */
    static void checkMap(Map<String, Class<?>> map) {
        if (map != null && map.size() > 0) {
            throw DbException.getUnsupportedException("map.size > 0");
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": url=" + url + " user=" + user;
    }

    /**
     * Convert an object to the default Java object for the given SQL type. For
     * example, LOB objects are converted to java.sql.Clob / java.sql.Blob.
     *
     * @param v the value
     * @return the object
     */
    Object convertToDefaultObject(Value v) {
        Object o;
        switch (v.getType()) {
        case Value.CLOB: {
            int id = getNextId(TraceObject.CLOB);
            o = new JdbcClob(this, v, id);
            break;
        }
        case Value.BLOB: {
            int id = getNextId(TraceObject.BLOB);
            o = new JdbcBlob(this, v, id);
            break;
        }
        case Value.JAVA_OBJECT:
            if (SysProperties.serializeJavaObject) {
                o = JdbcUtils.deserialize(v.getBytesNoCopy(), session.getDataHandler());
                break;
            }
        default:
            o = v.getObject();
        }
        return o;
    }

    CompareMode getCompareMode() {
        return session.getDataHandler().getCompareMode();
    }

    /**
     * INTERNAL
     */
    public void setTraceLevel(int level) {
        trace.setLevel(level);
    }

}
