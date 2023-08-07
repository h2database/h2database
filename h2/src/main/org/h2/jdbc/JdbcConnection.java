/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (https://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.jdbc;

import java.io.InputStream;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.h2.api.ErrorCode;
import org.h2.api.JavaObjectSerializer;
import org.h2.command.CommandInterface;
import org.h2.engine.CastDataProvider;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.IsolationLevel;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.engine.Session.StaticSettings;
import org.h2.engine.SessionRemote;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.util.CloseWatcher;
import org.h2.util.TimeZoneProvider;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueToObjectConverter;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;

/**
 * Represents a connection (session) to a database.
 * <p>
 * Thread safety: the connection is thread-safe.
 * Different statements from the same connection may try to execute their
 * commands in parallel, but they will be executed sequentially. If real
 * concurrent execution of these commands is needed, different connections
 * should be used.
 * </p>
 */
public class JdbcConnection extends TraceObject implements Connection, JdbcConnectionBackwardsCompat,
        CastDataProvider {

    private static final String NUM_SERVERS = "numServers";
    private static final String PREFIX_SERVER = "server";

    private static boolean keepOpenStackTrace;

    private final ReentrantLock lock = new ReentrantLock();

    private final String url;
    private final String user;

    // ResultSet.HOLD_CURSORS_OVER_COMMIT
    private int holdability = 1;

    private Session session;
    private CommandInterface commit, rollback;
    private CommandInterface getReadOnly, getGeneratedKeys;
    private CommandInterface setQueryTimeout, getQueryTimeout;

    private int savepointId;
    private String catalog;
    private Statement executingStatement;
    private final CloseWatcher watcher;
    private int queryTimeoutCache = -1;

    private Map<String, String> clientInfo;

    /**
     * INTERNAL
     * the session closable object does not leak as Eclipse warns - due to the
     * CloseWatcher.
     * @param url of this connection
     * @param info of this connection
     * @param user of this connection
     * @param password for the user
     * @param forbidCreation whether database creation is forbidden
     * @throws SQLException on failure
     */
    @SuppressWarnings("resource")
    public JdbcConnection(String url, Properties info, String user, Object password, boolean forbidCreation)
            throws SQLException {
        try {
            ConnectionInfo ci = new ConnectionInfo(url, info, user, password);
            if (forbidCreation) {
                ci.setProperty("FORBID_CREATION", "TRUE");
            }
            String baseDir = SysProperties.getBaseDir();
            if (baseDir != null) {
                ci.setBaseDir(baseDir);
            }
            // this will return an embedded or server connection
            session = new SessionRemote(ci).connectEmbeddedOrServer(false);
            setTrace(session.getTrace(), TraceObject.CONNECTION, getNextId(TraceObject.CONNECTION));
            this.user = ci.getUserName();
            if (isInfoEnabled()) {
                trace.infoCode("Connection " + getTraceObjectName()
                        + " = DriverManager.getConnection("
                        + quote(ci.getOriginalURL()) + ", " + quote(this.user)
                        + ", \"\");");
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
     * @param clone connection to clone
     */
    public JdbcConnection(JdbcConnection clone) {
        this.session = clone.session;
        setTrace(session.getTrace(), TraceObject.CONNECTION, getNextId(TraceObject.CONNECTION));
        this.user = clone.user;
        this.url = clone.url;
        this.catalog = clone.catalog;
        this.commit = clone.commit;
        this.getGeneratedKeys = clone.getGeneratedKeys;
        this.getQueryTimeout = clone.getQueryTimeout;
        this.getReadOnly = clone.getReadOnly;
        this.rollback = clone.rollback;
        this.watcher = null;
        if (clone.clientInfo != null) {
            this.clientInfo = new HashMap<>(clone.clientInfo);
        }
    }

    /**
     * INTERNAL
     * @param session of this connection
     * @param user of this connection
     * @param url of this connection
     */
    public JdbcConnection(Session session, String user, String url) {
        this.session = session;
        setTrace(session.getTrace(), TraceObject.CONNECTION, getNextId(TraceObject.CONNECTION));
        this.user = user;
        this.url = url;
        this.watcher = null;
    }

    /**
     * Locks this connection with a reentrant lock.
     *
     * <pre>
     * lock();
     * try {
     *     ...
     * } finally {
     *     unlock();
     * }
     * </pre>
     */
    protected final void lock() {
        lock.lock();
    }

    /**
     * Unlocks this connection.
     *
     * @see #lock()
     */
    protected final void unlock() {
        lock.unlock();
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
            Exception ex = DbException
                    .get(ErrorCode.TRACE_CONNECTION_NOT_CLOSED);
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
            debugCodeAssign("Statement", TraceObject.STATEMENT, id, "createStatement()");
            checkClosed();
            return new JdbcStatement(this, id, ResultSet.TYPE_FORWARD_ONLY, Constants.DEFAULT_RESULT_SET_CONCURRENCY);
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
     * @throws SQLException if the connection is closed or the result set type
     *             or concurrency are not supported
     */
    @Override
    public Statement createStatement(int resultSetType,
            int resultSetConcurrency) throws SQLException {
        try {
            int id = getNextId(TraceObject.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id,
                        "createStatement(" + resultSetType + ", " + resultSetConcurrency + ')');
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkClosed();
            return new JdbcStatement(this, id, resultSetType, resultSetConcurrency);
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
                        "createStatement(" + resultSetType + ", "
                                + resultSetConcurrency + ", "
                                + resultSetHoldability + ')');
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            checkClosed();
            return new JdbcStatement(this, id, resultSetType, resultSetConcurrency);
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
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id,
                        "prepareStatement(" + quote(sql) + ')');
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, ResultSet.TYPE_FORWARD_ONLY,
                    Constants.DEFAULT_RESULT_SET_CONCURRENCY, null);
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
        try {
            int id = getNextId(TraceObject.DATABASE_META_DATA);
            debugCodeAssign("DatabaseMetaData", TraceObject.DATABASE_META_DATA, id, "getMetaData()");
            checkClosed();
            return new JdbcDatabaseMetaData(this, trace, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     * @return session
     */
    public Session getSession() {
        return session;
    }

    /**
     * Closes this connection. All open statements, prepared statements and
     * result sets that where created by this connection become invalid after
     * calling this method. If there is an uncommitted transaction, it will be
     * rolled back.
     */
    @Override
    public void close() throws SQLException {
        lock();
        try {
            debugCodeCall("close");
            final Session session = this.session;
            if (session == null) {
                return;
            }
            CloseWatcher.unregister(watcher);
            session.cancel();
            session.lock();
            try {
                if (executingStatement != null) {
                    try {
                        executingStatement.cancel();
                    } catch (NullPointerException | SQLException e) {
                        // ignore
                    }
                }
                try {
                    if (!session.isClosed()) {
                        try {
                            if (session.hasPendingTransaction()) {
                                try {
                                    rollbackInternal();
                                } catch (DbException e) {
                                    // ignore if the connection is broken or database shut down
                                    if (e.getErrorCode() != ErrorCode.CONNECTION_BROKEN_1 &&
                                            e.getErrorCode() != ErrorCode.DATABASE_IS_CLOSED) {
                                        throw e;
                                    }
                                }
                            }
                            closePreparedCommands();
                        } finally {
                            session.close();
                        }
                    }
                } finally {
                    this.session = null;
                }
            } finally {
                session.unlock();
            }
        } catch (Throwable e) {
            throw logAndConvert(e);
        } finally {
            unlock();
        }
    }

    private void closePreparedCommands() {
        commit = closeAndSetNull(commit);
        rollback = closeAndSetNull(rollback);
        getReadOnly = closeAndSetNull(getReadOnly);
        getGeneratedKeys = closeAndSetNull(getGeneratedKeys);
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
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        lock();
        try {
            if (isDebugEnabled()) {
                debugCode("setAutoCommit(" + autoCommit + ')');
            }
            checkClosed();
            final Session session = this.session;
            session.lock();
            try {
                if (autoCommit && !session.getAutoCommit()) {
                    commit();
                }
                session.setAutoCommit(autoCommit);
            } finally {
                session.unlock();
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        } finally {
            unlock();
        }
    }

    /**
     * Gets the current setting for auto commit.
     *
     * @return true for on, false for off
     * @throws SQLException if the connection is closed
     */
    @Override
    public boolean getAutoCommit() throws SQLException {
        lock();
        try {
            checkClosed();
            debugCodeCall("getAutoCommit");
            return session.getAutoCommit();
        } catch (Exception e) {
            throw logAndConvert(e);
        } finally {
            unlock();
        }
    }

    /**
     * Commits the current transaction. This call has only an effect if auto
     * commit is switched off.
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public void commit() throws SQLException {
        lock();
        try {
            debugCodeCall("commit");
            checkClosed();
            if (SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT
                    && getAutoCommit()) {
                throw DbException.get(ErrorCode.METHOD_DISABLED_ON_AUTOCOMMIT_TRUE, "commit()");
            }
            commit = prepareCommand("COMMIT", commit);
            commit.executeUpdate(null);
        } catch (Exception e) {
            throw logAndConvert(e);
        } finally {
            unlock();
        }
    }

    /**
     * Rolls back the current transaction. This call has only an effect if auto
     * commit is switched off.
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public void rollback() throws SQLException {
        lock();
        try {
            debugCodeCall("rollback");
            checkClosed();
            if (SysProperties.FORCE_AUTOCOMMIT_OFF_ON_COMMIT
                    && getAutoCommit()) {
                throw DbException.get(ErrorCode.METHOD_DISABLED_ON_AUTOCOMMIT_TRUE, "rollback()");
            }
            rollbackInternal();
        } catch (Exception e) {
            throw logAndConvert(e);
        } finally {
            unlock();
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
                debugCode("setReadOnly(" + readOnly + ')');
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
            return result.currentRow()[0].getBoolean();
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
                CommandInterface cat = prepareCommand("CALL DATABASE()",
                        Integer.MAX_VALUE);
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
     * @throws SQLException if the connection is closed or the result set type
     *             or concurrency are not supported
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id,
                        "prepareStatement(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ')');
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, resultSetType, resultSetConcurrency, null);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Changes the current transaction isolation level. Calling this method will
     * commit an open transaction, even if the new level is the same as the old
     * one.
     *
     * @param level the new transaction isolation level:
     *            Connection.TRANSACTION_READ_UNCOMMITTED,
     *            Connection.TRANSACTION_READ_COMMITTED,
     *            Connection.TRANSACTION_REPEATABLE_READ,
     *            6 (SNAPSHOT), or
     *            Connection.TRANSACTION_SERIALIZABLE
     * @throws SQLException if the connection is closed or the isolation level
     *             is not valid
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        try {
            debugCodeCall("setTransactionIsolation", level);
            checkClosed();
            if (!getAutoCommit()) {
                commit();
            }
            session.setIsolationLevel(IsolationLevel.fromJdbc(level));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    void setQueryTimeout(int seconds) throws SQLException {
        try {
            debugCodeCall("setQueryTimeout", seconds);
            checkClosed();
            setQueryTimeout = prepareCommand("SET QUERY_TIMEOUT ?",
                    setQueryTimeout);
            setQueryTimeout.getParameters().get(0)
                    .setValue(ValueInteger.get(seconds * 1000), false);
            setQueryTimeout.executeUpdate(null);
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
                getQueryTimeout = prepareCommand(!session.isOldInformationSchema()
                        ? "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME=?"
                        : "SELECT `VALUE` FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME=?", getQueryTimeout);
                getQueryTimeout.getParameters().get(0)
                        .setValue(ValueVarchar.get("QUERY_TIMEOUT"), false);
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
     * @return the isolation level
     * @throws SQLException if the connection is closed
     */
    @Override
    public int getTransactionIsolation() throws SQLException {
        try {
            debugCodeCall("getTransactionIsolation");
            checkClosed();
            return session.getIsolationLevel().getJdbc();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Changes the current result set holdability.
     *
     * @param holdability ResultSet.HOLD_CURSORS_OVER_COMMIT or
     *            ResultSet.CLOSE_CURSORS_AT_COMMIT;
     * @throws SQLException if the connection is closed or the holdability is
     *             not supported
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
            if (isDebugEnabled()) {
                debugCode("setTypeMap(" + quoteMap(map) + ')');
            }
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
     * @throws SQLException if the connection is closed or the statement is not
     *             valid
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("CallableStatement", TraceObject.CALLABLE_STATEMENT, id,
                        "prepareCall(" + quote(sql) + ')');
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
     * @throws SQLException if the connection is closed or the result set type
     *             or concurrency are not supported
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        try {
            int id = getNextId(TraceObject.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("CallableStatement", TraceObject.CALLABLE_STATEMENT, id,
                        "prepareCall(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ')');
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
     * @throws SQLException if the connection is closed or the result set type,
     *             concurrency, or holdability are not supported
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        try {
            int id = getNextId(TraceObject.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("CallableStatement", TraceObject.CALLABLE_STATEMENT, id,
                        "prepareCall(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ", "
                                + resultSetHoldability + ')');
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
            debugCodeAssign("Savepoint", TraceObject.SAVEPOINT, id, "setSavepoint()");
            checkClosed();
            CommandInterface set = prepareCommand(
                    "SAVEPOINT " + JdbcSavepoint.getName(null, savepointId),
                    Integer.MAX_VALUE);
            set.executeUpdate(null);
            JdbcSavepoint savepoint = new JdbcSavepoint(this, savepointId, null,
                    trace, id);
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
                debugCodeAssign("Savepoint", TraceObject.SAVEPOINT, id, "setSavepoint(" + quote(name) + ')');
            }
            checkClosed();
            CommandInterface set = prepareCommand(
                    "SAVEPOINT " + JdbcSavepoint.getName(name, 0),
                    Integer.MAX_VALUE);
            set.executeUpdate(null);
            return new JdbcSavepoint(this, 0, name, trace, id);
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
            if (isDebugEnabled()) {
                debugCode("rollback(" + sp.getTraceObjectName() + ')');
            }
            checkClosed();
            sp.rollback();
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
            debugCode("releaseSavepoint(savepoint)");
            checkClosed();
            convertSavepoint(savepoint).release();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private static JdbcSavepoint convertSavepoint(Savepoint savepoint) {
        if (!(savepoint instanceof JdbcSavepoint)) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1,
                    String.valueOf(savepoint));
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
                        "prepareStatement(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ", "
                                + resultSetHoldability + ')');
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, resultSetType, resultSetConcurrency, null);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement.
     *
     * @param sql the SQL statement
     * @param autoGeneratedKeys
     *            {@link Statement#RETURN_GENERATED_KEYS} if generated keys should
     *            be available for retrieval, {@link Statement#NO_GENERATED_KEYS} if
     *            generated keys should not be available
     * @return the prepared statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id,
                        "prepareStatement(" + quote(sql) + ", " + autoGeneratedKeys + ')');
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, ResultSet.TYPE_FORWARD_ONLY,
                    Constants.DEFAULT_RESULT_SET_CONCURRENCY, autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement.
     *
     * @param sql the SQL statement
     * @param columnIndexes
     *            an array of column indexes indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return the prepared statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id,
                        "prepareStatement(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ')');
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, ResultSet.TYPE_FORWARD_ONLY,
                    Constants.DEFAULT_RESULT_SET_CONCURRENCY, columnIndexes);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement.
     *
     * @param sql the SQL statement
     * @param columnNames
     *            an array of column names indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return the prepared statement
     * @throws SQLException if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id,
                        "prepareStatement(" + quote(sql) + ", " + quoteArray(columnNames) + ')');
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, ResultSet.TYPE_FORWARD_ONLY,
                    Constants.DEFAULT_RESULT_SET_CONCURRENCY, columnNames);
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
        return old == null ? session.prepareCommand(sql, Integer.MAX_VALUE)
                : old;
    }

    private static int translateGetEnd(String sql, int i, char c) {
        int len = sql.length();
        switch (c) {
        case '$': {
            if (i < len - 1 && sql.charAt(i + 1) == '$'
                    && (i == 0 || sql.charAt(i - 1) <= ' ')) {
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
            checkRunOver(i + 1, len, sql);
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
            checkRunOver(i + 1, len, sql);
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
            throw DbException.getInternalError("c=" + c);
        }
    }

    /**
     * Convert JDBC escape sequences in the SQL statement. This method throws an
     * exception if the SQL statement is null.
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
        if (!escapeProcessing || sql.indexOf('{') < 0) {
            return sql;
        }
        return translateSQLImpl(sql);
    }

    private static String translateSQLImpl(String sql) {
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
     * INTERNAL. Check if this connection is closed.
     *
     * @throws DbException if the connection or session is closed
     */
    protected void checkClosed() {
        if (session == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        if (session.isClosed()) {
            throw DbException.get(ErrorCode.DATABASE_CALLED_AT_SHUTDOWN);
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
        rollback.executeUpdate(null);
    }

    /**
     * INTERNAL
     */
    void setExecutingStatement(Statement stat) {
        executingStatement = stat;
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
            checkClosed();
            return new JdbcClob(this, ValueVarchar.EMPTY, JdbcLob.State.NEW, id);
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
            checkClosed();
            return new JdbcBlob(this, ValueVarbinary.EMPTY, JdbcLob.State.NEW, id);
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
            checkClosed();
            return new JdbcClob(this, ValueVarchar.EMPTY, JdbcLob.State.NEW, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Create a new SQLXML object with no data.
     *
     * @return the object
     */
    @Override
    public SQLXML createSQLXML() throws SQLException {
        try {
            int id = getNextId(TraceObject.SQLXML);
            debugCodeAssign("SQLXML", TraceObject.SQLXML, id, "createSQLXML()");
            checkClosed();
            return new JdbcSQLXML(this, ValueVarchar.EMPTY, JdbcLob.State.NEW, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Create a new Array object.
     *
     * @param typeName the type name
     * @param elements the values
     * @return the array
     */
    @Override
    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        try {
            int id = getNextId(TraceObject.ARRAY);
            debugCodeAssign("Array", TraceObject.ARRAY, id, "createArrayOf()");
            checkClosed();
            Value value = ValueToObjectConverter.objectToValue(session, elements, Value.ARRAY);
            return new JdbcArray(this, value, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
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
    public boolean isValid(int timeout) {
        lock();
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
        } finally {
            unlock();
        }
    }

    /**
     * Set a client property. This method always throws a SQLClientInfoException
     * in standard mode. In compatibility mode the following properties are
     * supported:
     * <ul>
     * <li>DB2: The properties: ApplicationName, ClientAccountingInformation,
     * ClientUser and ClientCorrelationToken are supported.</li>
     * <li>MySQL: All property names are supported.</li>
     * <li>Oracle: All properties in the form &lt;namespace&gt;.&lt;key name&gt;
     * are supported.</li>
     * <li>PostgreSQL: The ApplicationName property is supported.</li>
     * </ul>
     * For unsupported properties a SQLClientInfoException is thrown.
     *
     * @param name the name of the property
     * @param value the value
     */
    @Override
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClientInfo(" + quote(name) + ", " + quote(value) + ')');
            }
            checkClosed();

            // no change to property: Ignore call. This early exit fixes a
            // problem with websphere liberty resetting the client info of a
            // pooled connection to its initial values.
            if (Objects.equals(value, getClientInfo(name))) {
                return;
            }

            if (isInternalProperty(name)) {
                throw new SQLClientInfoException(
                        "Property name '" + name + " is used internally by H2.",
                        Collections.emptyMap());
            }

            Pattern clientInfoNameRegEx = getMode().supportedClientInfoPropertiesRegEx;

            if (clientInfoNameRegEx != null
                    && clientInfoNameRegEx.matcher(name).matches()) {
                if (clientInfo == null) {
                    clientInfo = new HashMap<>();
                }
                clientInfo.put(name, value);
            } else {
                throw new SQLClientInfoException(
                        "Client info name '" + name + "' not supported.",
                        Collections.emptyMap());
            }
        } catch (Exception e) {
            throw convertToClientInfoException(logAndConvert(e));
        }
    }

    private static boolean isInternalProperty(String name) {
        return NUM_SERVERS.equals(name) || name.startsWith(PREFIX_SERVER);
    }

    private static SQLClientInfoException convertToClientInfoException(
            SQLException x) {
        if (x instanceof SQLClientInfoException) {
            return (SQLClientInfoException) x;
        }
        return new SQLClientInfoException(x.getMessage(), x.getSQLState(),
                x.getErrorCode(), null, null);
    }

    /**
     * Set the client properties. This replaces all existing properties. This
     * method always throws a SQLClientInfoException in standard mode. In
     * compatibility mode some properties may be supported (see
     * setProperty(String, String) for details).
     *
     * @param properties the properties (ignored)
     */
    @Override
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClientInfo(properties)");
            }
            checkClosed();
            if (clientInfo == null) {
                clientInfo = new HashMap<>();
            } else {
                clientInfo.clear();
            }
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                setClientInfo((String) entry.getKey(),
                        (String) entry.getValue());
            }
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
            debugCodeCall("getClientInfo");
            checkClosed();
            ArrayList<String> serverList = session.getClusterServers();
            Properties p = new Properties();

            if (clientInfo != null) {
                for (Map.Entry<String, String> entry : clientInfo.entrySet()) {
                    p.setProperty(entry.getKey(), entry.getValue());
                }
            }

            p.setProperty(NUM_SERVERS, Integer.toString(serverList.size()));
            for (int i = 0; i < serverList.size(); i++) {
                p.setProperty(PREFIX_SERVER + i, serverList.get(i));
            }

            return p;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get a client property.
     *
     * @param name the client info name
     * @return the property value or null if the property is not found or not
     *         supported.
     */
    @Override
    public String getClientInfo(String name) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCodeCall("getClientInfo", name);
            }
            checkClosed();
            if (name == null) {
                throw DbException.getInvalidValueException("name", null);
            }
            return getClientInfo().getProperty(name);
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
     * Create a Clob value from this reader.
     *
     * @param x the reader
     * @param length the length (if smaller or equal than 0, all data until the
     *            end of file is read)
     * @return the value
     */
    Value createClob(Reader x, long length) {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (length <= 0) {
            length = -1;
        }
        return session.addTemporaryLob(session.getDataHandler().getLobStorage().createClob(x, length));
    }

    /**
     * Create a Blob value from this input stream.
     *
     * @param x the input stream
     * @param length the length (if smaller or equal than 0, all data until the
     *            end of file is read)
     * @return the value
     */
    Value createBlob(InputStream x, long length) {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (length <= 0) {
            length = -1;
        }
        return session.addTemporaryLob(session.getDataHandler().getLobStorage().createBlob(x, length));
    }

    /**
     * Sets the given schema name to access. Current implementation is case
     * sensitive, i.e. requires schema name to be passed in correct case.
     *
     * @param schema the schema name
     */
    @Override
    public void setSchema(String schema) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCodeCall("setSchema", schema);
            }
            checkClosed();
            session.setCurrentSchemaName(schema);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Retrieves this current schema name for this connection.
     *
     * @return current schema name
     */
    @Override
    public String getSchema() throws SQLException {
        try {
            debugCodeCall("getSchema");
            checkClosed();
            return session.getCurrentSchemaName();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     *
     * @param executor the executor used by this method
     */
    @Override
    public void abort(Executor executor) {
        // not supported
    }

    /**
     * [Not supported]
     *
     * @param executor the executor used by this method
     * @param milliseconds the TCP connection timeout
     */
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
        // not supported
    }

    /**
     * [Not supported]
     */
    @Override
    public int getNetworkTimeout() {
        return 0;
    }

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

    CompareMode getCompareMode() {
        return session.getDataHandler().getCompareMode();
    }

    @Override
    public Mode getMode() {
        return session.getMode();
    }

    /**
     * INTERNAL
     * @return StaticSettings
     */
    public StaticSettings getStaticSettings() {
        checkClosed();
        return session.getStaticSettings();
    }

    @Override
    public ValueTimestampTimeZone currentTimestamp() {
        Session session = this.session;
        if (session == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        return session.currentTimestamp();
    }

    @Override
    public TimeZoneProvider currentTimeZone() {
        Session session = this.session;
        if (session == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        return session.currentTimeZone();
    }

    @Override
    public JavaObjectSerializer getJavaObjectSerializer() {
        Session session = this.session;
        if (session == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        return session.getJavaObjectSerializer();
    }

    @Override
    public boolean zeroBasedEnums() {
        Session session = this.session;
        if (session == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        return session.zeroBasedEnums();
    }

}
