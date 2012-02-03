/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.h2.command.CommandInterface;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.SessionInterface;
import org.h2.engine.SessionRemote;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.util.MemoryUtils;
import org.h2.util.ObjectUtils;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/*## Java 1.6 begin ##
import java.sql.Array;
import java.sql.NClob;
import java.sql.Struct;
import java.sql.SQLXML;
import java.sql.SQLClientInfoException;
## Java 1.6 end ##*/

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

    /**
     * The stack trace of when the connection was created.
     */
    protected Exception openStackTrace;

    private String url;
    private String user;

    // ResultSet.HOLD_CURSORS_OVER_COMMIT
    private int holdability = 1;

    private SessionInterface session;
    private CommandInterface commit, rollback;
    private CommandInterface setAutoCommitTrue, setAutoCommitFalse, getAutoCommit;
    private CommandInterface getReadOnly, getGeneratedKeys;
    private CommandInterface setLockMode, getLockMode;
    private CommandInterface setQueryTimeout, getQueryTimeout;

    //## Java 1.4 begin ##
    private int savepointId;
    //## Java 1.4 end ##
    private Trace trace;
    private boolean isInternal;
    private String catalog;
    private Statement executingStatement;

    /**
     * INTERNAL
     */
    public JdbcConnection(String url, Properties info) throws SQLException {
        this(new ConnectionInfo(url, info), true);
    }

    /**
     * INTERNAL
     */
    public JdbcConnection(ConnectionInfo ci, boolean useBaseDir) throws SQLException {
        try {
            if (useBaseDir) {
                String baseDir = SysProperties.getBaseDir();
                if (baseDir != null) {
                    ci.setBaseDir(baseDir);
                }
            }
            checkJavaVersion();
            // this will return an embedded or server connection
            session = new SessionRemote().createSession(ci);
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
            openStackTrace = new Exception("Stack Trace");
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
    }

    /**
     * INTERNAL
     */
    public JdbcConnection(SessionInterface session, String user, String url) {
        isInternal = true;
        this.session = session;
        trace = session.getTrace();
        int id = getNextId(TraceObject.CONNECTION);
        setTrace(trace, TraceObject.CONNECTION, id);
        this.user = user;
        this.url = url;
    }

    /**
     * Creates a new statement.
     *
     * @return the new statement
     * @throws SQLException if the connection is closed
     */
    public Statement createStatement() throws SQLException {
        try {
            int id = getNextId(TraceObject.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id, "createStatement()");
            }
            checkClosed();
            return new JdbcStatement(this, id, ResultSet.TYPE_FORWARD_ONLY, SysProperties.DEFAULT_RESULT_SET_CONCURRENCY, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a statement with the specified result set type and concurrency.
     *
     * @return the statement
     * @throws SQLException
     *             if the connection is closed or the result set type or
     *             concurrency are not supported
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            int id = getNextId(TraceObject.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id, "createStatement(" + resultSetType + ", " + resultSetConcurrency + ")");
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
     * @return the statement
     * @throws SQLException if the connection is closed or the result set type,
     *             concurrency, or holdability are not supported
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        try {
            int id = getNextId(TraceObject.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id,
                        "createStatement(" + resultSetType + ", " + resultSetConcurrency + ", " + resultSetHoldability + ")");
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
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, ResultSet.TYPE_FORWARD_ONLY, SysProperties.DEFAULT_RESULT_SET_CONCURRENCY, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Prepare a statement that will automatically close when the result set is
     * closed. This method is used to retrieve database meta data.
     *
     * @param sql the SQL statement.
     * @return the prepared statement
     */
    PreparedStatement prepareAutoCloseStatement(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, ResultSet.TYPE_FORWARD_ONLY, SysProperties.DEFAULT_RESULT_SET_CONCURRENCY, true);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the database meta data for this database.
     *
     * @return the database meta data
     * @throws SQLException
     *             if the connection is closed
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        try {
            int id = getNextId(TraceObject.DATABASE_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign("DatabaseMetaData", TraceObject.DATABASE_META_DATA, id, "getMetaData()");
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
    public synchronized void close() throws SQLException {
        try {
            debugCodeCall("close");
            openStackTrace = null;
            if (executingStatement != null) {
                executingStatement.cancel();
            }
            if (session == null) {
                return;
            }
            session.cancel();
            try {
                synchronized (session) {
                    if (!session.isClosed()) {
                        try {
                            rollbackInternal();
                            commit = closeAndSetNull(commit);
                            rollback = closeAndSetNull(rollback);
                            setAutoCommitTrue = closeAndSetNull(setAutoCommitTrue);
                            setAutoCommitFalse = closeAndSetNull(setAutoCommitFalse);
                            getAutoCommit = closeAndSetNull(getAutoCommit);
                            getReadOnly = closeAndSetNull(getReadOnly);
                            getGeneratedKeys = closeAndSetNull(getGeneratedKeys);
                            getLockMode = closeAndSetNull(getLockMode);
                            setLockMode = closeAndSetNull(setLockMode);
                            getQueryTimeout = closeAndSetNull(getQueryTimeout);
                            setQueryTimeout = closeAndSetNull(setQueryTimeout);
                        } finally {
                            session.close();
                        }
                    }
                }
            } finally {
                session = null;
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private CommandInterface closeAndSetNull(CommandInterface command) {
        if (command != null) {
            command.close();
        }
        return null;
    }

    /**
     * Switches auto commit on or off. Calling this function does not commit the
     * current transaction.
     *
     * @param autoCommit
     *            true for auto commit on, false for off
     * @throws SQLException
     *             if the connection is closed
     */
    public synchronized void setAutoCommit(boolean autoCommit) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setAutoCommit(" + autoCommit + ");");
            }
            checkClosed();
            if (autoCommit) {
                setAutoCommitTrue = prepareCommand("SET AUTOCOMMIT TRUE", setAutoCommitTrue);
                setAutoCommitTrue.executeUpdate();
            } else {
                setAutoCommitFalse = prepareCommand("SET AUTOCOMMIT FALSE", setAutoCommitFalse);
                setAutoCommitFalse.executeUpdate();
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current setting for auto commit.
     *
     * @return true for on, false for off
     * @throws SQLException
     *             if the connection is closed
     */
    public synchronized boolean getAutoCommit() throws SQLException {
        try {
            checkClosed();
            debugCodeCall("getAutoCommit");
            return getInternalAutoCommit();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private boolean getInternalAutoCommit() throws SQLException {
        getAutoCommit = prepareCommand("CALL AUTOCOMMIT()", getAutoCommit);
        ResultInterface result = getAutoCommit.executeQuery(0, false);
        result.next();
        boolean autoCommit = result.currentRow()[0].getBoolean().booleanValue();
        result.close();
        return autoCommit;
    }

    /**
     * Commits the current transaction. This call has only an effect if
     * auto commit is switched off.
     *
     * @throws SQLException
     *             if the connection is closed
     */
    public synchronized void commit() throws SQLException {
        try {
            debugCodeCall("commit");
            checkClosedForWrite();
            commit = prepareCommand("COMMIT", commit);
            commit.executeUpdate();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Rolls back the current transaction. This call has only an effect if
     * auto commit is switched off.
     *
     * @throws SQLException
     *             if the connection is closed
     */
    public synchronized void rollback() throws SQLException {
        try {
            debugCodeCall("rollback");
            checkClosedForWrite();
            rollbackInternal();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns true if this connection has been closed.
     *
     * @return true if close was called
     */
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
     * @return the translated statement
     * @throws SQLException
     *             if the connection is closed
     */
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
     * According to the JDBC specs, this
     * setting is only a hint to the database to enable optimizations - it does
     * not cause writes to be prohibited.
     *
     * @throws SQLException
     *             if the connection is closed
     */
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
     * @throws SQLException
     *             if the connection is closed
     */
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
     * Set the default catalog name.
     * This call is ignored.
     *
     * @throws SQLException if the connection is closed
     */
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
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed or the result set type or
     *             concurrency are not supported
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, resultSetType, resultSetConcurrency, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Changes the current transaction isolation level. Calling this method will
     * commit an open transaction, even if the new level is the same as the old
     * one, except if the level is not supported. Internally, this method calls
     * SET LOCK_MODE. The following isolation levels are supported:
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
    public void setTransactionIsolation(int level) throws SQLException {
        try {
            debugCodeCall("setTransactionIsolation", level);
            checkClosed();
            int lockMode;
            switch(level) {
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
                throw Message.getInvalidValueException("" + level, "level");
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
            setQueryTimeout.getParameters().get(0).setValue(ValueInt.get(seconds * 1000), false);
            setQueryTimeout.executeUpdate();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public int getQueryTimeout() throws SQLException {
        try {
            debugCodeCall("getQueryTimeout");
            checkClosed();
            getQueryTimeout = prepareCommand("SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME=?", getQueryTimeout);
            getQueryTimeout.getParameters().get(0).setValue(ValueString.get("QUERY_TIMEOUT"), false);
            ResultInterface result = getQueryTimeout.executeQuery(0, false);
            result.next();
            int queryTimeout = result.currentRow()[0].getInt();
            result.close();
            if (queryTimeout == 0) {
                return 0;
            }
            // round to the next second, otherwise 999 millis would return 0 seconds
            return (queryTimeout + 999) / 1000;
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
            switch(lockMode) {
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
                throw Message.throwInternalError("lockMode:" + lockMode);
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
     * @throws SQLException
     *             if the connection is closed
     */
    public Map<String, Class< ? >> getTypeMap() throws SQLException {
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
    public void setTypeMap(Map<String, Class< ? >> map) throws SQLException {
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
     * @return the callable statement
     * @throws SQLException
     *             if the connection is closed or the statement is not valid
     */
    public CallableStatement prepareCall(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("CallableStatement", TraceObject.CALLABLE_STATEMENT, id, "prepareCall(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcCallableStatement(this, sql, id, ResultSet.TYPE_FORWARD_ONLY, SysProperties.DEFAULT_RESULT_SET_CONCURRENCY);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a callable statement with the specified result set type and
     * concurrency.
     *
     * @return the callable statement
     * @throws SQLException
     *             if the connection is closed or the result set type or
     *             concurrency are not supported
     */
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            int id = getNextId(TraceObject.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("CallableStatement", TraceObject.CALLABLE_STATEMENT, id, "prepareCall(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcCallableStatement(this, sql, id, resultSetType, resultSetConcurrency);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a callable statement with the specified result set type,
     * concurrency, and holdability.
     *
     * @return the callable statement
     * @throws SQLException
     *             if the connection is closed or the result set type,
     *             concurrency, or holdability are not supported
     */
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        try {
            int id = getNextId(TraceObject.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("CallableStatement", TraceObject.CALLABLE_STATEMENT, id,
                        "prepareCall(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ", "
                        + resultSetHoldability + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcCallableStatement(this, sql, id, resultSetType, resultSetConcurrency);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new unnamed savepoint.
     *
     * @return the new savepoint
     */
//## Java 1.4 begin ##
    public Savepoint setSavepoint() throws SQLException {
        try {
            int id = getNextId(TraceObject.SAVEPOINT);
            if (isDebugEnabled()) {
                debugCodeAssign("Savepoint", TraceObject.SAVEPOINT, id, "setSavepoint()");
            }
            checkClosed();
            CommandInterface set = prepareCommand("SAVEPOINT " + JdbcSavepoint.getName(null, savepointId), Integer.MAX_VALUE);
            set.executeUpdate();
            JdbcSavepoint savepoint = new JdbcSavepoint(this, savepointId, null, trace, id);
            savepointId++;
            return savepoint;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
//## Java 1.4 end ##

    /**
     * Creates a new named savepoint.
     *
     * @param name the savepoint name
     * @return the new savepoint
     */
//## Java 1.4 begin ##
    public Savepoint setSavepoint(String name) throws SQLException {
        try {
            int id = getNextId(TraceObject.SAVEPOINT);
            if (isDebugEnabled()) {
                debugCodeAssign("Savepoint", TraceObject.SAVEPOINT, id, "setSavepoint(" + quote(name) + ")");
            }
            checkClosed();
            CommandInterface set = prepareCommand("SAVEPOINT " + JdbcSavepoint.getName(name, 0), Integer.MAX_VALUE);
            set.executeUpdate();
            JdbcSavepoint savepoint = new JdbcSavepoint(this, 0, name, trace, id);
            return savepoint;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
//## Java 1.4 end ##

    /**
     * Rolls back to a savepoint.
     *
     * @param savepoint the savepoint
     */
//## Java 1.4 begin ##
    public void rollback(Savepoint savepoint) throws SQLException {
        try {
            JdbcSavepoint sp = convertSavepoint(savepoint);
            debugCode("rollback(" + sp.getTraceObjectName() + ");");
            checkClosedForWrite();
            sp.rollback();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
//## Java 1.4 end ##

    /**
     * Releases a savepoint.
     *
     * @param savepoint the savepoint to release
     */
//## Java 1.4 begin ##
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        try {
            debugCode("releaseSavepoint(savepoint);");
            checkClosed();
            convertSavepoint(savepoint).release();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private JdbcSavepoint convertSavepoint(Savepoint savepoint) throws SQLException {
        if (!(savepoint instanceof JdbcSavepoint)) {
            throw Message.getSQLException(ErrorCode.SAVEPOINT_IS_INVALID_1, "" + savepoint);
        }
        return (JdbcSavepoint) savepoint;
    }
//## Java 1.4 end ##

    /**
     * Creates a prepared statement with the specified result set type,
     * concurrency, and holdability.
     *
     * @return the prepared statement
     * @throws SQLException if the connection is closed or the result set type,
     *             concurrency, or holdability are not supported
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id,
                        "prepareStatement(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ", "
                        + resultSetHoldability + ")");
            }
            checkTypeConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, id, resultSetType, resultSetConcurrency, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement. This method just calls
     * prepareStatement(String sql).
     *
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
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
     * Creates a new prepared statement. This method just calls
     * prepareStatement(String sql).
     *
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("prepareStatement(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ");");
            }
            return prepareStatement(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement. This method just calls
     * prepareStatement(String sql).
     *
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("prepareStatement(" + quote(sql) + ", " + quoteArray(columnNames) + ");");
            }
            return prepareStatement(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    private void checkJavaVersion() throws SQLException {
        try {
            //## Java 1.4 begin ##
            // check for existence of this class (avoiding Class . forName)
            Class< ? > clazz = java.sql.Savepoint.class;
            clazz.getClass();
            //## Java 1.4 end ##
        } catch (NoClassDefFoundError e) {
            throw Message.getSQLException(ErrorCode.UNSUPPORTED_JAVA_VERSION);
        }
    }

    /**
     * Prepare an command. This will parse the SQL statement.
     *
     * @param sql the SQL statement
     * @param fetchSize the fetch size (used in remote connections)
     * @return the command
     */
    CommandInterface prepareCommand(String sql, int fetchSize) throws SQLException {
        return session.prepareCommand(sql, fetchSize);
    }

    private CommandInterface prepareCommand(String sql, CommandInterface old) throws SQLException {
        return old == null ? session.prepareCommand(sql, Integer.MAX_VALUE) : old;
    }

    private int translateGetEnd(String sql, int i, char c) throws SQLException {
        int len = sql.length();
        switch(c) {
        case '$': {
            if (i < len - 1 && sql.charAt(i + 1) == '$' && (i == 0 || sql.charAt(i - 1) <= ' ')) {
                int j = sql.indexOf("$$", i + 2);
                if (j < 0) {
                    throw Message.getSyntaxError(sql, i);
                }
                return j + 1;
            }
            return i;
        }
        case '\'': {
            int j = sql.indexOf('\'', i + 1);
            if (j < 0) {
                throw Message.getSyntaxError(sql, i);
            }
            return j;
        }
        case '"': {
            int j = sql.indexOf('"', i + 1);
            if (j < 0) {
                throw Message.getSyntaxError(sql, i);
            }
            return j;
        }
        case '/': {
            checkRunOver(i+1, len, sql);
            if (sql.charAt(i + 1) == '*') {
                // block comment
                int j = sql.indexOf("*/", i + 2);
                if (j < 0) {
                    throw Message.getSyntaxError(sql, i);
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
            throw Message.throwInternalError("c=" + c);
        }
    }

    /**
     * Convert JDBC escape sequences in the SQL statement. This
     * method throws an exception if the SQL statement is null.
     *
     * @param sql the SQL statement with or without JDBC escape sequences
     * @return the SQL statement without JDBC escape sequences
     */
    private String translateSQL(String sql) throws SQLException {
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
    String translateSQL(String sql, boolean escapeProcessing) throws SQLException {
        if (sql == null) {
            throw Message.getInvalidValueException(sql, "SQL");
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
                    chars[i++] = ' ';
                    checkRunOver(i, len, sql);
                    while (Character.isSpaceChar(chars[i])) {
                        i++;
                        checkRunOver(i, len, sql);
                    }
                    if (sql.charAt(i) != '=') {
                        throw Message.getSyntaxError(sql, i, "=");
                    }
                    chars[i++] = ' ';
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
                    remove = 2;
                } else if (found(sql, start, "t")) {
                    remove = 1;
                } else if (found(sql, start, "d")) {
                    remove = 1;
                } else if (found(sql, start, "params")) {
                    remove = "params".length();
                }
                for (i = start; remove > 0; i++, remove--) {
                    chars[i] = ' ';
                }
                break;
            case '}':
                if (--level < 0) {
                    throw Message.getSyntaxError(sql, i);
                }
                chars[i] = ' ';
                break;
            case '$':
                if (SysProperties.DOLLAR_QUOTING) {
                    i = translateGetEnd(sql, i, c);
                }
                break;
            default:
            }
        }
        if (level != 0) {
            throw Message.getSyntaxError(sql, sql.length() - 1);
        }
        if (chars != null) {
            sql = new String(chars);
        }
        return sql;
    }

    private void checkRunOver(int i, int len, String sql) throws SQLException {
        if (i >= len) {
            throw Message.getSyntaxError(sql, i);
        }
    }

    private boolean found(String sql, int start, String other) {
        return sql.regionMatches(true, start, other, 0, other.length());
    }

    private void checkTypeConcurrency(int resultSetType, int resultSetConcurrency) throws SQLException {
        switch (resultSetType) {
        case ResultSet.TYPE_FORWARD_ONLY:
        case ResultSet.TYPE_SCROLL_INSENSITIVE:
        case ResultSet.TYPE_SCROLL_SENSITIVE:
            break;
        default:
            throw Message.getInvalidValueException("" + resultSetType, "resultSetType");
        }
        switch (resultSetConcurrency) {
        case ResultSet.CONCUR_READ_ONLY:
        case ResultSet.CONCUR_UPDATABLE:
            break;
        default:
            throw Message.getInvalidValueException("" + resultSetConcurrency, "resultSetConcurrency");
        }
    }

    private void checkHoldability(int resultSetHoldability) throws SQLException {
        // TODO compatibility / correctness: DBPool uses
        // ResultSet.HOLD_CURSORS_OVER_COMMIT
        //## Java 1.4 begin ##
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT
                && resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw Message.getInvalidValueException("" + resultSetHoldability, "resultSetHoldability");
        }
        //## Java 1.4 end ##
    }

    /**
     * INTERNAL.
     * Check if this connection is closed.
     * The next operation is a read request.
     *
     * @throws SQLException if the connection or session is closed
     */
    protected void checkClosed() throws SQLException {
        checkClosed(false);
    }

    /**
     * Check if this connection is closed.
     * The next operation may be a write request.
     *
     * @throws SQLException if the connection or session is closed
     */
    private void checkClosedForWrite() throws SQLException {
        checkClosed(true);
    }

    /**
     * INTERNAL.
     * Check if this connection is closed.
     *
     * @param write if the next operation is possibly writing
     * @throws SQLException if the connection or session is closed
     */
    protected void checkClosed(boolean write) throws SQLException {
        if (session == null) {
            throw Message.getSQLException(ErrorCode.OBJECT_CLOSED);
        }
        if (session.isClosed()) {
            throw Message.getSQLException(ErrorCode.DATABASE_CALLED_AT_SHUTDOWN);
        }
        if (session.isReconnectNeeded(write)) {
            trace.debug("reconnect");
            session = session.reconnect();
            setTrace(session.getTrace());
        }
    }

    String getURL() throws SQLException {
        checkClosed();
        return url;
    }

    String getUser() throws SQLException {
        checkClosed();
        return user;
    }

    protected void finalize() {
        if (!SysProperties.runFinalize) {
            return;
        }
        if (isInternal) {
            return;
        }
        if (session != null && openStackTrace != null) {
            trace.error("Connection not closed", openStackTrace);
            try {
                close();
            } catch (SQLException e) {
                trace.debug("finalize", e);
            }
        }
    }

    private void rollbackInternal() throws SQLException {
        rollback = prepareCommand("ROLLBACK", rollback);
        rollback.executeUpdate();
    }

    /**
     * INTERNAL
     */
    public int getPowerOffCount() {
        return (session == null || session.isClosed()) ? 0 : session.getPowerOffCount();
    }

    /**
     * INTERNAL
     */
    public void setPowerOffCount(int count) throws SQLException {
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

    ResultInterface getGeneratedKeys() throws SQLException {
        getGeneratedKeys = prepareCommand("CALL IDENTITY()", getGeneratedKeys);
        return getGeneratedKeys.executeQuery(0, false);
    }

    /**
     * Create a new empty Clob object.
     *
     * @return the object
     */
    public Clob createClob() throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("Clob", TraceObject.CLOB, id, "createClob()");
            checkClosedForWrite();
            ValueLob v = ValueLob.createSmallLob(Value.CLOB, MemoryUtils.EMPTY_BYTES);
            return new JdbcClob(this, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Create a new empty Blob object.
     *
     * @return the object
     */
    public Blob createBlob() throws SQLException {
        try {
            int id = getNextId(TraceObject.BLOB);
            debugCodeAssign("Blob", TraceObject.BLOB, id, "createClob()");
            checkClosedForWrite();
            ValueLob v = ValueLob.createSmallLob(Value.BLOB, MemoryUtils.EMPTY_BYTES);
            return new JdbcBlob(this, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Create a new empty NClob object.
     *
     * @return the object
     */
/*## Java 1.6 begin ##
    public NClob createNClob() throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("NClob", TraceObject.CLOB, id, "createNClob()");
            checkClosedForWrite();
            ValueLob v = ValueLob.createSmallLob(Value.CLOB, MemoryUtils.EMPTY_BYTES);
            return new JdbcClob(this, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Create a new empty SQLXML object.
     */
/*## Java 1.6 begin ##
    public SQLXML createSQLXML() throws SQLException {
        throw Message.getUnsupportedException("SQLXML");
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Create a new empty Array object.
     */
/*## Java 1.6 begin ##
    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        throw Message.getUnsupportedException("createArray");
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Create a new empty Struct object.
     */
/*## Java 1.6 begin ##
    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        throw Message.getUnsupportedException("Struct");
    }
## Java 1.6 end ##*/

    /**
     * Returns true if this connection is still valid.
     *
     * @param timeout the number of seconds to wait for the database to respond
     *            (ignored)
     * @return true if the connection is valid.
     */
    public synchronized boolean isValid(int timeout) {
        try {
            debugCodeCall("isValid", timeout);
            if (session == null || session.isClosed()) {
                return false;
            }
            // force a network round trip (if networked)
            getInternalAutoCommit();
            return true;
        } catch (Exception e) {
            // this method doesn't throw an exception, but it logs it
            logAndConvert(e);
            return false;
        }
    }

    /**
     * [Not supported] Set a client property.
     */
/*## Java 1.6 begin ##
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Set the client properties.
     */
/*## Java 1.6 begin ##
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Get the client properties.
     */
/*## Java 1.6 begin ##
    public Properties getClientInfo() throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Set a client property.
     */
/*## Java 1.6 begin ##
    public String getClientInfo(String name) throws SQLException {
        throw Message.getUnsupportedException("clientInfo");
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Return an object of this class if possible.
     *
     * @param iface the class
     */
/*## Java 1.6 begin ##
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw Message.getUnsupportedException("unwrap");
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     */
/*## Java 1.6 begin ##
    public boolean isWrapperFor(Class< ? > iface) throws SQLException {
        throw Message.getUnsupportedException("isWrapperFor");
    }
## Java 1.6 end ##*/

    /**
     * Create a Clob value from this reader.
     *
     * @param x the reader
     * @param length the length (if smaller or equal than 0, all data until the
     *            end of file is read)
     * @return the value
     */
    public Value createClob(Reader x, long length) throws SQLException {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (length <= 0) {
            length = -1;
        }
        Value v = ValueLob.createClob(x, length, session.getDataHandler());
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
    public Value createBlob(InputStream x, long length) throws SQLException {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (length <= 0) {
            length = -1;
        }
        Value v = ValueLob.createBlob(x, length, session.getDataHandler());
        return v;
    }

    private void checkMap(Map<String, Class< ? >> map) throws SQLException {
        if (map != null && map.size() > 0) {
            throw Message.getUnsupportedException("map.size > 0");
        }
    }

    /**
     * INTERNAL
     */
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
    Object convertToDefaultObject(Value v) throws SQLException {
        Object o;
        switch (v.getType()) {
        case Value.CLOB: {
            if (SysProperties.RETURN_LOB_OBJECTS) {
                int id = getNextId(TraceObject.CLOB);
                o = new JdbcClob(this, v, id);
            } else {
                o = v.getObject();
            }
            break;
        }
        case Value.BLOB: {
            if (SysProperties.RETURN_LOB_OBJECTS) {
                int id = getNextId(TraceObject.BLOB);
                o = new JdbcBlob(this, v, id);
            } else {
                o = v.getObject();
            }
            break;
        }
        case Value.JAVA_OBJECT:
            o = ObjectUtils.deserialize(v.getBytesNoCopy());
            break;
        default:
            o = v.getObject();
        }
        return o;
    }

}
