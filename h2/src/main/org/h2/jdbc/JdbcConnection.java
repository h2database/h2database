/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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
import org.h2.expression.ParameterInterface;
import org.h2.jdbcx.JdbcConnectionListener;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.util.ClassUtils;
import org.h2.util.TempFileDeleter;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;

//#ifdef JDK16
/*
import java.sql.Array;
import java.sql.NClob;
import java.sql.Struct;
import java.sql.SQLXML;
import java.sql.SQLClientInfoException;
*/
//#endif

/**
 * Represents a connection (session) to a database.
 */
public class JdbcConnection extends TraceObject implements Connection {
    // TODO test: check if enough synchronization on jdbc objects
    // TODO feature auto-reconnect on lost connection!

    private String url;
    private String user;
    
    private int holdability = 1; // ResultSet.HOLD_CURSORS_OVER_COMMIT
    
    private SessionInterface session;
    private CommandInterface commit, rollback;
    private CommandInterface setAutoCommitTrue, setAutoCommitFalse, getAutoCommit;
    private CommandInterface getReadOnly, getGeneratedKeys;
    private CommandInterface setLockMode, getLockMode;
    private Exception openStackTrace;
//#ifdef JDK14
    private int savepointId;
//#endif
    private Trace trace;
    private JdbcConnectionListener listener;
    private boolean isInternal;
    private String catalog;
    private Statement executingStatement;

    /**
     * Creates a new statement.
     *
     * @return the new statement
     * @throws SQLException if the connection is closed
     */
    public Statement createStatement() throws SQLException {
        try {
            int id = getNextId(TraceObject.STATEMENT);
            if (debug()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id, "createStatement()");
            }
            checkClosed();
            return new JdbcStatement(session, this, ResultSet.TYPE_FORWARD_ONLY, id, false);
        } catch (Throwable e) {
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
            if (debug()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id, "createStatement(" + resultSetType + ", " + resultSetConcurrency + ")");
            }
            checkClosed();
            checkTypeAndConcurrency(resultSetType, resultSetConcurrency);
            return new JdbcStatement(session, this, resultSetType, id, false);
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a statement with the specified result set type, concurrency, and holdability.
     *
     * @return the statement
     * @throws SQLException
     *             if the connection is closed or the result set type,
     *             concurrency, or holdability are not supported
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        try {
            int id = getNextId(TraceObject.STATEMENT);
            if (debug()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id, 
                        "createStatement(" + resultSetType + ", " + resultSetConcurrency + ", " + resultSetHoldability + ")");
            }
            checkClosed();
            checkTypeAndConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            return new JdbcStatement(session, this, resultSetType, id, false);
        } catch (Throwable e) {
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
            if (debug()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(session, this, sql, ResultSet.TYPE_FORWARD_ONLY, id, false);
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }

    PreparedStatement prepareAutoCloseStatement(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (debug()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(session, this, sql, ResultSet.TYPE_FORWARD_ONLY, id, true);
        } catch (Throwable e) {
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
            if (debug()) {
                debugCodeAssign("DatabaseMetaData", TraceObject.DATABASE_META_DATA, id, "getMetaData()");
            }
            checkClosed();
            return new JdbcDatabaseMetaData(this, trace, id);
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public void setJdbcConnectionListener(JdbcConnectionListener listener) {
        this.listener = listener;
    }

    /**
     * Closes this connection. All open statements, prepared statements and
     * result sets that where created by this connection become invalid after
     * calling this method. If there is an uncommitted transaction, it will be
     * rolled back.
     */
    public void close() throws SQLException {
        TempFileDeleter.deleteUnused();
        synchronized (this) {
            if (listener == null) {
                closeConnection();
            } else {
                listener.closed(this);
            }
        }
    }

    /**
     * INTERNAL
     */
    public void closeConnection() throws SQLException {
        try {
            debugCodeCall("close");
            if (executingStatement != null) {
                executingStatement.cancel();
            }
            if (session == null) {
                return;
            }
            synchronized (session) {
                try {
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
                        } finally {
                            session.close();
                        }
                    }
                } finally {
                    session = null;
                }
            }
        } catch (Throwable e) {
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
            if (debug()) {
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
        } catch (Throwable e) {
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
        } catch (Throwable e) {
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
            checkClosed();
            commit = prepareCommand("COMMIT", commit);
            commit.executeUpdate();
        } catch (Throwable e) {
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
            checkClosed();
            rollbackInternal();
        } catch (Throwable e) {
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
        } catch (Throwable e) {
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
        } catch (Throwable e) {
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
            if (debug()) {
                debugCode("setReadOnly(" + readOnly + ");");
            }
            checkClosed();
        } catch (Throwable e) {
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
        } catch (Throwable e) {
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
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current catalog name.
     *
     * @throws SQLException
     *             if the connection is closed
     */
    public String getCatalog() throws SQLException {
        try {
            debugCodeCall("getCatalog");
            checkClosed();
            if (catalog == null) {
                CommandInterface cat = prepareCommand("CALL DATABASE()");
                ResultInterface result = cat.executeQuery(0, false);
                result.next();
                catalog = result.currentRow()[0].getString();
                cat.close();
            }
            return catalog;
        } catch (Throwable e) {
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
        } catch (Throwable e) {
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
        } catch (Throwable e) {
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
            if (debug()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ")");
            }
            checkClosed();
            checkTypeAndConcurrency(resultSetType, resultSetConcurrency);
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(session, this, sql, resultSetType, id, false);
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Changes the current transaction isolation level. Calling this method will
     * commit any open transactions, even if the new level is the same as the
     * old one, except if the level is not supported.
     *
     * @param level the new transaction isolation level,
     *            Connection.TRANSACTION_READ_UNCOMMITTED,
     *            Connection.TRANSACTION_READ_COMMITTED, or
     *            Connection.TRANSACTION_SERIALIZABLE
     * @throws SQLException if the connection is closed or the isolation level is not supported
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
            ((ParameterInterface) setLockMode.getParameters().get(0)).setValue(ValueInt.get(lockMode));
            setLockMode.executeUpdate();
        } catch (Throwable e) {
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
                throw Message.getInternalError("lockMode:" + lockMode);
            }
            return transactionIsolationLevel;
        } catch (Throwable e) {
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
        } catch (Throwable e) {
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
        } catch (Throwable e) {
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
    public Map getTypeMap() throws SQLException {
        try {
            debugCodeCall("getTypeMap");
            checkClosed();
            return null;
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Partially supported] Sets the type map. This is only supported if the map is empty or null.
     */
    public void setTypeMap(Map map) throws SQLException {
        try {
            debugCode("setTypeMap(" + quoteMap(map) + ");");
            checkMap(map);
        } catch (Throwable e) {
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
            if (debug()) {
                debugCodeAssign("CallableStatement", TraceObject.CALLABLE_STATEMENT, id, "prepareCall(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcCallableStatement(session, this, sql, ResultSet.TYPE_FORWARD_ONLY, id);
        } catch (Throwable e) {
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
            if (debug()) {
                debugCodeAssign("CallableStatement", TraceObject.CALLABLE_STATEMENT, id, "prepareCall(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ")");
            }
            checkClosed();
            checkTypeAndConcurrency(resultSetType, resultSetConcurrency);
            sql = translateSQL(sql);
            return new JdbcCallableStatement(session, this, sql, resultSetType, id);
        } catch (Throwable e) {
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
            if (debug()) {
                debugCodeAssign("CallableStatement", TraceObject.CALLABLE_STATEMENT, id, 
                        "prepareCall(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ", "
                        + resultSetHoldability + ")");
            }
            checkClosed();
            checkTypeAndConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            sql = translateSQL(sql);
            return new JdbcCallableStatement(session, this, sql, resultSetType, id);
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new unnamed savepoint.
     *
     * @return the new savepoint
     */
//#ifdef JDK14
    public Savepoint setSavepoint() throws SQLException {
        try {
            int id = getNextId(TraceObject.SAVEPOINT);
            if (debug()) {
                debugCodeAssign("Savepoint", TraceObject.SAVEPOINT, id, "setSavepoint()");
            }
            checkClosed();
            CommandInterface set = prepareCommand("SAVEPOINT " + JdbcSavepoint.getName(null, savepointId));
            set.executeUpdate();
            JdbcSavepoint savepoint = new JdbcSavepoint(this, savepointId, null, trace, id);
            savepointId++;
            return savepoint;
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }
//#endif

    /**
     * Creates a new named savepoint.
     *
     * @return the new savepoint
     */
//#ifdef JDK14
    public Savepoint setSavepoint(String name) throws SQLException {
        try {
            int id = getNextId(TraceObject.SAVEPOINT);
            if (debug()) {
                debugCodeAssign("Savepoint", TraceObject.SAVEPOINT, id, "setSavepoint(" + quote(name) + ")");
            }
            checkClosed();
            CommandInterface set = prepareCommand("SAVEPOINT " + JdbcSavepoint.getName(name, 0));
            set.executeUpdate();
            JdbcSavepoint savepoint = new JdbcSavepoint(this, 0, name, trace, id);
            return savepoint;
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }
//#endif

    /**
     * Rolls back to a savepoint.
     */
//#ifdef JDK14
    public void rollback(Savepoint savepoint) throws SQLException {
        try {
            JdbcSavepoint sp = convertSavepoint(savepoint);
            debugCode("rollback(" + sp.getTraceObjectName() + ");");
            checkClosed();
            sp.rollback();
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }
//#endif

    /**
     * Releases a savepoint.
     */
//#ifdef JDK14
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        try {
            debugCode("releaseSavepoint(savepoint);");
            checkClosed();
            convertSavepoint(savepoint).release();
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }

    private JdbcSavepoint convertSavepoint(Savepoint savepoint) throws SQLException {
        if (!(savepoint instanceof JdbcSavepoint)) {
            throw Message.getSQLException(ErrorCode.SAVEPOINT_IS_INVALID_1, "" + savepoint);
        }
        return (JdbcSavepoint) savepoint;
    }
//#endif

    /**
     * Creates a prepared statement with the specified result set type, concurrency, and holdability.
     *
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed or the result set type, concurrency, or holdability are not supported
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        try {
            int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (debug()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id, 
                        "prepareStatement(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ", "
                        + resultSetHoldability + ")");
            }
            checkClosed();
            checkTypeAndConcurrency(resultSetType, resultSetConcurrency);
            checkHoldability(resultSetHoldability);
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(session, this, sql, resultSetType, id, false);
        } catch (Throwable e) {
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
            if (debug()) {
                debugCode("prepareStatement(" + quote(sql) + ", " + autoGeneratedKeys + ");");
            }
            return prepareStatement(sql);
        } catch (Throwable e) {
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
            if (debug()) {
                debugCode("prepareStatement(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ");");
            }
            return prepareStatement(sql);
        } catch (Throwable e) {
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
            if (debug()) {
                debugCode("prepareStatement(" + quote(sql) + ", " + quoteArray(columnNames) + ");");
            }
            return prepareStatement(sql);
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    /**
     * INTERNAL
     */
    public JdbcConnection(String url, Properties info) throws SQLException {
        try {
            checkJavaVersion();
            ConnectionInfo ci = new ConnectionInfo(url, info);
            if (ci.isRemote()) {
                session = new SessionRemote().createSession(ci);
            } else {
                SessionInterface si = (SessionInterface) ClassUtils.loadClass("org.h2.engine.Session").newInstance();
                String baseDir = SysProperties.getBaseDir();
                if (baseDir != null) {
                    ci.setBaseDir(baseDir);
                }
                session = si.createSession(ci);
            }
            trace = session.getTrace();
            int id = getNextId(TraceObject.CONNECTION);
            setTrace(trace, TraceObject.CONNECTION, id);
            this.user = ci.getUserName();
            if (info()) {
                trace.infoCode("Connection " + getTraceObjectName() 
                        + " = DriverManager.getConnection(" + quote(url)
                        + ", " + quote(user) + ", \"\")");
            }
            this.url = ci.getURL();
            openStackTrace = new Exception("Stack Trace");
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public JdbcConnection(SessionInterface session, String user, String url) throws SQLException {
        isInternal = true;
        this.session = session;
        trace = session.getTrace();
        int id = getNextId(TraceObject.CONNECTION);
        setTrace(trace, TraceObject.CONNECTION, id);
        this.user = user;
        this.url = url;
    }

    private void checkJavaVersion() throws SQLException {
        try {
//#ifdef JDK14
            // check for existence of this class (avoiding Class . forName)
            Class clazz = java.sql.Savepoint.class;
            clazz.getClass();
//#endif
        } catch (Throwable e) {
            throw Message.getSQLException(ErrorCode.UNSUPPORTED_JAVA_VERSION);
        }
    }

    CommandInterface prepareCommand(String sql) throws SQLException {
        return session.prepareCommand(sql);
    }

    CommandInterface prepareCommand(String sql, CommandInterface old) throws SQLException {
        return old == null ? session.prepareCommand(sql) : old;
    }
    
    private int translateGetEnd(String sql, int i, char c) throws SQLException {
        int len = sql.length();
        switch(c) {
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
            throw Message.getInternalError("c=" + c);
        }
    }

    String translateSQL(String sql) throws SQLException {
        if (sql == null || sql.indexOf('{') < 0) {
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
                    // TODO nativeSQL: '? = ...' : register out parameter
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

    private void checkTypeAndConcurrency(int resultSetType, int resultSetConcurrency) throws SQLException {
        // TODO compatibility / correctness: OpenOffice uses TYPE_SCROLL_SENSITIVE
//        if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
//            throw Message.getInvalidValueException("" + resultSetType, "resultSetType");
//        }
//        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
//            throw Message.getInvalidValueException("" + resultSetConcurrency, "resultSetConcurrency");
//        }
    }

    private void checkHoldability(int resultSetHoldability) throws SQLException {
        // TODO compatibility / correctness: DBPool uses ResultSet.HOLD_CURSORS_OVER_COMMIT
//#ifdef JDK14
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT
                && resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw Message.getInvalidValueException("" + resultSetHoldability, "resultSetHoldability");
        }
//#endif
    }

    void checkClosed() throws SQLException {
        if (session == null) {
            throw Message.getSQLException(ErrorCode.OBJECT_CLOSED);
        }
        if (session.isClosed()) {
            throw Message.getSQLException(ErrorCode.DATABASE_CALLED_AT_SHUTDOWN);
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
        if (session != null) {
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

    ResultInterface getGeneratedKeys(JdbcStatement statement, int id) throws SQLException {
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
            checkClosed();
            ValueLob v = ValueLob.createSmallLob(Value.CLOB, new byte[0]);
            return new JdbcClob(session, this, v, id);
        } catch (Throwable e) {
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
            checkClosed();
            ValueLob v = ValueLob.createSmallLob(Value.BLOB, new byte[0]);
            return new JdbcBlob(session, this, v, id);
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Create a new empty NClob object.
     *
     * @return the object
     */
//#ifdef JDK16
/*
    public NClob createNClob() throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("NClob", TraceObject.CLOB, id);
            debugCodeCall("createNClob");
            checkClosed();
            ValueLob v = ValueLob.createSmallLob(Value.CLOB, new byte[0]);
            return new JdbcClob(session, this, v, id);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }
*/
//#endif

    /**
     * [Not supported] Create a new empty SQLXML object.
     */
//#ifdef JDK16
/*
    public SQLXML createSQLXML() throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
//#endif
    
    /**
     * [Not supported] Create a new empty Array object.
     */
//#ifdef JDK16
/*
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
//#endif
    
    /**
     * [Not supported] Create a new empty Struct object.
     */
//#ifdef JDK16    
/*
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw Message.getUnsupportedException();
    }    
*/
//#endif

    /**
     * Returns true if this connection is still valid.
     *
     * @return true if the connection is valid.
     */
    public synchronized boolean isValid(int timeout) {
        try {
            debugCodeCall("isValid", timeout);
            checkClosed();
            getInternalAutoCommit();
            return true;
        } catch (Throwable e) {
            // this method doesn't throw an exception, but it logs it
            logAndConvert(e);
            return false;
        }
    }

    /**
     * [Not supported] Set a client property.
     */
//#ifdef JDK16    
/*
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }
*/
//#endif

    /**
     * [Not supported] Set the client properties.
     */
//#ifdef JDK16
/*
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }
*/
//#endif
    
    /**
     * [Not supported] Get the client properties.
     */
//#ifdef JDK16
/*
    public Properties getClientInfo() throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }
*/
//#endif    

    /**
     * [Not supported] Set a client property.
     */
    public String getClientInfo(String name) throws SQLException {
        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported] Return an object of this class if possible.
     */
//#ifdef JDK16
/*
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
//#endif

    /**
     * [Not supported] Checks if unwrap can return an object of this class.
     */
//#ifdef JDK16
/*
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
//#endif
    
    Value createClob(Reader x, long length) throws SQLException {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (length <= 0) {
            length = -1;
        }
        Value v = ValueLob.createClob(x, length, session.getDataHandler());
        return v;
    }

    Value createBlob(InputStream x, long length) throws SQLException {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (length <= 0) {
            length = -1;
        }
        Value v = ValueLob.createBlob(x, length, session.getDataHandler());
        return v;
    }
    
    private void checkMap(Map map) throws SQLException {
        if (map != null && map.size() > 0) {
            throw Message.getUnsupportedException();
        }
    }
    
    /**
     * INTERNAL
     */
    public String toString() {
        return getTraceObjectName() + ": url=" + url + " user=" + user;
    }
    
}
