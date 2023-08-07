/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Set;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Mode.CharPadding;
import org.h2.engine.Session;
import org.h2.engine.SessionLocal;
import org.h2.expression.ParameterInterface;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import org.h2.result.ResultWithPaddedStrings;
import org.h2.util.Utils;

/**
 * Represents a SQL statement. This object is only used on the server side.
 */
public abstract class Command implements CommandInterface {

    /**
     * The session.
     */
    protected final SessionLocal session;

    /**
     * The last start time.
     */
    protected long startTimeNanos;

    /**
     * The trace module.
     */
    private final Trace trace;

    /**
     * If this query was canceled.
     */
    private volatile boolean cancel;

    private final String sql;

    private boolean canReuse;

    Command(SessionLocal session, String sql) {
        this.session = session;
        this.sql = sql;
        trace = getDatabase().getTrace(Trace.COMMAND);
    }

    /**
     * Check if this command is transactional.
     * If it is not, then it forces the current transaction to commit.
     *
     * @return true if it is
     */
    public abstract boolean isTransactional();

    /**
     * Check if this command is a query.
     *
     * @return true if it is
     */
    @Override
    public abstract boolean isQuery();

    /**
     * Get the list of parameters.
     *
     * @return the list of parameters
     */
    @Override
    public abstract ArrayList<? extends ParameterInterface> getParameters();

    /**
     * Check if this command is read only.
     *
     * @return true if it is
     */
    public abstract boolean isReadOnly();

    /**
     * Get an empty result set containing the meta data.
     *
     * @return an empty result set
     */
    public abstract ResultInterface queryMeta();

    /**
     * Execute an updating statement (for example insert, delete, or update), if
     * this is possible.
     *
     * @param generatedKeysRequest
     *            {@code false} if generated keys are not needed, {@code true} if
     *            generated keys should be configured automatically, {@code int[]}
     *            to specify column indices to return generated keys from, or
     *            {@code String[]} to specify column names to return generated keys
     *            from
     * @return the update count and generated keys, if any
     * @throws DbException if the command is not an updating statement
     */
    public abstract ResultWithGeneratedKeys update(Object generatedKeysRequest);

    /**
     * Execute a query statement, if this is possible.
     *
     * @param maxrows the maximum number of rows returned
     * @return the local result set
     * @throws DbException if the command is not a query
     */
    public abstract ResultInterface query(long maxrows);

    @Override
    public final ResultInterface getMetaData() {
        return queryMeta();
    }

    /**
     * Start the stopwatch.
     */
    void start() {
        if (trace.isInfoEnabled() || getDatabase().getQueryStatistics()) {
            startTimeNanos = Utils.currentNanoTime();
        }
    }

    void setProgress(Database database, int state) {
        database.setProgress(state, sql, 0, 0);
    }

    /**
     * Check if this command has been canceled, and throw an exception if yes.
     *
     * @throws DbException if the statement has been canceled
     */
    protected void checkCanceled() {
        if (cancel) {
            cancel = false;
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
        }
    }

    @Override
    public void stop() {
        if (session.isOpen()) {
            commitIfNonTransactional();
            if (isTransactional() && session.getAutoCommit()) {
                session.commit(false);
            }
        }
        if (trace.isInfoEnabled() && startTimeNanos != 0L) {
            long timeMillis = (System.nanoTime() - startTimeNanos) / 1_000_000L;
            if (timeMillis > Constants.SLOW_QUERY_LIMIT_MS) {
                trace.info("slow query: {0} ms", timeMillis);
            }
        }
    }

    /**
     * Execute a query and return the result.
     * This method prepares everything and calls {@link #query(long)} finally.
     *
     * @param maxrows the maximum number of rows to return
     * @param scrollable if the result set must be scrollable (ignored)
     * @return the result set
     */
    @Override
    public ResultInterface executeQuery(long maxrows, boolean scrollable) {
        startTimeNanos = 0L;
        long start = 0L;
        Database database = getDatabase();
        session.waitIfExclusiveModeEnabled();
        boolean callStop = true;
        session.lock();
        try {
            session.startStatementWithinTransaction(this);
            Session oldSession = session.setThreadLocalSession();
            try {
                while (true) {
                    database.checkPowerOff();
                    try {
                        ResultInterface result = query(maxrows);
                        callStop = !result.isLazy();
                        if (database.getMode().charPadding == CharPadding.IN_RESULT_SETS) {
                            return ResultWithPaddedStrings.get(result);
                        }
                        return result;
                    } catch (DbException e) {
                        // cannot retry some commands
                        if (!isRetryable()) {
                            throw e;
                        }
                        start = filterConcurrentUpdate(e, start);
                    } catch (OutOfMemoryError e) {
                        callStop = false;
                        // there is a serious problem:
                        // the transaction may be applied partially
                        // in this case we need to panic:
                        // close the database
                        database.shutdownImmediately();
                        throw DbException.convert(e);
                    } catch (Throwable e) {
                        throw DbException.convert(e);
                    }
                }
            } catch (DbException e) {
                e = e.addSQL(sql);
                SQLException s = e.getSQLException();
                database.exceptionThrown(s, sql);
                if (s.getErrorCode() == ErrorCode.OUT_OF_MEMORY) {
                    callStop = false;
                    database.shutdownImmediately();
                    throw e;
                }
                database.checkPowerOff();
                throw e;
            } finally {
                session.resetThreadLocalSession(oldSession);
                session.endStatement();
                if (callStop) {
                    stop();
                }
            }
        } finally {
            session.unlock();
        }
    }

    @Override
    public ResultWithGeneratedKeys executeUpdate(Object generatedKeysRequest) {
        long start = 0;
        boolean callStop = true;
        session.lock();
        try {
            Database database = getDatabase();
            session.waitIfExclusiveModeEnabled();
            commitIfNonTransactional();
            SessionLocal.Savepoint rollback = session.setSavepoint();
            session.startStatementWithinTransaction(this);
            DbException ex = null;
            Session oldSession = session.setThreadLocalSession();
            try {
                while (true) {
                    database.checkPowerOff();
                    try {
                        return update(generatedKeysRequest);
                    } catch (DbException e) {
                        // cannot retry some commands
                        if (!isRetryable()) {
                            throw e;
                        }
                        start = filterConcurrentUpdate(e, start);
                    } catch (OutOfMemoryError e) {
                        callStop = false;
                        database.shutdownImmediately();
                        throw DbException.convert(e);
                    } catch (Throwable e) {
                        throw DbException.convert(e);
                    }
                }
            } catch (DbException e) {
                e = e.addSQL(sql);
                SQLException s = e.getSQLException();
                database.exceptionThrown(s, sql);
                if (s.getErrorCode() == ErrorCode.OUT_OF_MEMORY) {
                    callStop = false;
                    database.shutdownImmediately();
                    throw e;
                }
                try {
                    database.checkPowerOff();
                    if (s.getErrorCode() == ErrorCode.DEADLOCK_1) {
                        session.rollback();
                    } else {
                        session.rollbackTo(rollback);
                    }
                } catch (Throwable nested) {
                    e.addSuppressed(nested);
                }
                ex = e;
                throw e;
            } finally {
                session.resetThreadLocalSession(oldSession);
                try {
                    session.endStatement();
                    if (callStop) {
                        stop();
                    }
                } catch (Throwable nested) {
                    if (ex == null) {
                        throw nested;
                    } else {
                        ex.addSuppressed(nested);
                    }
                }
            }
        } finally {
            session.unlock();
        }
    }

    private void commitIfNonTransactional() {
        if (!isTransactional()) {
            boolean autoCommit = session.getAutoCommit();
            session.commit(true);
            if (!autoCommit && session.getAutoCommit()) {
                session.begin();
            }
        }
    }

    private long filterConcurrentUpdate(DbException e, long start) {
        int errorCode = e.getErrorCode();
        if (errorCode != ErrorCode.CONCURRENT_UPDATE_1 && errorCode != ErrorCode.ROW_NOT_FOUND_IN_PRIMARY_INDEX
                && errorCode != ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1) {
            throw e;
        }
        long now = Utils.currentNanoTime();
        if (start != 0L && now - start > session.getLockTimeout() * 1_000_000L) {
            throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, e);
        }
        return start == 0L ? now : start;
    }

    @Override
    public void close() {
        canReuse = true;
    }

    @Override
    public void cancel() {
        cancel = true;
    }

    @Override
    public String toString() {
        return sql + Trace.formatParams(getParameters());
    }

    public boolean isCacheable() {
        return false;
    }

    /**
     * Whether the command is already closed (in which case it can be re-used).
     *
     * @return true if it can be re-used
     */
    public boolean canReuse() {
        return canReuse;
    }

    /**
     * The command is now re-used, therefore reset the canReuse flag, and the
     * parameter values.
     */
    public void reuse() {
        canReuse = false;
        ArrayList<? extends ParameterInterface> parameters = getParameters();
        for (ParameterInterface param : parameters) {
            param.setValue(null, true);
        }
    }

    public void setCanReuse(boolean canReuse) {
        this.canReuse = canReuse;
    }

    public abstract Set<DbObject> getDependencies();

    /**
     * Returns is this command can be repeated again on locking failure.
     *
     * @return is this command can be repeated again on locking failure
     */
    protected abstract boolean isRetryable();

    protected final Database getDatabase() {
        return session.getDatabase();
    }
}
