/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.util.ObjectArray;

/**
 * @author Thomas
 */
public abstract class Command implements CommandInterface {
    private final String sql;
    protected final Session session;
    protected final Trace trace;
    protected long startTime;
    private volatile boolean cancel;

    public abstract boolean isTransactional();
    public abstract boolean isQuery();
    public abstract ObjectArray getParameters();
    public abstract boolean isReadOnly();

    public Command(Parser parser, String sql) {
        this.session = parser.getSession();
        this.sql = sql;
        trace = session.getDatabase().getTrace(Trace.COMMAND);
    }

    public int update() throws SQLException {
        throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    public LocalResult query(int maxrows) throws SQLException {
        throw Message.getSQLException(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    public abstract LocalResult queryMeta() throws SQLException;

    public final LocalResult getMetaDataLocal() throws SQLException {
        return queryMeta();
    }

    public final ResultInterface getMetaData() throws SQLException {
        return queryMeta();
    }
    
    public ResultInterface executeQuery(int maxrows, boolean scrollable) throws SQLException {
        return executeQueryLocal(maxrows);
    }

    public LocalResult executeQueryLocal(int maxrows) throws SQLException {
        startTime = System.currentTimeMillis();
        Database database = session.getDatabase();
        Object sync = SysProperties.multiThreadedKernel ? (Object)session : (Object)database;
        synchronized (sync) {
            try {
                database.checkPowerOff();
                session.setCurrentCommand(this);
                return query(maxrows);
            } catch(Throwable e) {
                SQLException s = Message.convert(e);
                database.exceptionThrown(s, sql);
                throw s;
            } finally {
                stop();
            }
        }
    }

    protected void start() {
        startTime = System.currentTimeMillis();
    }

    public void checkCancelled() throws SQLException {
        if (cancel) {
            throw Message.getSQLException(ErrorCode.STATEMENT_WAS_CANCELLED);
        }
        session.throttle();
    }

    private void stop() throws SQLException {
        session.setCurrentCommand(null);
        if (!isTransactional()) {
            session.commit(true);
        } else if (session.getAutoCommit()) {
            session.commit(false);
        } else if (SysProperties.multiThreadedKernel) {
            Database db = session.getDatabase();
            if (db != null && db.getLockMode() == Constants.LOCK_MODE_READ_COMMITTED) {
                session.unlockReadLocks();
            }
        }
        if (trace.info()) {
            long time = System.currentTimeMillis() - startTime;
            if (time > Constants.LONG_QUERY_LIMIT_MS) {
                trace.info("long query: " + time);
            }
        }
    }

    public int executeUpdate() throws SQLException {
        startTime = System.currentTimeMillis();
        Database database = session.getDatabase();
        Object sync = SysProperties.multiThreadedKernel ? (Object)session : (Object)database;
        synchronized (sync) {
            int rollback = session.getLogId();
            session.setCurrentCommand(this);            
            try {
                database.checkPowerOff();
                return update();
            } catch (SQLException e) {
                database.exceptionThrown(e, sql);
                database.checkPowerOff();
                session.rollbackTo(rollback);
                throw e;
            } finally {
                stop();
            }
        }
    }

    public void close() {
        // nothing to do
    }

    public void cancel() {
        this.cancel = true;
    }
    public String getSQL() {
        return null;
    }
    
}
