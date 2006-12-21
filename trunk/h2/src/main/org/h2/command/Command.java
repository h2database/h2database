/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

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
    protected Session session;
    protected long startTime;
    protected Trace trace;
    private volatile boolean cancel;

    public abstract boolean isTransactional();
    public abstract boolean isQuery();
    public abstract ObjectArray getParameters();
    public abstract boolean isReadOnly();

    public Command(Parser parser) {
        this.session = parser.getSession();
        trace = session.getDatabase().getTrace(Trace.COMMAND);
    }

    public int update() throws SQLException {
        throw Message.getSQLException(Message.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    public LocalResult query(int maxrows) throws SQLException {
        throw Message.getSQLException(Message.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    // TODO insert parameters into the original query, or allow this syntax
    // if(parameters != null && parameters.size() > 0) {
    // buff.append(" /* ");
    // for(int i=0; i<parameters.size(); i++) {
    // if(i>0) {
    // buff.append(", ");
    // }
    // Parameter param = (Parameter) parameters.get(i);
    // buff.append(i+1);
    // buff.append(" = ");
    // buff.append(param.getSQL());
    // }
    // buff.append(" */");
    // }

    public ResultInterface executeQuery(int maxrows, boolean scrollable) throws SQLException {
        return executeQueryLocal(maxrows);
    }

    public LocalResult executeQueryLocal(int maxrows) throws SQLException {
        startTime = System.currentTimeMillis();
        Database database = session.getDatabase();
        Object sync = Constants.MULTI_THREADED_KERNEL ? (Object)session : (Object)database;
        synchronized (sync) {
            try {
                database.checkPowerOff();
                session.setCurrentCommand(this);
                LocalResult result = query(maxrows);
                return result;
            } catch(Throwable e) {
                SQLException s = Message.convert(e);
                database.exceptionThrown(s);
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
            throw Message.getSQLException(Message.STATEMENT_WAS_CANCELLED);
        }
        session.throttle();
    }

    private void stop() throws SQLException {
        session.setCurrentCommand(null);
        if (!isTransactional()) {
            // meta data changes need to commit in any case
            session.commit();
        } else if (session.getAutoCommit()) {
            session.commit();
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
        Object sync = Constants.MULTI_THREADED_KERNEL ? (Object)session : (Object)database;
        synchronized (sync) {
            int rollback = session.getLogId();
            session.setCurrentCommand(this);            
            try {
                database.checkPowerOff();
                int result = update();
                return result;
            } catch (SQLException e) {
                database.exceptionThrown(e);
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
    
}
