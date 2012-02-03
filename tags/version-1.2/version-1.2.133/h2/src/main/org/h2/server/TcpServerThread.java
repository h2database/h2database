/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import org.h2.command.Command;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.Engine;
import org.h2.engine.Session;
import org.h2.engine.SessionRemote;
import org.h2.expression.Parameter;
import org.h2.expression.ParameterInterface;
import org.h2.expression.ParameterRemote;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.DbException;
import org.h2.result.ResultColumn;
import org.h2.result.ResultInterface;
import org.h2.util.SmallMap;
import org.h2.util.StringUtils;
import org.h2.value.Transfer;
import org.h2.value.Value;

/**
 * One server thread is opened per client connection.
 */
public class TcpServerThread implements Runnable {
    private TcpServer server;
    private Session session;
    private boolean stop;
    private Thread thread;
    private Transfer transfer;
    private Command commit;
    private SmallMap cache = new SmallMap(SysProperties.SERVER_CACHED_OBJECTS);
    private int threadId;
    private int clientVersion;
    private String sessionId;

    TcpServerThread(Socket socket, TcpServer server, int id) {
        this.server = server;
        this.threadId = id;
        transfer = new Transfer(null);
        transfer.setSocket(socket);
    }

    private void trace(String s) {
        server.trace(this + " " + s);
    }

    public void run() {
        try {
            transfer.init();
            trace("Connect");
            // TODO server: should support a list of allowed databases
            // and a list of allowed clients
            try {
                if (!server.allow(transfer.getSocket())) {
                    throw DbException.get(ErrorCode.REMOTE_CONNECTION_NOT_ALLOWED);
                }
                clientVersion = transfer.readInt();
                if (clientVersion < Constants.TCP_PROTOCOL_VERSION) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + clientVersion, "" + Constants.TCP_PROTOCOL_VERSION);
                }
                // max version (currently not used)
                transfer.readInt();
                String db = transfer.readString();
                String originalURL = transfer.readString();
                if (db == null && originalURL == null) {
                    String targetSessionId = transfer.readString();
                    int command = transfer.readInt();
                    stop = true;
                    if (command == SessionRemote.SESSION_CANCEL_STATEMENT) {
                        // cancel a running statement
                        int statementId = transfer.readInt();
                        server.cancelStatement(targetSessionId, statementId);
                    } else if (command == SessionRemote.SESSION_CHECK_KEY) {
                        // check if this is the correct server
                        db = server.checkKeyAndGetDatabaseName(targetSessionId);
                        if (!targetSessionId.equals(db)) {
                            transfer.writeInt(SessionRemote.STATUS_OK);
                        } else {
                            transfer.writeInt(SessionRemote.STATUS_ERROR);
                        }
                    }
                }
                String baseDir = server.getBaseDir();
                if (baseDir == null) {
                    baseDir = SysProperties.getBaseDir();
                }
                db = server.checkKeyAndGetDatabaseName(db);
                ConnectionInfo ci = new ConnectionInfo(db);
                if (baseDir != null) {
                    ci.setBaseDir(baseDir);
                }
                if (server.getIfExists()) {
                    ci.setProperty("IFEXISTS", "TRUE");
                }
                ci.setOriginalURL(originalURL);
                ci.setUserName(transfer.readString());
                ci.setUserPasswordHash(transfer.readBytes());
                ci.setFilePasswordHash(transfer.readBytes());
                int len = transfer.readInt();
                for (int i = 0; i < len; i++) {
                    ci.setProperty(transfer.readString(), transfer.readString());
                }
                Engine engine = Engine.getInstance();
                session = engine.getSession(ci);
                transfer.setSession(session);
                transfer.writeInt(SessionRemote.STATUS_OK);
                if (clientVersion >= Constants.TCP_PROTOCOL_VERSION) {
                    // version 6: reply what version to use
                    transfer.writeInt(Constants.TCP_PROTOCOL_VERSION);
                }
                transfer.flush();
                server.addConnection(threadId, originalURL, ci.getUserName());
                trace("Connected");
            } catch (Throwable e) {
                sendError(e);
                stop = true;
            }
            while (!stop) {
                try {
                    process();
                } catch (Throwable e) {
                    sendError(e);
                }
            }
            trace("Disconnect");
        } catch (Throwable e) {
            server.traceError(e);
        } finally {
            close();
        }
    }

    private void closeSession() {
        if (session != null) {
            try {
                Command rollback = session.prepareLocal("ROLLBACK");
                rollback.executeUpdate();
            } catch (Exception e) {
                server.traceError(e);
            }
            try {
                session.close();
                server.removeConnection(threadId);
            } catch (Exception e) {
                server.traceError(e);
            } finally {
                session = null;
            }
        }
    }

    /**
     * Close a connection.
     */
    void close() {
        try {
            stop = true;
            closeSession();
            transfer.close();
            trace("Close");
        } catch (Exception e) {
            server.traceError(e);
        }
        server.remove(this);
    }

    private void sendError(Throwable t) {
        try {
            SQLException e = DbException.convert(t).getSQLException();
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            String trace = writer.toString();
            String message;
            String sql;
            if (e instanceof JdbcSQLException) {
                JdbcSQLException j = (JdbcSQLException) e;
                message = j.getOriginalMessage();
                sql = j.getSQL();
            } else {
                message = e.getMessage();
                sql = null;
            }
            transfer.writeInt(SessionRemote.STATUS_ERROR).writeString(e.getSQLState()).writeString(message)
                    .writeString(sql).writeInt(e.getErrorCode()).writeString(trace).flush();
        } catch (IOException e2) {
            server.traceError(e2);
            // if writing the error does not work, close the connection
            stop = true;
        }
    }

    private void setParameters(Command command) throws IOException {
        int len = transfer.readInt();
        ArrayList< ? extends ParameterInterface> params = command.getParameters();
        for (int i = 0; i < len; i++) {
            Parameter p = (Parameter) params.get(i);
            p.setValue(transfer.readValue());
        }
    }

    private void process() throws IOException, SQLException {
        int operation = transfer.readInt();
        switch (operation) {
        case SessionRemote.SESSION_PREPARE_READ_PARAMS:
        case SessionRemote.SESSION_PREPARE: {
            int id = transfer.readInt();
            String sql = transfer.readString();
            int old = session.getModificationId();
            Command command = session.prepareLocal(sql);
            boolean readonly = command.isReadOnly();
            cache.addObject(id, command);
            boolean isQuery = command.isQuery();
            ArrayList< ? extends ParameterInterface> params = command.getParameters();
            transfer.writeInt(getState(old)).writeBoolean(isQuery).writeBoolean(readonly)
                    .writeInt(params.size());
            if (operation == SessionRemote.SESSION_PREPARE_READ_PARAMS) {
                for (ParameterInterface p : params) {
                    ParameterRemote.writeMetaData(transfer, p);
                }
            }
            transfer.flush();
            break;
        }
        case SessionRemote.SESSION_CLOSE: {
            closeSession();
            transfer.writeInt(SessionRemote.STATUS_OK).flush();
            close();
            break;
        }
        case SessionRemote.COMMAND_COMMIT: {
            if (commit == null) {
                commit = session.prepareLocal("COMMIT");
            }
            int old = session.getModificationId();
            commit.executeUpdate();
            transfer.writeInt(getState(old)).flush();
            break;
        }
        case SessionRemote.COMMAND_GET_META_DATA: {
            int id = transfer.readInt();
            int objectId = transfer.readInt();
            Command command = (Command) cache.getObject(id, false);
            ResultInterface result = command.getMetaData();
            cache.addObject(objectId, result);
            int columnCount = result.getVisibleColumnCount();
            transfer.writeInt(SessionRemote.STATUS_OK).writeInt(columnCount).writeInt(0);
            for (int i = 0; i < columnCount; i++) {
                ResultColumn.writeColumn(transfer, result, i);
            }
            transfer.flush();
            break;
        }
        case SessionRemote.COMMAND_EXECUTE_QUERY: {
            int id = transfer.readInt();
            int objectId = transfer.readInt();
            int maxRows = transfer.readInt();
            int fetchSize = transfer.readInt();
            Command command = (Command) cache.getObject(id, false);
            setParameters(command);
            int old = session.getModificationId();
            ResultInterface result = command.executeQuery(maxRows, false);
            cache.addObject(objectId, result);
            int columnCount = result.getVisibleColumnCount();
            int state = getState(old);
            transfer.writeInt(state).writeInt(columnCount);
            int rowCount = result.getRowCount();
            transfer.writeInt(rowCount);
            for (int i = 0; i < columnCount; i++) {
                ResultColumn.writeColumn(transfer, result, i);
            }
            int fetch = Math.min(rowCount, fetchSize);
            for (int i = 0; i < fetch; i++) {
                sendRow(result);
            }
            transfer.flush();
            break;
        }
        case SessionRemote.COMMAND_EXECUTE_UPDATE: {
            int id = transfer.readInt();
            Command command = (Command) cache.getObject(id, false);
            setParameters(command);
            int old = session.getModificationId();
            int updateCount = command.executeUpdate();
            int status;
            if (session.isClosed()) {
                status = SessionRemote.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status).writeInt(updateCount).writeBoolean(session.getAutoCommit());
            transfer.flush();
            break;
        }
        case SessionRemote.COMMAND_CLOSE: {
            int id = transfer.readInt();
            Command command = (Command) cache.getObject(id, true);
            if (command != null) {
                command.close();
                cache.freeObject(id);
            }
            break;
        }
        case SessionRemote.RESULT_FETCH_ROWS: {
            int id = transfer.readInt();
            int count = transfer.readInt();
            ResultInterface result = (ResultInterface) cache.getObject(id, false);
            transfer.writeInt(SessionRemote.STATUS_OK);
            for (int i = 0; i < count; i++) {
                sendRow(result);
            }
            transfer.flush();
            break;
        }
        case SessionRemote.RESULT_RESET: {
            int id = transfer.readInt();
            ResultInterface result = (ResultInterface) cache.getObject(id, false);
            result.reset();
            break;
        }
        case SessionRemote.RESULT_CLOSE: {
            int id = transfer.readInt();
            ResultInterface result = (ResultInterface) cache.getObject(id, true);
            if (result != null) {
                result.close();
                cache.freeObject(id);
            }
            break;
        }
        case SessionRemote.CHANGE_ID: {
            int oldId = transfer.readInt();
            int newId = transfer.readInt();
            Object obj = cache.getObject(oldId, false);
            cache.freeObject(oldId);
            cache.addObject(newId, obj);
            break;
        }
        case SessionRemote.SESSION_SET_ID: {
            sessionId = transfer.readString();
            transfer.writeInt(SessionRemote.STATUS_OK).flush();
            break;
        }
        default:
            trace("Unknown operation: " + operation);
            closeSession();
            close();
        }
    }

    private int getState(int oldModificationId) {
        if (session.getModificationId() == oldModificationId) {
            return SessionRemote.STATUS_OK;
        }
        return SessionRemote.STATUS_OK_STATE_CHANGED;
    }

    private void sendRow(ResultInterface result) throws IOException {
        if (result.next()) {
            transfer.writeBoolean(true);
            Value[] v = result.currentRow();
            for (int i = 0; i < result.getVisibleColumnCount(); i++) {
                transfer.writeValue(v[i]);
            }
        } else {
            transfer.writeBoolean(false);
        }
    }

    void setThread(Thread thread) {
        this.thread = thread;
    }

    Thread getThread() {
        return thread;
    }

    /**
     * Cancel a running statement.
     *
     * @param targetSessionId the session id
     * @param statementId the statement to cancel
     */
    void cancelStatement(String targetSessionId, int statementId) throws SQLException {
        if (StringUtils.equals(targetSessionId, this.sessionId)) {
            Command cmd = (Command) cache.getObject(statementId, false);
            cmd.cancel();
        }
    }

}
