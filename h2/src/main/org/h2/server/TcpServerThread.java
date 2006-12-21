/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.Engine;
import org.h2.engine.Session;
import org.h2.engine.SessionRemote;
import org.h2.expression.Parameter;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.ResultColumn;
import org.h2.util.ObjectArray;
import org.h2.util.SmallMap;
import org.h2.value.Transfer;
import org.h2.value.Value;

public class TcpServerThread implements Runnable {
    private TcpServer server;
    private Session session;
    private boolean stop;
    private Thread thread;
    private Transfer transfer;
    private Command commit;
    private SmallMap cache = new SmallMap(Constants.SERVER_CACHED_OBJECTS);

    public TcpServerThread(Socket socket, TcpServer server) {
        this.server = server;
        transfer = new Transfer(null);
        transfer.setSocket(socket);
    }

    public void run() {
        try {
            transfer.init();
            server.log("Connect");
            // TODO server: should support a list of allowed databases and a list of allowed clients
            try {
                int version = transfer.readInt();
                if(!server.allow(transfer.getSocket())) {
                    throw Message.getSQLException(Message.REMOTE_CONNECTION_NOT_ALLOWED);
                }
                if(version != Constants.TCP_DRIVER_VERSION) {
                    throw Message.getSQLException(Message.DRIVER_VERSION_ERROR_2,
                            new String[] { "" + version, "" + Constants.TCP_DRIVER_VERSION }, null);
                }
                String db = transfer.readString();
                String originalURL = transfer.readString();
                String baseDir = server.getBaseDir();
                ConnectionInfo ci = new ConnectionInfo(db);
                if(baseDir != null) {
                    ci.setBaseDir(baseDir);
                }
                if(server.getIfExists()) {
                    ci.setProperty("IFEXISTS", "TRUE");
                }
                ci.setOriginalURL(originalURL);
                ci.setUserName(transfer.readString());
                ci.setUserPasswordHash(transfer.readBytes());
                ci.setFilePasswordHash(transfer.readBytes());
                int len = transfer.readInt();
                for(int i=0; i<len; i++) {
                    ci.setProperty(transfer.readString(), transfer.readString());
                }
                Engine engine = Engine.getInstance();
                session = engine.getSession(ci);
                transfer.setSession(session);
                transfer.writeInt(SessionRemote.STATUS_OK).flush();
                server.log("Connected");
            } catch(Throwable e) {
                sendError(e);
                stop = true;
            }
            while (!stop) {
                try {
                    process();
                } catch(Throwable e) {
                    sendError(e);
                }
            }
            server.log("Disconnect");
        } catch(Exception e) {
            server.logError(e);
        } finally {
            close();
        }
    }

    private void closeSession() {
        if(session != null) {
            try {
                Command rollback = session.prepareLocal("ROLLBACK");
                rollback.executeUpdate();
                session.close();
            } catch(Exception e) {
                server.logError(e);
            } finally {
                session = null;
            }
        }
    }

    public void close() {
        try {
            stop = true;
            closeSession();
            transfer.close();
            server.log("Close");
        } catch(Exception e) {
            server.logError(e);
        }
        server.remove(this);
    }

    private void sendError(Throwable e) {
        try {
            SQLException s = Message.convert(e);
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            String trace = writer.toString();
            transfer.writeInt(SessionRemote.STATUS_ERROR).
                writeString(s.getSQLState()).
                writeString(e.getMessage()).
                writeInt(s.getErrorCode()).
                writeString(trace).
                flush();
        } catch(IOException e2) {
            server.logError(e2);
            // if writing the error does not work, close the connection
            stop = true;
        }
    }

    private void setParameters(Command command) throws IOException, SQLException {
        int len = transfer.readInt();
        ObjectArray params = command.getParameters();
        for(int i=0; i<len; i++) {
            Parameter p = (Parameter) params.get(i);
            p.setValue(transfer.readValue());
        }
    }

    private void process() throws IOException, SQLException {
        int operation = transfer.readInt();
        switch(operation) {
        case SessionRemote.SESSION_PREPARE: {
            int id = transfer.readInt();
            String sql = transfer.readString();
            Command command = session.prepareLocal(sql);
            boolean readonly = command.isReadOnly();
            cache.addObject(id, command);
            boolean isQuery = command.isQuery();
            int paramCount = command.getParameters().size();
            transfer.writeInt(SessionRemote.STATUS_OK).writeBoolean(isQuery).writeBoolean(readonly).writeInt(paramCount).flush();
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
            commit.executeUpdate();
            transfer.writeInt(SessionRemote.STATUS_OK).flush();
            break;
        }
        case SessionRemote.COMMAND_EXECUTE_QUERY: {
            int id = transfer.readInt();
            int objectId = transfer.readInt();
            int maxRows = transfer.readInt();
            int readRows = transfer.readInt();
            Command command =  (Command)cache.getObject(id, false);
            setParameters(command);
            LocalResult result = command.executeQueryLocal(maxRows);
            cache.addObject(objectId, result);
            int columnCount = result.getVisibleColumnCount();
            transfer.writeInt(SessionRemote.STATUS_OK).writeInt(columnCount);
            int rowCount = result.getRowCount();
            transfer.writeInt(rowCount);
            for(int i=0; i<columnCount; i++) {
                ResultColumn.writeColumn(transfer, result, i);
            }
            if(rowCount<readRows) {
                for(int i=0; i<=rowCount; i++) {
                    sendRow(result);
                }
            }
            transfer.flush();
            break;
        }
        case SessionRemote.COMMAND_EXECUTE_UPDATE: {
            int id = transfer.readInt();
            Command command =  (Command)cache.getObject(id, false);
            setParameters(command);
            int updateCount = command.executeUpdate();
            int status = SessionRemote.STATUS_OK;
            if(session.isClosed()) {
                status = SessionRemote.STATUS_CLOSED;
            }
            transfer.writeInt(status).writeInt(updateCount).writeBoolean(session.getAutoCommit());
            transfer.flush();
            break;
        }
        case SessionRemote.COMMAND_CLOSE: {
            int id = transfer.readInt();
            Command command =  (Command)cache.getObject(id, true);
            if(command != null) {
                command.close();
                cache.freeObject(id);
            }
            break;
        }
        case SessionRemote.RESULT_FETCH_ROW: {
            int id = transfer.readInt();
            LocalResult result = (LocalResult)cache.getObject(id, false);
            transfer.writeInt(SessionRemote.STATUS_OK);
            sendRow(result);
            transfer.flush();
            break;
        }
        case SessionRemote.RESULT_RESET: {
            int id = transfer.readInt();
            LocalResult result = (LocalResult)cache.getObject(id, false);
            result.reset();
            break;
        }
        case SessionRemote.RESULT_CLOSE: {
            int id = transfer.readInt();
            LocalResult result = (LocalResult)cache.getObject(id, true);
            if(result != null) {
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
        default:
            server.logInternalError("Unknown operation: " + operation);
            server.log("Unknown operation: " + operation);
            closeSession();
            close();
        }
    }

    private void sendRow(LocalResult result) throws IOException, SQLException {
        boolean n = result.next();
        transfer.writeBoolean(n);
        if(n) {
            Value[] v = result.currentRow();
            for(int i=0; i<result.getVisibleColumnCount(); i++) {
                transfer.writeValue(v[i]);
            }
        }
    }

    public void setThread(Thread thread) {
        this.thread = thread;
    }

    public Thread getThread() {
        return thread;
    }

}
