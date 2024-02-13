/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.h2.api.ErrorCode;
import org.h2.command.Command;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.Engine;
import org.h2.engine.GeneratedKeysMode;
import org.h2.engine.Session;
import org.h2.engine.SessionLocal;
import org.h2.engine.SessionRemote;
import org.h2.engine.SysProperties;
import org.h2.expression.Parameter;
import org.h2.expression.ParameterInterface;
import org.h2.expression.ParameterRemote;
import org.h2.jdbc.JdbcException;
import org.h2.jdbc.meta.DatabaseMetaServer;
import org.h2.message.DbException;
import org.h2.result.BatchResult;
import org.h2.result.ResultColumn;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import org.h2.store.LobStorageInterface;
import org.h2.util.IOUtils;
import org.h2.util.NetUtils;
import org.h2.util.NetworkConnectionInfo;
import org.h2.util.SmallLRUCache;
import org.h2.util.SmallMap;
import org.h2.util.TimeZoneProvider;
import org.h2.value.Transfer;
import org.h2.value.Value;
import org.h2.value.ValueLob;

/**
 * One server thread is opened per client connection.
 */
public class TcpServerThread implements Runnable {

    protected final Transfer transfer;
    private final TcpServer server;
    private SessionLocal session;
    private boolean stop;
    private Thread thread;
    private Command commit;
    private final SmallMap cache =
            new SmallMap(SysProperties.SERVER_CACHED_OBJECTS);
    private final SmallLRUCache<Long, CachedInputStream> lobs =
            SmallLRUCache.newInstance(Math.max(
                SysProperties.SERVER_CACHED_OBJECTS,
                SysProperties.SERVER_RESULT_SET_FETCH_SIZE * 5));
    private final int threadId;
    private int clientVersion;
    private String sessionId;
    private long lastRemoteSettingsId;

    TcpServerThread(Socket socket, TcpServer server, int id) {
        this.server = server;
        this.threadId = id;
        transfer = new Transfer(null, socket);
    }

    private void trace(String s) {
        server.trace(this + " " + s);
    }

    @Override
    public void run() {
        try {
            transfer.init();
            trace("Connect");
            // TODO server: should support a list of allowed databases
            // and a list of allowed clients
            try {
                Socket socket = transfer.getSocket();
                if (socket == null) {
                    // the transfer is already closed, prevent NPE in TcpServer#allow(Socket)
                    return;
                }
                if (!server.allow(transfer.getSocket())) {
                    throw DbException.get(ErrorCode.REMOTE_CONNECTION_NOT_ALLOWED);
                }
                int minClientVersion = transfer.readInt();
                if (minClientVersion < 6) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2,
                            Integer.toString(minClientVersion), "" + Constants.TCP_PROTOCOL_VERSION_MIN_SUPPORTED);
                }
                int maxClientVersion = transfer.readInt();
                if (maxClientVersion < Constants.TCP_PROTOCOL_VERSION_MIN_SUPPORTED) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2,
                            Integer.toString(maxClientVersion), "" + Constants.TCP_PROTOCOL_VERSION_MIN_SUPPORTED);
                } else if (minClientVersion > Constants.TCP_PROTOCOL_VERSION_MAX_SUPPORTED) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2,
                            Integer.toString(minClientVersion), "" + Constants.TCP_PROTOCOL_VERSION_MAX_SUPPORTED);
                }
                if (maxClientVersion >= Constants.TCP_PROTOCOL_VERSION_MAX_SUPPORTED) {
                    clientVersion = Constants.TCP_PROTOCOL_VERSION_MAX_SUPPORTED;
                } else {
                    clientVersion = maxClientVersion;
                }
                transfer.setVersion(clientVersion);
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
                ci.setOriginalURL(originalURL);
                ci.setUserName(transfer.readString());
                ci.setUserPasswordHash(transfer.readBytes());
                ci.setFilePasswordHash(transfer.readBytes());
                int len = transfer.readInt();
                for (int i = 0; i < len; i++) {
                    ci.setProperty(transfer.readString(), transfer.readString());
                }
                // override client's requested properties with server settings
                if (baseDir != null) {
                    ci.setBaseDir(baseDir);
                }
                if (server.getIfExists()) {
                    ci.setProperty("FORBID_CREATION", "TRUE");
                }
                transfer.writeInt(SessionRemote.STATUS_OK);
                transfer.writeInt(clientVersion);
                transfer.flush();
                if (ci.getFilePasswordHash() != null) {
                    ci.setFileEncryptionKey(transfer.readBytes());
                }
                ci.setNetworkConnectionInfo(new NetworkConnectionInfo(
                        NetUtils.ipToShortForm(new StringBuilder(server.getSSL() ? "ssl://" : "tcp://"),
                                socket.getLocalAddress().getAddress(), true) //
                                .append(':').append(socket.getLocalPort()).toString(), //
                        socket.getInetAddress().getAddress(), socket.getPort(),
                        new StringBuilder().append('P').append(clientVersion).toString()));
                if (clientVersion < Constants.TCP_PROTOCOL_VERSION_20) {
                    // For DatabaseMetaData
                    ci.setProperty("OLD_INFORMATION_SCHEMA", "TRUE");
                    // For H2 Console
                    ci.setProperty("NON_KEYWORDS", "VALUE");
                }
                session = Engine.createSession(ci);
                transfer.setSession(session);
                server.addConnection(threadId, originalURL, ci.getUserName());
                trace("Connected");
                lastRemoteSettingsId = session.getDatabase().getRemoteSettingsId();
            } catch (OutOfMemoryError e) {
                // catch this separately otherwise such errors will never hit the console
                server.traceError(e);
                sendError(e, true);
                stop = true;
            } catch (Throwable e) {
                sendError(e,true);
                stop = true;
            }
            while (!stop) {
                try {
                    process();
                } catch (Throwable e) {
                    sendError(e, true);
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
            RuntimeException closeError = null;
            try {
                session.close();
                server.removeConnection(threadId);
            } catch (RuntimeException e) {
                closeError = e;
                server.traceError(e);
            } catch (Exception e) {
                server.traceError(e);
            } finally {
                session = null;
            }
            if (closeError != null) {
                throw closeError;
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
        } catch (Exception e) {
            server.traceError(e);
        } finally {
            transfer.close();
            trace("Close");
            server.remove(this);
        }
    }

    private void sendError(Throwable t, boolean withStatus) {
        try {
            if (withStatus) {
                transfer.writeInt(SessionRemote.STATUS_ERROR);
            }
            sendSQLException(DbException.convert(t).getSQLException());
            transfer.flush();
        } catch (Exception e) {
            if (!transfer.isClosed()) {
                server.traceError(e);
            }
            // if writing the error does not work, close the connection
            stop = true;
        }
    }

    private void sendSQLException(SQLException e) throws IOException {
        StringWriter writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        String trace = writer.toString();
        String message;
        String sql;
        if (e instanceof JdbcException) {
            JdbcException j = (JdbcException) e;
            message = j.getOriginalMessage();
            sql = j.getSQL();
        } else {
            message = e.getMessage();
            sql = null;
        }
        transfer.writeString(e.getSQLState()).writeString(message).writeString(sql).writeInt(e.getErrorCode())
                .writeString(trace);
    }

    private void setParameters(Command command) throws IOException {
        int len = transfer.readInt();
        ArrayList<? extends ParameterInterface> params = command.getParameters();
        for (int i = 0; i < len; i++) {
            Parameter p = (Parameter) params.get(i);
            p.setValue(transfer.readValue(null));
        }
    }

    private void process() throws IOException {
        final SessionLocal session = this.session;
        int operation = transfer.readInt();
        switch (operation) {
        case SessionRemote.SESSION_PREPARE:
        case SessionRemote.SESSION_PREPARE_READ_PARAMS2: {
            int id = transfer.readInt();
            String sql = transfer.readString();
            int old = session.getModificationId();
            Command command = session.prepareLocal(sql);
            boolean readonly = command.isReadOnly();
            cache.addObject(id, command);
            boolean isQuery = command.isQuery();

            transfer.writeInt(getState(old)).writeBoolean(isQuery).
                    writeBoolean(readonly);

            if (operation != SessionRemote.SESSION_PREPARE) {
                transfer.writeInt(command.getCommandType());
            }

            ArrayList<? extends ParameterInterface> params = command.getParameters();

            transfer.writeInt(params.size());

            if (operation != SessionRemote.SESSION_PREPARE) {
                for (ParameterInterface p : params) {
                    ParameterRemote.writeMetaData(transfer, p);
                }
            }
            transfer.flush();
            break;
        }
        case SessionRemote.SESSION_CLOSE: {
            stop = true;
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
            commit.executeUpdate(null);
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
            transfer.writeInt(SessionRemote.STATUS_OK).
                    writeInt(columnCount).writeRowCount(0L);
            for (int i = 0; i < columnCount; i++) {
                ResultColumn.writeColumn(transfer, result, i);
            }
            transfer.flush();
            break;
        }
        case SessionRemote.COMMAND_EXECUTE_QUERY: {
            int id = transfer.readInt();
            int objectId = transfer.readInt();
            long maxRows = transfer.readRowCount();
            int fetchSize = transfer.readInt();
            Command command = (Command) cache.getObject(id, false);
            setParameters(command);
            int old = session.getModificationId();
            ResultInterface result;
            session.lock();
            try {
                result = command.executeQuery(maxRows, false);
            } finally {
                session.unlock();
            }
            cache.addObject(objectId, result);
            int columnCount = result.getVisibleColumnCount();
            int state = getState(old);
            transfer.writeInt(state).writeInt(columnCount);
            long rowCount = result.isLazy() ? -1L : result.getRowCount();
            transfer.writeRowCount(rowCount);
            for (int i = 0; i < columnCount; i++) {
                ResultColumn.writeColumn(transfer, result, i);
            }
            sendRows(result, rowCount >= 0L ? Math.min(rowCount, fetchSize) : fetchSize);
            transfer.flush();
            break;
        }
        case SessionRemote.COMMAND_EXECUTE_UPDATE: {
            int id = transfer.readInt();
            Command command = (Command) cache.getObject(id, false);
            setParameters(command);
            Object generatedKeysRequest = readGeneratedKeysRequest();
            int old = session.getModificationId();
            ResultWithGeneratedKeys result;
            session.lock();
            try {
                result = command.executeUpdate(generatedKeysRequest);
            } finally {
                session.unlock();
            }
            int status;
            if (session.isClosed()) {
                status = SessionRemote.STATUS_CLOSED;
                stop = true;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            transfer.writeRowCount(result.getUpdateCount());
            transfer.writeBoolean(session.getAutoCommit());
            if (generatedKeysRequest != Boolean.FALSE) {
                sendGeneratedKeys(result.getGeneratedKeys());
            }
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
            sendRows(result, count);
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
            if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_20) {
                session.setTimeZone(TimeZoneProvider.ofId(transfer.readString()));
            }
            transfer.writeInt(SessionRemote.STATUS_OK)
                .writeBoolean(session.getAutoCommit())
                .flush();
            break;
        }
        case SessionRemote.SESSION_SET_AUTOCOMMIT: {
            boolean autoCommit = transfer.readBoolean();
            session.setAutoCommit(autoCommit);
            transfer.writeInt(SessionRemote.STATUS_OK).flush();
            break;
        }
        case SessionRemote.SESSION_HAS_PENDING_TRANSACTION: {
            transfer.writeInt(SessionRemote.STATUS_OK).
                writeInt(session.hasPendingTransaction() ? 1 : 0).flush();
            break;
        }
        case SessionRemote.LOB_READ: {
            long lobId = transfer.readLong();
            byte[] hmac = transfer.readBytes();
            long offset = transfer.readLong();
            int length = transfer.readInt();
            transfer.verifyLobMac(hmac, lobId);
            CachedInputStream in = lobs.get(lobId);
            if (in == null || in.getPos() != offset) {
                LobStorageInterface lobStorage = session.getDataHandler().getLobStorage();
                // only the lob id is used
                InputStream lobIn = lobStorage.getInputStream(lobId, -1);
                in = new CachedInputStream(lobIn);
                lobs.put(lobId, in);
                lobIn.skip(offset);
            }
            // limit the buffer size
            length = Math.min(16 * Constants.IO_BUFFER_SIZE, length);
            byte[] buff = new byte[length];
            length = IOUtils.readFully(in, buff, length);
            transfer.writeInt(SessionRemote.STATUS_OK);
            transfer.writeInt(length);
            transfer.writeBytes(buff, 0, length);
            transfer.flush();
            break;
        }
        case SessionRemote.GET_JDBC_META: {
            int code = transfer.readInt();
            int length = transfer.readInt();
            Value[] args = new Value[length];
            for (int i = 0; i < length; i++) {
                args[i] = transfer.readValue(null);
            }
            int old = session.getModificationId();
            ResultInterface result;
            session.lock();
            try {
                result = DatabaseMetaServer.process(session, code, args);
            } finally {
                session.unlock();
            }
            int columnCount = result.getVisibleColumnCount();
            int state = getState(old);
            transfer.writeInt(state).writeInt(columnCount);
            long rowCount = result.getRowCount();
            transfer.writeRowCount(rowCount);
            for (int i = 0; i < columnCount; i++) {
                ResultColumn.writeColumn(transfer, result, i);
            }
            sendRows(result, rowCount);
            transfer.flush();
            break;
        }
        case SessionRemote.COMMAND_EXECUTE_BATCH_UPDATE: {
            int id = transfer.readInt();
            Command command = (Command) cache.getObject(id, false);
            int size = transfer.readInt();
            ArrayList<Value[]> batchParameters = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int len = transfer.readInt();
                Value[] parameters = new Value[len];
                for (int j = 0; j < len; j++) {
                    parameters[j] = transfer.readValue(null);
                }
                batchParameters.add(parameters);
            }
            Object generatedKeysRequest = readGeneratedKeysRequest();
            int old = session.getModificationId();
            BatchResult result;
            session.lock();
            try {
                result = command.executeBatchUpdate(batchParameters, generatedKeysRequest);
            } finally {
                session.unlock();
            }
            int status;
            if (session.isClosed()) {
                status = SessionRemote.STATUS_CLOSED;
                stop = true;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            for (long updateCount : result.getUpdateCounts()) {
                transfer.writeLong(updateCount);
            }
            if (generatedKeysRequest != Boolean.FALSE) {
                sendGeneratedKeys(result.getGeneratedKeys());
            }
            List<SQLException> exceptions = result.getExceptions();
            transfer.writeInt(exceptions.size());
            for (SQLException exception : exceptions) {
                sendSQLException(exception);
            }
            transfer.writeBoolean(session.getAutoCommit());
            transfer.flush();
            break;
        }
        default:
            trace("Unknown operation: " + operation);
            close();
        }
    }

    private Object readGeneratedKeysRequest() throws IOException {
        int mode = transfer.readInt();
        switch (mode) {
        case GeneratedKeysMode.NONE:
            return Boolean.FALSE;
        case GeneratedKeysMode.AUTO:
            return Boolean.TRUE;
        case GeneratedKeysMode.COLUMN_NUMBERS: {
            int len = transfer.readInt();
            int[] keys = new int[len];
            for (int i = 0; i < len; i++) {
                keys[i] = transfer.readInt();
            }
            return keys;
        }
        case GeneratedKeysMode.COLUMN_NAMES: {
            int len = transfer.readInt();
            String[] keys = new String[len];
            for (int i = 0; i < len; i++) {
                keys[i] = transfer.readString();
            }
            return keys;
        }
        default:
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                    "Unsupported generated keys' mode " + mode);
        }
    }

    private void sendGeneratedKeys(ResultInterface generatedKeys) throws IOException {
        int columnCount = generatedKeys.getVisibleColumnCount();
        transfer.writeInt(columnCount);
        long rowCount = generatedKeys.getRowCount();
        transfer.writeRowCount(rowCount);
        for (int i = 0; i < columnCount; i++) {
            ResultColumn.writeColumn(transfer, generatedKeys, i);
        }
        sendRows(generatedKeys, rowCount);
        generatedKeys.close();
    }

    private int getState(int oldModificationId) {
        if (session == null) {
            return SessionRemote.STATUS_CLOSED;
        }
        if (session.getModificationId() == oldModificationId) {
            long remoteSettingsId = session.getDatabase().getRemoteSettingsId();
            if (lastRemoteSettingsId == remoteSettingsId) {
                return SessionRemote.STATUS_OK;
            }
            lastRemoteSettingsId = remoteSettingsId;
        }
        return SessionRemote.STATUS_OK_STATE_CHANGED;
    }

    private void sendRows(ResultInterface result, long count) throws IOException {
        int columnCount = result.getVisibleColumnCount();
        boolean lazy = result.isLazy();
        Session oldSession = lazy ? session.setThreadLocalSession() : null;
        try {
            while (count-- > 0L) {
                boolean hasNext;
                try {
                    hasNext = result.next();
                } catch (Exception e) {
                    transfer.writeByte((byte) -1);
                    sendError(e, false);
                    break;
                }
                if (hasNext) {
                    transfer.writeByte((byte) 1);
                    Value[] values = result.currentRow();
                    for (int i = 0; i < columnCount; i++) {
                        Value v = values[i];
                        if (lazy && v instanceof ValueLob) {
                            ValueLob v2 = ((ValueLob) v).copyToResult();
                            if (v2 != v) {
                                v = session.addTemporaryLob(v2);
                            }
                        }
                        transfer.writeValue(v);
                    }
                } else {
                    transfer.writeByte((byte) 0);
                    break;
                }
            }
        } finally {
            if (lazy) {
                session.resetThreadLocalSession(oldSession);
            }
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
    void cancelStatement(String targetSessionId, int statementId) {
        if (Objects.equals(targetSessionId, this.sessionId)) {
            Command cmd = (Command) cache.getObject(statementId, false);
            cmd.cancel();
        }
    }

    /**
     * An input stream with a position.
     */
    static class CachedInputStream extends FilterInputStream {

        private static final ByteArrayInputStream DUMMY =
                new ByteArrayInputStream(new byte[0]);
        private long pos;

        CachedInputStream(InputStream in) {
            super(in == null ? DUMMY : in);
            if (in == null) {
                pos = -1;
            }
        }

        @Override
        public int read(byte[] buff, int off, int len) throws IOException {
            len = super.read(buff, off, len);
            if (len > 0) {
                pos += len;
            }
            return len;
        }

        @Override
        public int read() throws IOException {
            int x = in.read();
            if (x >= 0) {
                pos++;
            }
            return x;
        }

        @Override
        public long skip(long n) throws IOException {
            n = super.skip(n);
            if (n > 0) {
                pos += n;
            }
            return n;
        }

        public long getPos() {
            return pos;
        }

    }

}
