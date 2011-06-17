/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;

import org.h2.api.DatabaseEventListener;
import org.h2.command.CommandInterface;
import org.h2.command.CommandRemote;
import org.h2.command.dml.SetTypes;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.expression.ParameterInterface;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.result.ResultInterface;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.util.ByteUtils;
import org.h2.util.ClassUtils;
import org.h2.util.FileUtils;
import org.h2.util.NetUtils;
import org.h2.util.ObjectArray;
import org.h2.util.RandomUtils;
import org.h2.util.SmallLRUCache;
import org.h2.util.StringUtils;
import org.h2.util.TempFileDeleter;
import org.h2.value.Transfer;
import org.h2.value.Value;
import org.h2.value.ValueString;

/**
 * The client side part of a session when using the server mode. This object
 * communicates with a Session on the server side.
 */
public class SessionRemote extends SessionWithState implements SessionFactory, DataHandler {

    public static final int SESSION_PREPARE = 0;
    public static final int SESSION_CLOSE = 1;
    public static final int COMMAND_EXECUTE_QUERY = 2;
    public static final int COMMAND_EXECUTE_UPDATE = 3;
    public static final int COMMAND_CLOSE = 4;
    public static final int RESULT_FETCH_ROWS = 5;
    public static final int RESULT_RESET = 6;
    public static final int RESULT_CLOSE = 7;
    public static final int COMMAND_COMMIT = 8;
    public static final int CHANGE_ID = 9;
    public static final int COMMAND_GET_META_DATA = 10;
    public static final int SESSION_PREPARE_READ_PARAMS = 11;
    public static final int SESSION_SET_ID = 12;
    public static final int SESSION_CANCEL_STATEMENT = 13;
    public static final int SESSION_CHECK_KEY = 14;

    public static final int STATUS_ERROR = 0;
    public static final int STATUS_OK = 1;
    public static final int STATUS_CLOSED = 2;
    public static final int STATUS_OK_STATE_CHANGED = 3;

    private TraceSystem traceSystem;
    private Trace trace;
    private ObjectArray<Transfer> transferList = ObjectArray.newInstance();
    private int nextId;
    private boolean autoCommit = true;
    private CommandInterface switchOffAutoCommit;
    private ConnectionInfo connectionInfo;
    private int objectId;
    private String databaseName;
    private String cipher;
    private byte[] fileEncryptionKey;
    private Object lobSyncObject = new Object();
    private String sessionId;
    private int clientVersion = Constants.TCP_PROTOCOL_VERSION_5;
    private boolean autoReconnect;
    private int lastReconnect;
    private SessionInterface embedded;
    private DatabaseEventListener eventListener;

    public SessionRemote() {
        // nothing to do
    }

    private SessionRemote(ConnectionInfo ci) {
        this.connectionInfo = ci;
    }

    private Transfer initTransfer(ConnectionInfo ci, String db, String server) throws IOException, SQLException {
        Socket socket = NetUtils.createSocket(server, Constants.DEFAULT_SERVER_PORT, ci.isSSL());
        Transfer trans = new Transfer(this);
        trans.setSocket(socket);
        trans.setSSL(ci.isSSL());
        trans.init();
        trans.writeInt(clientVersion);
        if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_6) {
            trans.writeInt(clientVersion);
        }
        trans.writeString(db);
        trans.writeString(ci.getOriginalURL());
        trans.writeString(ci.getUserName());
        trans.writeBytes(ci.getUserPasswordHash());
        trans.writeBytes(ci.getFilePasswordHash());
        String[] keys = ci.getKeys();
        trans.writeInt(keys.length);
        for (String key : keys) {
            trans.writeString(key).writeString(ci.getProperty(key));
        }
        try {
            done(trans);
            if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_6) {
                clientVersion = trans.readInt();
            }
        } catch (SQLException e) {
            trans.close();
            throw e;
        }
        autoCommit = true;
        return trans;
    }

    public void cancel() {
        // this method is called when closing the connection
        // the statement that is currently running is not canceled in this case
        // however Statement.cancel is supported
    }

    /**
     * Cancel the statement with the given id.
     *
     * @param id the statement id
     */
    public void cancelStatement(int id) {
        if (clientVersion <= Constants.TCP_PROTOCOL_VERSION_5) {
            // older servers don't support this feature
            return;
        }
        for (Transfer transfer : transferList) {
            try {
                Transfer trans = transfer.openNewConnection();
                trans.init();
                trans.writeInt(clientVersion);
                if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_6) {
                    trans.writeInt(clientVersion);
                }
                trans.writeString(null);
                trans.writeString(null);
                trans.writeString(sessionId);
                trans.writeInt(SessionRemote.SESSION_CANCEL_STATEMENT);
                trans.writeInt(id);
                trans.close();
            } catch (IOException e) {
                trace.debug("Could not cancel statement", e);
            }
        }
    }

    private void switchOffAutoCommitIfCluster() throws SQLException {
        if (autoCommit && transferList.size() > 1) {
            if (switchOffAutoCommit == null) {
                switchOffAutoCommit = prepareCommand("SET AUTOCOMMIT FALSE", Integer.MAX_VALUE);
            }
            // this will call setAutoCommit(false)
            switchOffAutoCommit.executeUpdate();
            // so we need to switch it on
            autoCommit = true;
        }
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    /**
     * Calls COMMIT if the session is in cluster mode.
     */
    public void autoCommitIfCluster() throws SQLException {
        if (autoCommit && transferList != null && transferList.size() > 1) {
            // server side auto commit is off because of race conditions
            // (update set id=1 where id=0, but update set id=2 where id=0 is
            // faster)
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                Transfer transfer = transferList.get(i);
                try {
                    traceOperation("COMMAND_COMMIT", 0);
                    transfer.writeInt(SessionRemote.COMMAND_COMMIT);
                    done(transfer);
                } catch (IOException e) {
                    removeServer(e, i--, ++count);
                }
            }
        }
    }

    private String getFilePrefix(String dir) {
        StringBuilder buff = new StringBuilder(dir);
        buff.append('/');
        for (int i = 0; i < databaseName.length(); i++) {
            char ch = databaseName.charAt(i);
            if (Character.isLetterOrDigit(ch)) {
                buff.append(ch);
            } else {
                buff.append('_');
            }
        }
        return buff.toString();
    }

    public int getPowerOffCount() {
        return 0;
    }

    public void setPowerOffCount(int count) throws SQLException {
        throw Message.getUnsupportedException("remote");
    }

    public SessionInterface createSession(ConnectionInfo ci) throws SQLException {
        return new SessionRemote(ci).connectEmbeddedOrServer(false);
    }

    private SessionInterface connectEmbeddedOrServer(boolean openNew) throws SQLException {
        ConnectionInfo ci = connectionInfo;
        if (ci.isRemote()) {
            connectServer(ci);
            return this;
        }
        // create the session using reflection,
        // so that the JDBC layer can be compiled without it
        boolean autoServerMode = Boolean.valueOf(ci.getProperty("AUTO_SERVER", "false")).booleanValue();
        ConnectionInfo backup = null;
        try {
            if (autoServerMode) {
                backup = (ConnectionInfo) ci.clone();
                connectionInfo = (ConnectionInfo) ci.clone();
            }
            SessionFactory sf = (SessionFactory) ClassUtils.loadSystemClass("org.h2.engine.SessionFactoryEmbedded").newInstance();
            if (openNew) {
                ci.setProperty("OPEN_NEW", "true");
            }
            return sf.createSession(ci);
        } catch (SQLException e) {
            int errorCode = e.getErrorCode();
            if (errorCode == ErrorCode.DATABASE_ALREADY_OPEN_1) {
                if (autoServerMode) {
                    String serverKey = (String) ((JdbcSQLException) e).getPayload();
                    if (serverKey != null) {
                        backup.setServerKey(serverKey);
                        // OPEN_NEW must be removed now, otherwise
                        // opening a session with AUTO_SERVER fails
                        // if another connection is already open
                        backup.removeProperty("OPEN_NEW", null);
                        connectServer(backup);
                        return this;
                    }
                }
            }
            throw e;
        } catch (Exception e) {
            throw Message.convert(e);
        }
    }

    private void connectServer(ConnectionInfo ci) throws SQLException {
        String name = ci.getName();
        if (name.startsWith("//")) {
            name = name.substring("//".length());
        }
        int idx = name.indexOf('/');
        if (idx < 0) {
            throw ci.getFormatException();
        }
        databaseName = name.substring(idx + 1);
        String server = name.substring(0, idx);
        traceSystem = new TraceSystem(null, false);
        try {
            String traceLevelFile = ci.getProperty(SetTypes.TRACE_LEVEL_FILE, null);
            if (traceLevelFile != null) {
                int level = Integer.parseInt(traceLevelFile);
                String prefix = getFilePrefix(SysProperties.CLIENT_TRACE_DIRECTORY);
                String file = FileUtils.createTempFile(prefix, Constants.SUFFIX_TRACE_FILE, false, false);
                traceSystem.setFileName(file);
                traceSystem.setLevelFile(level);
            }
            String traceLevelSystemOut = ci.getProperty(SetTypes.TRACE_LEVEL_SYSTEM_OUT, null);
            if (traceLevelSystemOut != null) {
                int level = Integer.parseInt(traceLevelSystemOut);
                traceSystem.setLevelSystemOut(level);
            }
        } catch (Exception e) {
            throw Message.convert(e);
        }
        trace = traceSystem.getTrace(Trace.JDBC);
        String serverList = null;
        if (server.indexOf(',') >= 0) {
            serverList = StringUtils.quoteStringSQL(server);
            ci.setProperty("CLUSTER", serverList);
        }
        autoReconnect = Boolean.valueOf(ci.getProperty("AUTO_RECONNECT", "false")).booleanValue();
        // AUTO_SERVER implies AUTO_RECONNECT
        autoReconnect |= Boolean.valueOf(ci.getProperty("AUTO_SERVER", "false")).booleanValue();
        if (autoReconnect && serverList != null) {
            throw Message.getUnsupportedException("autoReconnect && serverList != null");
        }
        if (autoReconnect) {
            eventListener = ci.getDatabaseEventListenerObject();
            if (eventListener == null) {
                String className = ci.getProperty("DATABASE_EVENT_LISTENER");
                if (className != null) {
                    className = StringUtils.trim(className, true, true, "'");
                    try {
                        eventListener = (DatabaseEventListener) ClassUtils.loadUserClass(className).newInstance();
                    } catch (Throwable e) {
                        throw Message.convert(e);
                    }
                }
            }
        }
        cipher = ci.getProperty("CIPHER");
        if (cipher != null) {
            fileEncryptionKey = RandomUtils.getSecureBytes(32);
        }
        String[] servers = StringUtils.arraySplit(server, ',', true);
        int len = servers.length;
        transferList.clear();
        // TODO cluster: support at most 2 connections
        boolean switchOffCluster = false;
        try {
            for (int i = 0; i < len; i++) {
                try {
                    Transfer trans = initTransfer(ci, databaseName, servers[i]);
                    transferList.add(trans);
                } catch (IOException e) {
                    switchOffCluster = true;
                }
            }
            checkClosed();
            if (switchOffCluster) {
                switchOffCluster();
            }
            switchOffAutoCommitIfCluster();
        } catch (SQLException e) {
            traceSystem.close();
            throw e;
        }
        upgradeClientVersionIfPossible();
    }

    private void upgradeClientVersionIfPossible() {
        try {
            // TODO check if a newer client version can be used
            // not required when sending TCP_DRIVER_VERSION_6
            CommandInterface command = prepareCommand("SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME=?", 1);
            ParameterInterface param = command.getParameters().get(0);
            param.setValue(ValueString.get("info.BUILD_ID"), false);
            ResultInterface result = command.executeQuery(1, false);
            if (result.next()) {
                Value[] v = result.currentRow();
                int version = v[0].getInt();
                if (version > 71) {
                    clientVersion = Constants.TCP_PROTOCOL_VERSION_6;
                }
            }
            result.close();
        } catch (Exception e) {
            trace.error("Error trying to upgrade client version", e);
            // ignore
        }
        if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_6) {
            sessionId = ByteUtils.convertBytesToString(RandomUtils.getSecureBytes(32));
            synchronized (this) {
                for (Transfer transfer : transferList) {
                    try {
                        traceOperation("SESSION_SET_ID", 0);
                        transfer.writeInt(SessionRemote.SESSION_SET_ID);
                        transfer.writeString(sessionId);
                        done(transfer);
                    } catch (Exception e) {
                        trace.error("sessionSetId", e);
                    }
                }
            }

        }
    }

    private void switchOffCluster() throws SQLException {
        CommandInterface ci = prepareCommand("SET CLUSTER ''", Integer.MAX_VALUE);
        ci.executeUpdate();
    }

    /**
     * Remove a server from the list of cluster nodes and disables the cluster
     * mode.
     *
     * @param e the exception (used for debugging)
     * @param i the index of the server to remove
     * @param count the retry count index
     */
    public void removeServer(IOException e, int i, int count) throws SQLException {
        transferList.remove(i);
        if (autoReconnect(count)) {
            return;
        }
        checkClosed();
        switchOffCluster();
    }

    public CommandInterface prepareCommand(String sql, int fetchSize) throws SQLException {
        synchronized (this) {
            checkClosed();
            return new CommandRemote(this, transferList, sql, fetchSize);
        }
    }

    /**
     * Automatically re-connect if necessary and if configured to do so.
     *
     * @param count the retry count index
     * @return true if reconnected
     */
    public boolean autoReconnect(int count) throws SQLException {
        if (!isClosed()) {
            return false;
        }
        if (!autoReconnect || !autoCommit) {
            return false;
        }
        if (count > SysProperties.MAX_RECONNECT) {
            return false;
        }
        lastReconnect++;
        embedded = connectEmbeddedOrServer(false);
        if (embedded == this) {
            // connected to a server somewhere else
            embedded = null;
        } else {
            // opened an embedded connection now -
            // must connect to this database in server mode
            // unfortunately
            connectEmbeddedOrServer(true);
        }
        recreateSessionState();
        if (eventListener != null) {
            eventListener.setProgress(DatabaseEventListener.STATE_RECONNECTED, databaseName, count,
                    SysProperties.MAX_RECONNECT);
        }
        return true;
    }

    /**
     * Check if this session is closed and throws an exception if so.
     *
     * @throws SQLException if the session is closed
     */
    public void checkClosed() throws SQLException {
        if (isClosed()) {
            throw Message.getSQLException(ErrorCode.CONNECTION_BROKEN);
        }
    }

    public void close() throws SQLException {
        if (transferList != null) {
            synchronized (this) {
                for (Transfer transfer : transferList) {
                    try {
                        traceOperation("SESSION_CLOSE", 0);
                        transfer.writeInt(SessionRemote.SESSION_CLOSE);
                        done(transfer);
                        transfer.close();
                    } catch (Exception e) {
                        trace.error("close", e);
                    }
                }
            }
            transferList = null;
        }
        traceSystem.close();
        if (embedded != null) {
            embedded.close();
            embedded = null;
        }
    }

    public Trace getTrace() {
        return traceSystem.getTrace(Trace.JDBC);
    }

    public int getNextId() {
        return nextId++;
    }

    public int getCurrentId() {
        return nextId;
    }

    /**
     * Called to flush the output after data has been sent to the server and
     * just before receiving data. This method also reads the status code from
     * the server and throws any exception the server sent.
     *
     * @param transfer the transfer object
     * @throws SQLException if the server sent an exception
     * @throws IOException if there is a communication problem between client
     *             and server
     */
    public void done(Transfer transfer) throws SQLException, IOException {
        transfer.flush();
        int status = transfer.readInt();
        if (status == STATUS_ERROR) {
            String sqlstate = transfer.readString();
            String message = transfer.readString();
            String sql = transfer.readString();
            int errorCode = transfer.readInt();
            String stackTrace = transfer.readString();
            throw new JdbcSQLException(message, sql, sqlstate, errorCode, null, stackTrace);
        } else if (status == STATUS_CLOSED) {
            transferList = null;
        } else if (status == STATUS_OK_STATE_CHANGED) {
            sessionStateChanged = true;
        }
    }

    /**
     * Returns true if the connection is in cluster mode.
     *
     * @return true if it is
     */
    public boolean isClustered() {
        return transferList.size() > 1;
    }

    public boolean isClosed() {
        return transferList == null || transferList.size() == 0;
    }

    /**
     * Write the operation to the trace system if debug trace is enabled.
     *
     * @param operation the operation performed
     * @param id the id of the operation
     */
    public void traceOperation(String operation, int id) {
        if (trace.isDebugEnabled()) {
            trace.debug(operation + " " + id);
        }
    }

    public int allocateObjectId(boolean needFresh, boolean dataFile) {
        return objectId++;
    }

    public void checkPowerOff() {
        // ok
    }

    public void checkWritingAllowed() {
        // ok
    }

    public int compareTypeSave(Value a, Value b) {
        throw Message.throwInternalError();
    }

    public String createTempFile() throws SQLException {
        try {
            String prefix = getFilePrefix(System.getProperty("java.io.tmpdir"));
            return FileUtils.createTempFile(prefix, Constants.SUFFIX_TEMP_FILE, true, false);
        } catch (IOException e) {
            throw Message.convertIOException(e, databaseName);
        }
    }

    public void freeUpDiskSpace() {
        // nothing to do
    }

    public int getChecksum(byte[] data, int start, int end) {
        return 0;
    }

    public String getDatabasePath() {
        return "";
    }

    public String getLobCompressionAlgorithm(int type) {
        return null;
    }

    public int getMaxLengthInplaceLob() {
        return Constants.DEFAULT_MAX_LENGTH_CLIENTSIDE_LOB;
    }

    public void handleInvalidChecksum() throws SQLException {
        throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "wrong checksum");
    }

    public FileStore openFile(String name, String mode, boolean mustExist) throws SQLException {
        if (mustExist && !FileUtils.exists(name)) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, name);
        }
        FileStore store;
        if (cipher == null) {
            store = FileStore.open(this, name, mode);
        } else {
            store = FileStore.open(this, name, mode, cipher, fileEncryptionKey, 0);
        }
        store.setCheckedWriting(false);
        try {
            store.init();
        } catch (SQLException e) {
            store.closeSilently();
            throw e;
        }
        return store;
    }

    public DataHandler getDataHandler() {
        return this;
    }

    public Object getLobSyncObject() {
        return lobSyncObject;
    }

    public boolean getLobFilesInDirectories() {
        return false;
    }

    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return null;
    }

    public int getClientVersion() {
        return clientVersion;
    }

    public int getLastReconnect() {
        return lastReconnect;
    }

    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

    public boolean isReconnectNeeded(boolean write) {
        return false;
    }

    public SessionInterface reconnect() {
        return this;
    }

}
