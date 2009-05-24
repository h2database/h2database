/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.io.IOException;
import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.SessionRemote;
import org.h2.expression.ParameterInterface;
import org.h2.expression.ParameterRemote;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.result.ResultRemote;
import org.h2.util.ObjectArray;
import org.h2.value.Transfer;
import org.h2.value.Value;

/**
 * Represents the client-side part of a SQL statement.
 * This class is not used in embedded mode.
 */
public class CommandRemote implements CommandInterface {

    private final ObjectArray<Transfer> transferList;
    private final ObjectArray<ParameterInterface> parameters;
    private final Trace trace;
    private final String sql;
    private final int fetchSize;
    private SessionRemote session;
    private int id;
    private boolean isQuery;
    private boolean readonly;
    private int paramCount;
    private int created;

    public CommandRemote(SessionRemote session, ObjectArray<Transfer> transferList, String sql, int fetchSize) throws SQLException {
        this.transferList = transferList;
        trace = session.getTrace();
        this.sql = sql;
        parameters = ObjectArray.newInstance();
        prepare(session, true);
        // set session late because prepare might fail - in this case we don't
        // need to close the object
        this.session = session;
        this.fetchSize = fetchSize;
        created = session.getLastReconnect();
    }

    private void prepare(SessionRemote s, boolean createParams) throws SQLException {
        id = s.getNextId();
        paramCount = 0;
        boolean readParams = s.getClientVersion() >= Constants.TCP_PROTOCOL_VERSION_6;
        for (int i = 0, count = 0; i < transferList.size(); i++) {
            try {
                Transfer transfer = transferList.get(i);
                if (readParams && createParams) {
                    s.traceOperation("SESSION_PREPARE_READ_PARAMS", id);
                    transfer.writeInt(SessionRemote.SESSION_PREPARE_READ_PARAMS).writeInt(id).writeString(sql);
                } else {
                    s.traceOperation("SESSION_PREPARE", id);
                    transfer.writeInt(SessionRemote.SESSION_PREPARE).writeInt(id).writeString(sql);
                }
                s.done(transfer);
                isQuery = transfer.readBoolean();
                readonly = transfer.readBoolean();
                paramCount = transfer.readInt();
                if (createParams) {
                    parameters.clear();
                    for (int j = 0; j < paramCount; j++) {
                        if (readParams) {
                            ParameterRemote p = new ParameterRemote(j);
                            p.readMetaData(transfer);
                            parameters.add(p);
                        } else {
                            parameters.add(new ParameterRemote(j));
                        }
                    }
                }
            } catch (IOException e) {
                s.removeServer(e, i--, ++count);
            }
        }
    }

    public boolean isQuery() {
        return isQuery;
    }

    public ObjectArray<ParameterInterface> getParameters() {
        return parameters;
    }

    private void prepareIfRequired() throws SQLException {
        if (session.getLastReconnect() != created) {
            // in this case we need to prepare again in every case
            id = Integer.MIN_VALUE;
        }
        session.checkClosed();
        if (id <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS) {
            // object is too old - we need to prepare again
            prepare(session, false);
        }
    }

    public ResultInterface getMetaData() throws SQLException {
        synchronized (session) {
            if (!isQuery) {
                return null;
            }
            int objectId = session.getNextId();
            ResultRemote result = null;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                Transfer transfer = transferList.get(i);
                try {
                    // TODO cluster: support load balance with values for each server / auto detect
                    session.traceOperation("COMMAND_GET_META_DATA", id);
                    transfer.writeInt(SessionRemote.COMMAND_GET_META_DATA).writeInt(id).writeInt(objectId);
                    session.done(transfer);
                    int columnCount = transfer.readInt();
                    result = new ResultRemote(session, transfer, objectId, columnCount, Integer.MAX_VALUE);
                    break;
                } catch (IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            session.autoCommitIfCluster();
            return result;
        }
    }

    public ResultInterface executeQuery(int maxRows, boolean scrollable) throws SQLException {
        checkParameters();
        synchronized (session) {
            int objectId = session.getNextId();
            ResultRemote result = null;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                Transfer transfer = transferList.get(i);
                try {
                    // TODO cluster: support load balance with values for each
                    // server / auto detect
                    session.traceOperation("COMMAND_EXECUTE_QUERY", id);
                    transfer.writeInt(SessionRemote.COMMAND_EXECUTE_QUERY).writeInt(id).writeInt(objectId).writeInt(
                            maxRows);
                    int fetch;
                    if (session.isClustered() || scrollable) {
                        fetch = Integer.MAX_VALUE;
                    } else {
                        fetch = fetchSize;
                    }
                    transfer.writeInt(fetch);
                    sendParameters(transfer);
                    session.done(transfer);
                    int columnCount = transfer.readInt();
                    if (result != null) {
                        result.close();
                        result = null;
                    }
                    result = new ResultRemote(session, transfer, objectId, columnCount, fetch);
                    if (readonly) {
                        break;
                    }
                } catch (IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            session.autoCommitIfCluster();
            session.readSessionState();
            return result;
        }
    }

    public int executeUpdate() throws SQLException {
        checkParameters();
        synchronized (session) {
            int updateCount = 0;
            boolean autoCommit = false;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                Transfer transfer = transferList.get(i);
                try {
                    session.traceOperation("COMMAND_EXECUTE_UPDATE", id);
                    transfer.writeInt(SessionRemote.COMMAND_EXECUTE_UPDATE).writeInt(id);
                    sendParameters(transfer);
                    session.done(transfer);
                    updateCount = transfer.readInt();
                    autoCommit = transfer.readBoolean();
                } catch (IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            session.setAutoCommit(autoCommit);
            session.autoCommitIfCluster();
            session.readSessionState();
            return updateCount;
        }
    }

    private void checkParameters() throws SQLException {
        for (ParameterInterface p : parameters) {
            p.checkSet();
        }
    }

    private void sendParameters(Transfer transfer) throws IOException, SQLException {
        int len = parameters.size();
        transfer.writeInt(len);
        for (ParameterInterface p : parameters) {
            transfer.writeValue(p.getParamValue());
        }
    }

    public void close() {
        if (session == null || session.isClosed()) {
            return;
        }
        synchronized (session) {
            session.traceOperation("COMMAND_CLOSE", id);
            for (Transfer transfer : transferList) {
                try {
                    transfer.writeInt(SessionRemote.COMMAND_CLOSE).writeInt(id);
                } catch (IOException e) {
                    trace.error("close", e);
                }
            }
        }
        session = null;
        try {
            for (ParameterInterface p : parameters) {
                Value v = p.getParamValue();
                if (v != null) {
                    v.close();
                }
            }
        } catch (SQLException e) {
            trace.error("close", e);
        }
        parameters.clear();
    }

    /**
     * Cancel this current statement.
     */
    public void cancel() {
        session.cancelStatement(id);
    }

    public String toString() {
        return TraceObject.toString(sql, getParameters());
    }

}
