/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
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

/**
 * Represents the client-side part of a SQL statement.
 * This class is not used in embedded mode.
 */
public class CommandRemote implements CommandInterface {

    private final ObjectArray transferList;
    private final ObjectArray parameters;
    private final Trace trace;
    private final String sql;
    private final int fetchSize;
    private SessionRemote session;
    private int id;
    private boolean isQuery;
    private boolean readonly;
    private int paramCount;

    public CommandRemote(SessionRemote session, ObjectArray transferList, String sql, int fetchSize) throws SQLException {
        this.transferList = transferList;
        trace = session.getTrace();
        this.sql = sql;
        parameters = new ObjectArray();
        prepare(session, true);
        // set session late because prepare might fail - in this case we don't
        // need to close the object
        this.session = session;
        this.fetchSize = fetchSize;
    }

    private void prepare(SessionRemote session, boolean createParams) throws SQLException {
        id = session.getNextId();
        paramCount = 0;
        boolean readParams = session.getClientVersion() >= Constants.TCP_DRIVER_VERSION_6;
        for (int i = 0; i < transferList.size(); i++) {
            try {
                Transfer transfer = (Transfer) transferList.get(i);
                if (readParams) {
                    session.traceOperation("SESSION_PREPARE_READ_PARAMS", id);
                    transfer.writeInt(SessionRemote.SESSION_PREPARE_READ_PARAMS).writeInt(id).writeString(sql);
                } else {
                    session.traceOperation("SESSION_PREPARE", id);
                    transfer.writeInt(SessionRemote.SESSION_PREPARE).writeInt(id).writeString(sql);
                }
                session.done(transfer);
                isQuery = transfer.readBoolean();
                readonly = transfer.readBoolean();
                paramCount = transfer.readInt();
                if (createParams) {
                    parameters.clear();
                    for (int j = 0; j < paramCount; j++) {
                        if (readParams) {
                            ParameterRemote p = new ParameterRemote(j);
                            p.read(transfer);
                            parameters.add(p);
                        } else {
                            parameters.add(new ParameterRemote(j));
                        }
                    }
                }
            } catch (IOException e) {
                session.removeServer(i--);
            }
        }
    }

    public boolean isQuery() {
        return isQuery;
    }

    public ObjectArray getParameters() {
        return parameters;
    }

    public ResultInterface getMetaData() throws SQLException {
        synchronized (session) {
            session.checkClosed();
            if (!isQuery) {
                return null;
            }
            if (id <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS) {
                // object is too old - we need to prepare again
                prepare(session, false);
            }
            int objectId = session.getNextId();
            ResultRemote result = null;
            for (int i = 0; i < transferList.size(); i++) {
                Transfer transfer = (Transfer) transferList.get(i);
                try {
                    // TODO cluster: support load balance with values for each server / auto detect
                    session.traceOperation("COMMAND_GET_META_DATA", id);
                    transfer.writeInt(SessionRemote.COMMAND_GET_META_DATA).writeInt(id).writeInt(objectId);
                    session.done(transfer);
                    int columnCount = transfer.readInt();
                    result = new ResultRemote(session, transfer, objectId, columnCount, Integer.MAX_VALUE);
                    break;
                } catch (IOException e) {
                    session.removeServer(i--);
                }
            }
            session.autoCommitIfCluster();
            return result;
        }
    }

    public ResultInterface executeQuery(int maxRows, boolean scrollable) throws SQLException {
        checkParameters();
        synchronized (session) {
            session.checkClosed();
            if (id <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS) {
                // object is too old - we need to prepare again
                prepare(session, false);
            }
            int objectId = session.getNextId();
            ResultRemote result = null;
            for (int i = 0; i < transferList.size(); i++) {
                Transfer transfer = (Transfer) transferList.get(i);
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
                    session.removeServer(i--);
                }
            }
            session.autoCommitIfCluster();
            return result;
        }
    }

    public int executeUpdate() throws SQLException {
        checkParameters();
        synchronized (session) {
            session.checkClosed();
            if (id <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS) {
                // object is too old - we need to prepare again
                prepare(session, false);
            }
            int updateCount = 0;
            boolean autoCommit = false;
            for (int i = 0; i < transferList.size(); i++) {
                try {
                    Transfer transfer = (Transfer) transferList.get(i);
                    session.traceOperation("COMMAND_EXECUTE_UPDATE", id);
                    transfer.writeInt(SessionRemote.COMMAND_EXECUTE_UPDATE).writeInt(id);
                    sendParameters(transfer);
                    session.done(transfer);
                    updateCount = transfer.readInt();
                    autoCommit = transfer.readBoolean();
                } catch (IOException e) {
                    session.removeServer(i--);
                }
            }
            session.setAutoCommit(autoCommit);
            session.autoCommitIfCluster();
            return updateCount;
        }
    }

    private void checkParameters() throws SQLException {
        int len = parameters.size();
        for (int i = 0; i < len; i++) {
            ParameterInterface p = (ParameterInterface) parameters.get(i);
            p.checkSet();
        }
    }

    private void sendParameters(Transfer transfer) throws IOException, SQLException {
        int len = parameters.size();
        transfer.writeInt(len);
        for (int i = 0; i < len; i++) {
            ParameterInterface p = (ParameterInterface) parameters.get(i);
            transfer.writeValue(p.getParamValue());
        }
    }

    public void close() {
        if (session == null || session.isClosed()) {
            return;
        }
        synchronized (session) {
            for (int i = 0; i < transferList.size(); i++) {
                try {
                    Transfer transfer = (Transfer) transferList.get(i);
                    session.traceOperation("COMMAND_CLOSE", id);
                    transfer.writeInt(SessionRemote.COMMAND_CLOSE).writeInt(id);
                } catch (IOException e) {
                    // TODO cluster: do we need to to handle io exception on
                    // close?
                    trace.error("close", e);
                }
            }
            session = null;
        }
    }

    /**
     * Cancel this current statement.
     * This method is not yet implemented for this class.
     */
    public void cancel() {
        // TODO server: support cancel
    }

    public String toString() {
        return TraceObject.toString(sql, getParameters());
    }

}
