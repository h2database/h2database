/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.io.IOException;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.SessionInterface;
import org.h2.engine.SessionRemote;
import org.h2.expression.ParameterInterface;
import org.h2.expression.ParameterRemote;
import org.h2.message.Trace;
import org.h2.result.ResultInterface;
import org.h2.result.ResultRemote;
import org.h2.util.ObjectArray;
import org.h2.value.Transfer;

public class CommandRemote implements CommandInterface {

    private SessionRemote session;
    private ObjectArray transferList;
    private int id;
    private boolean isQuery;
    private ObjectArray parameters;
    private Trace trace;
    private String sql;
    private int paramCount;
    
    private void prepare(SessionRemote session) throws SQLException {
        id = session.getNextId();
        paramCount = 0;
        for(int i=0; i<transferList.size(); i++) {
            try {
                Transfer transfer = (Transfer) transferList.get(i);
                session.traceOperation("SESSION_PREPARE", id);
                transfer.writeInt(SessionRemote.SESSION_PREPARE).writeInt(id).writeString(sql);
                session.done(transfer);
                isQuery = transfer.readBoolean();
                paramCount = transfer.readInt();
            } catch(IOException e) {
                session.removeServer(i);
            }
        }
    }

    public CommandRemote(SessionRemote session, ObjectArray transferList, String sql) throws SQLException {
        this.transferList = transferList;
        trace = session.getTrace();
        this.sql = sql;
        parameters = new ObjectArray();
        prepare(session);
        for(int i=0; i<paramCount; i++) {
            parameters.add(new ParameterRemote(i));
        }
        // set session late because prepare might fail - in this case we don't need to close the object
        this.session = session;
    }

    public boolean isQuery() {
        return isQuery;
    }

    public ObjectArray getParameters() {
        return parameters;
    }
    
    public ResultInterface executeQuery(int maxRows, boolean scrollable) throws SQLException {
        checkParameters();        
        synchronized(session) {
            session.checkClosed();
            if(id <= session.getCurrentId() - Constants.SERVER_CACHED_OBJECTS) {
                // object is too old - we need to prepare again
                prepare(session);
            }
            int objectId = session.getNextId();
            ResultRemote result = null;
            // TODO cluster: test sequences and so on
            for(int i=0; i<transferList.size(); i++) {
                Transfer transfer = (Transfer) transferList.get(i);
                try {
                    // TODO cluster: support load balance with values for each server / auto detect
                    session.traceOperation("COMMAND_EXECUTE_QUERY", id);
                    transfer.writeInt(SessionRemote.COMMAND_EXECUTE_QUERY).writeInt(id).writeInt(objectId).writeInt(maxRows);
                    int readRows;
                    if(session.isClustered() || scrollable) {
                        readRows = Integer.MAX_VALUE;
                    } else {
                        readRows = Constants.SERVER_SMALL_RESULTSET_SIZE;
                    }
                    transfer.writeInt(readRows);
                    sendParameters(transfer);
                    session.done(transfer);
                    int columnCount = transfer.readInt();
                    if(result != null) {
                        result.close();
                        result = null;
                    }
                    result = new ResultRemote(session, transfer, objectId, columnCount, readRows);
                } catch(IOException e) {
                    session.removeServer(i);
                }
            }
            session.autoCommitIfCluster();
            return result;
        }
    }

    public int executeUpdate() throws SQLException {
        checkParameters();
        synchronized(session) {
            session.checkClosed();
            if(id <= session.getCurrentId() - Constants.SERVER_CACHED_OBJECTS) {
                // object is too old - we need to prepare again
                prepare(session);
            }            
            int updateCount = 0;
            boolean autoCommit = false;
            for(int i=0; i<transferList.size(); i++) {
                try {
                    Transfer transfer = (Transfer) transferList.get(i);
                    session.traceOperation("COMMAND_EXECUTE_UPDATE", id);
                    transfer.writeInt(SessionRemote.COMMAND_EXECUTE_UPDATE).writeInt(id);
                    sendParameters(transfer);
                    session.done(transfer);
                    updateCount = transfer.readInt();
                    autoCommit = transfer.readBoolean();
                } catch(IOException e) {
                    session.removeServer(i);
                }
            }
            session.setAutoCommit(autoCommit);
            session.autoCommitIfCluster();
            return updateCount;
        }
    }
    
    private void checkParameters() throws SQLException {
        int len = parameters.size();
        for(int i=0; i<len; i++) {
            ParameterInterface p = (ParameterInterface)parameters.get(i);
            p.checkSet();
        }
    }

    private void sendParameters(Transfer transfer) throws IOException, SQLException {
        int len = parameters.size();
        transfer.writeInt(len);
        for(int i=0; i<len; i++) {
            ParameterInterface p = (ParameterInterface)parameters.get(i);
            transfer.writeValue(p.getParamValue());
        }
    }

    public SessionInterface getSession() {
        return session;
    }

    public void close() {
        if(session == null || session.isClosed()) {
            return;
        }
        synchronized(session) {
            for(int i=0; i<transferList.size(); i++) {
                try {
                    Transfer transfer = (Transfer) transferList.get(i);
                    session.traceOperation("COMMAND_CLOSE", id);
                    transfer.writeInt(SessionRemote.COMMAND_CLOSE).writeInt(id);
                } catch (IOException e) {
                    // TODO cluster: do we need to to handle ioexception on close?
                    trace.error("close", e);
                }
            }
            session = null;
        }
    }

//    public void finalize() {
//        if(!Database.RUN_FINALIZERS) {
//            return;
//        }        
//        close();
//    }

    public void cancel() {
        // TODO server: support cancel
    }

}
