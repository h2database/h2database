/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.io.IOException;
import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.SessionRemote;
import org.h2.message.Message;
import org.h2.util.ObjectArray;
import org.h2.value.Transfer;
import org.h2.value.Value;

public class ResultRemote implements ResultInterface {

    private SessionRemote session;
    private Transfer transfer;
    private int id;
    private ResultColumn[] columns;
    private Value[] currentRow;
    private int rowId, rowCount;
    private ObjectArray result;
    private ObjectArray lobValues;
    
    private boolean isUpdateCount;
    private int updateCount;    
    
    public ResultRemote(int updateCount) {
        this.isUpdateCount = true;
        this.updateCount = updateCount;
    }
    
    public boolean isUpdateCount() {
        return isUpdateCount;
    }
    
    public int getUpdateCount() {
        return updateCount;
    }    

    public ResultRemote(SessionRemote session, Transfer transfer, int id, int columnCount, int readRows) throws IOException, SQLException {
        this.session = session;
        this.transfer = transfer;
        this.id = id;
        this.columns = new ResultColumn[columnCount];
        rowCount = transfer.readInt();
        for (int i = 0; i < columnCount; i++) {
            columns[i] = new ResultColumn(transfer);
        }
        rowId = -1;
        if(rowCount < readRows) {
            result = new ObjectArray();
            readFully();
            sendClose();
        }
    }
    
    private void readFully() throws SQLException {
        while(true) {
            Value[] values = fetchRow(false);
            if(values == null) {
                break;
            }
            result.add(values);
        }
    }

    public String getAlias(int i) {
        return columns[i].alias;
    }

    public String getSchemaName(int i) {
        return columns[i].schemaName;
    }
    
    public String getTableName(int i) {
        return columns[i].tableName;
    }

    public String getColumnName(int i) {
        return columns[i].columnName;
    }

    public int getColumnType(int i) {
        return columns[i].columnType;
    }

    public long getColumnPrecision(int i) {
        return columns[i].precision;
    }

    public int getColumnScale(int i) {
        return columns[i].scale;
    }
    
    public int getDisplaySize(int i) {
        return columns[i].displaySize;
    }
    
    public boolean isAutoIncrement(int i) {
        return columns[i].autoIncrement;
    }    
    
    public int getNullable(int i) {
        return columns[i].nullable;
    }

    public void reset() throws SQLException {
        rowId = -1;
        currentRow = null;
        if(session == null) {
            return;
        }
        synchronized (session) {
            session.checkClosed();
            try {
                session.traceOperation("RESULT_RESET", id);
                transfer.writeInt(SessionRemote.RESULT_RESET).writeInt(id).flush();
            } catch (IOException e) {
                throw Message.convertIOException(e, null);
            }
        }
    }

    public Value[] currentRow() {
        return currentRow;
    }

    public boolean next() throws SQLException {
        // TODO optimization: don't need rowCount and fetchRow setting
        if (rowId < rowCount) {
            rowId++;
            if (rowId < rowCount) {
                if(session == null) {
                    currentRow = (Value[]) result.get(rowId);
                } else {
                    currentRow = fetchRow(true);
                }
                return true;
            }
            currentRow = null;
        }
        return false;
    }

    public int getRowId() {
        return rowId;
    }

    public int getVisibleColumnCount() {
        return columns.length;
    }

    public int getRowCount() {
        return rowCount;
    }
    
    private void sendClose() {
        if (session == null) {
            return;
        }
        // TODO result sets: no reset possible for larger remote result sets
        synchronized (session) {
            try {
                session.traceOperation("RESULT_CLOSE", id);
                transfer.writeInt(SessionRemote.RESULT_CLOSE).writeInt(id);
            } catch (IOException e) {
                session.getTrace().error("close", e);
            } finally {
                transfer = null;
                session = null;
            }
        }
    }

    public void close() {
        result = null;
        sendClose();
        if(lobValues != null) {
            for(int i=0; i<lobValues.size(); i++) {
                Value v = (Value) lobValues.get(i);
                try {
                    v.close();
                } catch(SQLException e) {
                    session.getTrace().error("delete lob " + v.getSQL(), e);
                }
            }
            lobValues = null;
        }
    }

    private Value[] fetchRow(boolean sendFetch) throws SQLException {
        synchronized (session) {
            session.checkClosed();
            try {
                if(id <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS / 2) {
                    // object is too old - we need to map it to a new id
                    int newId = session.getNextId();
                    session.traceOperation("CHANGE_ID", id);
                    transfer.writeInt(SessionRemote.CHANGE_ID).writeInt(id).writeInt(newId);
                    id = newId;
                    // TODO remote result set: very old result sets may be already removed on the server (theoretically) - how to solve this?
                }
                if(sendFetch) {
                    session.traceOperation("RESULT_FETCH_ROW", id);
                    transfer.writeInt(SessionRemote.RESULT_FETCH_ROW).writeInt(id);
                    session.done(transfer);
                }
                boolean row = transfer.readBoolean();
                if (row) {
                    int len = columns.length;
                    Value[] values = new Value[len]; 
                    for (int i = 0; i < len; i++) {
                        Value v = transfer.readValue();
                        values[i] = v;
                        if(v.isFileBased()) {
                            if(lobValues == null) {
                                lobValues = new ObjectArray();
                            }
                            lobValues.add(v);
                        }
                    }
                    return values;
                } else {
                    sendClose();
                    return null;
                }
            } catch (IOException e) {
                throw Message.convertIOException(e, null);
            }
        }
    }

}
