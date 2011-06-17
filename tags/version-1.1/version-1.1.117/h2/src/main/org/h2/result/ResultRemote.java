/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.io.IOException;
import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.SessionRemote;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.util.ObjectArray;
import org.h2.value.Transfer;
import org.h2.value.Value;

/**
 * The client side part of a result set that is kept on the server.
 * In many cases, the complete data is kept on the client side,
 * but for large results only a subset is in-memory.
 */
public class ResultRemote implements ResultInterface {

    private int fetchSize;
    private SessionRemote session;
    private Transfer transfer;
    private int id;
    private ResultColumn[] columns;
    private Value[] currentRow;
    private int rowId, rowCount, rowOffset;
    private ObjectArray<Value[]> result;
    private ObjectArray<Value> lobValues;

    public ResultRemote(SessionRemote session, Transfer transfer, int id, int columnCount, int fetchSize)
            throws IOException, SQLException {
        this.session = session;
        this.transfer = transfer;
        this.id = id;
        this.columns = new ResultColumn[columnCount];
        rowCount = transfer.readInt();
        for (int i = 0; i < columnCount; i++) {
            columns[i] = new ResultColumn(transfer);
        }
        rowId = -1;
        result = ObjectArray.newInstance();
        this.fetchSize = fetchSize;
        fetchRows(false);
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
        if (session == null) {
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
        if (rowId < rowCount) {
            rowId++;
            remapIfOld();
            if (rowId < rowCount) {
                if (rowId - rowOffset >= result.size()) {
                    fetchRows(true);
                }
                currentRow = result.get(rowId - rowOffset);
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
        try {
            synchronized (session) {
                session.traceOperation("RESULT_CLOSE", id);
                transfer.writeInt(SessionRemote.RESULT_CLOSE).writeInt(id);
            }
        } catch (IOException e) {
            session.getTrace().error("close", e);
        } finally {
            transfer = null;
            session = null;
        }
    }

    public void close() {
        if (session == null) {
            return;
        }
        result = null;
        Trace trace = session.getTrace();
        sendClose();
        if (lobValues != null) {
            for (Value v : lobValues) {
                try {
                    v.close();
                } catch (SQLException e) {
                    trace.error("delete lob " + v.getTraceSQL(), e);
                }
            }
            lobValues = null;
        }
    }

    private void remapIfOld() throws SQLException {
        if (session == null) {
            return;
        }
        try {
            if (id <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS / 2) {
                // object is too old - we need to map it to a new id
                int newId = session.getNextId();
                session.traceOperation("CHANGE_ID", id);
                transfer.writeInt(SessionRemote.CHANGE_ID).writeInt(id).writeInt(newId);
                id = newId;
                // TODO remote result set: very old result sets may be
                // already removed on the server (theoretically) - how to
                // solve this?
            }
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    private void fetchRows(boolean sendFetch) throws SQLException {
        synchronized (session) {
            session.checkClosed();
            try {
                rowOffset += result.size();
                result.clear();
                int fetch = Math.min(fetchSize, rowCount - rowOffset);
                if (sendFetch) {
                    session.traceOperation("RESULT_FETCH_ROWS", id);
                    transfer.writeInt(SessionRemote.RESULT_FETCH_ROWS).writeInt(id).writeInt(fetch);
                    session.done(transfer);
                }
                for (int r = 0; r < fetch; r++) {
                    boolean row = transfer.readBoolean();
                    if (!row) {
                        break;
                    }
                    int len = columns.length;
                    Value[] values = new Value[len];
                    for (int i = 0; i < len; i++) {
                        Value v = transfer.readValue();
                        values[i] = v;
                        if (v.isFileBased()) {
                            if (lobValues == null) {
                                lobValues = ObjectArray.newInstance();
                            }
                            lobValues.add(v);
                        }
                    }
                    result.add(values);
                }
                if (rowOffset + result.size() >= rowCount) {
                    sendClose();
                }
            } catch (IOException e) {
                throw Message.convertIOException(e, null);
            }
        }
    }

    public String toString() {
        return "columns: " + columns.length + " rows: " + rowCount + " pos: " + rowId;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

}
