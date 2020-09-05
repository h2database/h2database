/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.io.IOException;
import java.util.ArrayList;

import org.h2.engine.Session;
import org.h2.engine.SessionRemote;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.value.Transfer;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * The client side part of a result set that is kept on the server.
 * In many cases, the complete data is kept on the client side,
 * but for large results only a subset is in-memory.
 */
public abstract class ResultRemote implements ResultInterface {
    public static ResultRemote of(SessionRemote session, Transfer transfer, int id,
            int columnCount, int fetchSize) throws IOException {
        int rowCount = transfer.readInt();
        ResultRemote rr;
        if (rowCount < 0) {
            rr = new LazyResultRemote(session, transfer, id, columnCount, fetchSize);
        } else {
            rr = new RowCountResultRemote(session, transfer, id, columnCount, fetchSize, rowCount);
        }
        rr.fetchRows(false);
        return rr;
    }

    protected int fetchSize;
    protected SessionRemote session;
    protected Transfer transfer;
    protected int id;
    protected final ResultColumn[] columns;
    protected Value[] currentRow;
    protected int rowId, rowOffset;
    protected ArrayList<Value[]> result;
    protected final Trace trace;

    ResultRemote(SessionRemote session, Transfer transfer, int id,
            int columnCount, int fetchSize) throws IOException {
        this.session = session;
        trace = session.getTrace();
        this.transfer = transfer;
        this.id = id;
        this.columns = new ResultColumn[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columns[i] = new ResultColumn(transfer);
        }
        rowId = -1;
        result = new ArrayList<>();
        this.fetchSize = fetchSize;
    }

    @Override
    public boolean isLazy() {
        return false;
    }

    @Override
    public String getAlias(int i) {
        return columns[i].alias;
    }

    @Override
    public String getSchemaName(int i) {
        return columns[i].schemaName;
    }

    @Override
    public String getTableName(int i) {
        return columns[i].tableName;
    }

    @Override
    public String getColumnName(int i) {
        return columns[i].columnName;
    }

    @Override
    public TypeInfo getColumnType(int i) {
        return columns[i].columnType;
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return columns[i].autoIncrement;
    }

    @Override
    public int getNullable(int i) {
        return columns[i].nullable;
    }

    @Override
    public void reset() {
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
                throw DbException.convertIOException(e, null);
            }
        }
    }

    @Override
    public Value[] currentRow() {
        return currentRow;
    }

    @Override
    public int getRowId() {
        return rowId;
    }

    @Override
    public int getVisibleColumnCount() {
        return columns.length;
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
            trace.error(e, "close");
        } finally {
            transfer = null;
            session = null;
        }
    }

    @Override
    public void close() {
        result = null;
        sendClose();
    }

    private void remapIfOld() {
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
            throw DbException.convertIOException(e, null);
        }
    }

    protected abstract int remoteFetchSize();

    protected abstract boolean remoteFetchOver(boolean over);

    protected void fetchRows(boolean sendFetch) {
        synchronized (session) {
            session.checkClosed();
            try {
                rowOffset += result.size();
                result.clear();
                int fetch = remoteFetchSize();
                if (sendFetch) {
                    remapIfOld();
                    session.traceOperation("RESULT_FETCH_ROWS", id);
                    transfer.writeInt(SessionRemote.RESULT_FETCH_ROWS).
                            writeInt(id).writeInt(fetch);
                    session.done(transfer);
                }
                int r = 0;
                for (; r < fetch; r++) {
                    boolean row = transfer.readBoolean();
                    if (!row) {
                        break;
                    }
                    int len = columns.length;
                    Value[] values = new Value[len];
                    for (int i = 0; i < len; i++) {
                        Value v = transfer.readValue();
                        values[i] = v;
                    }
                    result.add(values);
                }
                if (remoteFetchOver(r < fetch)) {
                    sendClose();
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
        }
    }

    @Override
    public String toString() {
        return "columns: " + columns.length + " pos: " + rowId;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public boolean needToClose() {
        return true;
    }

    @Override
    public ResultInterface createShallowCopy(Session targetSession) {
        // The operation is not supported on remote result.
        return null;
    }

    @Override
    public boolean isClosed() {
        return result == null;
    }

}
