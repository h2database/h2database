/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.io.IOException;
import java.util.ArrayList;

import org.h2.api.ErrorCode;
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
public final class ResultRemote extends FetchedResult {

    private int fetchSize;
    private SessionRemote session;
    private Transfer transfer;
    private int id;
    private final ResultColumn[] columns;
    private long rowCount;
    private long rowOffset;
    private ArrayList<Value[]> result;
    private final Trace trace;

    public ResultRemote(SessionRemote session, Transfer transfer, int id,
            int columnCount, int fetchSize) throws IOException {
        this.session = session;
        trace = session.getTrace();
        this.transfer = transfer;
        this.id = id;
        this.columns = new ResultColumn[columnCount];
        rowCount = transfer.readRowCount();
        for (int i = 0; i < columnCount; i++) {
            columns[i] = new ResultColumn(transfer);
        }
        rowId = -1;
        this.fetchSize = fetchSize;
        if (rowCount >= 0) {
            fetchSize = (int) Math.min(rowCount, fetchSize);
            result = new ArrayList<>(fetchSize);
        } else {
            result = new ArrayList<>();
        }
        session.lock();
        try {
            try {
                if (fetchRows(fetchSize)) {
                    rowCount = result.size();
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
        } finally {
            session.unlock();
        }
    }

    @Override
    public boolean isLazy() {
        return rowCount < 0L;
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
    public boolean isIdentity(int i) {
        return columns[i].identity;
    }

    @Override
    public int getNullable(int i) {
        return columns[i].nullable;
    }

    @Override
    public void reset() {
        if (rowCount < 0L || rowOffset > 0L) {
            throw DbException.get(ErrorCode.RESULT_SET_NOT_SCROLLABLE);
        }
        rowId = -1;
        currentRow = null;
        nextRow = null;
        afterLast = false;
        final SessionRemote session = this.session;
        if (session == null) {
            return;
        }
        session.lock();
        try {
            session.checkClosed();
            try {
                session.traceOperation("RESULT_RESET", id);
                transfer.writeInt(SessionRemote.RESULT_RESET).writeInt(id).flush();
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
        } finally {
            session.unlock();
        }
    }

    @Override
    public int getVisibleColumnCount() {
        return columns.length;
    }

    @Override
    public long getRowCount() {
        if (rowCount < 0L) {
            throw DbException.getUnsupportedException("Row count is unknown for lazy result.");
        }
        return rowCount;
    }

    @Override
    public boolean hasNext() {
        if (afterLast) {
            return false;
        }
        if (nextRow == null) {
            if (rowCount < 0L || rowId < rowCount - 1) {
                long nextRowId = rowId + 1;
                if (session != null) {
                    remapIfOld();
                    if (nextRowId - rowOffset >= result.size()) {
                        fetchAdditionalRows();
                    }
                }
                int index = (int) (nextRowId - rowOffset);
                nextRow = index < result.size() ? result.get(index) : null;
            }
        }
        return nextRow != null;
    }

    private void sendClose() {
        final SessionRemote session = this.session;
        if (session == null) {
            return;
        }
        // TODO result sets: no reset possible for larger remote result sets
        session.lock();
        try {
            session.traceOperation("RESULT_CLOSE", id);
            transfer.writeInt(SessionRemote.RESULT_CLOSE).writeInt(id);
        } catch (IOException e) {
            trace.error(e, "close");
        } finally {
            session.unlock();
            transfer = null;
            this.session = null;
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

    private void fetchAdditionalRows() {
        final SessionRemote session = this.session;
        session.lock();
        try {
            session.checkClosed();
            try {
                rowOffset += result.size();
                result.clear();
                int fetch = fetchSize;
                if (rowCount >= 0) {
                    fetch = (int) Math.min(fetch, rowCount - rowOffset);
                } else if (fetch == Integer.MAX_VALUE) {
                    fetch = SysProperties.SERVER_RESULT_SET_FETCH_SIZE;
                }
                session.traceOperation("RESULT_FETCH_ROWS", id);
                transfer.writeInt(SessionRemote.RESULT_FETCH_ROWS).writeInt(id).writeInt(fetch);
                session.done(transfer);
                fetchRows(fetch);
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
        } finally {
            session.unlock();
        }
    }

    private boolean fetchRows(int fetch) throws IOException {
        int len = columns.length;
        for (int r = 0; r < fetch; r++) {
            switch (transfer.readByte()) {
            case 1: {
                Value[] values = new Value[len];
                for (int i = 0; i < len; i++) {
                    values[i] = transfer.readValue(columns[i].columnType);
                }
                result.add(values);
                break;
            }
            case 0:
                sendClose();
                return true;
            case -1:
                throw SessionRemote.readException(transfer);
            default:
                throw DbException.getInternalError();
            }
        }
        if (rowCount >= 0L && rowOffset + result.size() >= rowCount) {
            sendClose();
        }
        return false;
    }

    @Override
    public String toString() {
        return "columns: " + columns.length + (rowCount < 0L ? " lazy" : " rows: " + rowCount) + " pos: " + rowId;
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
    public boolean isClosed() {
        return result == null;
    }

}
