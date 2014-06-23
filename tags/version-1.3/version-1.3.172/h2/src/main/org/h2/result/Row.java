/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Constants;
import org.h2.store.Data;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;
import org.h2.value.ValueLong;

/**
 * Represents a row in a table.
 */
public class Row implements SearchRow {

    public static final int MEMORY_CALCULATE = -1;
    public static final Row[] EMPTY_ARRAY = {};

    private long key;
    private final Value[] data;
    private int memory;
    private int version;
    private boolean deleted;
    private int sessionId;

    public Row(Value[] data, int memory) {
        this.data = data;
        this.memory = memory;
    }

    /**
     * Get a copy of the row that is distinct from (not equal to) this row.
     * This is used for FOR UPDATE to allow pseudo-updating a row.
     *
     * @return a new row with the same data
     */
    public Row getCopy() {
        Value[] d2 = new Value[data.length];
        System.arraycopy(data, 0, d2, 0, data.length);
        Row r2 = new Row(d2, memory);
        r2.key = key;
        r2.version = version + 1;
        r2.sessionId = sessionId;
        return r2;
    }

    @Override
    public void setKeyAndVersion(SearchRow row) {
        setKey(row.getKey());
        setVersion(row.getVersion());
    }

    @Override
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public long getKey() {
        return key;
    }

    @Override
    public void setKey(long key) {
        this.key = key;
    }

    @Override
    public Value getValue(int i) {
        return i == -1 ? ValueLong.get(key) : data[i];
    }

    /**
     * Get the number of bytes required for the data.
     *
     * @param dummy the template buffer
     * @return the number of bytes
     */
    public int getByteCount(Data dummy) {
        int size = 0;
        for (Value v : data) {
            size += dummy.getValueLen(v);
        }
        return size;
    }

    @Override
    public void setValue(int i, Value v) {
        if (i == -1) {
            this.key = v.getLong();
        } else {
            data[i] = v;
        }
    }

    public boolean isEmpty() {
        return data == null;
    }

    @Override
    public int getColumnCount() {
        return data.length;
    }

    @Override
    public int getMemory() {
        if (memory != MEMORY_CALCULATE) {
            return memory;
        }
        int m = Constants.MEMORY_ROW;
        if (data != null) {
            int len = data.length;
            m += Constants.MEMORY_OBJECT + len * Constants.MEMORY_POINTER;
            for (int i = 0; i < len; i++) {
                Value v = data[i];
                if (v != null) {
                    m += v.getMemory();
                }
            }
        }
        this.memory = m;
        return m;
    }

    @Override
    public String toString() {
        StatementBuilder buff = new StatementBuilder("( /* key:");
        buff.append(getKey());
        if (version != 0) {
            buff.append(" v:" + version);
        }
        if (isDeleted()) {
            buff.append(" deleted");
        }
        buff.append(" */ ");
        if (data != null) {
            for (Value v : data) {
                buff.appendExceptFirst(", ");
                buff.append(v == null ? "null" : v.getTraceSQL());
            }
        }
        return buff.append(')').toString();
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public int getSessionId() {
        return sessionId;
    }

    /**
     * This record has been committed. The session id is reset.
     */
    public void commit() {
        this.sessionId = 0;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public Value[] getValueList() {
        return data;
    }

}
