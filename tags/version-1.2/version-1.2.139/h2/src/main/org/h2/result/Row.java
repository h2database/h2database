/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.store.Data;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;

/**
 * Represents a row in a table.
 */
public class Row implements SearchRow {

    public static final int MEMORY_CALCULATE = -1;
    private long key;
    private final Value[] data;
    private final int memory;
    private int version;
    private boolean deleted;
    private int sessionId;

    public Row(Value[] data, int memory) {
        this.data = data;
        if (memory != MEMORY_CALCULATE) {
            this.memory = 16 + memory * 4;
        } else {
            this.memory = MEMORY_CALCULATE;
        }
    }

    public void setKeyAndVersion(SearchRow row) {
        setKey(row.getKey());
        setVersion(row.getVersion());
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public Value getValue(int i) {
        return data[i];
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

    public void setValue(int i, Value v) {
        data[i] = v;
    }

    public boolean isEmpty() {
        return data == null;
    }

    public int getColumnCount() {
        return data.length;
    }

    public int getMemorySize() {
        if (memory != MEMORY_CALCULATE) {
            return memory;
        }
        int m = 8;
        for (int i = 0; data != null && i < data.length; i++) {
            m += data[i].getMemory();
        }
        return m;
    }

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

}
