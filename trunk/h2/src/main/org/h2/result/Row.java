/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.SQLException;

import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.store.Record;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;

/**
 * Represents a row in a table.
 */
public class Row extends Record implements SearchRow {
    public static final int MEMORY_CALCULATE = -1;
    private final Value[] data;
    private final int memory;
    private int version;

    public Row(Value[] data, int memory) {
        this.data = data;
        this.memory = memory;
    }

    public void setPosAndVersion(SearchRow row) {
        setPos(row.getPos());
        setVersion(row.getVersion());
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Value getValue(int i) {
        return data[i];
    }

    public void write(DataPage buff) throws SQLException {
        buff.writeInt(data.length);
        for (Value v : data) {
            buff.writeValue(v);
        }
    }

    public int getByteCount(DataPage dummy) throws SQLException {
        int len = data.length;
        int size = DataPage.LENGTH_INT;
        for (int i = 0; i < len; i++) {
            Value v = data[i];
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
            return blockCount * (DiskFile.BLOCK_SIZE / 8) + memory * 4;
        }
        int m = blockCount * (DiskFile.BLOCK_SIZE / 16);
        for (int i = 0; data != null && i < data.length; i++) {
            m += data[i].getMemory();
        }
        return m;
    }

    public String toString() {
        StatementBuilder buff = new StatementBuilder("( /* pos:");
        buff.append(getPos());
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

}
