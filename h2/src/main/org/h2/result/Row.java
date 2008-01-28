/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.SQLException;

import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.store.Record;
import org.h2.value.Value;

/**
 * Represents a row in a table.
 */
public class Row extends Record implements SearchRow {
    public static final int MEMORY_CALCULATE = -1;
    private final Value[] data;
    private final int memory;

    public Row(Value[] data, int memory) {
        this.data = data;
        this.memory = memory;
    }

    public Row(Row old) {
        this.data = old.data;
        this.memory = old.memory;
    }

    public Value getValue(int i) {
        return data[i];
    }

    public void write(DataPage buff) throws SQLException {
        buff.writeInt(data.length);
        for (int i = 0; i < data.length; i++) {
            Value v = data[i];
            buff.writeValue(v);
        }
    }

    public int getByteCount(DataPage dummy) throws SQLException {
        int len = data.length;
        int size = dummy.getIntLen();
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
            return blockCount * (DiskFile.BLOCK_SIZE / 16) + memory * 4;
        }
        int m = blockCount * (DiskFile.BLOCK_SIZE / 16);
        for (int i = 0; data != null && i < data.length; i++) {
            m += data[i].getMemory();
        }
        return m;
    }

    public String toString() {
        StringBuffer buff = new StringBuffer(data.length * 5);
        buff.append("( /* pos:");
        buff.append(getPos());
        if (getDeleted()) {
            buff.append(" deleted");
        }
        buff.append(" */ ");
        for (int i = 0; i < data.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(data[i].getSQL());
        }
        buff.append(')');
        return buff.toString();
    }

}
