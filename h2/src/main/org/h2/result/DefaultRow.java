/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Constants;
import org.h2.value.Value;
import org.h2.value.ValueLong;

/**
 * The default implementation of a row in a table.
 */
public class DefaultRow extends Row {

    protected final Value[] data;

    public DefaultRow(Value[] data) {
        super(MEMORY_CALCULATE);
        this.data = data;
    }

    public DefaultRow(Value[] data, int memory) {
        super(memory);
        this.data = data;
    }

    @Override
    public Value getValue(int i) {
        return i == ROWID_INDEX ? ValueLong.get(key) : data[i];
    }

    @Override
    public void setValue(int i, Value v) {
        if (i == ROWID_INDEX) {
            key = v.getLong();
        } else {
            data[i] = v;
        }
    }

    @Override
    public int getColumnCount() {
        return data.length;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("( /* key:").append(key).append(" */ ");
        for (int i = 0, length = data.length; i < length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            Value v = data[i];
            builder.append(v == null ? "null" : v.getTraceSQL());
        }
        return builder.append(')').toString();
    }

    @Override
    protected int calculateMemory() {
        int m = Constants.MEMORY_ROW + Constants.MEMORY_ARRAY + data.length * Constants.MEMORY_POINTER;
        for (Value v : data) {
            if (v != null) {
                m += v.getMemory();
            }
        }
        return m;
    }

    @Override
    public Value[] getValueList() {
        return data;
    }

    @Override
    public boolean hasSharedData(Row other) {
        return other instanceof DefaultRow && data == ((DefaultRow) other).data;
    }

}
