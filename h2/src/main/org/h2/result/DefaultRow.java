/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Constants;
import org.h2.value.Value;
import org.h2.value.ValueBigint;

/**
 * The default implementation of a row in a table.
 */
public class DefaultRow extends Row {

    /**
     * The constant that means "memory usage is unknown and needs to be calculated first".
     */
    public static final int MEMORY_CALCULATE = -1;

    /**
     * The values of the row (one entry per column).
     */
    protected final Value[] data;

    private int memory;

    DefaultRow(int columnCount) {
        this.data = new Value[columnCount];
        this.memory = MEMORY_CALCULATE;
    }

    public DefaultRow(Value[] data) {
        this.data = data;
        this.memory = MEMORY_CALCULATE;
    }

    public DefaultRow(Value[] data, int memory) {
        this.data = data;
        this.memory = memory;
    }

    @Override
    public Value getValue(int i) {
        return i == ROWID_INDEX ? ValueBigint.get(key) : data[i];
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
    public int getMemory() {
        if (memory != MEMORY_CALCULATE) {
            return memory;
        }
        return memory = calculateMemory();
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

    /**
     * Calculate the estimated memory used for this row, in bytes.
     *
     * @return the memory
     */
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

    @Override
    public void copyFrom(SearchRow source) {
        setKey(source.getKey());
        for (int i = 0; i < getColumnCount(); i++) {
            setValue(i, source.getValue(i));
        }
    }
}
