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
 * Represents a row in a table.
 */
public class Row implements SearchRow {

    public static final int MEMORY_CALCULATE = -1;

    public static final Row[] EMPTY_ARRAY = {};

    private long key;
    protected final Value[] data;
    private int memory;

    /**
     * Creates a new row.
     *
     * @param data values of columns, or null
     * @param memory used memory
     * @return the allocated row
     */
    public static Row get(Value[] data, int memory) {
        return new Row(data, memory);
    }

    /**
     * Creates a new row with the specified key.
     *
     * @param data values of columns, or null
     * @param memory used memory
     * @param key the key
     * @return the allocated row
     */
    public static Row get(Value[] data, int memory, long key) {
        Row r = new Row(data, memory);
        r.setKey(key);
        return r;
    }

    protected Row(Value[] data, int memory) {
        this.data = data;
        this.memory = memory;
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
        return i == SearchRow.ROWID_INDEX ? ValueLong.get(key) : data[i];
    }

    @Override
    public void setValue(int i, Value v) {
        if (i == SearchRow.ROWID_INDEX) {
            this.key = v.getLong();
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
        int m = Constants.MEMORY_ROW;
        if (data != null) {
            int len = data.length;
            m += Constants.MEMORY_OBJECT + len * Constants.MEMORY_POINTER;
            for (Value v : data) {
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
        return toString(key,  data);
    }

    /**
     * Convert a row to a string.
     *
     * @param key the key
     * @param data the row data
     * @return the string representation
     */
    static String toString(long key, Value[] data) {
        StringBuilder builder = new StringBuilder("( /* key:").append(key);
        builder.append(" */ ");
        if (data != null) {
            for (int i = 0, length = data.length; i < length; i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                Value v = data[i];
                builder.append(v == null ? "null" : v.getTraceSQL());
            }
        }
        return builder.append(')').toString();
    }

    /**
     * Get values.
     *
     * @return values
     */
    public Value[] getValueList() {
        return data;
    }

    /**
     * Check whether this row and the specified row share the same underlying
     * data with values. This method must return {@code false} when values are
     * not equal and may return either {@code true} or {@code false} when they
     * are equal. This method may be used only for optimizations and should not
     * perform any slow checks, such as equality checks for all pairs of values.
     *
     * @param other
     *            the other row
     * @return {@code true} if rows share the same underlying data,
     *         {@code false} otherwise or when unknown
     */
    public boolean hasSharedData(Row other) {
        return data == other.data;
    }

}
