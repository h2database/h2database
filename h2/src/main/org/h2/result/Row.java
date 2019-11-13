/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.value.Value;

/**
 * Represents a row in a table.
 */
public abstract class Row extends SearchRow {

    private int memory;

    /**
     * Creates a new row.
     *
     * @param data values of columns, or null
     * @param memory used memory
     * @return the allocated row
     */
    public static Row get(Value[] data, int memory) {
        return new DefaultRow(data, memory);
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
        Row r = new DefaultRow(data, memory);
        r.setKey(key);
        return r;
    }

    protected Row(int memory) {
        this.memory = memory;
    }

    @Override
    public int getMemory() {
        if (memory != MEMORY_CALCULATE) {
            return memory;
        }
        return memory = calculateMemory();
    }

    /**
     * Calculate the estimated memory used for this row, in bytes.
     *
     * @return the memory
     */
    protected abstract int calculateMemory();

    /**
     * Get values.
     *
     * @return values
     */
    public abstract Value[] getValueList();

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
        return false;
    }

}
