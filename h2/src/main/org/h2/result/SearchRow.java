/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.CastDataProvider;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * The base class for rows stored in a table, and for partial rows stored in the
 * index.
 */
public abstract class SearchRow extends Value {

    /**
     * Index of a virtual "_ROWID_" column within a row or a table
     */
    public static final int ROWID_INDEX = -1;

    /**
     * If the key is this value, then the key is considered equal to all other
     * keys, when comparing.
     */
    public static long MATCH_ALL_ROW_KEY = Long.MIN_VALUE + 1;

    /**
     * The constant that means "memory usage is unknown and needs to be calculated first".
     */
    public static final int MEMORY_CALCULATE = -1;

    /**
     * The row key.
     */
    protected long key;

    /**
     * Get the column count.
     *
     * @return the column count
     */
    public abstract int getColumnCount();

    /**
     * Determine if specified column contains NULL
     * @param index column index
     * @return true if NULL
     */
    public boolean isNull(int index) {
        return getValue(index) == ValueNull.INSTANCE;
    }

    /**
     * Get the value for the column
     *
     * @param index the column number (starting with 0)
     * @return the value
     */
    public abstract Value getValue(int index);

    /**
     * Set the value for given column
     *
     * @param index the column number (starting with 0)
     * @param v the new value
     */
    public abstract void setValue(int index, Value v);

    /**
     * Set the unique key of the row.
     *
     * @param key the key
     */
    public void setKey(long key) {
        this.key = key;
    }

    /**
     * Get the unique key of the row.
     *
     * @return the key
     */
    public long getKey() {
        return key;
    }

    /**
     * Get the estimated memory used for this row, in bytes.
     *
     * @return the memory
     */
    @Override
    public abstract int getMemory();

    /**
     * Copy all relevant values from the source to this row.
     * @param source source of column values
     */
    public abstract void copyFrom(SearchRow source);

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_ROW_EMPTY;
    }

    @Override
    public int getValueType() {
        return Value.ROW;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append("ROW (");
        for (int index = 0, count = getColumnCount(); index < count; index++) {
            if (index != 0) {
                builder.append(", ");
            }
            getValue(index).getSQL(builder, sqlFlags);
        }
        return builder.append(')');
    }

    @Override
    public String getString() {
        return getTraceSQL();
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider) {
        throw new UnsupportedOperationException();
    }
}
