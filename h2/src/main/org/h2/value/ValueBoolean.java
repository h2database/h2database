/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;

import org.h2.engine.CastDataProvider;

/**
 * Implementation of the BOOLEAN data type.
 */
public final class ValueBoolean extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 1;

    /**
     * The maximum display size of a boolean.
     * Example: FALSE
     */
    public static final int DISPLAY_SIZE = 5;

    /**
     * TRUE value.
     */
    public static final ValueBoolean TRUE = new ValueBoolean(true);

    /**
     * FALSE value.
     */
    public static final ValueBoolean FALSE = new ValueBoolean(false);

    private final boolean value;

    private ValueBoolean(boolean value) {
        this.value = value;
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_BOOLEAN;
    }

    @Override
    public int getValueType() {
        return BOOLEAN;
    }

    @Override
    public int getMemory() {
        // Singleton TRUE and FALSE values
        return 0;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return builder.append(getString());
    }

    @Override
    public String getString() {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public boolean getBoolean() {
        return value;
    }

    @Override
    public byte getByte() {
        return value ? (byte) 1 : (byte) 0;
    }

    @Override
    public short getShort() {
        return value ? (short) 1 : (short) 0;
    }

    @Override
    public int getInt() {
        return value ? 1 : 0;
    }

    @Override
    public long getLong() {
        return value ? 1L : 0L;
    }

    @Override
    public BigDecimal getBigDecimal() {
        return value ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    @Override
    public float getFloat() {
        return value ? 1f : 0f;
    }

    @Override
    public double getDouble() {
        return value ? 1d : 0d;
    }

    @Override
    public Value negate() {
        return value ? FALSE : TRUE;
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        return Boolean.compare(value, ((ValueBoolean) o).value);
    }

    @Override
    public int hashCode() {
        return value ? 1 : 0;
    }

    /**
     * Get the boolean value for the given boolean.
     *
     * @param b the boolean
     * @return the value
     */
    public static ValueBoolean get(boolean b) {
        return b ? TRUE : FALSE;
    }

    @Override
    public boolean equals(Object other) {
        // there are only ever two instances, so the instance must match
        return this == other;
    }

}
