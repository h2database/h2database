/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;

import org.h2.engine.CastDataProvider;
import org.h2.util.StringUtils;

/**
 * Base implementation of the ENUM data type.
 *
 * This base implementation is only used in 2.0.* clients when they work with
 * 1.4.* servers.
 */
public class ValueEnumBase extends Value {

    final String label;
    private final int ordinal;

    protected ValueEnumBase(final String label, final int ordinal) {
        this.label = label;
        this.ordinal = ordinal;
    }

    @Override
    public Value add(Value v) {
        ValueInteger iv = v.convertToInt(null);
        return convertToInt(null).add(iv);
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider) {
        return Integer.compare(getInt(), v.getInt());
    }

    @Override
    public Value divide(Value v, TypeInfo quotientType) {
        ValueInteger iv = v.convertToInt(null);
        return convertToInt(null).divide(iv, quotientType);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ValueEnumBase &&
            getInt() == ((ValueEnumBase) other).getInt();
    }

    /**
     * Get or create an enum value with the given label and ordinal.
     *
     * @param label the label
     * @param ordinal the ordinal
     * @return the value
     */
    public static ValueEnumBase get(String label, int ordinal) {
        return new ValueEnumBase(label, ordinal);
    }

    @Override
    public int getInt() {
        return ordinal;
    }

    @Override
    public long getLong() {
        return ordinal;
    }

    @Override
    public BigDecimal getBigDecimal() {
        return BigDecimal.valueOf(ordinal);
    }

    @Override
    public float getFloat() {
        return ordinal;
    }

    @Override
    public double getDouble() {
        return ordinal;
    }

    @Override
    public int getSignum() {
        return Integer.signum(ordinal);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return StringUtils.quoteStringSQL(builder, label);
    }

    @Override
    public String getString() {
        return label;
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_ENUM_UNDEFINED;
    }

    @Override
    public int getValueType() {
        return ENUM;
    }

    @Override
    public int getMemory() {
        return 120;
    }

    @Override
    public int hashCode() {
        int results = 31;
        results += getString().hashCode();
        results += getInt();
        return results;
    }

    @Override
    public Value modulus(Value v) {
        ValueInteger iv = v.convertToInt(null);
        return convertToInt(null).modulus(iv);
    }

    @Override
    public Value multiply(Value v) {
        ValueInteger iv = v.convertToInt(null);
        return convertToInt(null).multiply(iv);
    }

    @Override
    public Value subtract(Value v) {
        ValueInteger iv = v.convertToInt(null);
        return convertToInt(null).subtract(iv);
    }

}
