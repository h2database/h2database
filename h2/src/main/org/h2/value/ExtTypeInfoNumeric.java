/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

/**
 * Extended parameters of the NUMERIC and DECFLOAT data types.
 */
public final class ExtTypeInfoNumeric extends ExtTypeInfo {

    /**
     * NUMERIC or DECFLOAT data type without parameters.
     */
    public static final ExtTypeInfoNumeric NUMERIC = new ExtTypeInfoNumeric(false, false, false);

    /**
     * NUMERIC data type with precision parameter.
     */
    public static final ExtTypeInfoNumeric NUMERIC_PRECISION = new ExtTypeInfoNumeric(false, true, false);

    /**
     * NUMERIC data type with precision and scale parameters.
     */
    public static final ExtTypeInfoNumeric NUMERIC_PRECISION_SCALE = new ExtTypeInfoNumeric(false, true, true);

    /**
     * DECIMAL data type without parameters.
     */
    public static final ExtTypeInfoNumeric DECIMAL = new ExtTypeInfoNumeric(true, false, false);

    /**
     * DECIMAL data type with precision parameter.
     */
    public static final ExtTypeInfoNumeric DECIMAL_PRECISION = new ExtTypeInfoNumeric(true, true, false);

    /**
     * DECIMAL data type with precision and scale parameters.
     */
    public static final ExtTypeInfoNumeric DECIMAL_PRECISION_SCALE = new ExtTypeInfoNumeric(true, true, true);

    private final boolean decimal, withPrecision, withScale;

    private ExtTypeInfoNumeric(boolean decimal, boolean withPrecision, boolean withScale) {
        this.decimal = decimal;
        this.withPrecision = withPrecision;
        this.withScale = withScale;
    }

    /**
     * Returns whether data type is DECIMAL.
     *
     * @return {@code true} for DECIMAL, {@code false} for NUMERIC.
     */
    public boolean decimal() {
        return decimal;
    }

    /**
     * Returns {@code true} if precision was specified.
     *
     * @return {@code true} if precision was specified, {@code false} otherwise
     */
    public boolean withPrecision() {
        return withPrecision;
    }

    /**
     * Returns {@code true} if scale was specified.
     *
     * @return {@code true} if scale was specified, {@code false} otherwise
     */
    public boolean withScale() {
        return withScale;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return builder.append(decimal ? "DECIMAL" : "NUMERIC");
    }

}
