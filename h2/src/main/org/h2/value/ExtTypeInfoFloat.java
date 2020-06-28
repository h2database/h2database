/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

/**
 * Extended parameters of the FLOAT data type.
 */
public final class ExtTypeInfoFloat extends ExtTypeInfo {

    /**
     * Extended parameters of the FLOAT data type without explicitly specified
     * precision;
     */
    public static final ExtTypeInfoFloat NO_ARG = new ExtTypeInfoFloat(0);

    /**
     * Returns extended parameters of the FLOAT data type.
     *
     * @param precision
     *            the precision, 0 if not specified
     * @return the extended parameters for the specified parameters
     */
    public static ExtTypeInfoFloat get(int precision) {
        return precision > 0 ? new ExtTypeInfoFloat(precision) : NO_ARG;
    }

    private final int precision;

    /**
     * Creates new instance of extended parameters of FLOAT data type.
     *
     * @param precision
     *            the precision
     */
    public ExtTypeInfoFloat(int precision) {
        this.precision = precision;
    }

    /**
     * Returns precision.
     *
     * @return precision, or 0 if not specified
     */
    public int getPrecision() {
        return precision;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append("FLOAT");
        if (precision > 0) {
            builder.append('(').append(precision).append(')');
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return 143_582_611 + precision;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ExtTypeInfoFloat)) {
            return false;
        }
        return precision == ((ExtTypeInfoFloat) obj).precision;
    }

}
