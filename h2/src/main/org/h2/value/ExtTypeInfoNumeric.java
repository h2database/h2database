/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

/**
 * Extended parameters of the NUMERIC data type.
 */
public final class ExtTypeInfoNumeric extends ExtTypeInfo {

    /**
     * DECIMAL data type.
     */
    public static final ExtTypeInfoNumeric DECIMAL = new ExtTypeInfoNumeric();

    private ExtTypeInfoNumeric() {
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return builder.append("DECIMAL");
    }

}
