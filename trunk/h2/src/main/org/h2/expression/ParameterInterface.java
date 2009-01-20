/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.value.Value;

/**
 * The interface for client side (remote) and server side parameters.
 */
public interface ParameterInterface {

    /**
     * Set the value of the parameter.
     *
     * @param value the new value
     * @param closeOld if the old value (if one is set) should be closed
     */
    void setValue(Value value, boolean closeOld) throws SQLException;

    /**
     * Get the value of the parameter if set.
     *
     * @return the value or null
     */
    Value getParamValue() throws SQLException;

    /**
     * Check if the value is set.
     *
     * @throws SQLException if not set.
     */
    void checkSet() throws SQLException;

    /**
     * Get the expected data type of the parameter if no value is set, or the
     * data type of the value if one is set.
     *
     * @return the data type
     */
    int getType();

    /**
     * Get the expected precision of this parameter.
     *
     * @return the expected precision
     */
    long getPrecision();

    /**
     * Get the expected scale of this parameter.
     *
     * @return the expected scale
     */
    int getScale();

    /**
     * Check if this column is nullable.
     *
     * @return Column.NULLABLE_*
     */
    int getNullable();

}
