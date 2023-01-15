/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.message.DbException;
import org.h2.value.TypeInfo;
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
    void setValue(Value value, boolean closeOld);

    /**
     * Get the value of the parameter if set.
     *
     * @return the value or null
     */
    Value getParamValue();

    /**
     * Check if the value is set.
     *
     * @throws DbException if not set.
     */
    void checkSet() throws DbException;

    /**
     * Is the value of a parameter set.
     *
     * @return true if set
     */
    boolean isValueSet();

    /**
     * Returns the expected data type if no value is set, or the
     * data type of the value if one is set.
     *
     * @return the data type
     */
    TypeInfo getType();

    /**
     * Check if this column is nullable.
     *
     * @return Column.NULLABLE_*
     */
    int getNullable();

}
