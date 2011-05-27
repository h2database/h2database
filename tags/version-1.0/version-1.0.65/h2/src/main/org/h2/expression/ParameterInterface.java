/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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
     */
    void setValue(Value value);

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
}
