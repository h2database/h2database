/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.value.Value;

import java.sql.SQLException;

/**
 * Abstract class for the computation of an aggregate.
 */
abstract class JavaAggregateData {

    /**
     * Add a value to this aggregate.
     * @param args Java aggregate function arguments.
     */
    abstract void add(Value args);

    /**
     * Get the aggregate result.
     *
     * @return the value
     */
    abstract Object getValue() throws SQLException;
}
