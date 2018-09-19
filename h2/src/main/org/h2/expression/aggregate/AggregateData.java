/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.engine.Database;
import org.h2.expression.aggregate.Aggregate.AggregateType;
import org.h2.value.Value;

/**
 * Abstract class for the computation of an aggregate.
 */
abstract class AggregateData {

    /**
     * Add a value to this aggregate.
     *
     * @param database the database
     * @param dataType the datatype of the computed result
     * @param distinct if the calculation should be distinct
     * @param v the value
     */
    abstract void add(Database database, int dataType, boolean distinct, Value v);

    /**
     * Get the aggregate result.
     *
     * @param database the database
     * @param dataType the datatype of the computed result
     * @param distinct if distinct is used
     * @return the value
     */
    abstract Value getValue(Database database, int dataType, boolean distinct);
}
