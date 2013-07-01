/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Database;
import org.h2.value.Value;

/**
 * Abstract class for the computation of an aggregate.
 */
abstract class AggregateData {
    
    /**
     * Create an AggregateData object of the correct subtype.
     * 
     * @param aggregateType the type of the aggregate operation
     * @param dataType the datatype of the computed result
     */
    static AggregateData create(int aggregateType, int dataType) {
        if (aggregateType == Aggregate.SELECTIVITY) {
            return new AggregateDataSelectivity(dataType);
        } else if (aggregateType == Aggregate.GROUP_CONCAT) {
             return new AggregateDataGroupConcat();
        } else if (aggregateType == Aggregate.COUNT_ALL) {
            return new AggregateDataCountAll(dataType);
        } else if (aggregateType == Aggregate.COUNT) {
            return new AggregateDataCount(dataType);
        } else if (aggregateType == Aggregate.HISTOGRAM) {
            return new AggregateDataHistogram(dataType);
        } else {
            return new AggregateDataDefault(aggregateType, dataType);
        }
    }

    /**
     * Add a value to this aggregate.
     *
     * @param database the database
     * @param distinct if the calculation should be distinct
     * @param v the value
     */
    abstract void add(Database database, boolean distinct, Value v);
    
    /**
     * Get the aggregate result.
     *
     * @param database the database
     * @param distinct if distinct is used
     * @return the value
     */
    abstract Value getValue(Database database, boolean distinct);
}
