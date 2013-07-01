/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Database;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating an aggregate.
 */
class AggregateDataCount extends AggregateData {
    private final int dataType;
    private long count;
    private ValueHashMap<AggregateDataCount> distinctValues;

    /**
     * @param dataType the datatype of the computed result
     */
    AggregateDataCount(int dataType) {
        this.dataType = dataType;
    }

    /**
     * Add a value to this aggregate.
     * 
     * @param database the database
     * @param distinct if the calculation should be distinct
     * @param v the value
     */
    @Override
    void add(Database database, boolean distinct, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        count++;
        if (distinct) {
            if (distinctValues == null) {
                distinctValues = ValueHashMap.newInstance();
            }
            distinctValues.put(v, this);
            return;
        }
    }

    /**
     * Get the aggregate result.
     * 
     * @param database the database
     * @param distinct if distinct is used
     * @return the value
     */
    @Override
    Value getValue(Database database, boolean distinct) {
        if (distinct) {
            if (distinctValues != null) {
                count = distinctValues.size();
            } else {
                count = 0;
            }
        }
        Value v = ValueLong.get(count);
        return v.convertTo(dataType);
    }

}
