/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Database;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating a COUNT(*) aggregate.
 */
class AggregateDataCountAll extends AggregateData {
    private final int dataType;
    private long count;

    AggregateDataCountAll(int dataType) {
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
        if (distinct) {
            throw DbException.throwInternalError();
        }
        count++;
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
            throw DbException.throwInternalError();
        }
        Value v = ValueLong.get(count);
        return v == null ? ValueNull.INSTANCE : v.convertTo(dataType);
    }

}
