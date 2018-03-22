/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import org.h2.engine.Database;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating an aggregate that needs collecting of all
 * values.
 *
 * <p>
 * NULL values are not collected. {@link #getValue(Database, int, boolean)}
 * method returns {@code null}. Use {@link #getArray()} for instances of this
 * class instead. Notice that subclasses like {@link AggregateDataMedian} may
 * override {@link #getValue(Database, int, boolean)} to return useful result.
 * </p>
 */
class AggregateDataCollecting extends AggregateData {
    Collection<Value> values;

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        Collection<Value> c = values;
        if (c == null) {
            values = c = distinct ? new HashSet<Value>() : new ArrayList<Value>();
        }
        c.add(v);
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        return null;
    }

    /**
     * Returns array with values or {@code null}.
     *
     * @return array with values or {@code null}
     */
    Value[] getArray() {
        Collection<Value> values = this.values;
        if (values == null) {
            return null;
        }
        return values.toArray(new Value[0]);
    }
}
