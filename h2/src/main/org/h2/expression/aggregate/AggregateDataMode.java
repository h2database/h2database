/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.Map.Entry;

import org.h2.engine.Database;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating a MODE aggregate.
 */
class AggregateDataMode extends AggregateData {

    private ValueHashMap<LongDataCounter> distinctValues;

    @Override
    void add(Database database, int dataType, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        if (distinctValues == null) {
            distinctValues = new ValueHashMap<>();
        }
        LongDataCounter a = distinctValues.get(v);
        if (a == null) {
            a = new LongDataCounter();
            distinctValues.put(v, a);
        }
        a.count++;
    }

    @Override
    Value getValue(Database database, int dataType) {
        Value v = ValueNull.INSTANCE;
        if (distinctValues != null) {
            long count = 0L;
            for (Entry<Value, LongDataCounter> entry : distinctValues.entries()) {
                long c = entry.getValue().count;
                if (c > count) {
                    v = entry.getKey();
                    count = c;
                }
            }
        }
        return v.convertTo(dataType);
    }

    Value getOrderedValue(Database database, int dataType, boolean desc) {
        Value v = ValueNull.INSTANCE;
        if (distinctValues != null) {
            long count = 0L;
            for (Entry<Value, LongDataCounter> entry : distinctValues.entries()) {
                long c = entry.getValue().count;
                if (c > count) {
                    v = entry.getKey();
                    count = c;
                } else if (c == count) {
                    Value v2 = entry.getKey();
                    int cmp = database.compareTypeSafe(v, v2);
                    if (desc) {
                        if (cmp >= 0) {
                            continue;
                        }
                    } else if (cmp <= 0) {
                        continue;
                    }
                    v = v2;
                }
            }
        }
        return v.convertTo(dataType);
    }

}
