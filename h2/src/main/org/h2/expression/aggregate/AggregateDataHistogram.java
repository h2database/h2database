/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map.Entry;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Mode;
import org.h2.util.ValueHashMap;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLong;

/**
 * Data stored while calculating a HISTOGRAM aggregate.
 */
class AggregateDataHistogram extends AggregateData {

    private ValueHashMap<LongDataCounter> distinctValues;

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (distinctValues == null) {
            distinctValues = ValueHashMap.newInstance();
        }
        LongDataCounter a = distinctValues.get(v);
        if (a == null) {
            if (distinctValues.size() >= Constants.SELECTIVITY_DISTINCT_COUNT) {
                return;
            }
            a = new LongDataCounter();
            distinctValues.put(v, a);
        }
        a.count++;
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        if (distinctValues == null) {
            return ValueArray.get(new Value[0]).convertTo(dataType);
        }
        ValueArray[] values = new ValueArray[distinctValues.size()];
        int i = 0;
        for (Entry<Value, LongDataCounter> entry : distinctValues.entries()) {
            LongDataCounter d = entry.getValue();
            values[i] = ValueArray.get(new Value[] { entry.getKey(), ValueLong.get(distinct ? 1L : d.count) });
            i++;
        }
        final Mode mode = database.getMode();
        final CompareMode compareMode = database.getCompareMode();
        Arrays.sort(values, new Comparator<ValueArray>() {
            @Override
            public int compare(ValueArray v1, ValueArray v2) {
                Value a1 = v1.getList()[0];
                Value a2 = v2.getList()[0];
                return a1.compareTo(a2, mode, compareMode);
            }
        });
        return ValueArray.get(values).convertTo(dataType);
    }

}
