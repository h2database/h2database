/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;
import java.util.Random;

import org.h2.engine.SessionLocal;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating an ANY_VALUE aggregate.
 */
final class AggregateDataAnyValue extends AggregateData {

    private static final int MAX_VALUES = 256;

    ArrayList<Value> values = new ArrayList<>();

    private long filter = -1L;

    /**
     * Creates new instance of data for ANY_VALUE.
     */
    AggregateDataAnyValue() {
    }

    @Override
    void add(SessionLocal session, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        long filter = this.filter;
        if (filter == Long.MIN_VALUE || (session.getRandom().nextLong() | filter) == filter) {
            values.add(v);
            if (values.size() == MAX_VALUES) {
                compact(session);
            }
        }
    }

    private void compact(SessionLocal session) {
        filter <<= 1;
        Random random = session.getRandom();
        for (int s = 0, t = 0; t < MAX_VALUES / 2; s += 2, t++) {
            int idx = s;
            if (random.nextBoolean()) {
                idx++;
            }
            values.set(t, values.get(idx));
        }
        values.subList(MAX_VALUES / 2, MAX_VALUES).clear();
    }

    @Override
    Value getValue(SessionLocal session) {
        int count = values.size();
        if (count == 0) {
            return ValueNull.INSTANCE;
        }
        return values.get(session.getRandom().nextInt(count));
    }

}
