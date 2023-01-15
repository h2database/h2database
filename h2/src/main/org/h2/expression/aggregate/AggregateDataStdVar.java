/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating a STDDEV_POP, STDDEV_SAMP, VAR_SAMP, VAR_POP,
 * REGR_SXX, or REGR_SYY aggregate.
 */
final class AggregateDataStdVar extends AggregateData {

    private final AggregateType aggregateType;

    private long count;

    private double m2, mean;

    /**
     * @param aggregateType
     *            the type of the aggregate operation
     */
    AggregateDataStdVar(AggregateType aggregateType) {
        this.aggregateType = aggregateType;
    }

    @Override
    void add(SessionLocal session, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        // Using Welford's method, see also
        // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
        // https://www.johndcook.com/standard_deviation.html
        double x = v.getDouble();
        if (++count == 1) {
            mean = x;
            m2 = 0;
        } else {
            double delta = x - mean;
            mean += delta / count;
            m2 += delta * (x - mean);
        }
    }

    @Override
    Value getValue(SessionLocal session) {
        double v;
        switch (aggregateType) {
        case STDDEV_SAMP:
        case VAR_SAMP:
            if (count < 2) {
                return ValueNull.INSTANCE;
            }
            v = m2 / (count - 1);
            if (aggregateType == AggregateType.STDDEV_SAMP) {
                v = Math.sqrt(v);
            }
            break;
        case STDDEV_POP:
        case VAR_POP:
            if (count < 1) {
                return ValueNull.INSTANCE;
            }
            v = m2 / count;
            if (aggregateType == AggregateType.STDDEV_POP) {
                v = Math.sqrt(v);
            }
            break;
        case REGR_SXX:
        case REGR_SYY:
            if (count < 1) {
                return ValueNull.INSTANCE;
            }
            v = m2;
            break;
        default:
            throw DbException.getInternalError("type=" + aggregateType);
        }
        return ValueDouble.get(v);
    }

}
