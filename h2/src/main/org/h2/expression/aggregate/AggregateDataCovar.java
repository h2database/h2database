/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
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
 * Data stored while calculating a COVAR_POP, COVAR_SAMP, or REGR_SXY aggregate.
 */
final class AggregateDataCovar extends AggregateDataBinarySet {

    private final AggregateType aggregateType;

    private long count;

    private double sumY, sumX, sumYX;

    /**
     * @param aggregateType
     *            the type of the aggregate operation
     */
    AggregateDataCovar(AggregateType aggregateType) {
        this.aggregateType = aggregateType;
    }

    @Override
    void add(SessionLocal session, Value yValue, Value xValue) {
        double y = yValue.getDouble(), x = xValue.getDouble();
        sumY += y;
        sumX += x;
        sumYX += y * x;
        count++;
    }

    @Override
    Value getValue(SessionLocal session) {
        double v;
        switch (aggregateType) {
        case COVAR_POP:
            if (count < 1) {
                return ValueNull.INSTANCE;
            }
            v = (sumYX - sumX * sumY / count) / count;
            break;
        case COVAR_SAMP:
            if (count < 2) {
                return ValueNull.INSTANCE;
            }
            v = (sumYX - sumX * sumY / count) / (count - 1);
            break;
        case REGR_SXY:
            if (count < 1) {
                return ValueNull.INSTANCE;
            }
            v = sumYX - sumX * sumY / count;
            break;
        default:
            throw DbException.getInternalError("type=" + aggregateType);
        }
        return ValueDouble.get(v);
    }

}
