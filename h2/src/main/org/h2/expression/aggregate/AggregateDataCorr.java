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
 * Data stored while calculating a CORR, REG_SLOPE, REG_INTERCEPT, or REGR_R2
 * aggregate.
 */
final class AggregateDataCorr extends AggregateDataBinarySet {

    private final AggregateType aggregateType;

    private long count;

    private double sumY, sumX, sumYX;

    private double m2y, meanY;

    private double m2x, meanX;

    AggregateDataCorr(AggregateType aggregateType) {
        this.aggregateType = aggregateType;
    }

    @Override
    void add(SessionLocal session, Value yValue, Value xValue) {
        double y = yValue.getDouble(), x = xValue.getDouble();
        sumY += y;
        sumX += x;
        sumYX += y * x;
        if (++count == 1) {
            meanY = y;
            meanX = x;
            m2x = m2y = 0;
        } else {
            double delta = y - meanY;
            meanY += delta / count;
            m2y += delta * (y - meanY);
            delta = x - meanX;
            meanX += delta / count;
            m2x += delta * (x - meanX);
        }
    }

    @Override
    Value getValue(SessionLocal session) {
        if (count < 1) {
            return ValueNull.INSTANCE;
        }
        double v;
        switch (aggregateType) {
        case CORR:
            if (m2y == 0 || m2x == 0) {
                return ValueNull.INSTANCE;
            }
            v = (sumYX - sumX * sumY / count) / Math.sqrt(m2y * m2x);
            break;
        case REGR_SLOPE:
            if (m2x == 0) {
                return ValueNull.INSTANCE;
            }
            v = (sumYX - sumX * sumY / count) / m2x;
            break;
        case REGR_INTERCEPT:
            if (m2x == 0) {
                return ValueNull.INSTANCE;
            }
            v = meanY - (sumYX - sumX * sumY / count) / m2x * meanX;
            break;
        case REGR_R2: {
            if (m2x == 0) {
                return ValueNull.INSTANCE;
            }
            if (m2y == 0) {
                return ValueDouble.ONE;
            }
            v = sumYX - sumX * sumY / count;
            v = v * v / (m2y * m2x);
            break;
        }
        default:
            throw DbException.getInternalError("type=" + aggregateType);
        }
        return ValueDouble.get(v);
    }

}
