/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.math.BigInteger;

import org.h2.api.IntervalQualifier;
import org.h2.engine.SessionLocal;
import org.h2.util.IntervalUtils;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueInterval;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating an AVG aggregate.
 */
final class AggregateDataAvg extends AggregateData {

    private final TypeInfo dataType, sumDataType;
    private long count;
    private Value value;

    /**
     * @param dataType
     *            the data type of the computed result
     */
    AggregateDataAvg(TypeInfo dataType) {
        this.dataType = dataType;
        sumDataType = Aggregate.getSumType(dataType);
    }

    @Override
    void add(SessionLocal session, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        count++;
        v = v.convertTo(sumDataType);
        value = value == null ? v : value.add(v);
    }

    @Override
    Value getValue(SessionLocal session) {
        if (count == 0) {
            return ValueNull.INSTANCE;
        }
        Value v;
        if (DataType.isIntervalType(dataType.getValueType())) {
            v = IntervalUtils.intervalFromAbsolute(
                    IntervalQualifier.valueOf(dataType.getValueType() - Value.INTERVAL_YEAR),
                    IntervalUtils.intervalToAbsolute((ValueInterval) value).divide(BigInteger.valueOf(count)));
        } else {
            Value b = ValueBigint.get(count).convertTo(Value.getHigherOrder(value.getValueType(), Value.BIGINT));
            v = value.convertTo(Value.getHigherOrder(value.getValueType(), Value.BIGINT)).divide(b,
                    ValueBigint.DECIMAL_PRECISION);
        }
        return v.castTo(dataType, session);
    }

}
