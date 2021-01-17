/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.engine.SessionLocal;
import org.h2.expression.function.BitFunction;
import org.h2.message.DbException;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating an aggregate.
 */
final class AggregateDataDefault extends AggregateData {

    private final AggregateType aggregateType;
    private final TypeInfo dataType;
    private long count;
    private Value value;

    /**
     * @param aggregateType the type of the aggregate operation
     * @param dataType the data type of the computed result
     */
    AggregateDataDefault(AggregateType aggregateType, TypeInfo dataType) {
        this.aggregateType = aggregateType;
        this.dataType = dataType;
    }

    @Override
    void add(SessionLocal session, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        count++;
        switch (aggregateType) {
        case SUM:
            if (value == null) {
                value = v.convertTo(dataType.getValueType());
            } else {
                v = v.convertTo(value.getValueType());
                value = value.add(v);
            }
            break;
        case AVG:
            if (value == null) {
                value = v.convertTo(DataType.getAddProofType(dataType.getValueType()));
            } else {
                v = v.convertTo(value.getValueType());
                value = value.add(v);
            }
            break;
        case MIN:
            if (value == null || session.compare(v, value) < 0) {
                value = v;
            }
            break;
        case MAX:
            if (value == null || session.compare(v, value) > 0) {
                value = v;
            }
            break;
        case EVERY:
            v = v.convertToBoolean();
            if (value == null) {
                value = v;
            } else {
                value = ValueBoolean.get(value.getBoolean() && v.getBoolean());
            }
            break;
        case ANY:
            v = v.convertToBoolean();
            if (value == null) {
                value = v;
            } else {
                value = ValueBoolean.get(value.getBoolean() || v.getBoolean());
            }
            break;
        case BIT_AND_AGG:
        case BIT_NAND_AGG:
            if (value == null) {
                value = v;
            } else {
                value = BitFunction.getBitwise(BitFunction.BITAND, dataType, value, v);
            }
            break;
        case BIT_OR_AGG:
        case BIT_NOR_AGG:
            if (value == null) {
                value = v;
            } else {
                value = BitFunction.getBitwise(BitFunction.BITOR, dataType, value, v);
            }
            break;
        case BIT_XOR_AGG:
        case BIT_XNOR_AGG:
            if (value == null) {
                value = v;
            } else {
                value = BitFunction.getBitwise(BitFunction.BITXOR, dataType, value, v);
            }
            break;
        default:
            throw DbException.getInternalError("type=" + aggregateType);
        }
    }

    @Override
    Value getValue(SessionLocal session) {
        Value v = null;
        switch (aggregateType) {
        case SUM:
        case MIN:
        case MAX:
        case BIT_AND_AGG:
        case BIT_OR_AGG:
        case BIT_XOR_AGG:
        case ANY:
        case EVERY:
            v = value;
            break;
        case BIT_NAND_AGG:
        case BIT_NOR_AGG:
        case BIT_XNOR_AGG:
            if (value != null) {
                v = BitFunction.getBitwise(BitFunction.BITNOT, dataType, value, null);
            }
            break;
        case AVG:
            if (value != null) {
                v = divide(value, count);
            }
            break;
        default:
            throw DbException.getInternalError("type=" + aggregateType);
        }
        return v == null ? ValueNull.INSTANCE : v.convertTo(dataType);
    }

    private static Value divide(Value a, long by) {
        if (by == 0) {
            return ValueNull.INSTANCE;
        }
        int type = Value.getHigherOrder(a.getValueType(), Value.BIGINT);
        Value b = ValueBigint.get(by).convertTo(type);
        a = a.convertTo(type).divide(b, ValueBigint.DECIMAL_PRECISION);
        return a;
    }

}
