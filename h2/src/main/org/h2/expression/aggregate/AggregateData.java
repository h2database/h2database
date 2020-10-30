/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.expression.aggregate.AggregateDataCollecting.NullCollectionMode;
import org.h2.message.DbException;
import org.h2.util.json.JsonConstructorUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * Abstract class for the computation of an aggregate.
 */
abstract class AggregateData {

    /**
     * Create an AggregateData object of the correct sub-type.
     *
     * @param aggregateType the type of the aggregate operation
     * @param distinct if the calculation should be distinct
     * @param dataType the data type of the computed result
     * @param orderedWithOrder
     *            if aggregate is an ordered aggregate with ORDER BY clause
     * @param flags the aggregate function flags
     * @return the aggregate data object of the specified type
     */
    static AggregateData create(AggregateType aggregateType, boolean distinct, TypeInfo dataType,
            boolean orderedWithOrder, int flags) {
        switch (aggregateType) {
        case COUNT_ALL:
            return new AggregateDataCount(true);
        case COUNT:
            if (!distinct) {
                return new AggregateDataCount(false);
            }
            break;
        case RANK:
        case DENSE_RANK:
        case PERCENT_RANK:
        case CUME_DIST:
        case PERCENTILE_CONT:
        case PERCENTILE_DISC:
        case MEDIAN:
            break;
        case MIN:
        case MAX:
        case BIT_AND_AGG:
        case BIT_OR_AGG:
        case BIT_NAND_AGG:
        case BIT_NOR_AGG:
        case ANY:
        case EVERY:
            return new AggregateDataDefault(aggregateType, dataType);
        case SUM:
        case AVG:
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
        case BIT_XOR_AGG:
        case BIT_XNOR_AGG:
            if (!distinct) {
                return new AggregateDataDefault(aggregateType, dataType);
            }
            break;
        case HISTOGRAM:
            return new AggregateDataDistinctWithCounts(false, Constants.SELECTIVITY_DISTINCT_COUNT);
        case LISTAGG:
            // NULL values are excluded by Aggregate
            return new AggregateDataCollecting(distinct, orderedWithOrder, NullCollectionMode.USED_OR_IMPOSSIBLE);
        case ARRAY_AGG:
            return new AggregateDataCollecting(distinct, orderedWithOrder, NullCollectionMode.USED_OR_IMPOSSIBLE);
        case MODE:
            return new AggregateDataDistinctWithCounts(true, Integer.MAX_VALUE);
        case ENVELOPE:
            return new AggregateDataEnvelope();
        case JSON_ARRAYAGG:
            return new AggregateDataCollecting(distinct, orderedWithOrder,
                    (flags & JsonConstructorUtils.JSON_ABSENT_ON_NULL) != 0 ? NullCollectionMode.EXCLUDED
                            : NullCollectionMode.USED_OR_IMPOSSIBLE);
        case JSON_OBJECTAGG:
            // ROW(key, value) are collected, so NULL values can't be passed
            return new AggregateDataCollecting(distinct, false, NullCollectionMode.USED_OR_IMPOSSIBLE);
        default:
            throw DbException.getInternalError("type=" + aggregateType);
        }
        return new AggregateDataCollecting(distinct, false, NullCollectionMode.IGNORED);
    }

    /**
     * Add a value to this aggregate.
     *
     * @param session the session
     * @param v the value
     */
    abstract void add(SessionLocal session, Value v);

    /**
     * Get the aggregate result.
     *
     * @param session the session
     * @return the value
     */
    abstract Value getValue(SessionLocal session);
}
