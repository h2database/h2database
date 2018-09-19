/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.command.dml.Select;

/**
 * Creates aggregated data holder.
 */
public abstract class AggregateDataFactory {
    /**
     * Default implementation of local result factory.
     */
    public static final AggregateDataFactory DEFAULT = new AggregateDataFactory.DefaultAggregateDataFactory();

    /**
     * Create an AggregateData object of the correct sub-type.
     *
     * @param agg the aggregate operation.
     * @return the aggregate data object of the specified type
     */
    public abstract AggregateData create(Select select, Aggregate agg);

    /**
     * Create an JavaAggregateData object.
     *
     * @param userAgg user aggregate function.
     * @param distinct distinct flag.
     * @return the aggregate data object.
     */
    public abstract JavaAggregateData create(org.h2.api.Aggregate userAgg, int argsCount);

    /**
     * Default implementation of aggregate data factory.
     */
    private static final class DefaultAggregateDataFactory extends AggregateDataFactory {
        /**
         *
         */
        DefaultAggregateDataFactory() {
            //No-op.
        }

        @Override
        public AggregateData create(Select select, Aggregate agg) {
            switch (agg.getAggregateType()) {
                case SELECTIVITY:
                    return new AggregateDataSelectivity();
                case GROUP_CONCAT:
                    return new AggregateDataGroupConcat(select, agg.getOrderByList(), agg.getOrderBySort(),
                            agg.getGroupConcatSeparator());
                case ARRAY_AGG:
                    return new AggregateDataArray(select, agg.getOrderByList(), agg.getOrderBySort());
                case COUNT_ALL:
                    return new AggregateDataCountAll();
                case COUNT:
                    return new AggregateDataCount();
                case HISTOGRAM:
                    return new AggregateDataHistogram();
                case MEDIAN:
                    return new AggregateDataMedian();
                case MODE:
                    return new AggregateDataMode(select, agg.getOrderByList());
                case ENVELOPE:
                    return new AggregateDataEnvelope();
                default:
                    return new AggregateDataDefault(agg.getAggregateType());
            }
        }

        @Override
        public JavaAggregateData create(org.h2.api.Aggregate userAgg, int argsCount) {
            return new JavaAggregateDataDistinct(userAgg, argsCount);
        }
    }
}
