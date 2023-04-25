/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * Data stored while calculating an aggregate that needs collecting of all
 * values or a distinct aggregate.
 *
 * <p>
 * NULL values are not collected. {@link #getValue(SessionLocal)} method
 * returns {@code null}. Use {@link #getArray()} for instances of this class
 * instead.
 * </p>
 */
final class AggregateDataCollecting extends AggregateData implements Iterable<Value> {

    /**
     * NULL values collection mode.
     */
    enum NullCollectionMode {

        /**
         * Rows with NULL value are completely ignored.
         */
        IGNORED,

        /**
         * Rows with NULL values are processed causing the result to be not
         * NULL, but NULL values aren't collected.
         */
        EXCLUDED,

        /**
         * Rows with NULL values are aggregated just like rows with any other
         * values, should also be used when NULL values aren't passed to
         * {@linkplain AggregateDataCollecting}.
         */
        USED_OR_IMPOSSIBLE;

    }

    private final boolean distinct;

    private final boolean orderedWithOrder;

    private final NullCollectionMode nullCollectionMode;

    Collection<Value> values;

    private Value shared;

    /**
     * Creates new instance of data for collecting aggregates.
     *
     * @param distinct
     *            if distinct is used
     * @param orderedWithOrder
     *            if aggregate is an ordered aggregate with ORDER BY clause
     * @param nullCollectionMode
     *            NULL values collection mode
     */
    AggregateDataCollecting(boolean distinct, boolean orderedWithOrder, NullCollectionMode nullCollectionMode) {
        this.distinct = distinct;
        this.orderedWithOrder = orderedWithOrder;
        this.nullCollectionMode = nullCollectionMode;
    }

    @Override
    void add(SessionLocal session, Value v) {
        if (nullCollectionMode == NullCollectionMode.IGNORED && isNull(v)) {
            return;
        }
        Collection<Value> c = values;
        if (c == null) {
            if (distinct) {
                Comparator<Value> comparator = session.getDatabase().getCompareMode();
                if (orderedWithOrder) {
                    comparator = Comparator.comparing(t -> ((ValueRow) t).getList()[0], comparator);
                }
                c = new TreeSet<>(comparator);
            } else {
                c = new ArrayList<>();
            }
            values = c;
        }
        if (nullCollectionMode == NullCollectionMode.EXCLUDED && isNull(v)) {
            return;
        }
        c.add(v);
    }

    private boolean isNull(Value v) {
        return (orderedWithOrder ? ((ValueRow) v).getList()[0] : v) == ValueNull.INSTANCE;
    }

    @Override
    Value getValue(SessionLocal session) {
        return null;
    }

    /**
     * Returns the count of values.
     *
     * @return the count of values
     */
    int getCount() {
        return values != null ? values.size() : 0;
    }

    /**
     * Returns array with values or {@code null}.
     *
     * @return array with values or {@code null}
     */
    Value[] getArray() {
        Collection<Value> values = this.values;
        if (values == null) {
            return null;
        }
        return values.toArray(Value.EMPTY_VALUES);
    }

    @Override
    public Iterator<Value> iterator() {
        return values != null ? values.iterator() : Collections.emptyIterator();
    }

    /**
     * Sets value of a shared argument.
     *
     * @param shared the shared value
     */
    void setSharedArgument(Value shared) {
        if (this.shared == null) {
            this.shared = shared;
        } else if (!this.shared.equals(shared)) {
            throw DbException.get(ErrorCode.INVALID_VALUE_2, "Inverse distribution function argument",
                    this.shared.getTraceSQL() + "<>" + shared.getTraceSQL());
        }
    }

    /**
     * Returns value of a shared argument.
     *
     * @return value of a shared argument
     */
    Value getSharedArgument() {
        return shared;
    }

}
