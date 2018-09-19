/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.*;

import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Database;
import org.h2.result.SortOrder;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating an aggregate that needs collecting of all
 * values.
 *
 * <p>
 * NULL values are not collected. {@link #getValue(Database, int, boolean)}
 * method returns {@code null}. Use {@link #getArray()} for instances of this
 * class instead. Notice that subclasses like {@link AggregateDataMedian} may
 * override {@link #getValue(Database, int, boolean)} to return useful result.
 * </p>
 */
abstract class AggregateDataCollecting extends AggregateData {
    private Collection<Value> values;
    protected Select select;
    protected SortOrder sortOrder;
    protected ArrayList<SelectOrderBy> orderByList;

    public AggregateDataCollecting(Select select, ArrayList<SelectOrderBy> orderByList, SortOrder sortOrder) {
        this.sortOrder = sortOrder;
        this.orderByList= orderByList;
        this.select = select;
    }

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        Collection<Value> c = values;
        if (c == null) {
            values = c = distinct ? new HashSet<Value>() : new ArrayList<Value>();
        }
        c.add(v);
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
        return values.toArray(new Value[0]);
    }

     void sortWithOrderBy(Value[] array) {
        if (sortOrder != null) {
            Arrays.sort(array, new Comparator<Value>() {
                @Override
                public int compare(Value v1, Value v2) {
                    return sortOrder.compare(((ValueArray) v1).getList(), ((ValueArray) v2).getList());
                }
            });
        } else {
            Arrays.sort(array, select.getSession().getDatabase().getCompareMode());
        }
    }
}
