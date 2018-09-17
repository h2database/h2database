/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;
import java.util.HashMap;

import org.h2.command.dml.Select;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInt;

/**
 * A window function.
 */
public class WindowFunction extends AbstractAggregate {

    /**
     * A type of a window function.
     */
    public enum WindowFunctionType {

    /**
     * The type for ROW_NUMBER() window function.
     */
    ROW_NUMBER,

    /**
     * The type for RANK() window function.
     */
    RANK,

    /**
     * The type for DENSE_RANK() window function.
     */
    DENSE_RANK,

    /**
     * The type for PERCENT_RANK() window function.
     */
    PERCENT_RANK,

    /**
     * The type for CUME_DIST() window function.
     */
    CUME_DIST,

        ;

        /**
         * Returns the type of window function with the specified name, or null.
         *
         * @param name
         *            name of a window function
         * @return the type of window function, or null.
         */
        public static WindowFunctionType get(String name) {
            switch (name) {
            case "ROW_NUMBER":
                return WindowFunctionType.ROW_NUMBER;
            case "RANK":
                return RANK;
            case "DENSE_RANK":
                return WindowFunctionType.DENSE_RANK;
            case "PERCENT_RANK":
                return WindowFunctionType.PERCENT_RANK;
            case "CUME_DIST":
                return WindowFunctionType.CUME_DIST;
            default:
                return null;
            }
        }

    }

    private final WindowFunctionType type;

    /**
     * Creates new instance of a window function.
     *
     * @param type
     *            the type
     * @param select
     *            the select statement
     */
    public WindowFunction(WindowFunctionType type, Select select) {
        super(select, false);
        this.type = type;
    }

    @Override
    public boolean isAggregate() {
        return false;
    }

    @Override
    protected void updateAggregate(Session session, Object aggregateData) {
        throw DbException.getUnsupportedException("Window function");
    }

    @Override
    protected void updateGroupAggregates(Session session, int stage) {
        // Nothing to do
    }

    @Override
    protected int getNumExpressions() {
        return 0;
    }

    @Override
    protected void rememberExpressions(Session session, Value[] array) {
        // Nothing to do
    }

    @Override
    protected void updateFromExpressions(Session session, Object aggregateData, Value[] array) {
        throw DbException.getUnsupportedException("Window function");
    }

    @Override
    protected Object createAggregateData() {
        throw DbException.getUnsupportedException("Window function");
    }

    @Override
    protected void getOrderedResultLoop(Session session, HashMap<Integer, Value> result, ArrayList<Value[]> ordered,
            int rowIdColumn) {
        if (type == WindowFunctionType.CUME_DIST) {
            getCumeDist(session, result, ordered, rowIdColumn);
            return;
        }
        int size = ordered.size();
        int number = 0;
        for (int i = 0; i < size; i++) {
            Value[] row = ordered.get(i);
            int rowId = row[rowIdColumn].getInt();
            Value v;
            switch (type) {
            case ROW_NUMBER:
                v = ValueInt.get(i + 1);
                break;
            case RANK:
            case DENSE_RANK:
            case PERCENT_RANK: {
                if (i == 0) {
                    number = 1;
                } else {
                    if (getOverOrderBySort().compare(ordered.get(i - 1), row) != 0) {
                        switch (type) {
                        case RANK:
                        case PERCENT_RANK:
                            number = i + 1;
                            break;
                        default: // DENSE_RANK
                            number++;
                        }
                    }
                }
                if (type == WindowFunctionType.PERCENT_RANK) {
                    int nm = number - 1;
                    v = nm == 0 ? ValueDouble.ZERO : ValueDouble.get((double) nm / (size - 1));
                } else {
                    v = ValueInt.get(number);
                }
                break;
            }
            case CUME_DIST: {
                int nm = number;
                v = ValueDouble.get((double) nm / size);
                break;
            }
            default:
                throw DbException.throwInternalError("type=" + type);
            }
            result.put(rowId, v);
        }
    }

    private void getCumeDist(Session session, HashMap<Integer, Value> result, ArrayList<Value[]> orderedData,
            int last) {
        int size = orderedData.size();
        for (int start = 0; start < size;) {
            Value[] array = orderedData.get(start);
            int end = start + 1;
            while (end < size && overOrderBySort.compare(array, orderedData.get(end)) == 0) {
                end++;
            }
            ValueDouble v = ValueDouble.get((double) end / size);
            for (int i = start; i < end; i++) {
                int rowId = orderedData.get(i)[last].getInt();
                result.put(rowId, v);
            }
            start = end;
        }
    }

    @Override
    protected Value getAggregatedValue(Session session, Object aggregateData) {
        throw DbException.getUnsupportedException("Window function");
    }

    @Override
    public int getType() {
        switch (type) {
        case ROW_NUMBER:
        case RANK:
        case DENSE_RANK:
            return Value.INT;
        case PERCENT_RANK:
        case CUME_DIST:
            return Value.DOUBLE;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public long getPrecision() {
        switch (type) {
        case ROW_NUMBER:
        case RANK:
        case DENSE_RANK:
            return ValueInt.PRECISION;
        case PERCENT_RANK:
        case CUME_DIST:
            return ValueDouble.PRECISION;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }

    @Override
    public int getDisplaySize() {
        switch (type) {
        case ROW_NUMBER:
        case RANK:
        case DENSE_RANK:
            return ValueInt.DISPLAY_SIZE;
        case PERCENT_RANK:
        case CUME_DIST:
            return ValueDouble.DISPLAY_SIZE;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }

    @Override
    public String getSQL() {
        String text;
        switch (type) {
        case ROW_NUMBER:
            text = "ROW_NUMBER";
            break;
        case RANK:
            text = "RANK";
            break;
        case DENSE_RANK:
            text = "DENSE_RANK";
            break;
        case PERCENT_RANK:
            text = "PERCENT_RANK";
            break;
        case CUME_DIST:
            text = "CUME_DIST";
            break;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        StringBuilder builder = new StringBuilder().append(text).append("()");
        return appendTailConditions(builder).toString();
    }

    @Override
    public int getCost() {
        int cost = 1;
        return cost;
    }

}
