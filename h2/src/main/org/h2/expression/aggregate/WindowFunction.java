/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.command.dml.Select;
import org.h2.engine.Session;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.DbException;
import org.h2.value.Value;
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
            default:
                return null;
            }
        }

    }

    private static class RowNumberData {

        int number;

        RowNumberData() {
        }

    }

    private static final class RankData extends RowNumberData {

        Value[] previousRow;

        int previousNumber;

        RankData() {
        }

    }

    private WindowFunctionType type;

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
        switch (type) {
        case ROW_NUMBER:
            ((RowNumberData) aggregateData).number++;
            break;
        case RANK:
        case DENSE_RANK: {
            RankData data = (RankData) aggregateData;
            data.number++;
            data.previousNumber++;
            break;
        }
        default:
            throw DbException.throwInternalError("type=" + type);
        }
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
        switch (type) {
        case ROW_NUMBER:
            ((RowNumberData) aggregateData).number++;
            break;
        case RANK:
        case DENSE_RANK: {
            RankData data = (RankData) aggregateData;
            data.number++;
            Value[] previous = data.previousRow;
            if (previous == null) {
                data.previousNumber++;
            } else {
                if (getOverOrderBySort().compare(previous, array) != 0) {
                    if (type == WindowFunctionType.RANK) {
                        data.previousNumber = data.number;
                    } else /* DENSE_RANK */ {
                        data.previousNumber++;
                    }
                }
            }
            data.previousRow = array;
            break;
        }
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }

    @Override
    protected Object createAggregateData() {
        switch (type) {
        case ROW_NUMBER:
            return new RowNumberData();
        case RANK:
        case DENSE_RANK:
            return new RankData();
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }

    @Override
    protected Value getAggregatedValue(Session session, Object aggregateData) {
        switch (type) {
        case ROW_NUMBER:
            return ValueInt.get(((RowNumberData) aggregateData).number);
        case RANK:
        case DENSE_RANK:
            return ValueInt.get(((RankData) aggregateData).previousNumber);
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }

    @Override
    public int getType() {
        return Value.INT;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public long getPrecision() {
        return ValueInt.PRECISION;
    }

    @Override
    public int getDisplaySize() {
        return ValueInt.DISPLAY_SIZE;
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
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        StringBuilder builder = new StringBuilder().append(text).append("()");
        return appendTailConditions(builder).toString();
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (visitor.getType() == ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL) {
            return false;
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = 1;
        return cost;
    }

}
