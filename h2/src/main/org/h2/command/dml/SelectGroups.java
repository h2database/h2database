/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 * Grouped data for aggregates.
 *
 * <p>
 * Call sequence:
 * </p>
 * <ul>
 * <li>{@link #reset()}.</li>
 * <li>For each source row {@link #nextSource()} should be invoked.</li>
 * <li>{@link #done()}.</li>
 * <li>{@link #next()} is invoked inside a loop until it returns null.</li>
 * </ul>
 * <p>
 * Call sequence for lazy group sorted result:
 * </p>
 * <ul>
 * <li>{@link #resetLazy()} (not required before the first execution).</li>
 * <li>For each source group {@link #nextLazyGroup()} should be invoked.</li>
 * <li>For each source row {@link #nextLazyRow()} should be invoked. Each group
 * can have one or more rows.</li>
 * </ul>
 */
public abstract class SelectGroups {

    public static final class Grouped extends SelectGroups {

        private final int[] groupIndex;

        /**
         * Map of group-by key to group-by expression data e.g. AggregateData
         */
        private HashMap<ValueArray, Object[]> groupByData;

        /**
         * Key into groupByData that produces currentGroupByExprData. Not used
         * in lazy mode.
         */
        private ValueArray currentGroupsKey;

        /**
         * Cursor for {@link #next()} method.
         */
        private Iterator<Entry<ValueArray, Object[]>> cursor;

        /**
         * The key for the default group.
         */
        // Can be static, but TestClearReferences complains about it
        private final ValueArray defaultGroup = ValueArray.get(new Value[0]);

        public Grouped(Session session, ArrayList<Expression> expressions, int[] groupIndex) {
            super(session, expressions);
            this.groupIndex = groupIndex;
        }

        @Override
        public void reset() {
            super.reset();
            groupByData = new HashMap<>();
            currentGroupsKey = null;
            cursor = null;
        }

        @Override
        public void nextSource() {
            if (groupIndex == null) {
                currentGroupsKey = defaultGroup;
            } else {
                Value[] keyValues = new Value[groupIndex.length];
                // update group
                for (int i = 0; i < groupIndex.length; i++) {
                    int idx = groupIndex[i];
                    Expression expr = expressions.get(idx);
                    keyValues[i] = expr.getValue(session);
                }
                currentGroupsKey = ValueArray.get(keyValues);
            }
            Object[] values = groupByData.get(currentGroupsKey);
            if (values == null) {
                values = new Object[Math.max(exprToIndexInGroupByData.size(), expressions.size())];
                groupByData.put(currentGroupsKey, values);
            }
            currentGroupByExprData = values;
            currentGroupRowId++;
        }

        @Override
        void updateCurrentGroupExprData() {
            // this can be null in lazy mode
            if (currentGroupsKey != null) {
                // since we changed the size of the array, update the object in
                // the groups map
                groupByData.put(currentGroupsKey, currentGroupByExprData);
            }
        }

        @Override
        public void done() {
            super.done();
            if (groupIndex == null && groupByData.size() == 0) {
                groupByData.put(defaultGroup,
                        new Object[Math.max(exprToIndexInGroupByData.size(), expressions.size())]);
            }
            cursor = groupByData.entrySet().iterator();
        }

        @Override
        public ValueArray next() {
            if (cursor.hasNext()) {
                Map.Entry<ValueArray, Object[]> entry = cursor.next();
                currentGroupByExprData = entry.getValue();
                currentGroupRowId++;
                return entry.getKey();
            }
            return null;
        }

        @Override
        public void resetLazy() {
            super.resetLazy();
            currentGroupsKey = null;
        }
    }

    public static final class Plain extends SelectGroups {

        private ArrayList<Object[]> rows;

        /**
         * Cursor for {@link #next()} method.
         */
        private Iterator<Object[]> cursor;

        public Plain(Session session, ArrayList<Expression> expressions) {
            super(session, expressions);
        }

        @Override
        public void reset() {
            super.reset();
            rows = new ArrayList<>();
            cursor = null;
        }

        @Override
        public void nextSource() {
            Object[] values = new Object[Math.max(exprToIndexInGroupByData.size(), expressions.size())];
            rows.add(values);
            currentGroupByExprData = values;
            currentGroupRowId++;
        }

        @Override
        void updateCurrentGroupExprData() {
            rows.set(rows.size() - 1, currentGroupByExprData);
        }

        @Override
        public void done() {
            super.done();
            cursor = rows.iterator();
        }

        @Override
        public ValueArray next() {
            if (cursor.hasNext()) {
                Object[] values = cursor.next();
                currentGroupByExprData = values;
                currentGroupRowId++;
                return ValueArray.get(new Value[0]);
            }
            return null;
        }
    }

    final Session session;

    final ArrayList<Expression> expressions;

    /**
     * The array of current group-by expression data e.g. AggregateData.
     */
    Object[] currentGroupByExprData;

    /**
     * Maps an expression object to an index, to use in accessing the Object[]
     * pointed to by groupByData.
     */
    final HashMap<Expression, Integer> exprToIndexInGroupByData = new HashMap<>();

    /**
     * Maps an expression object to its data.
     */
    private final HashMap<Expression, Object> windowData = new HashMap<>();

    /**
     * The id of the current group.
     */
    int currentGroupRowId;

    /**
     * Creates new instance of grouped data.
     *
     * @param session
     *            the session
     * @param expressions
     *            the expressions
     * @param isGroupQuery
     *            is this query is a group query
     * @param groupIndex
     *            the indexes of group expressions, or null
     */
    public static SelectGroups getInstance(Session session, ArrayList<Expression> expressions, boolean isGroupQuery,
            int[] groupIndex) {
        return session.getDatabase().getSelectGroupsFactory().create(session, expressions, isGroupQuery, groupIndex);
    }

    SelectGroups(Session session, ArrayList<Expression> expressions) {
        this.session = session;
        this.expressions = expressions;
    }

    /**
     * Is there currently a group-by active
     */
    public boolean isCurrentGroup() {
        return currentGroupByExprData != null;
    }

    /**
     * Get the group-by data for the current group and the passed in expression.
     *
     * @param expr
     *            expression
     * @param window
     *            true if expression is a window expression
     * @return expression data or null
     */
    public Object getCurrentGroupExprData(Expression expr, boolean window) {
        if (window) {
            return windowData.get(expr);
        }
        Integer index = exprToIndexInGroupByData.get(expr);
        if (index == null) {
            return null;
        }
        return currentGroupByExprData[index];
    }

    /**
     * Set the group-by data for the current group and the passed in expression.
     *
     * @param expr
     *            expression
     * @param object
     *            expression data to set
     * @param window
     *            true if expression is a window expression
     */
    public void setCurrentGroupExprData(Expression expr, Object obj, boolean window) {
        if (window) {
            Object old = windowData.put(expr, obj);
            assert old == null;
            return;
        }
        Integer index = exprToIndexInGroupByData.get(expr);
        if (index != null) {
            assert currentGroupByExprData[index] == null;
            currentGroupByExprData[index] = obj;
            return;
        }
        index = exprToIndexInGroupByData.size();
        exprToIndexInGroupByData.put(expr, index);
        if (index >= currentGroupByExprData.length) {
            currentGroupByExprData = Arrays.copyOf(currentGroupByExprData, currentGroupByExprData.length * 2);
            updateCurrentGroupExprData();
        }
        currentGroupByExprData[index] = obj;
    }

    abstract void updateCurrentGroupExprData();

    /**
     * Returns identity of the current row. Used by aggregates to check whether
     * they already processed this row or not.
     *
     * @return identity of the current row
     */
    public int getCurrentGroupRowId() {
        return currentGroupRowId;
    }

    /**
     * Resets this group data for reuse.
     */
    public void reset() {
        currentGroupByExprData = null;
        exprToIndexInGroupByData.clear();
        windowData.clear();
        currentGroupRowId = 0;
    }

    /**
     * Invoked for each source row to evaluate group key and setup all necessary
     * data for aggregates.
     */
    public abstract void nextSource();

    /**
     * Invoked after all source rows are evaluated.
     */
    public void done() {
        currentGroupRowId = 0;
    }

    /**
     * Returns the key of the next group.
     *
     * @return the key of the next group, or null
     */
    public abstract ValueArray next();

    /**
     * Resets this group data for reuse in lazy mode.
     */
    public void resetLazy() {
        currentGroupByExprData = null;
        currentGroupRowId = 0;
    }

    /**
     * Moves group data to the next group in lazy mode.
     */
    public void nextLazyGroup() {
        currentGroupByExprData = new Object[Math.max(exprToIndexInGroupByData.size(), expressions.size())];
    }

    /**
     * Moves group data to the next row in lazy mode.
     */
    public void nextLazyRow() {
        currentGroupRowId++;
    }

}
