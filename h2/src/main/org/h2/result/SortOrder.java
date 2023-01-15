/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.h2.command.query.QueryOrderBy;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.mode.DefaultNullOrdering;
import org.h2.table.Column;
import org.h2.table.TableFilter;
import org.h2.util.Utils;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * A sort order represents an ORDER BY clause in a query.
 */
public final class SortOrder implements Comparator<Value[]> {

    /**
     * This bit mask means the values should be sorted in ascending order.
     */
    public static final int ASCENDING = 0;

    /**
     * This bit mask means the values should be sorted in descending order.
     */
    public static final int DESCENDING = 1;

    /**
     * This bit mask means NULLs should be sorted before other data, no matter
     * if ascending or descending order is used.
     */
    public static final int NULLS_FIRST = 2;

    /**
     * This bit mask means NULLs should be sorted after other data, no matter
     * if ascending or descending order is used.
     */
    public static final int NULLS_LAST = 4;

    private final SessionLocal session;

    /**
     * The column indexes of the order by expressions within the query.
     */
    private final int[] queryColumnIndexes;

    /**
     * The sort type bit mask (DESCENDING, NULLS_FIRST, NULLS_LAST).
     */
    private final int[] sortTypes;

    /**
     * The order list.
     */
    private final ArrayList<QueryOrderBy> orderList;

    /**
     * Construct a new sort order object with default sort directions.
     *
     * @param session the session
     * @param queryColumnIndexes the column index list
     */
    public SortOrder(SessionLocal session, int[] queryColumnIndexes) {
        this (session, queryColumnIndexes, new int[queryColumnIndexes.length], null);
    }

    /**
     * Construct a new sort order object.
     *
     * @param session the session
     * @param queryColumnIndexes the column index list
     * @param sortType the sort order bit masks
     * @param orderList the original query order list (if this is a query)
     */
    public SortOrder(SessionLocal session, int[] queryColumnIndexes, int[] sortType,
            ArrayList<QueryOrderBy> orderList) {
        this.session = session;
        this.queryColumnIndexes = queryColumnIndexes;
        this.sortTypes = sortType;
        this.orderList = orderList;
    }

    /**
     * Create the SQL snippet that describes this sort order.
     * This is the SQL snippet that usually appears after the ORDER BY clause.
     *
     * @param builder string builder to append to
     * @param list the expression list
     * @param visible the number of columns in the select list
     * @param sqlFlags formatting flags
     * @return the specified string builder
     */
    public StringBuilder getSQL(StringBuilder builder, Expression[] list, int visible, int sqlFlags) {
        int i = 0;
        for (int idx : queryColumnIndexes) {
            if (i > 0) {
                builder.append(", ");
            }
            if (idx < visible) {
                builder.append(idx + 1);
            } else {
                list[idx].getUnenclosedSQL(builder, sqlFlags);
            }
            typeToString(builder, sortTypes[i++]);
        }
        return builder;
    }

    /**
     * Appends type information (DESC, NULLS FIRST, NULLS LAST) to the specified statement builder.
     * @param builder string builder
     * @param type sort type
     */
    public static void typeToString(StringBuilder builder, int type) {
        if ((type & DESCENDING) != 0) {
            builder.append(" DESC");
        }
        if ((type & NULLS_FIRST) != 0) {
            builder.append(" NULLS FIRST");
        } else if ((type & NULLS_LAST) != 0) {
            builder.append(" NULLS LAST");
        }
    }

    /**
     * Compare two expression lists.
     *
     * @param a the first expression list
     * @param b the second expression list
     * @return the result of the comparison
     */
    @Override
    public int compare(Value[] a, Value[] b) {
        for (int i = 0, len = queryColumnIndexes.length; i < len; i++) {
            int idx = queryColumnIndexes[i];
            int type = sortTypes[i];
            Value ao = a[idx];
            Value bo = b[idx];
            boolean aNull = ao == ValueNull.INSTANCE, bNull = bo == ValueNull.INSTANCE;
            if (aNull || bNull) {
                if (aNull == bNull) {
                    continue;
                }
                return session.getDatabase().getDefaultNullOrdering().compareNull(aNull, type);
            }
            int comp = session.compare(ao, bo);
            if (comp != 0) {
                return (type & DESCENDING) == 0 ? comp : -comp;
            }
        }
        return 0;
    }

    /**
     * Sort a list of rows.
     *
     * @param rows the list of rows
     */
    public void sort(ArrayList<Value[]> rows) {
        rows.sort(this);
    }

    /**
     * Sort a list of rows using offset and limit.
     *
     * @param rows the list of rows
     * @param fromInclusive the start index, inclusive
     * @param toExclusive the end index, exclusive
     */
    public void sort(ArrayList<Value[]> rows, int fromInclusive, int toExclusive) {
        if (toExclusive == 1 && fromInclusive == 0) {
            rows.set(0, Collections.min(rows, this));
            return;
        }
        Value[][] arr = rows.toArray(new Value[0][]);
        Utils.sortTopN(arr, fromInclusive, toExclusive, this);
        for (int i = fromInclusive; i < toExclusive; i++) {
            rows.set(i, arr[i]);
        }
    }

    /**
     * Get the column index list. This is the column indexes of the order by
     * expressions within the query.
     * <p>
     * For the query "select name, id from test order by id, name" this is {1,
     * 0} as the first order by expression (the column "id") is the second
     * column of the query, and the second order by expression ("name") is the
     * first column of the query.
     *
     * @return the list
     */
    public int[] getQueryColumnIndexes() {
        return queryColumnIndexes;
    }

    /**
     * Get the column for the given table filter, if the sort column is for this
     * filter.
     *
     * @param index the column index (0, 1,..)
     * @param filter the table filter
     * @return the column, or null
     */
    public Column getColumn(int index, TableFilter filter) {
        if (orderList == null) {
            return null;
        }
        QueryOrderBy order = orderList.get(index);
        Expression expr = order.expression;
        if (expr == null) {
            return null;
        }
        expr = expr.getNonAliasExpression();
        if (expr.isConstant()) {
            return null;
        }
        if (!(expr instanceof ExpressionColumn)) {
            return null;
        }
        ExpressionColumn exprCol = (ExpressionColumn) expr;
        if (exprCol.getTableFilter() != filter) {
            return null;
        }
        return exprCol.getColumn();
    }

    /**
     * Get the sort order bit masks.
     *
     * @return the list
     */
    public int[] getSortTypes() {
        return sortTypes;
    }

    /**
     * Returns the original query order list.
     *
     * @return the original query order list
     */
    public ArrayList<QueryOrderBy> getOrderList() {
        return orderList;
    }

    /**
     * Returns sort order bit masks with {@link SortOrder#NULLS_FIRST} or
     * {@link SortOrder#NULLS_LAST} explicitly set.
     *
     * @return bit masks with either {@link SortOrder#NULLS_FIRST} or {@link SortOrder#NULLS_LAST}
     *         explicitly set.
     */
    public int[] getSortTypesWithNullOrdering() {
        return addNullOrdering(session.getDatabase(), sortTypes.clone());
    }

    /**
     * Add explicit {@link SortOrder#NULLS_FIRST} or {@link SortOrder#NULLS_LAST} where they
     * aren't already specified.
     *
     * @param database
     *            the database
     * @param sortTypes
     *            bit masks
     * @return the specified array with possibly modified bit masks
     */
    public static int[] addNullOrdering(Database database, int[] sortTypes) {
        DefaultNullOrdering defaultNullOrdering = database.getDefaultNullOrdering();
        for (int i = 0, length = sortTypes.length; i < length; i++) {
            sortTypes[i] = defaultNullOrdering.addExplicitNullOrdering(sortTypes[i]);
        }
        return sortTypes;
    }

    /**
     * Returns comparator for row values.
     *
     * @return comparator for row values.
     */
    public Comparator<Value> getRowValueComparator() {
        return (o1, o2) -> compare(((ValueRow) o1).getList(), ((ValueRow) o2).getList());
    }

}
