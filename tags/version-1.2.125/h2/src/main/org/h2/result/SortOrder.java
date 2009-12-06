/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.Database;
import org.h2.expression.Expression;
import org.h2.util.ObjectArray;
import org.h2.util.RandomUtils;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A sort order represents an ORDER BY clause in a query.
 */
public class SortOrder {

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

    /**
     * The default sort order for NULL.
     */
    private static final int DEFAULT_NULL_SORT = SysProperties.SORT_NULLS_HIGH ? 1 : -1;

    private final Database database;
    private final int len;
    private final int[] indexes;
    private final int[] sortTypes;

    /**
     * Construct a new sort order object.
     *
     * @param database the database
     * @param index the column index list
     * @param sortType the sort order bit masks
     */
    public SortOrder(Database database, int[] index, int[] sortType) {
        this.database = database;
        this.indexes = index;
        this.sortTypes = sortType;
        len = index.length;
    }

    /**
     * Create the SQL snippet that describes this sort order.
     * This is the SQL snippet that usually appears after the ORDER BY clause.
     *
     * @param list the expression list
     * @param visible the number of columns in the select list
     * @return the SQL snippet
     */
    public String getSQL(Expression[] list, int visible) {
        StatementBuilder buff = new StatementBuilder();
        int i = 0;
        for (int idx : indexes) {
            buff.appendExceptFirst(", ");
            if (idx < visible) {
                buff.append(idx + 1);
            } else {
                buff.append('=').append(StringUtils.unEnclose(list[idx].getSQL()));
            }
            int type = sortTypes[i++];
            if ((type & DESCENDING) != 0) {
                buff.append(" DESC");
            }
            if ((type & NULLS_FIRST) != 0) {
                buff.append(" NULLS FIRST");
            } else if ((type & NULLS_LAST) != 0) {
                buff.append(" NULLS LAST");
            }
        }
        return buff.toString();
    }

    /**
     * Compare two expressions where one of them is NULL.
     *
     * @param aNull whether the first expression is null
     * @param bNull whether the second expression is null
     * @param sortType the sort bit mask to use
     * @return the result of the comparison (-1 meaning the first expression
     *         should appear before the second, 0 if they are equal)
     */
    public static int compareNull(boolean aNull, boolean bNull, int sortType) {
        if ((sortType & NULLS_FIRST) != 0) {
            return aNull ? -1 : 1;
        } else if ((sortType & NULLS_LAST) != 0) {
            return aNull ? 1 : -1;
        } else {
            // see also JdbcDatabaseMetaData.nullsAreSorted*
            int comp = aNull ? DEFAULT_NULL_SORT : -DEFAULT_NULL_SORT;
            return (sortType & DESCENDING) == 0 ? comp : -comp;
        }
    }

    /**
     * Compare two expression lists.
     *
     * @param a the first expression list
     * @param b the second expression list
     * @return the result of the comparison
     */
    public int compare(Value[] a, Value[] b) throws SQLException {
        for (int i = 0; i < len; i++) {
            int idx = indexes[i];
            int type = sortTypes[i];
            Value ao = a[idx];
            Value bo = b[idx];
            boolean aNull = ao == ValueNull.INSTANCE, bNull = bo == ValueNull.INSTANCE;
            if (aNull || bNull) {
                if (aNull == bNull) {
                    continue;
                }
                return compareNull(aNull, bNull, type);
            }
            int comp = database.compare(ao, bo);
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
    public void sort(ObjectArray<Value[]> rows) throws SQLException {
        sort(rows, 0, rows.size() - 1);
    }

    private void swap(ObjectArray<Value[]> rows, int a, int b) {
        Value[] t = rows.get(a);
        rows.set(a, rows.get(b));
        rows.set(b, t);
    }

    private void sort(ObjectArray<Value[]> rows, int l, int r) throws SQLException {
        // quicksort
        int i, j;
        while (r - l > 10) {
            // randomized pivot to avoid worst case
            i = RandomUtils.nextInt(r - l - 4) + l + 2;
            if (compare(rows.get(l), rows.get(r)) > 0) {
                swap(rows, l, r);
            }
            if (compare(rows.get(i), rows.get(l)) < 0) {
                swap(rows, l, i);
            } else if (compare(rows.get(i), rows.get(r)) > 0) {
                swap(rows, i, r);
            }
            j = r - 1;
            swap(rows, i, j);
            Value[] p = rows.get(j);
            i = l;
            while (true) {
                do {
                    ++i;
                } while (compare(rows.get(i), p) < 0);
                do {
                    --j;
                } while (compare(rows.get(j), p) > 0);
                if (i >= j) {
                    break;
                }
                swap(rows, i, j);
            }
            swap(rows, i, r - 1);
            sort(rows, l, i - 1);
            l = i + 1;
        }
        for (i = l + 1; i <= r; i++) {
            Value[] t = rows.get(i);
            for (j = i - 1; j >= l && (compare(rows.get(j), t) > 0); j--) {
                rows.set(j + 1, rows.get(j));
            }
            rows.set(j + 1, t);
        }
    }

    /**
     * Get the column index list.
     *
     * @return the list
     */
    public int[] getIndexes() {
        return indexes;
    }

    /**
     * Get the sort order bit masks.
     *
     * @return the list
     */
    public int[] getSortTypes() {
        return sortTypes;
    }

}
