/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.expression.Expression;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * @author Thomas
 */

public class SortOrder {
    public static final int ASCENDING = 0, DESCENDING = 1;
    public static final int NULLS_FIRST = 2, NULLS_LAST = 4;

    private Database database;
    private int len;
    private int[] indexes;
    private int[] sortTypes;

    public SortOrder(Database database, int[] index, int[] sortType) {
        this.database = database;
        this.indexes = index;
        this.sortTypes = sortType;
        len = index.length;
    }

    public String getSQL(Expression[] list, int visible) {
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < len; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            int idx = indexes[i];
            if (idx < visible) {
                buff.append(idx + 1);
            } else {
                buff.append("=");
                buff.append(StringUtils.unEnclose(list[idx].getSQL()));
            }
            int type = sortTypes[i];
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

    public int compare(Value[] a, Value[] b) throws SQLException {
        for (int i = 0; i < len; i++) {
            int idx = indexes[i];
            int type = sortTypes[i];
            Value o1 = a[idx];
            Value o2 = b[idx];
            boolean b1 = o1 == ValueNull.INSTANCE, b2 = o2 == ValueNull.INSTANCE;
            if (b1 || b2) {
                if (b1 == b2) {
                    continue;
                }
                if ((type & NULLS_FIRST) != 0) {
                    return b1 ? -1 : 1;
                } else if ((type & NULLS_LAST) != 0) {
                    return b1 ? 1 : -1;
                } else {
                    // this depends on NULL_SORT_DEFAULT
                    int comp;
                    if (Constants.NULL_SORT_DEFAULT == Constants.NULL_SORT_LOW) {
                        comp = b1 ? -1 : 1;
                        return (type & DESCENDING) == 0 ? comp : -comp;
                    } else if (Constants.NULL_SORT_DEFAULT == Constants.NULL_SORT_HIGH) {
                        comp = b1 ? 1 : -1;
                        return (type & DESCENDING) == 0 ? comp : -comp;
                    } else if (Constants.NULL_SORT_DEFAULT == Constants.NULL_SORT_START) {
                        return b1 ? 1 : -1;
                    } else {
                        return b1 ? -1 : 1;
                    }
                }
            }
            int comp = database.compare(o1, o2);
            if (comp != 0) {
                return (type & DESCENDING) == 0 ? comp : -comp;
            }
        }
        return 0;
    }

    public void sort(ObjectArray rows) throws SQLException {
        sort(rows, 0, rows.size() - 1);
    }

    private void swap(ObjectArray rows, int a, int b) {
        Object t = rows.get(a);
        rows.set(a, rows.get(b));
        rows.set(b, t);
    }

    private void sort(ObjectArray rows, int l, int r) throws SQLException {
        int i, j;
        while (r - l > 10) {
            i = (r + l) >> 1;
            if (compare((Value[]) rows.get(l), (Value[]) rows.get(r)) > 0) {
                swap(rows, l, r);
            }
            if (compare((Value[]) rows.get(i), (Value[]) rows.get(l)) < 0) {
                swap(rows, l, i);
            } else if (compare((Value[]) rows.get(i), (Value[]) rows.get(r)) > 0) {
                swap(rows, i, r);
            }
            j = r - 1;
            swap(rows, i, j);
            Value[] p = (Value[]) rows.get(j);
            i = l;
            while (true) {
                do {
                    ++i;
                } while (compare((Value[]) rows.get(i), p) < 0);
                do {
                    --j;
                } while (compare((Value[]) rows.get(j), p) > 0);
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
            Value[] t = (Value[]) rows.get(i);
            for (j = i - 1; j >= l && (compare((Value[]) rows.get(j), t) > 0); j--) {
                rows.set(j + 1, rows.get(j));
            }
            rows.set(j + 1, t);
        }
    }

    public int[] getIndexes() {
        return indexes;
    }

    public int[] getSortTypes() {
        return sortTypes;
    }

}
