/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;
import java.util.HashSet;

import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * The filter used to walk through an index. This class supports IN(..)
 * and IN(SELECT ...) optimizations.
 */
public class IndexCursor implements Cursor {
    private Session session;
    private Index index;
    private Table table;
    private IndexColumn[] indexColumns;
    private boolean alwaysFalse;

    private SearchRow start, end;
    private Cursor cursor;
    private Column inColumn;
    private int inListIndex;
    private Value[] inList;
    private ResultInterface inResult;
    private HashSet<Value> inResultTested;

    public void setIndex(Index index) {
        this.index = index;
        this.table = index.getTable();
        Column[] columns = table.getColumns();
        indexColumns = new IndexColumn[columns.length];
        IndexColumn[] idxCols = index.getIndexColumns();
        if (idxCols != null) {
            for (int i = 0; i < columns.length; i++) {
                int idx = index.getColumnIndex(columns[i]);
                if (idx >= 0) {
                    indexColumns[i] = idxCols[idx];
                }
            }
        }
    }

    /**
     * Re-evaluate the start and end values of the index search for rows.
     *
     * @param s the session
     * @param indexConditions the index conditions
     */
    public void find(Session s, ArrayList<IndexCondition> indexConditions) {
        this.session = s;
        alwaysFalse = false;
        start = end = null;
        inList = null;
        inColumn = null;
        inResult = null;
        inResultTested = new HashSet<Value>();
        for (IndexCondition condition : indexConditions) {
            if (condition.isAlwaysFalse()) {
                alwaysFalse = true;
                break;
            }
            Column column = condition.getColumn();
            if (condition.getCompareType() == Comparison.IN_LIST) {
                if (start == null && end == null) {
                    if (canUseIndexForIn(column)) {
                        this.inColumn = column;
                        inList = condition.getCurrentValueList(s);
                        inListIndex = 0;
                    }
                }
            } else if (condition.getCompareType() == Comparison.IN_QUERY) {
                if (start == null && end == null) {
                    if (canUseIndexForIn(column)) {
                        this.inColumn = column;
                        inResult = condition.getCurrentResult(s);
                    }
                }
            } else {
                Value v = condition.getCurrentValue(s);
                boolean isStart = condition.isStart();
                boolean isEnd = condition.isEnd();
                int id = column.getColumnId();
                IndexColumn idxCol = indexColumns[id];
                if (idxCol != null && (idxCol.sortType & SortOrder.DESCENDING) != 0) {
                    // if the index column is sorted the other way, we swap end and start
                    // NULLS_FIRST / NULLS_LAST is not a problem, as nulls never match anyway
                    boolean temp = isStart;
                    isStart = isEnd;
                    isEnd = temp;
                }
                if (isStart) {
                    start = getSearchRow(start, id, v, true);
                }
                if (isEnd) {
                    end = getSearchRow(end, id, v, false);
                }
                if (isStart || isEnd) {
                    // an X=? condition will produce less rows than
                    // an X IN(..) condition
                    inColumn = null;
                    inList = null;
                    inResult = null;
                }
                if (!SysProperties.OPTIMIZE_IS_NULL) {
                    if (isStart && isEnd) {
                        if (v == ValueNull.INSTANCE) {
                            // join on a column=NULL is always false
                            alwaysFalse = true;
                        }
                    }
                }
            }
        }
        if (inColumn != null) {
            return;
        }
        if (!alwaysFalse) {
            cursor = index.find(s, start, end);
        }
    }

    private boolean canUseIndexForIn(Column column) {
        if (inColumn != null) {
            // only one IN(..) condition can be used at the same time
            return false;
        }
        // The first column of the index must match this column,
        // or it must be a VIEW index (where the column is null).
        // Multiple IN conditions with views are not supported, see
        // IndexCondition.getMask.
        IndexColumn[] cols = index.getIndexColumns();
        if (cols == null) {
            return true;
        }
        IndexColumn idxCol = cols[0];
        return idxCol == null || idxCol.column == column;
    }

    private SearchRow getSearchRow(SearchRow row, int id, Value v, boolean max) {
        if (row == null) {
            row = table.getTemplateRow();
        } else {
            v = getMax(row.getValue(id), v, max);
        }
        row.setValue(id, v);
        return row;
    }

    private Value getMax(Value a, Value b, boolean bigger) {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        }
        if (SysProperties.OPTIMIZE_IS_NULL) {
            // IS NULL must be checked later
            if (a == ValueNull.INSTANCE) {
                return b;
            } else if (b == ValueNull.INSTANCE) {
                return a;
            }
        }
        int comp = a.compareTo(b, table.getDatabase().getCompareMode());
        if (comp == 0) {
            return a;
        }
        if (SysProperties.OPTIMIZE_IS_NULL) {
            if (a == ValueNull.INSTANCE || b == ValueNull.INSTANCE) {
                // column IS NULL AND column <op> <not null> is always false
                return null;
            }
        }
        if (!bigger) {
            comp = -comp;
        }
        return comp > 0 ? a : b;
    }

    /**
     * Check if the result is empty for sure.
     *
     * @return true if it is
     */
    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    public Row get() {
        return cursor.get();
    }

    public SearchRow getSearchRow() {
        return cursor.getSearchRow();
    }

    public boolean next() {
        while (true) {
            if (cursor == null) {
                nextCursor();
                if (cursor == null) {
                    return false;
                }
            }
            if (cursor.next()) {
                return true;
            }
            cursor = null;
        }
    }

    private void nextCursor() {
        if (inList != null) {
            while (inListIndex < inList.length) {
                Value v = inList[inListIndex++];
                if (v != ValueNull.INSTANCE) {
                    find(v);
                    break;
                }
            }
        } else if (inResult != null) {
            while (inResult.next()) {
                Value v = inResult.currentRow()[0];
                if (v != ValueNull.INSTANCE) {
                    v = inColumn.convert(v);
                    if (inResultTested.add(v)) {
                        find(v);
                        break;
                    }
                }
            }
        }
    }

    private void find(Value v) {
        v = inColumn.convert(v);
        int id = inColumn.getColumnId();
        if (start == null) {
            start = table.getTemplateRow();
        }
        start.setValue(id, v);
        cursor = index.find(session, start, start);
    }

    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
