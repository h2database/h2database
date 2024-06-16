/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;

import org.h2.engine.SessionLocal;
import org.h2.expression.condition.Comparison;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * The filter used to walk through an index. This class supports IN(..)
 * and IN(SELECT ...) optimizations.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class IndexCursor implements Cursor {

    private SessionLocal session;
    private Index index;
    private boolean reverse;
    private Table table;
    private IndexColumn[] indexColumns;
    private boolean alwaysFalse;

    private SearchRow start, end, intersects;
    private Cursor cursor;
    /**
     * Contains a {@link Column} or {@code Column[]} depending on the condition type.
     * @see IndexCondition#isCompoundColumns()
     */
    private Object inColumn;
    private int inListIndex;
    private Value[] inList;
    private ResultInterface inResult;

    public IndexCursor() {
    }

    public void setIndex(Index index, boolean reverse) {
        this.index = index;
        this.reverse = reverse;
        this.table = index.getTable();
        Column[] columns = table.getColumns();
        indexColumns = new IndexColumn[columns.length];
        IndexColumn[] idxCols = index.getIndexColumns();
        if (idxCols != null) {
            for (int i = 0, len = columns.length; i < len; i++) {
                int idx = index.getColumnIndex(columns[i]);
                if (idx >= 0) {
                    indexColumns[i] = idxCols[idx];
                }
            }
        }
    }

    /**
     * Prepare this index cursor to make a lookup in index.
     *
     * @param s Session.
     * @param indexConditions Index conditions.
     */
    public void prepare(SessionLocal s, ArrayList<IndexCondition> indexConditions) {
        session = s;
        alwaysFalse = false;
        start = end = null;
        inList = null;
        inColumn = null;
        inResult = null;
        intersects = null;
        for (IndexCondition condition : indexConditions) {
            if (condition.isAlwaysFalse()) {
                alwaysFalse = true;
                break;
            }
            // If index can perform only full table scan do not try to use it for regular
            // lookups, each such lookup will perform an own table scan.
            if (index.isFindUsingFullTableScan()) {
                continue;
            }
            if (condition.isCompoundColumns()) {
                Column[] columns = condition.getColumns();
                if (condition.getCompareType() == Comparison.IN_LIST) {
                    if (start == null && end == null) {
                        if (canUseIndexForIn(columns)) {
                            this.inColumn = columns;
                            inList = condition.getCurrentValueList(s);
                            inListIndex = 0;
                        }
                    }
                    continue;
                } else {
                    throw DbException.getInternalError("Multiple columns can only be used with compound IN lists.");
                }
            }
            Column column = condition.getColumn();
            switch (condition.getCompareType()) {
            case Comparison.IN_LIST:
            case Comparison.IN_ARRAY:
                if (start == null && end == null) {
                    if (canUseIndexForIn(column)) {
                        this.inColumn = column;
                        inList = condition.getCurrentValueList(s);
                        inListIndex = 0;
                    }
                }
                break;
            case Comparison.IN_QUERY:
                if (start == null && end == null) {
                    if (canUseIndexForIn(column)) {
                        this.inColumn = column;
                        inResult = condition.getCurrentResult();
                    }
                }
                break;
            default:
                Value v = condition.getCurrentValue(s);
                boolean isStart = condition.isStart();
                boolean isEnd = condition.isEnd();
                boolean isIntersects = condition.isSpatialIntersects();
                int columnId = column.getColumnId();
                if (columnId != SearchRow.ROWID_INDEX) {
                    IndexColumn idxCol = indexColumns[columnId];
                    if (idxCol != null && (idxCol.sortType & SortOrder.DESCENDING) != 0) {
                        // if the index column is sorted the other way, we swap
                        // end and start NULLS_FIRST / NULLS_LAST is not a
                        // problem, as nulls never match anyway
                        boolean temp = isStart;
                        isStart = isEnd;
                        isEnd = temp;
                    }
                }
                if (isStart) {
                    start = getSearchRow(start, columnId, v, true);
                }
                if (isEnd) {
                    end = getSearchRow(end, columnId, v, false);
                }
                if (isIntersects) {
                    intersects = getSpatialSearchRow(intersects, columnId, v);
                }
                // An X=? condition will produce less rows than
                // an X IN(..) condition, unless the X IN condition can use the index.
                if ((isStart || isEnd) && !canUseIndexFor((Column) inColumn)) {
                    inColumn = null;
                    inList = null;
                    inResult = null;
                }
                break;
            }
        }
        if (inColumn != null) {
            start = table.getTemplateRow();
        }
    }

    /**
     * Re-evaluate the start and end values of the index search for rows.
     *
     * @param s the session
     * @param indexConditions the index conditions
     */
    public void find(SessionLocal s, ArrayList<IndexCondition> indexConditions) {
        prepare(s, indexConditions);
        if (inColumn != null) {
            return;
        }
        if (!alwaysFalse) {
            SearchRow first, last;
            if (reverse) {
                first = end;
                last = start;
            } else {
                first = start;
                last = end;
            }
            if (intersects != null && index instanceof SpatialIndex) {
                cursor = ((SpatialIndex) index).findByGeometry(session, first, last, reverse, intersects);
            } else if (index != null) {
                cursor = index.find(session, first, last, reverse);
            }
        }
    }

    private boolean canUseIndexForIn(Column column) {
        if (inColumn != null) {
            // only one IN(..) condition can be used at the same time
            return false;
        }
        return canUseIndexFor(column);
    }

    private boolean canUseIndexFor(Column column) {
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

    private boolean canUseIndexForIn(Column[] columns) {
        if (inColumn != null) {
            // only one IN(..) condition can be used at the same time
            return false;
        }
        return canUseIndexForIn(index, columns);
    }

    /**
     * Return {@code true} if {@link Index#getIndexColumns()} and the {@code columns} parameter contains the same
     * elements in the same order. All column of the index must match the column in the {@code columns} array, or
     * it must be a VIEW index (where the column is null).
     * @see IndexCondition#getMask(ArrayList)
     */
    public static boolean canUseIndexForIn(Index index, Column[] columns) {
        IndexColumn[] cols = index.getIndexColumns();
        if (cols == null || cols.length != columns.length) {
            return false;
        }
        for (int i = 0; i < cols.length; i++) {
            IndexColumn idxCol = cols[i];
            if (idxCol != null && idxCol.column != columns[i]) {
                return false;
            }
        }
        return true;
    }

    private SearchRow getSpatialSearchRow(SearchRow row, int columnId, Value v) {
        if (row == null) {
            row = table.getTemplateRow();
        } else if (row.getValue(columnId) != null) {
            // if an object needs to overlap with both a and b,
            // then it needs to overlap with the union of a and b
            // (not the intersection)
            ValueGeometry vg = row.getValue(columnId).convertToGeometry(null);
            v = v.convertToGeometry(null).getEnvelopeUnion(vg);
        }
        if (columnId == SearchRow.ROWID_INDEX) {
            row.setKey(v == ValueNull.INSTANCE ? Long.MIN_VALUE : v.getLong());
        } else {
            row.setValue(columnId, v);
        }
        return row;
    }

    private SearchRow getSearchRow(SearchRow row, int columnId, Value v, boolean max) {
        if (row == null) {
            row = table.getTemplateRow();
        } else {
            v = getMax(row.getValue(columnId), v, max);
        }
        if (columnId == SearchRow.ROWID_INDEX) {
            row.setKey(v == ValueNull.INSTANCE ? Long.MIN_VALUE : v.getLong());
        } else {
            row.setValue(columnId, v);
        }
        return row;
    }

    private Value getMax(Value a, Value b, boolean bigger) {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        }
        // IS NULL must be checked later
        if (a == ValueNull.INSTANCE) {
            return b;
        } else if (b == ValueNull.INSTANCE) {
            return a;
        }
        int comp = session.compare(a, b);
        if (comp == 0) {
            return a;
        }
        return (comp > 0) == bigger ? a : b;
    }

    /**
     * Check if the result is empty for sure.
     *
     * @return true if it is
     */
    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    /**
     * Get start search row.
     *
     * @return search row
     */
    public SearchRow getStart() {
        return start;
    }

    /**
     * Get end search row.
     *
     * @return search row
     */
    public SearchRow getEnd() {
        return end;
    }

    @Override
    public Row get() {
        if (cursor == null) {
            return null;
        }
        return cursor.get();
    }

    @Override
    public SearchRow getSearchRow() {
        return cursor.getSearchRow();
    }

    @Override
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
                    if (inColumn instanceof Column[]) {
                        v = Column.convert(session, (Column[]) inColumn, (ValueRow) v);
                    } else {
                        v = ((Column) inColumn).convert(session, v);
                    }
                    find(v);
                    break;
                }
            }
        }
    }

    private void find(Value v) {
        if (inColumn instanceof Column[]) {
            Column[] columns = (Column[]) inColumn;
            ValueRow converted = Column.convert(session, columns, ((ValueRow) v));
            Value[] values = converted.getList();
            for (int i = columns.length; --i >= 0; ) {
                start.setValue(columns[i].getColumnId(), values[i]);
            }
        }
        else {
            Column column = (Column) inColumn;
            v = column.convert(session, v);
            int id = column.getColumnId();
            start.setValue(id, v);
        }
        cursor = index.find(session, start, start, false);
    }

    @Override
    public boolean previous() {
        throw DbException.getInternalError(toString());
    }

}
