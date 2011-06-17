/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import java.util.HashSet;
import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * The filter used to walk through an index. This class filters supports IN(..)
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
    private LocalResult inResult;
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
     * @param session the session
     * @param indexConditions the index conditions
     */
    public void find(Session session, ObjectArray<IndexCondition> indexConditions) throws SQLException {
        this.session = session;
        alwaysFalse = false;
        start = end = null;
        inList = null;
        inResult = null;
        inResultTested = new HashSet<Value>();
        for (IndexCondition condition : indexConditions) {
            if (condition.isAlwaysFalse()) {
                alwaysFalse = true;
                break;
            }
            Column column = condition.getColumn();
            int type = column.getType();
            int id = column.getColumnId();
            if (condition.getCompareType() == Comparison.IN_LIST) {
                this.inColumn = column;
                inList = condition.getCurrentValueList(session);
                inListIndex = 0;
                return;
            } else if (condition.getCompareType() == Comparison.IN_QUERY) {
                this.inColumn = column;
                inResult = condition.getCurrentResult(session);
                return;
            } else {
                Value v = condition.getCurrentValue(session).convertTo(type);
                boolean isStart = condition.isStart();
                boolean isEnd = condition.isEnd();
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
            }
        }
        if (!alwaysFalse) {
            cursor = index.find(session, start, end);
        }
    }

    private SearchRow getSearchRow(SearchRow row, int id, Value v, boolean max) throws SQLException {
        if (row == null) {
            row = table.getTemplateRow();
        } else {
            v = getMax(row.getValue(id), v, max);
        }
        row.setValue(id, v);
        return row;
    }

    private Value getMax(Value a, Value b, boolean bigger) throws SQLException {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        }
        int comp = a.compareTo(b, table.getDatabase().getCompareMode());
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

    public Row get() throws SQLException {
        return cursor.get();
    }

    public int getPos() {
        return cursor.getPos();
    }

    public SearchRow getSearchRow() throws SQLException {
        return cursor.getSearchRow();
    }

    public boolean next() throws SQLException {
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

    private void nextCursor() throws SQLException {
        if (inList != null) {
            if (inListIndex < inList.length) {
                Value v = inList[inListIndex++];
                find(v);
            }
        } else if (inResult != null) {
            while (inResult.next()) {
                Value v = inResult.currentRow()[0];
                v = v.convertTo(inColumn.getType());
                if (inResultTested.add(v)) {
                    find(v);
                    break;
                }
            }
        }
    }

    private void find(Value v) throws SQLException {
        v = v.convertTo(inColumn.getType());
        int id = inColumn.getColumnId();
        if (start == null) {
            start = table.getTemplateRow();
        }
        start.setValue(id, v);
        cursor = index.find(session, start, start);
    }

    public boolean previous() {
        throw Message.throwInternalError();
    }

}
