/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.SchemaObjectBase;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Most index implementations extend the base index.
 */
public abstract class BaseIndex extends SchemaObjectBase implements Index {

    protected IndexColumn[] indexColumns;
    protected Column[] columns;
    protected int[] columnIds;
    protected Table table;
    protected IndexType indexType;
    protected boolean isMultiVersion;

    /**
     * Initialize the base index.
     *
     * @param table the table
     * @param id the object id
     * @param name the index name
     * @param indexColumns the columns that are indexed or null if this is not
     *            yet known
     * @param indexType the index type
     */
    void initBaseIndex(Table table, int id, String name, IndexColumn[] indexColumns, IndexType indexType) {
        initSchemaObjectBase(table.getSchema(), id, name, Trace.INDEX);
        this.indexType = indexType;
        this.table = table;
        if (indexColumns != null) {
            this.indexColumns = indexColumns;
            columns = new Column[indexColumns.length];
            columnIds = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                Column col = indexColumns[i].column;
                columns[i] = col;
                columnIds[i] = col.getColumnId();
            }
        }
    }

    /**
     * Close this index.
     *
     * @param session the session
     */
    public abstract void close(Session session) throws SQLException;

    /**
     * Add a row to this index.
     *
     * @param session the session
     * @param row the row to add
     */
    public abstract void add(Session session, Row row) throws SQLException;

    /**
     * Remove a row from the index.
     *
     * @param session the session
     * @param row the row
     */
    public abstract void remove(Session session, Row row) throws SQLException;

    /**
     * Create a cursor to iterate over a number of rows.
     *
     * @param session the session
     * @param first the first row to return (null if no limit)
     * @param last the last  row to return (null if no limit)
     * @return the cursor to iterate over the results
     */
    public abstract Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException;

    /**
     * Calculate the cost to find rows.
     *
     * @param session the session
     * @param masks the condition mask
     * @return the cost
     */
    public abstract double getCost(Session session, int[] masks) throws SQLException;

    /**
     * Remove the index.
     *
     * @param session the session
     */
    public abstract void remove(Session session) throws SQLException;

    /**
     * Truncate the index.
     *
     * @param session the session
     */
    public abstract void truncate(Session session) throws SQLException;

    /**
     * Check if this index can quickly find the first or last value.
     *
     * @return true if it can
     */
    public abstract boolean canGetFirstOrLast();

    /**
     * Find the first (or last) value of this index. The cursor returned is
     * positioned on the correct row, or on null if no row has been found.
     *
     * @param session the session
     * @param first true for the first value, false for the last
     * @return a cursor (never null)
     */
    public abstract Cursor findFirstOrLast(Session session, boolean first) throws SQLException;

    /**
     * Check if this index needs to be re-built.
     *
     * @return true if it must be re-built.
     */
    public abstract boolean needRebuild();

    public String getDropSQL() {
        return null;
    }

    public SQLException getDuplicateKeyException() {
        String sql = getName() + " ON " + table.getSQL() + "(" + getColumnListSQL() + ")";
        return Message.getSQLException(ErrorCode.DUPLICATE_KEY_1, sql);
    }

    public String getPlanSQL() {
        return getSQL();
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        table.removeIndex(this);
        remove(session);
        database.removeMeta(session, getId());
    }

    public boolean canFindNext() {
        return false;
    }

    /**
     * Find a row or a list of rows that is larger and create a cursor to
     * iterate over the result. The base implementation doesn't support this feature.
     *
     * @param session the session
     * @param higherThan the lower limit (excluding)
     * @param last the last row, or null for no limit
     * @return the cursor
     * @throws SQLException
     */
    public Cursor findNext(Session session, SearchRow higherThan, SearchRow last) throws SQLException {
        throw Message.throwInternalError();
    }

    public int getLookupCost(long rowCount) {
        return 2;
    }

    /**
     * Calculate the cost for the given mask as if this index was a typical
     * b-tree range index.
     *
     * @param masks the search mask
     * @param rowCount the number of rows in the index
     * @return the calculated cost
     */
    public long getCostRangeIndex(int[] masks, long rowCount) {
        rowCount += Constants.COST_ROW_OFFSET;
        long cost = rowCount;
        long rows = rowCount;
        int totalSelectivity = 0;
        for (int i = 0; masks != null && i < columns.length; i++) {
            Column column = columns[i];
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                if (i == columns.length - 1 && getIndexType().isUnique()) {
                    cost = getLookupCost(rowCount) + 1;
                    break;
                }
                totalSelectivity = 100 - ((100 - totalSelectivity) * (100 - column.getSelectivity()) / 100);
                long distinctRows = rowCount * totalSelectivity / 100;
                if (distinctRows <= 0) {
                    distinctRows = 1;
                }
                rows = Math.max(rowCount / distinctRows, 1);
                cost = getLookupCost(rowCount) + rows;
            } else if ((mask & IndexCondition.RANGE) == IndexCondition.RANGE) {
                cost = getLookupCost(rowCount) + rows / 4;
                break;
            } else if ((mask & IndexCondition.START) == IndexCondition.START) {
                cost = getLookupCost(rowCount) + rows / 3;
                break;
            } else if ((mask & IndexCondition.END) == IndexCondition.END) {
                cost = rows / 3;
                break;
            } else {
                break;
            }
        }
        return cost;
    }

    public int compareRows(SearchRow rowData, SearchRow compare) throws SQLException {
        for (int i = 0; i < indexColumns.length; i++) {
            int index = columnIds[i];
            Value v = compare.getValue(index);
            if (v == null) {
                // can't compare further
                return 0;
            }
            int c = compareValues(rowData.getValue(index), v, indexColumns[i].sortType);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    public boolean containsNullAndAllowMultipleNull(SearchRow newRow) {
        Mode mode = database.getMode();
        if (mode.uniqueIndexSingleNull) {
            return false;
        } else if (mode.uniqueIndexSingleNullExceptAllColumnsAreNull) {
            for (int index : columnIds) {
                Value v = newRow.getValue(index);
                if (v != ValueNull.INSTANCE) {
                    return false;
                }
            }
            return true;
        }
        for (int index : columnIds) {
            Value v = newRow.getValue(index);
            if (v == ValueNull.INSTANCE) {
                return true;
            }
        }
        return false;
    }

    public int compareKeys(SearchRow rowData, SearchRow compare) {
        int k1 = rowData.getPos();
        int k2 = compare.getPos();
        if (k1 == k2) {
            if (isMultiVersion) {
                int v1 = rowData.getVersion();
                int v2 = compare.getVersion();
                return v1 == v2 ? 0 : v1 < v2 ? 1 : -1;
            }
            return 0;
        }
        return k1 > k2 ? 1 : -1;
    }

    private int compareValues(Value a, Value b, int sortType) throws SQLException {
        boolean aNull = a == null, bNull = b == null;
        if (aNull || bNull) {
            if (aNull == bNull) {
                return 0;
            }
            return SortOrder.compareNull(aNull, bNull, sortType);
        }
        int comp = table.compareTypeSave(a, b);
        if ((sortType & SortOrder.DESCENDING) != 0) {
            comp = -comp;
        }
        return comp;
    }

    public int getColumnIndex(Column col) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] == col) {
                return i;
            }
        }
        return -1;
    }

    public String getColumnListSQL() {
        StatementBuilder buff = new StatementBuilder();
        for (IndexColumn c : indexColumns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        return buff.toString();
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        StringBuilder buff = new StringBuilder("CREATE ");
        buff.append(indexType.getSQL());
        if (!indexType.isPrimaryKey()) {
            buff.append(' ').append(quotedName);
        }
        buff.append(" ON ").append(table.getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append('(').append(getColumnListSQL()).append(')');
        return buff.toString();
    }

    public String getCreateSQL() {
        return getCreateSQLForCopy(table, getSQL());
    }

    public IndexColumn[] getIndexColumns() {
        return indexColumns;
    }

    public Column[] getColumns() {
        return columns;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public int getType() {
        return DbObject.INDEX;
    }

    public Table getTable() {
        return table;
    }

    public void commit(int operation, Row row) {
        // nothing to do
    }

    void setMultiVersion(boolean multiVersion) {
        this.isMultiVersion = multiVersion;
    }

}
