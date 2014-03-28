/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.SchemaObjectBase;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.MathUtils;
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
     * @param newTable the table
     * @param id the object id
     * @param name the index name
     * @param newIndexColumns the columns that are indexed or null if this is
     *            not yet known
     * @param newIndexType the index type
     */
    protected void initBaseIndex(Table newTable, int id, String name,
            IndexColumn[] newIndexColumns, IndexType newIndexType) {
        initSchemaObjectBase(newTable.getSchema(), id, name, Trace.INDEX);
        this.indexType = newIndexType;
        this.table = newTable;
        if (newIndexColumns != null) {
            this.indexColumns = newIndexColumns;
            columns = new Column[newIndexColumns.length];
            int len = columns.length;
            columnIds = new int[len];
            for (int i = 0; i < len; i++) {
                Column col = newIndexColumns[i].column;
                columns[i] = col;
                columnIds[i] = col.getColumnId();
            }
        }
    }

    /**
     * Check that the index columns are not CLOB or BLOB.
     *
     * @param columns the columns
     */
    protected static void checkIndexColumnTypes(IndexColumn[] columns) {
        for (IndexColumn c : columns) {
            int type = c.column.getType();
            if (type == Value.CLOB || type == Value.BLOB) {
                throw DbException.getUnsupportedException(
                        "Index on BLOB or CLOB column: " + c.column.getCreateSQL());
            }
        }
    }

    @Override
    public String getDropSQL() {
        return null;
    }

    /**
     * Create a duplicate key exception with a message that contains the index
     * name.
     *
     * @param key the key values
     * @return the exception
     */
    protected DbException getDuplicateKeyException(String key) {
        String sql = getName() + " ON " + table.getSQL() +
                "(" + getColumnListSQL() + ")";
        if (key != null) {
            sql += " VALUES " + key;
        }
        DbException e = DbException.get(ErrorCode.DUPLICATE_KEY_1, sql);
        e.setSource(this);
        return e;
    }

    @Override
    public String getPlanSQL() {
        return getSQL();
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        table.removeIndex(this);
        remove(session);
        database.removeMeta(session, getId());
    }

    @Override
    public boolean canFindNext() {
        return false;
    }


    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return find(filter.getSession(), first, last);
    }

    /**
     * Find a row or a list of rows that is larger and create a cursor to
     * iterate over the result. The base implementation doesn't support this
     * feature.
     *
     * @param session the session
     * @param higherThan the lower limit (excluding)
     * @param last the last row, or null for no limit
     * @return the cursor
     * @throws DbException always
     */
    @Override
    public Cursor findNext(Session session, SearchRow higherThan, SearchRow last) {
        throw DbException.throwInternalError();
    }

    /**
     * Calculate the cost for the given mask as if this index was a typical
     * b-tree range index. This is the estimated cost required to search one
     * row, and then iterate over the given number of rows.
     *
     * @param masks the search mask
     * @param rowCount the number of rows in the index
     * @param filter the table filter
     * @param sortOrder the sort order
     * @return the estimated cost
     */
    protected long getCostRangeIndex(int[] masks, long rowCount,
            TableFilter filter, SortOrder sortOrder) {
        rowCount += Constants.COST_ROW_OFFSET;
        long cost = rowCount;
        long rows = rowCount;
        int totalSelectivity = 0;
        if (masks == null) {
            return cost;
        }
        for (int i = 0, len = columns.length; i < len; i++) {
            Column column = columns[i];
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                if (i == columns.length - 1 && getIndexType().isUnique()) {
                    cost = 3;
                    break;
                }
                totalSelectivity = 100 - ((100 - totalSelectivity) *
                        (100 - column.getSelectivity()) / 100);
                long distinctRows = rowCount * totalSelectivity / 100;
                if (distinctRows <= 0) {
                    distinctRows = 1;
                }
                rows = Math.max(rowCount / distinctRows, 1);
                cost = 2 + rows;
            } else if ((mask & IndexCondition.RANGE) == IndexCondition.RANGE) {
                cost = 2 + rows / 4;
                break;
            } else if ((mask & IndexCondition.START) == IndexCondition.START) {
                cost = 2 + rows / 3;
                break;
            } else if ((mask & IndexCondition.END) == IndexCondition.END) {
                cost = rows / 3;
                break;
            } else {
                break;
            }
        }
        // if the ORDER BY clause matches the ordering of this index,
        // it will be cheaper than another index, so adjust the cost accordingly
        if (sortOrder != null) {
            boolean sortOrderMatches = true;
            int coveringCount = 0;
            int[] sortTypes = sortOrder.getSortTypes();
            for (int i = 0, len = sortTypes.length; i < len; i++) {
                if (i >= indexColumns.length) {
                    // we can still use this index if we are sorting by more
                    // than it's columns, it's just that the coveringCount
                    // is lower than with an index that contains
                    // more of the order by columns
                    break;
                }
                Column col = sortOrder.getColumn(i, filter);
                if (col == null) {
                    sortOrderMatches = false;
                    break;
                }
                IndexColumn indexCol = indexColumns[i];
                if (col != indexCol.column) {
                    sortOrderMatches = false;
                    break;
                }
                int sortType = sortTypes[i];
                if (sortType != indexCol.sortType) {
                    sortOrderMatches = false;
                    break;
                }
                coveringCount++;
            }
            if (sortOrderMatches) {
                // "coveringCount" makes sure that when we have two
                // or more covering indexes, we choose the one
                // that covers more
                cost -= coveringCount;
            }
        }
        return cost;
    }

    @Override
    public int compareRows(SearchRow rowData, SearchRow compare) {
        if (rowData == compare) {
            return 0;
        }
        for (int i = 0, len = indexColumns.length; i < len; i++) {
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

    /**
     * Check if one of the columns is NULL and multiple rows with NULL are
     * allowed using the current compatibility mode for unique indexes. Note:
     * NULL behavior is complicated in SQL.
     *
     * @param newRow the row to check
     * @return true if one of the columns is null and multiple nulls in unique
     *         indexes are allowed
     */
    protected boolean containsNullAndAllowMultipleNull(SearchRow newRow) {
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

    /**
     * Compare the positions of two rows.
     *
     * @param rowData the first row
     * @param compare the second row
     * @return 0 if both rows are equal, -1 if the first row is smaller,
     *         otherwise 1
     */
    int compareKeys(SearchRow rowData, SearchRow compare) {
        long k1 = rowData.getKey();
        long k2 = compare.getKey();
        if (k1 == k2) {
            if (isMultiVersion) {
                int v1 = rowData.getVersion();
                int v2 = compare.getVersion();
                return MathUtils.compareInt(v2, v1);
            }
            return 0;
        }
        return k1 > k2 ? 1 : -1;
    }

    private int compareValues(Value a, Value b, int sortType) {
        if (a == b) {
            return 0;
        }
        boolean aNull = a == null, bNull = b == null;
        if (aNull || bNull) {
            return SortOrder.compareNull(aNull, sortType);
        }
        int comp = table.compareTypeSave(a, b);
        if ((sortType & SortOrder.DESCENDING) != 0) {
            comp = -comp;
        }
        return comp;
    }

    @Override
    public int getColumnIndex(Column col) {
        for (int i = 0, len = columns.length; i < len; i++) {
            if (columns[i].equals(col)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Get the list of columns as a string.
     *
     * @return the list of columns
     */
    private String getColumnListSQL() {
        StatementBuilder buff = new StatementBuilder();
        for (IndexColumn c : indexColumns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        return buff.toString();
    }

    @Override
    public String getCreateSQLForCopy(Table targetTable, String quotedName) {
        StringBuilder buff = new StringBuilder("CREATE ");
        buff.append(indexType.getSQL());
        buff.append(' ');
        if (table.isHidden()) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(quotedName);
        buff.append(" ON ").append(targetTable.getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append('(').append(getColumnListSQL()).append(')');
        return buff.toString();
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQLForCopy(table, getSQL());
    }

    @Override
    public IndexColumn[] getIndexColumns() {
        return indexColumns;
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }

    @Override
    public IndexType getIndexType() {
        return indexType;
    }

    @Override
    public int getType() {
        return DbObject.INDEX;
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public void commit(int operation, Row row) {
        // nothing to do
    }

    void setMultiVersion(boolean multiVersion) {
        this.isMultiVersion = multiVersion;
    }

    @Override
    public Row getRow(Session session, long key) {
        throw DbException.getUnsupportedException(toString());
    }

    @Override
    public boolean isHidden() {
        return table.isHidden();
    }

    @Override
    public boolean isRowIdIndex() {
        return false;
    }

    @Override
    public boolean canScan() {
        return true;
    }

    @Override
    public void setSortedInsertMode(boolean sortedInsertMode) {
        // ignore
    }

}
