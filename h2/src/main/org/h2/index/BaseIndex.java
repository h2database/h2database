/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.RowFactory;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.SchemaObjectBase;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Most index implementations extend the base index.
 */
public abstract class BaseIndex extends SchemaObjectBase implements Index {

    protected IndexColumn[] indexColumns;
    protected Column[] columns;
    protected int[] columnIds;
    protected final Table table;
    protected final IndexType indexType;
    private final RowFactory rowFactory;

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
    protected BaseIndex(Table newTable, int id, String name,
            IndexColumn[] newIndexColumns, IndexType newIndexType) {
        super(newTable.getSchema(), id, name, Trace.INDEX);
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
        rowFactory = database.getRowFactory().createRowFactory(
                database, database.getCompareMode(), database.getMode(),
                database, table.getColumns(),
                newIndexType.isScan() ? null : newIndexColumns);
    }

    @Override
    public RowFactory getRowFactory() {
        return rowFactory;
    }

    /**
     * Check that the index columns are not CLOB or BLOB.
     *
     * @param columns the columns
     */
    protected static void checkIndexColumnTypes(IndexColumn[] columns) {
        for (IndexColumn c : columns) {
            if (!DataType.isIndexable(c.column.getType())) {
                throw DbException.getUnsupportedException(
                        "Index on column: " + c.column.getCreateSQL());
            }
        }
    }

    /**
     * Create a duplicate key exception with a message that contains the index
     * name.
     *
     * @param key the key values
     * @return the exception
     */
    public DbException getDuplicateKeyException(String key) {
        StringBuilder builder = new StringBuilder();
        getSQL(builder, TRACE_SQL_FLAGS).append(" ON ");
        table.getSQL(builder, TRACE_SQL_FLAGS).append('(');
        builder.append(getColumnListSQL(TRACE_SQL_FLAGS));
        builder.append(')');
        if (key != null) {
            builder.append(" VALUES ").append(key);
        }
        DbException e = DbException.get(ErrorCode.DUPLICATE_KEY_1, builder.toString());
        e.setSource(this);
        return e;
    }

    /**
     * Get "PRIMARY KEY ON <table> [(column)]".
     *
     * @param mainIndexColumn the column index
     * @return the message
     */
    protected StringBuilder getDuplicatePrimaryKeyMessage(int mainIndexColumn) {
        StringBuilder builder = new StringBuilder("PRIMARY KEY ON ");
        table.getSQL(builder, TRACE_SQL_FLAGS);
        if (mainIndexColumn >= 0 && mainIndexColumn < indexColumns.length) {
            builder.append('(');
            indexColumns[mainIndexColumn].getSQL(builder, TRACE_SQL_FLAGS).append(')');
        }
        return builder;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        table.removeIndex(this);
        remove(session);
        database.removeMeta(session, getId());
    }

    /**
     * Calculate the cost for the given mask as if this index was a typical
     * b-tree range index. This is the estimated cost required to search one
     * row, and then iterate over the given number of rows.
     *
     * @param masks the IndexCondition search masks, one for each column in the
     *            table
     * @param rowCount the number of rows in the index
     * @param filters all joined table filters
     * @param filter the current table filter index
     * @param sortOrder the sort order
     * @param isScanIndex whether this is a "table scan" index
     * @param allColumnsSet the set of all columns
     * @return the estimated cost
     */
    protected final long getCostRangeIndex(int[] masks, long rowCount,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            boolean isScanIndex, AllColumnsForPlan allColumnsSet) {
        rowCount += Constants.COST_ROW_OFFSET;
        int totalSelectivity = 0;
        long rowsCost = rowCount;
        if (masks != null) {
            int i = 0, len = columns.length;
            boolean tryAdditional = false;
            while (i < len) {
                Column column = columns[i++];
                int index = column.getColumnId();
                int mask = masks[index];
                if ((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                    if (i == len && getIndexType().isUnique()) {
                        rowsCost = 3;
                        break;
                    }
                    totalSelectivity = 100 - ((100 - totalSelectivity) *
                            (100 - column.getSelectivity()) / 100);
                    long distinctRows = rowCount * totalSelectivity / 100;
                    if (distinctRows <= 0) {
                        distinctRows = 1;
                    }
                    rowsCost = 2 + Math.max(rowCount / distinctRows, 1);
                } else if ((mask & IndexCondition.RANGE) == IndexCondition.RANGE) {
                    rowsCost = 2 + rowsCost / 4;
                    tryAdditional = true;
                    break;
                } else if ((mask & IndexCondition.START) == IndexCondition.START) {
                    rowsCost = 2 + rowsCost / 3;
                    tryAdditional = true;
                    break;
                } else if ((mask & IndexCondition.END) == IndexCondition.END) {
                    rowsCost = rowsCost / 3;
                    tryAdditional = true;
                    break;
                } else {
                    if (mask == 0) {
                        // Adjust counter of used columns (i)
                        i--;
                    }
                    break;
                }
            }
            // Some additional columns can still be used
            if (tryAdditional) {
                while (i < len && masks[columns[i].getColumnId()] != 0) {
                    i++;
                    rowsCost--;
                }
            }
            // Increase cost of indexes with additional unused columns
            rowsCost += len - i;
        }
        // If the ORDER BY clause matches the ordering of this index,
        // it will be cheaper than another index, so adjust the cost
        // accordingly.
        long sortingCost = 0;
        if (sortOrder != null) {
            sortingCost = 100 + rowCount / 10;
        }
        if (sortOrder != null && !isScanIndex) {
            boolean sortOrderMatches = true;
            int coveringCount = 0;
            int[] sortTypes = sortOrder.getSortTypes();
            TableFilter tableFilter = filters == null ? null : filters[filter];
            for (int i = 0, len = sortTypes.length; i < len; i++) {
                if (i >= indexColumns.length) {
                    // We can still use this index if we are sorting by more
                    // than it's columns, it's just that the coveringCount
                    // is lower than with an index that contains
                    // more of the order by columns.
                    break;
                }
                Column col = sortOrder.getColumn(i, tableFilter);
                if (col == null) {
                    sortOrderMatches = false;
                    break;
                }
                IndexColumn indexCol = indexColumns[i];
                if (!col.equals(indexCol.column)) {
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
                // that covers more.
                sortingCost = 100 - coveringCount;
            }
        }
        // If we have two indexes with the same cost, and one of the indexes can
        // satisfy the query without needing to read from the primary table
        // (scan index), make that one slightly lower cost.
        boolean needsToReadFromScanIndex = true;
        if (!isScanIndex && allColumnsSet != null) {
            boolean foundAllColumnsWeNeed = true;
            ArrayList<Column> foundCols = allColumnsSet.get(getTable());
            if (foundCols != null) {
                for (Column c : foundCols) {
                    boolean found = false;
                    for (Column c2 : columns) {
                        if (c == c2) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        foundAllColumnsWeNeed = false;
                        break;
                    }
                }
            }
            if (foundAllColumnsWeNeed) {
                needsToReadFromScanIndex = false;
            }
        }
        long rc;
        if (isScanIndex) {
            rc = rowsCost + sortingCost + 20;
        } else if (needsToReadFromScanIndex) {
            rc = rowsCost + rowsCost + sortingCost + 20;
        } else {
            // The (20-x) calculation makes sure that when we pick a covering
            // index, we pick the covering index that has the smallest number of
            // columns (the more columns we have in index - the higher cost).
            // This is faster because a smaller index will fit into fewer data
            // blocks.
            rc = rowsCost + sortingCost + columns.length;
        }
        return rc;
    }

    @Override
    public int compareRows(SearchRow rowData, SearchRow compare) {
        if (rowData == compare) {
            return 0;
        }
        for (int i = 0, len = indexColumns.length; i < len; i++) {
            int index = columnIds[i];
            Value v1 = rowData.getValue(index);
            Value v2 = compare.getValue(index);
            if (v1 == null || v2 == null) {
                // can't compare further
                return 0;
            }
            int c = compareValues(v1, v2, indexColumns[i].sortType);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    /**
     * Check if this row may have duplicates with the same indexed values in the
     * current compatibility mode. Duplicates with {@code NULL} values are
     * allowed in some modes.
     *
     * @param searchRow
     *            the row to check
     * @return {@code true} if specified row may have duplicates,
     *         {@code false otherwise}
     */
    public boolean mayHaveNullDuplicates(SearchRow searchRow) {
        switch (database.getMode().uniqueIndexNullsHandling) {
        case ALLOW_DUPLICATES_WITH_ANY_NULL:
            for (int index : columnIds) {
                if (searchRow.getValue(index) == ValueNull.INSTANCE) {
                    return true;
                }
            }
            return false;
        case ALLOW_DUPLICATES_WITH_ALL_NULLS:
            for (int index : columnIds) {
                if (searchRow.getValue(index) != ValueNull.INSTANCE) {
                    return false;
                }
            }
            return true;
        default:
            return false;
        }
    }

    /**
     * Compare the positions of two rows.
     *
     * @param rowData the first row
     * @param compare the second row
     * @return 0 if both rows are equal, -1 if the first row is smaller,
     *         otherwise 1
     */
    public int compareKeys(SearchRow rowData, SearchRow compare) {
        long k1 = rowData.getKey();
        long k2 = compare.getKey();
        if (k1 == k2) {
            return 0;
        }
        return k1 > k2 ? 1 : -1;
    }

    private int compareValues(Value a, Value b, int sortType) {
        if (a == b) {
            return 0;
        }
        boolean aNull = a == ValueNull.INSTANCE;
        boolean bNull = b == ValueNull.INSTANCE;
        if (aNull || bNull) {
            return SortOrder.compareNull(aNull, sortType);
        }
        int comp = table.compareValues(database, a, b);
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

    @Override
    public boolean isFirstColumn(Column column) {
        return column.equals(columns[0]);
    }

    /**
     * Get the list of columns as a string.
     *
     * @param sqlFlags formatting flags
     * @return the list of columns
     */
    private String getColumnListSQL(int sqlFlags) {
        return IndexColumn.writeColumns(new StringBuilder(), indexColumns, sqlFlags).toString();
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
        buff.append(" ON ");
        targetTable.getSQL(buff, DEFAULT_SQL_FLAGS);
        if (comment != null) {
            buff.append(" COMMENT ");
            StringUtils.quoteStringSQL(buff, comment);
        }
        buff.append('(').append(getColumnListSQL(DEFAULT_SQL_FLAGS)).append(')');
        return buff.toString();
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQLForCopy(table, getSQL(DEFAULT_SQL_FLAGS));
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
    public boolean isHidden() {
        return table.isHidden();
    }
}
