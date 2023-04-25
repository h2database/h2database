/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.result.SortOrder;
import org.h2.util.HasSQL;
import org.h2.util.ParserUtil;

/**
 * This represents a column item of an index. This is required because some
 * indexes support descending sorted columns.
 */
public class IndexColumn {

    /**
     * Do not append ordering.
     */
    public static final int SQL_NO_ORDER = 0x8000_0000;

    /**
     * The column name.
     */
    public final String columnName;

    /**
     * The column, or null if not set.
     */
    public Column column;

    /**
     * The sort type. Ascending (the default) and descending are supported;
     * nulls can be sorted first or last.
     */
    public int sortType = SortOrder.ASCENDING;

    /**
     * Appends the specified columns to the specified builder.
     *
     * @param builder
     *            string builder
     * @param columns
     *            index columns
     * @param sqlFlags
     *            formatting flags
     * @return the specified string builder
     */
    public static StringBuilder writeColumns(StringBuilder builder, IndexColumn[] columns, int sqlFlags) {
        return writeColumns(builder, columns, 0, columns.length, sqlFlags);
    }

    /**
     * Appends the specified columns to the specified builder.
     *
     * @param builder
     *            string builder
     * @param startOffset
     *            start offset, inclusive
     * @param endOffset
     *            end offset, exclusive
     * @param columns
     *            index columns
     * @param sqlFlags
     *            formatting flags
     * @return the specified string builder
     */
    public static StringBuilder writeColumns(StringBuilder builder, IndexColumn[] columns, int startOffset,
            int endOffset, int sqlFlags) {
        for (int i = startOffset; i < endOffset; i++) {
            if (i > startOffset) {
                builder.append(", ");
            }
            columns[i].getSQL(builder,  sqlFlags);
        }
        return builder;
    }

    /**
     * Appends the specified columns to the specified builder.
     *
     * @param builder
     *            string builder
     * @param columns
     *            index columns
     * @param separator
     *            separator
     * @param suffix
     *            additional SQL to append after each column
     * @param sqlFlags
     *            formatting flags
     * @return the specified string builder
     */
    public static StringBuilder writeColumns(StringBuilder builder, IndexColumn[] columns, String separator,
            String suffix, int sqlFlags) {
        for (int i = 0, l = columns.length; i < l; i++) {
            if (i > 0) {
                builder.append(separator);
            }
            columns[i].getSQL(builder, sqlFlags).append(suffix);
        }
        return builder;
    }

    /**
     * Creates a new instance with the specified name.
     *
     * @param columnName
     *            the column name
     */
    public IndexColumn(String columnName) {
        this.columnName = columnName;
    }

    /**
     * Creates a new instance with the specified name.
     *
     * @param columnName
     *            the column name
     * @param sortType
     *            the sort type
     */
    public IndexColumn(String columnName, int sortType) {
        this.columnName = columnName;
        this.sortType = sortType;
    }

    /**
     * Creates a new instance with the specified column.
     *
     * @param column
     *            the column
     */
    public IndexColumn(Column column) {
        columnName = null;
        this.column = column;
    }

    /**
     * Appends the SQL snippet for this index column to the specified string builder.
     *
     * @param builder
     *            string builder
     * @param sqlFlags
     *            formatting flags
     * @return the specified string builder
     */
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if (column != null) {
            column.getSQL(builder, sqlFlags);
        } else {
            ParserUtil.quoteIdentifier(builder, columnName, sqlFlags);
        }
        if ((sqlFlags & SQL_NO_ORDER) == 0) {
            SortOrder.typeToString(builder, sortType);
        }
        return builder;
    }

    /**
     * Create an array of index columns from a list of columns. The default sort
     * type is used.
     *
     * @param columns the column list
     * @return the index column array
     */
    public static IndexColumn[] wrap(Column[] columns) {
        IndexColumn[] list = new IndexColumn[columns.length];
        for (int i = 0; i < list.length; i++) {
            list[i] = new IndexColumn(columns[i]);
        }
        return list;
    }

    /**
     * Map the columns using the column names and the specified table.
     *
     * @param indexColumns the column list with column names set
     * @param table the table from where to map the column names to columns
     */
    public static void mapColumns(IndexColumn[] indexColumns, Table table) {
        for (IndexColumn col : indexColumns) {
            col.column = table.getColumn(col.columnName);
        }
    }

    @Override
    public String toString() {
        return getSQL(new StringBuilder("IndexColumn "), HasSQL.TRACE_SQL_FLAGS).toString();
    }
}
