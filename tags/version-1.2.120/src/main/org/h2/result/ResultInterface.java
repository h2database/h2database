/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.SQLException;

import org.h2.value.Value;

/**
 * The result interface is used by the LocalResult and ResultRemote class.
 * A result may contain rows, or just an update count.
 */
public interface ResultInterface {

    /**
     * Go to the beginning of the result, that means
     * before the first row.
     */
    void reset() throws SQLException;

    /**
     * Get the current row.
     *
     * @return the row
     */
    Value[] currentRow();

    /**
     * Go to the next row.
     *
     * @return true if a row exists
     */
    boolean next() throws SQLException;

    /**
     * Get the current row id, starting with 0.
     * -1 is returned when next() was not called yet.
     *
     * @return the row id
     */
    int getRowId();

    /**
     * Get the number of visible columns.
     * More columns may exist internally for sorting or grouping.
     *
     * @return the number of columns
     */
    int getVisibleColumnCount();

    /**
     * Get the number of rows in this object.
     *
     * @return the number of rows
     */
    int getRowCount();

    /**
     * Close the result and delete any temporary files
     */
    void close();

    /**
     * Get the column alias name for the column.
     *
     * @param i the column number (starting with 0)
     * @return the alias name
     */
    String getAlias(int i);

    /**
     * Get the schema name for the column, if one exists.
     *
     * @param i the column number (starting with 0)
     * @return the schema name or null
     */
    String getSchemaName(int i);

    /**
     * Get the table name for the column, if one exists.
     *
     * @param i the column number (starting with 0)
     * @return the table name or null
     */
    String getTableName(int i);

    /**
     * Get the column name.
     *
     * @param i the column number (starting with 0)
     * @return the column name
     */
    String getColumnName(int i);

    /**
     * Get the column data type.
     *
     * @param i the column number (starting with 0)
     * @return the column data type
     */
    int getColumnType(int i);

    /**
     * Get the precision for this column.
     *
     * @param i the column number (starting with 0)
     * @return the precision
     */
    long getColumnPrecision(int i);

    /**
     * Get the scale for this column.
     *
     * @param i the column number (starting with 0)
     * @return the scale
     */
    int getColumnScale(int i);

    /**
     * Get the display size for this column.
     *
     * @param i the column number (starting with 0)
     * @return the display size
     */
    int getDisplaySize(int i);

    /**
     * Check if this is an auto-increment column.
     *
     * @param i the column number (starting with 0)
     * @return true for auto-increment columns
     */
    boolean isAutoIncrement(int i);

    /**
     * Check if this column is nullable.
     *
     * @param i the column number (starting with 0)
     * @return Column.NULLABLE_*
     */
    int getNullable(int i);

    /**
     * Set the fetch size for this result set.
     *
     * @param fetchSize the new fetch size
     */
    void setFetchSize(int fetchSize);

    /**
     * Get the current fetch size for this result set.
     *
     * @return the fetch size
     */
    int getFetchSize();

}
