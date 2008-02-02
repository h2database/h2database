/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.SchemaObject;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;

/**
 * An index. Indexes are used to speed up searching data.
 */
public interface Index extends SchemaObject {

    /**
     * Indicates that there is no head record yet.
     */
    int EMPTY_HEAD = -1;

    /**
     * Create a duplicate key exception with a message that contains the index name
     *
     * @return the exception
     */
    SQLException getDuplicateKeyException();

    /**
     * Get the message to show in a EXPLAIN statement.
     *
     * @return the plan
     */
    String getPlanSQL();

    /**
     * Close this index.
     *
     * @param session the session used to write data
     */
    void close(Session session) throws SQLException;

    /**
     * Add a row to the index.
     *
     * @param session the session to use
     * @param row the data
     */
    void add(Session session, Row row) throws SQLException;

    /**
     * Remove a row from the index.
     *
     * @param session the session
     * @param row the data
     */
    void remove(Session session, Row row) throws SQLException;

    /**
     * Find a row or a list of rows and create a cursor to iterate over the result.
     *
     * @param session the session
     * @param first the first row, or null for no limit
     * @param last the last row, or null for no limit
     * @return the cursor
     */
    Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException;

    /**
     * Estimate the cost to search for rows given the search mask.
     *
     * @param session the session
     * @param masks the search mask
     * @return the estimated cost
     */
    double getCost(Session session, int[] masks) throws SQLException;

    /**
     * Remove the index.
     *
     * @param session the session
     */
    void remove(Session session) throws SQLException;

    /**
     * Remove all rows from the index.
     *
     * @param session the session
     */
    void truncate(Session session) throws SQLException;

    /**
     * Check if the index can directly look up the lowest or highest value of a column.
     *
     * @return true if it can
     */
    boolean canGetFirstOrLast();

    /**
     * Check if the index can get the next higher value.
     *
     * @return true if it can
     */
    boolean canFindNext();

    /**
     * Find a row or a list of rows that is larger and create a cursor to iterate over the result.
     *
     * @param session the session
     * @param higherThan the lower limit (excluding)
     * @param last the last row, or null for no limit
     * @return the cursor
     */

    Cursor findNext(Session session, SearchRow higherThan, SearchRow last) throws SQLException;

    /**
     * Find the lowest or highest value of a column.
     *
     * @param session the session
     * @param first true if the first (lowest for ascending indexes) or last value should be returned
     * @return the search row with the value
     */
    SearchRow findFirstOrLast(Session session, boolean first) throws SQLException;

    /**
     * Check if the index needs to be rebuilt.
     * This method is called after opening an index.
     *
     * @return true if a rebuild is required.
     */
    boolean needRebuild();

    /**
     * Get the row count of this table, for the given session.
     *
     * @param session the session
     * @return the row count
     */
    long getRowCount(Session session);

    /**
     * Estimate the cost required to search a number of rows.
     *
     * @param rowCount the row count
     * @return the estimated cost
     */
    int getLookupCost(long rowCount);

    /**
     * Estimate the cost required to search one row, and then iterate over the given number of rows.
     *
     * @param masks the search mask
     * @param rowCount the row count
     * @return the estimated cost
     */
    long getCostRangeIndex(int[] masks, long rowCount) throws SQLException;

    /**
     * Compare two rows.
     *
     * @param rowData the first row
     * @param compare the second row
     * @return 0 if both rows are equal, -1 if the first row is smaller, otherwise 1
     */
    int compareRows(SearchRow rowData, SearchRow compare) throws SQLException;

    /**
     * Check if a row is NULL.
     *
     * @param newRow
     * @return if it is null
     */
    boolean isNull(Row newRow);

    /**
     * Compare the positions of two rows.
     *
     * @param rowData the first row
     * @param compare the second row
     * @return 0 if both rows are equal, -1 if the first row is smaller, otherwise 1
     */
    int compareKeys(SearchRow rowData, SearchRow compare);

    /**
     * Get the index of a column in the list of index columns
     *
     * @param col the column
     * @return the index (0 meaning first column)
     */
    int getColumnIndex(Column col);

    /**
     * Get the list of columns as a string.
     *
     * @return the list of columns
     */
    String getColumnListSQL();

    /**
     * Get the indexed columns as index columns (with ordering information).
     *
     * @return the index columns
     */
    IndexColumn[] getIndexColumns();

    /**
     * Get the indexed columns.
     *
     * @return the columns
     */
    Column[] getColumns();

    /**
     * Get the index type.
     *
     * @return the index type
     */
    IndexType getIndexType();

    /**
     * Get the table on which this index is based.
     *
     * @return the table
     */
    Table getTable();

    /**
     * Commit the operation for a row. This is only important for multi-version indexes.
     *
     * @param operation the operation type
     * @param row the row
     */
    void commit(int operation, Row row) throws SQLException;

}
