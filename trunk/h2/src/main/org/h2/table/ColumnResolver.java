/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;

import org.h2.command.dml.Select;
import org.h2.value.Value;

/**
 * A column resolver is list of column (for example, a table) that can map a
 * column name to an actual column.
 */
public interface ColumnResolver {

    /**
     * Get the table alias.
     *
     * @return the table alias
     */
    String getTableAlias();

    /**
     * Get the column list.
     *
     * @return the column list
     */
    Column[] getColumns();

    /**
     * Get the list of system columns, if any.
     *
     * @return the system columns
     */
    Column[] getSystemColumns();

    /**
     * Get the schema name.
     *
     * @return the schema name
     */
    String getSchemaName();

    /**
     * Get the value for the given column.
     *
     * @param column the column
     * @return the value
     */
    Value getValue(Column column) throws SQLException;

    /**
     * Get the table filter.
     *
     * @return the table filter
     */
    TableFilter getTableFilter();

    /**
     * Get the select statement.
     *
     * @return the select statement
     */
    Select getSelect();

}
