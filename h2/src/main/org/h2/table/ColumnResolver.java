/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.command.dml.Select;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
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
     * Get the column with the specified name.
     *
     * @param name
     *            the column name, must be a derived name if this column
     *            resolver has a derived column list
     * @return the column with the specified name, or {@code null}
     */
    Column findColumn(String name);

    /**
     * Get the name of the specified column.
     *
     * @param column column
     * @return column name
     */
    String getColumnName(Column column);

    /**
     * Returns whether this column resolver has a derived column list.
     *
     * @return {@code true} if this column resolver has a derived column list,
     *         {@code false} otherwise
     */
    boolean hasDerivedColumnList();

    /**
     * Get the list of system columns, if any.
     *
     * @return the system columns or null
     */
    Column[] getSystemColumns();

    /**
     * Get the row id pseudo column, if there is one.
     *
     * @return the row id column or null
     */
    Column getRowIdColumn();

    /**
     * Get the schema name or null.
     *
     * @return the schema name or null
     */
    String getSchemaName();

    /**
     * Get the value for the given column.
     *
     * @param column the column
     * @return the value
     */
    Value getValue(Column column);

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

    /**
     * Get the expression that represents this column.
     *
     * @param expressionColumn the expression column
     * @param column the column
     * @return the optimized expression
     */
    Expression optimize(ExpressionColumn expressionColumn, Column column);

}
