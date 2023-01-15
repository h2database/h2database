/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.command.query.Select;
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
    default String getTableAlias() {
        return null;
    }

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
    default String getColumnName(Column column) {
        return column.getName();
    }

    /**
     * Returns whether this column resolver has a derived column list.
     *
     * @return {@code true} if this column resolver has a derived column list,
     *         {@code false} otherwise
     */
    default boolean hasDerivedColumnList() {
        return false;
    }

    /**
     * Get the list of system columns, if any.
     *
     * @return the system columns or null
     */
    default Column[] getSystemColumns() {
        return null;
    }

    /**
     * Get the row id pseudo column, if there is one.
     *
     * @return the row id column or null
     */
    default Column getRowIdColumn() {
        return null;
    }

    /**
     * Get the schema name or null.
     *
     * @return the schema name or null
     */
    default String getSchemaName() {
        return null;
    }

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
    default TableFilter getTableFilter() {
        return null;
    }

    /**
     * Get the select statement.
     *
     * @return the select statement
     */
    default Select getSelect() {
        return null;
    }

    /**
     * Get the expression that represents this column.
     *
     * @param expressionColumn the expression column
     * @param column the column
     * @return the optimized expression
     */
    default Expression optimize(ExpressionColumn expressionColumn, Column column) {
        return expressionColumn;
    }

}
