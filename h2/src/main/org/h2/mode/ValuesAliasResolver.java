/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import org.h2.command.dml.Insert;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.value.Value;

/**
 * Column resolver for INSERT ... VALUES ... AS alias syntax.
 * This allows referencing inserted values using alias.column_name
 * in ON DUPLICATE KEY UPDATE clauses.
 */
public final class ValuesAliasResolver implements ColumnResolver {

    private final String alias;
    private final Column[] columns;
    private final Insert insertCommand;

    /**
     * Creates a new VALUES alias resolver.
     *
     * @param alias the alias name
     * @param columns the table columns
     * @param insertCommand the INSERT command
     */
    public ValuesAliasResolver(String alias, Column[] columns, Insert insertCommand) {
        this.alias = alias;
        this.columns = columns;
        this.insertCommand = insertCommand;
    }

    @Override
    public String getTableAlias() {
        return alias;
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }

    @Override
    public Column findColumn(String name) {
        for (Column column : columns) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column;
            }
        }
        return null;
    }

    @Override
    public Value getValue(Column column) {
        if (insertCommand == null) {
            return null;
        }
        return insertCommand.getOnDuplicateKeyValue(column.getColumnId());
    }

    @Override
    public String getSchemaName() {
        return null;
    }

    /**
     * Get the alias name.
     *
     * @return the alias name
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Get the INSERT command.
     *
     * @return the INSERT command
     */
    public Insert getInsertCommand() {
        return insertCommand;
    }

}
