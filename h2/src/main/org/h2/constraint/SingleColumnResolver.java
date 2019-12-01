/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constraint;

import org.h2.engine.Database;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * The single column resolver resolves the VALUE column.
 * It is used to parse a domain constraint.
 */
public class SingleColumnResolver implements ColumnResolver {

    private final Database database;
    private final Column column;
    private Value value;
    private boolean renamed;

    public SingleColumnResolver(Database database, TypeInfo typeInfo) {
        this.database = database;
        this.column = new Column("VALUE", typeInfo);
    }

    public void setValue(Value value) {
        this.value = value;
    }

    @Override
    public Value getValue(Column col) {
        return value;
    }

    @Override
    public Column[] getColumns() {
        return new Column[] { column };
    }

    @Override
    public Column findColumn(String name) {
        if (database.equalsIdentifiers("VALUE", name)) {
            return column;
        }
        return null;
    }

    void setColumnName(String newName) {
        column.rename(newName);
        renamed = true;
    }

    void resetColumnName() {
        column.rename("VALUE");
        renamed = false;
    }

    /**
     * Return whether column name should be used. If not, a VALUE without quotes
     * should be used unconditionally.
     *
     * @return whether column name should be used
     */
    public boolean isRenamed() {
        return renamed;
    }

}
