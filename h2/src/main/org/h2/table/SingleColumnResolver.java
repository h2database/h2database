/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.engine.Database;
import org.h2.value.Value;

/**
 * The single column resolver is like a table with exactly one row.
 * It is used to parse a simple one-column check constraint.
 */
public class SingleColumnResolver implements ColumnResolver {

    private final Database database;
    private final Column column;
    private Value value;

    SingleColumnResolver(Database database, Column column) {
        this.database = database;
        this.column = column;
    }

    void setValue(Value value) {
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
        if (database.equalsIdentifiers(column.getName(), name)) {
            return column;
        }
        return null;
    }

}
