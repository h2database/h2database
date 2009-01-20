/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

//## Java 1.5 begin ##
import org.h2.jaqu.TableDefinition.FieldDefinition;
//## Java 1.5 end ##

/**
 * This class represents a column of a table in a query.
 *
 * @param <X> the column data type
 */
//## Java 1.5 begin ##
class SelectColumn<X> {
    private SelectTable selectTable;
    private FieldDefinition<X> fieldDef;

    SelectColumn(SelectTable table, FieldDefinition<X> fieldDef) {
        this.selectTable = table;
        this.fieldDef = fieldDef;
    }

    void appendSQL(SqlStatement stat) {
        if (selectTable.getQuery().isJoin()) {
            stat.appendSQL(selectTable.getAs() + "." + fieldDef.columnName);
        } else {
            stat.appendSQL(fieldDef.columnName);
        }
    }

    FieldDefinition<X> getFieldDefinition() {
        return fieldDef;
    }

    SelectTable getSelectTable() {
        return selectTable;
    }

    Object getCurrentValue() {
        return fieldDef.getValue(selectTable.getCurrent());
    }
}
//## Java 1.5 end ##
