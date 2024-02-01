/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;

/**
 * This class represents the statement
 * ALTER TABLE RENAME
 */
public class AlterTableRename extends AlterTable {

    private String newTableName;

    public AlterTableRename(SessionLocal session, Schema schema) {
        super(session, schema);
    }

    public void setNewTableName(String name) {
        newTableName = name;
    }

    @Override
    public long update(Table table) {
        Database db = getDatabase();
        Table t = getSchema().findTableOrView(session, newTableName);
        if (t != null || newTableName.equals(table.getName())) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, newTableName);
        }
        if (table.isTemporary()) {
            throw DbException.getUnsupportedException("temp table");
        }
        db.renameSchemaObject(session, table, newTableName);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_TABLE_RENAME;
    }

}
