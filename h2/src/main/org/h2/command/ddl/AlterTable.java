/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;

/**
 * The base class for ALTER TABLE commands.
 */
public abstract class AlterTable extends SchemaCommand {

    String tableName;

    boolean ifTableExists;

    AlterTable(SessionLocal session, Schema schema) {
        super(session, schema);
    }

    public final void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public final void setIfTableExists(boolean b) {
        ifTableExists = b;
    }

    @Override
    public final long update() {
        Table table = getSchema().findTableOrView(session, tableName);
        if (table == null) {
            if (ifTableExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
        }
        session.getUser().checkTableRight(table, Right.SCHEMA_OWNER);
        return update(table);
    }

    abstract long update(Table table);

}
