/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.ddl.SchemaCommand;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;

/**
 * This class represents the statement
 * ALTER TABLE SET
 */
public class AlterTableSet extends SchemaCommand {

    private boolean ifTableExists;
    private String tableName;
    private final int type;

    private final boolean value;
    private boolean checkExisting;

    public AlterTableSet(SessionLocal session, Schema schema, int type, boolean value) {
        super(session, schema);
        this.type = type;
        this.value = value;
    }

    public void setCheckExisting(boolean b) {
        this.checkExisting = b;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    public void setIfTableExists(boolean b) {
        this.ifTableExists = b;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public long update() {
        Table table = getSchema().resolveTableOrView(session, tableName);
        if (table == null) {
            if (ifTableExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
        }
        session.getUser().checkTableRight(table, Right.SCHEMA_OWNER);
        table.lock(session, Table.EXCLUSIVE_LOCK);
        switch (type) {
        case CommandInterface.ALTER_TABLE_SET_REFERENTIAL_INTEGRITY:
            table.setCheckForeignKeyConstraints(session, value, value ?
                    checkExisting : false);
            break;
        default:
            throw DbException.getInternalError("type="+type);
        }
        return 0;
    }

    @Override
    public int getType() {
        return type;
    }

}
