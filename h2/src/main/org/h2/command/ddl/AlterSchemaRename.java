/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;

/**
 * This class represents the statement
 * ALTER SCHEMA RENAME
 */
public class AlterSchemaRename extends DefineCommand {

    private Schema oldSchema;
    private String newSchemaName;

    public AlterSchemaRename(SessionLocal session) {
        super(session);
    }

    public void setOldSchema(Schema schema) {
        oldSchema = schema;
    }

    public void setNewName(String name) {
        newSchemaName = name;
    }

    @Override
    public long update() {
        session.getUser().checkSchemaAdmin();
        Database db = getDatabase();
        if (!oldSchema.canDrop()) {
            throw DbException.get(ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1, oldSchema.getName());
        }
        if (db.findSchema(newSchemaName) != null || newSchemaName.equals(oldSchema.getName())) {
            throw DbException.get(ErrorCode.SCHEMA_ALREADY_EXISTS_1, newSchemaName);
        }
        db.renameDatabaseObject(session, oldSchema, newSchemaName);
        ArrayList<SchemaObject> all = new ArrayList<>();
        for (Schema schema : db.getAllSchemas()) {
            schema.getAll(all);
            for (SchemaObject schemaObject : all) {
                db.updateMeta(session, schemaObject);
            }
            all.clear();
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_SCHEMA_RENAME;
    }

}
