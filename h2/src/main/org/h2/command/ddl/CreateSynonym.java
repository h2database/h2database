/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.TableSynonym;
import org.h2.table.Table;

/**
 * This class represents the statement
 * CREATE SYNONYM
 */
public class CreateSynonym extends SchemaCommand {

    private final CreateSynonymData data = new CreateSynonymData();
    private boolean ifNotExists;
    private boolean orReplace;
    private String comment;

    public CreateSynonym(Session session, Schema schema) {
        super(session, schema);
    }

    public void setName(String name) {
        data.synonymName = name;
    }

    public void setSynonymFor(String tableName) {
        data.synonymFor = tableName;
    }

    public void setSynonymForSchema(Schema synonymForSchema) {
        data.synonymForSchema = synonymForSchema;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setOrReplace(boolean orReplace) { this.orReplace = orReplace; }

    @Override
    public int update() {
        if (!transactional) {
            session.commit(true);
        }
        Database db = session.getDatabase();
        data.session = session;
        db.lockMeta(session);

        if (data.synonymForSchema.findTableOrView(session, data.synonymFor) != null) {
            return createTableSynonym(db);
        } else if (data.synonymForSchema.findFunction(data.synonymFor) != null) {
            return createFunctionSynonym(db);
        } else if (data.synonymForSchema.findSequence(data.synonymFor) != null) {
            // TODO Implement sequence synonym
        }

        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1,
                data.synonymForSchema.getName() + "." + data.synonymFor);

    }

    private int createFunctionSynonym(Database db) {
        FunctionAlias old = getSchema().findFunction(data.synonymName);
//        if (old != null) {
//            if (orReplace && old instanceof TableSynonym) {
//                // ok, we replacing the existing synonym
//            } else if (ifNotExists && old instanceof TableSynonym) {
//                return 0;
//            } else {
//                throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, data.synonymName);
//            }
//        }
//
//        TableSynonym table;
//        if (old != null) {
//            table = (TableSynonym) old;
//            data.schema = table.getSchema();
//            table.updateData(data);
//            table.setModified();
//        } else {
//            data.id = getObjectId();
//            table = getSchema().createSynonym(data);
//            table.setComment(comment);
//            db.addSchemaObject(session, table);
//        }

        return 0;
    }

    private int createTableSynonym(Database db) {
        Table old = getSchema().findTableOrView(session, data.synonymName);
        if (old != null) {
            if (orReplace && old instanceof TableSynonym) {
                // ok, we replacing the existing synonym
            } else if (ifNotExists && old instanceof TableSynonym) {
                return 0;
            } else {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, data.synonymName);
            }
        }

        TableSynonym table;
        if (old != null) {
            table = (TableSynonym) old;
            data.schema = table.getSchema();
            table.updateData(data);
            table.setModified();
        } else {
            data.id = getObjectId();
            table = getSchema().createSynonym(data);
            table.setComment(comment);
            db.addSchemaObject(session, table);
        }

        return 0;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_SYNONYM;
    }


}
