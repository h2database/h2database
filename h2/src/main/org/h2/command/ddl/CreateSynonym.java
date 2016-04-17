/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.TableSynonym;
import org.h2.table.Table;
import org.h2.util.New;

import java.util.HashSet;

/**
 * This class represents the statement
 * CREATE SYNONYM
 */
public class CreateSynonym extends SchemaCommand {

    private final CreateSynonymData data = new CreateSynonymData();
    private boolean ifNotExists;
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

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        if (!transactional) {
            session.commit(true);
        }
        Database db = session.getDatabase();

        // TODO: Check when/if meta data is unlocked...
        db.lockMeta(session);

        if (getSchema().findTableOrView(session, data.synonymName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, data.synonymName);
        }

        data.id = getObjectId();
        data.session = session;
        TableSynonym table = getSchema().createSynonym(data);
        table.setComment(comment);

        db.addSchemaObject(session, table);

        try {
            HashSet<DbObject> set = New.hashSet();
            set.clear();
            table.addDependencies(set);
            for (DbObject obj : set) {
                if (obj == table) {
                    continue;
                }
                if (obj.getType() == DbObject.TABLE_OR_VIEW) {
                    if (obj instanceof Table) {
                        Table t = (Table) obj;
                        if (t.getId() > table.getId()) {
                            throw DbException.get(
                                    ErrorCode.FEATURE_NOT_SUPPORTED_1,
                                    "TableSynonym depends on another table " +
                                    "with a higher ID: " + t +
                                    ", this is currently not supported, " +
                                    "as it would prevent the database from " +
                                    "being re-opened");
                        }
                    }
                }
            }
        } catch (DbException e) {
            db.checkPowerOff();
            db.removeSchemaObject(session, table);
            if (!transactional) {
                session.commit(true);
            }
            throw e;
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
