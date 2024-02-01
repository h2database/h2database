/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.query.Query;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.MaterializedView;
import org.h2.table.Table;
import org.h2.table.TableType;

/**
 * This class represents the statement CREATE MATERIALIZED VIEW
 */
public class CreateMaterializedView extends SchemaOwnerCommand {

    /** Re-use the CREATE TABLE functionality to avoid duplicating a bunch of logic */
    private final CreateTable createTable;
    private boolean orReplace;
    private boolean ifNotExists;
    private String viewName;
    private String comment;
    private Query select;
    private String selectSQL;

    public CreateMaterializedView(SessionLocal session, Schema schema) {
        super(session, schema);
        createTable = new CreateTable(session, schema);
    }

    public void setViewName(String name) {
        this.viewName = name;
        this.createTable.setTableName(name + "$1");
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setSelectSQL(String selectSQL) {
        this.selectSQL = selectSQL;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
        this.createTable.setIfNotExists(ifNotExists);
    }

    public void setSelect(Query query) {
        this.select = query;
        this.createTable.setQuery(query);
    }

    public void setOrReplace(boolean orReplace) {
        this.orReplace = orReplace;
    }

    @Override
    long update(Schema schema) {
        final Database db = getDatabase();
        final Table old = schema.findTableOrView(session, viewName);
        MaterializedView view = null;
        if (old != null) {
            if (ifNotExists) {
                return 0;
            }
            if (!orReplace || TableType.MATERIALIZED_VIEW != old.getTableType()) {
                throw DbException.get(ErrorCode.VIEW_ALREADY_EXISTS_1, viewName);
            }
            view = (MaterializedView) old;
        }
        final int id = getObjectId();
        // Re-use the CREATE TABLE functionality to avoid duplicating a bunch of logic.
        createTable.update();
        // Look up the freshly created table.
        final Table underlyingTable = schema.getTableOrView(session, viewName + "$1");
        if (view == null) {
            view = new MaterializedView(schema, id, viewName, underlyingTable, select, selectSQL);
        } else {
            view.replace(underlyingTable, select, selectSQL);
            view.setModified();
        }
        if (comment != null) {
            view.setComment(comment);
        }
        for (Table table : select.getTables()) {
            table.addDependentMaterializedView(view);
        }
        if (old == null) {
            db.addSchemaObject(session, view);
            db.unlockMeta(session);
        } else {
            db.updateMeta(session, view);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_MATERIALIZED_VIEW;
    }

}
