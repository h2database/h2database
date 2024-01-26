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
import org.h2.table.MaterializedView;
import org.h2.table.Table;
import org.h2.table.TableType;

/**
 * This class represents the statement DROP MATERIALIZED VIEW
 */
public class DropMaterializedView extends SchemaCommand {

    private String viewName;
    private boolean ifExists;

    public DropMaterializedView(SessionLocal session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    @Override
    public long update() {
        Table view = getSchema().findTableOrView(session, viewName);
        if (view == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.VIEW_NOT_FOUND_1, viewName);
            }
        } else {
            if (TableType.MATERIALIZED_VIEW != view.getTableType()) {
                throw DbException.get(ErrorCode.VIEW_NOT_FOUND_1, viewName);
            }
            session.getUser().checkSchemaOwner(view.getSchema());

            final MaterializedView materializedView = (MaterializedView) view;

            for (Table table : materializedView.getSelect().getTables()) {
                table.removeDependentMaterializedView(materializedView);
            }

            final Database database = getDatabase();
            database.lockMeta(session);
            database.removeSchemaObject(session, view);

            // make sure its all unlocked
            database.unlockMeta(session);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_MATERIALIZED_VIEW;
    }

}
