/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.command.CommandInterface;
import org.h2.engine.SessionLocal;
import org.h2.schema.Schema;
import org.h2.table.MaterializedView;

/**
 * This class represents the statement REFRESH MATERIALIZED VIEW
 */
public class RefreshMaterializedView extends SchemaOwnerCommand {

    private MaterializedView view;

    public RefreshMaterializedView(SessionLocal session, Schema schema) {
        super(session, schema);
    }

    public void setView(MaterializedView view) {
        this.view = view;
    }

    @Override
    long update(Schema schema) {
        // Re-use logic from the existing code for TRUNCATE and CREATE TABLE

        TruncateTable truncate = new TruncateTable(session);
        truncate.setTable(view.getUnderlyingTable());
        truncate.update();

        CreateTable createTable = new CreateTable(session, schema);
        createTable.setQuery(view.getSelect());
        createTable.insertAsData(view.getUnderlyingTable());
        view.setModified();
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.REFRESH_MATERIALIZED_VIEW;
    }

}
