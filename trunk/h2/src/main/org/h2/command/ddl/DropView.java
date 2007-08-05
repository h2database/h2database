/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.Table;

public class DropView extends SchemaCommand {

    private String viewName;
    private boolean ifExists;

    public DropView(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public int update() throws SQLException {
        // TODO rights: what rights are required to drop a view?
        session.commit(true);
        Table view = getSchema().findTableOrView(session, viewName);
        if(view == null) {
            if(!ifExists) {
                throw Message.getSQLException(Message.VIEW_NOT_FOUND_1, viewName);
            }
        } else {
            if(!view.getTableType().equals(Table.VIEW)) {
                throw Message.getSQLException(Message.VIEW_NOT_FOUND_1, viewName);
            }
            session.getUser().checkRight(view, Right.ALL);
            view.lock(session, true);
            session.getDatabase().removeSchemaObject(session, view);
        }
        return 0;
    }

}
