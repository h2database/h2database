/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.Table;

/**
 * This class represents the statement
 * DROP VIEW
 */
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
        session.commit(true);
        Table view = getSchema().findTableOrView(session, viewName);
        if (view == null) {
            if (!ifExists) {
                throw Message.getSQLException(ErrorCode.VIEW_NOT_FOUND_1, viewName);
            }
        } else {
            if (!Table.VIEW.equals(view.getTableType())) {
                throw Message.getSQLException(ErrorCode.VIEW_NOT_FOUND_1, viewName);
            }
            session.getUser().checkRight(view, Right.ALL);
            view.lock(session, true, true);
            session.getDatabase().removeSchemaObject(session, view);
        }
        return 0;
    }

}
