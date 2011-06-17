/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.command.dml.Query;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.TableView;

/**
 * This class represents the statement
 * CREATE VIEW
 */
public class CreateView extends SchemaCommand {

    private Query select;
    private String viewName;
    private boolean ifNotExists;
    private String selectSQL;
    private String[] columnNames;
    private String comment;
    private boolean recursive;

    public CreateView(Session session, Schema schema) {
        super(session, schema);
    }

    public void setViewName(String name) {
        viewName = name;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }

    public void setSelect(Query select) {
        this.select = select;
    }

    public int update() throws SQLException {
        session.commit(true);
        Database db = session.getDatabase();
        if (getSchema().findTableOrView(session, viewName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw Message.getSQLException(ErrorCode.VIEW_ALREADY_EXISTS_1, viewName);
        }
        int id = getObjectId(true, true);
        String querySQL;
        if (select == null) {
            querySQL = selectSQL;
        } else {
            querySQL = select.getSQL();
        }
        Session sysSession = db.getSystemSession();
        TableView view;
        try {
            Schema schema = session.getDatabase().getSchema(session.getCurrentSchemaName());
            sysSession.setCurrentSchema(schema);
            view = new TableView(getSchema(), id, viewName, querySQL, null, columnNames, sysSession, recursive);
        } finally {
            sysSession.setCurrentSchema(db.getSchema(Constants.SCHEMA_MAIN));
        }
        view.setComment(comment);
        try {
            view.recompileQuery(session);
        } catch (SQLException e) {
            // this is not strictly required - ignore exceptions, specially when using FORCE
        }
        db.addSchemaObject(session, view);
        return 0;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setSelectSQL(String selectSQL) {
        this.selectSQL = selectSQL;
    }

    public void setColumnNames(String[] cols) {
        this.columnNames = cols;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

}
