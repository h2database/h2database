/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.dml.Query;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Parameter;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableType;
import org.h2.table.TableView;
import org.h2.value.Value;

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
    private boolean orReplace;
    private boolean force;

    public CreateView(Session session, Schema schema) {
        super(session, schema);
    }

    public void setViewName(String name) {
        viewName = name;
    }

    public void setSelect(Query select) {
        this.select = select;
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

    public void setOrReplace(boolean orReplace) {
        this.orReplace = orReplace;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    @Override
    public int update() {
        session.commit(true);
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        TableView view = null;
        Table old = getSchema().findTableOrView(session, viewName);
        if (old != null) {
            if (ifNotExists) {
                return 0;
            }
            if (!orReplace || TableType.VIEW != old.getTableType()) {
                throw DbException.get(ErrorCode.VIEW_ALREADY_EXISTS_1, viewName);
            }
            view = (TableView) old;
        }
        int id = getObjectId();
        String querySQL;
        if (select == null) {
            querySQL = selectSQL;
        } else {
            ArrayList<Parameter> params = select.getParameters();
            if (params != null && params.size() > 0) {
                throw DbException.getUnsupportedException("parameters in views");
            }
            querySQL = select.getPlanSQL();
        }
        // The view creates a Prepared command object, which belongs to a
        // session, so we pass the system session down.
        Session sysSession = db.getSystemSession();
        synchronized (sysSession) {
            try {
                if (view == null) {
                    Schema schema = session.getDatabase().getSchema(
                            session.getCurrentSchemaName());
                    sysSession.setCurrentSchema(schema);
                    Column[] columnTemplates = null;
                    if (columnNames != null) {
                        columnTemplates = new Column[columnNames.length];
                        for (int i = 0; i < columnNames.length; ++i) {
                            columnTemplates[i] = new Column(columnNames[i], Value.UNKNOWN);
                        }
                    }
                    view = new TableView(getSchema(), id, viewName, querySQL, null,
                            columnTemplates, sysSession, false);
                } else {
                    view.replace(querySQL, sysSession, false, force);
                    view.setModified();
                }
            } finally {
                sysSession.setCurrentSchema(db.getSchema(Constants.SCHEMA_MAIN));
            }
        }
        if (comment != null) {
            view.setComment(comment);
        }
        if (old == null) {
            db.addSchemaObject(session, view);
        } else {
            db.updateMeta(session, view);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_VIEW;
    }

}
