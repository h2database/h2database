/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.command.Prepared;
import org.h2.command.dml.Query;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.h2.table.TableView;

import java.util.ArrayList;
import java.util.List;

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
    private boolean orReplace;
    private boolean force;

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


    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        Table existingView = getSchema().findTableOrView(session, viewName);

        List<DependentView> dependentViewSql = new ArrayList<DependentView>();

        if (existingView != null) {
            if (ifNotExists) {
                return 0;
            }

            if (orReplace && existingView.getTableType().equals(Table.VIEW)) {
                db.renameSchemaObject(session, existingView, db.getTempTableName(session));
                loadDependentViewSql(existingView, dependentViewSql);
            } else {
                throw DbException.get(ErrorCode.VIEW_ALREADY_EXISTS_1, viewName);
            }
        }
        int id = getObjectId();
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
        } catch (DbException e) {
            // this is not strictly required - ignore exceptions, specially when using FORCE
        }
        db.addSchemaObject(session, view);

        if (existingView != null) {
            recreateDependentViews(db, existingView, dependentViewSql, view);
        }
        return 0;
    }


    private void recreateDependentViews(Database db, Table existingView, List<DependentView> dependentViewSql, TableView view) {
        String failedView = null;
        try {
            // recreate the dependent views
            for (DependentView dependentView : dependentViewSql) {
                failedView = dependentView.viewName;
                if (force) {
                    execute(dependentView.createForceSql, true);
                } else {
                    execute(dependentView.createSql, true);
                }
            }
            // Delete the original view
            db.removeSchemaObject(session, existingView);

        } catch (DbException e) {
            db.removeSchemaObject(session, view);

            // Put back the old view
            db.renameSchemaObject(session, existingView, viewName);

            // Try to put back the dependent views
            for (DependentView dependentView : dependentViewSql) {
                execute(dependentView.createForceSql, true);
            }

            throw DbException.get(ErrorCode.CANNOT_DROP_2, e, existingView.getName(), failedView);
        }
    }

    private void loadDependentViewSql(DbObject tableOrView, List<DependentView> recreate) {
        for (DbObject view : tableOrView.getChildren()) {
            if (view instanceof TableView) {
                recreate.add(new DependentView((TableView) view));
                loadDependentViewSql(view, recreate);
            }
        }
    }

    // Class that holds a snapshot of dependent view information.
    // We can't just work with TableViews directly because they become invalid
    // when we drop the parent view.
    private class DependentView {
        String viewName;
        String createSql;
        String createForceSql;

        private DependentView(TableView view) {
            this.viewName = view.getName();
            this.createSql = view.getCreateSQL(true, false);
            this.createForceSql = view.getCreateSQL(true, true);
        }
    }

    private void execute(String sql, boolean ddl) {
        Prepared command = session.prepare(sql);
        command.update();
        if (ddl) {
            session.commit(true);
        }
    }
}
