/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.table.Table;
import org.h2.util.ObjectArray;

/**
 * This class represents the statement
 * DROP ALL OBJECTS
 */
public class DropDatabase extends DefineCommand {

    private boolean dropAllObjects;
    private boolean deleteFiles;

    public DropDatabase(Session session) {
        super(session);
    }

    public int update() throws SQLException {
        if (dropAllObjects) {
            dropAllObjects();
        }
        if (deleteFiles) {
            session.getDatabase().setDeleteFilesOnDisconnect(true);
        }
        return 0;
    }

    private void dropAllObjects() throws SQLException {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        ObjectArray list;
        // TODO local temp tables are not removed
        list = db.getAllSchemas();
        for (int i = 0; i < list.size(); i++) {
            Schema schema = (Schema) list.get(i);
            if (schema.canDrop()) {
                db.removeDatabaseObject(session, schema);
            }
        }
        list = db.getAllSchemaObjects(DbObject.TABLE_OR_VIEW);
        for (int i = 0; i < list.size(); i++) {
            Table t = (Table) list.get(i);
            if (t.getName() != null && Table.VIEW.equals(t.getTableType())) {
                db.removeSchemaObject(session, t);
            }
        }
        for (int i = 0; i < list.size(); i++) {
            Table t = (Table) list.get(i);
            if (t.getName() != null && Table.TABLE_LINK.equals(t.getTableType())) {
                db.removeSchemaObject(session, t);
            }
        }
        for (int i = 0; i < list.size(); i++) {
            Table t = (Table) list.get(i);
            if (t.getName() != null && Table.TABLE.equals(t.getTableType())) {
                db.removeSchemaObject(session, t);
            }
        }
        session.findLocalTempTable(null);
        list = db.getAllSchemaObjects(DbObject.SEQUENCE);
        // maybe constraints and triggers on system tables will be allowed in
        // the future
        list.addAll(db.getAllSchemaObjects(DbObject.CONSTRAINT));
        list.addAll(db.getAllSchemaObjects(DbObject.TRIGGER));
        list.addAll(db.getAllSchemaObjects(DbObject.CONSTANT));
        for (int i = 0; i < list.size(); i++) {
            SchemaObject obj = (SchemaObject) list.get(i);
            db.removeSchemaObject(session, obj);
        }
        list = db.getAllUsers();
        for (int i = 0; i < list.size(); i++) {
            User user = (User) list.get(i);
            if (user != session.getUser()) {
                db.removeDatabaseObject(session, user);
            }
        }
        list = db.getAllRoles();
        for (int i = 0; i < list.size(); i++) {
            Role role = (Role) list.get(i);
            String sql = role.getCreateSQL();
            // the role PUBLIC must not be dropped
            if (sql != null) {
                db.removeDatabaseObject(session, role);
            }
        }
        list = db.getAllRights();
        list.addAll(db.getAllFunctionAliases());
        list.addAll(db.getAllAggregates());
        list.addAll(db.getAllUserDataTypes());
        for (int i = 0; i < list.size(); i++) {
            DbObject obj = (DbObject) list.get(i);
            String sql = obj.getCreateSQL();
            // the role PUBLIC must not be dropped
            if (sql != null) {
                db.removeDatabaseObject(session, obj);
            }
        }
    }

    public void setDropAllObjects(boolean b) {
        this.dropAllObjects = b;
    }

    public void setDeleteFiles(boolean b) {
        this.deleteFiles = b;
    }

}
