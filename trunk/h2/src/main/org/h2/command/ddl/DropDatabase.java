/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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
        // TODO local temp tables are not removed
        ObjectArray<Schema> schemas = db.getAllSchemas();
        for (int i = 0; i < schemas.size(); i++) {
            Schema schema = schemas.get(i);
            if (schema.canDrop()) {
                db.removeDatabaseObject(session, schema);
            }
        }
        ObjectArray<Table> tables = db.getAllTablesAndViews();
        for (int i = 0; i < tables.size(); i++) {
            Table t = tables.get(i);
            if (t.getName() != null && Table.VIEW.equals(t.getTableType())) {
                db.removeSchemaObject(session, t);
            }
        }
        for (int i = 0; i < tables.size(); i++) {
            Table t = tables.get(i);
            if (t.getName() != null && Table.TABLE_LINK.equals(t.getTableType())) {
                db.removeSchemaObject(session, t);
            }
        }
        for (int i = 0; i < tables.size(); i++) {
            Table t = tables.get(i);
            if (t.getName() != null && Table.TABLE.equals(t.getTableType())) {
                db.removeSchemaObject(session, t);
            }
        }
        session.findLocalTempTable(null);
        ObjectArray<SchemaObject> list = ObjectArray.newInstance();
        list.addAll(db.getAllSchemaObjects(DbObject.SEQUENCE));
        // maybe constraints and triggers on system tables will be allowed in
        // the future
        list.addAll(db.getAllSchemaObjects(DbObject.CONSTRAINT));
        list.addAll(db.getAllSchemaObjects(DbObject.TRIGGER));
        list.addAll(db.getAllSchemaObjects(DbObject.CONSTANT));
        for (int i = 0; i < list.size(); i++) {
            SchemaObject obj = list.get(i);
            db.removeSchemaObject(session, obj);
        }
        ObjectArray<User> users = db.getAllUsers();
        for (int i = 0; i < users.size(); i++) {
            User user = users.get(i);
            if (user != session.getUser()) {
                db.removeDatabaseObject(session, user);
            }
        }
        ObjectArray<Role> roles = db.getAllRoles();
        for (int i = 0; i < roles.size(); i++) {
            Role role = roles.get(i);
            String sql = role.getCreateSQL();
            // the role PUBLIC must not be dropped
            if (sql != null) {
                db.removeDatabaseObject(session, role);
            }
        }
        ObjectArray<DbObject> dbObjects = ObjectArray.newInstance();
        dbObjects.addAll(db.getAllRights());
        dbObjects.addAll(db.getAllFunctionAliases());
        dbObjects.addAll(db.getAllAggregates());
        dbObjects.addAll(db.getAllUserDataTypes());
        for (int i = 0; i < dbObjects.size(); i++) {
            DbObject obj = dbObjects.get(i);
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
