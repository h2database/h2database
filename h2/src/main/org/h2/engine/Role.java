/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.table.Table;
import org.h2.util.ObjectArray;

/**
 * Represents a role. Roles can be granted to users, and to other roles.
 */
public class Role extends RightOwner {

    private final boolean system;

    public Role(Database database, int id, String roleName, boolean system) {
        super(database, id, roleName, Trace.USER);
        this.system = system;
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.throwInternalError();
    }

    public String getDropSQL() {
        return null;
    }

    /**
     * Get the CREATE SQL statement for this object.
     *
     * @param ifNotExists true if IF NOT EXISTS should be used
     * @return the SQL statement
     */
    public String getCreateSQL(boolean ifNotExists) {
        if (system) {
            return null;
        }
        StringBuffer buff = new StringBuffer("CREATE ROLE ");
        if (ifNotExists) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(getSQL());
        return buff.toString();
    }

    public String getCreateSQL() {
        return getCreateSQL(false);
    }

    public int getType() {
        return DbObject.ROLE;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        ObjectArray<User> users = database.getAllUsers();
        for (int i = 0; i < users.size(); i++) {
            User user = users.get(i);
            Right right = user.getRightForRole(this);
            if (right != null) {
                database.removeDatabaseObject(session, right);
            }
        }
        ObjectArray<Role> roles = database.getAllRoles();
        for (int i = 0; i < roles.size(); i++) {
            Role r2 = roles.get(i);
            Right right = r2.getRightForRole(this);
            if (right != null) {
                database.removeDatabaseObject(session, right);
            }
        }
        ObjectArray<Right> rights = database.getAllRights();
        for (int i = 0; i < rights.size(); i++) {
            Right right = rights.get(i);
            if (right.getGrantee() == this) {
                database.removeDatabaseObject(session, right);
            }
        }
        database.removeMeta(session, getId());
        invalidate();
    }

    public void checkRename() {
        // ok
    }

}
