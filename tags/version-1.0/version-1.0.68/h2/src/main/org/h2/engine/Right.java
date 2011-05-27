/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.table.Table;

/**
 * An access right. Rights are regular database objects, but have generated
 * names.
 */
public class Right extends DbObjectBase {

    public static final int SELECT = 1, DELETE = 2, INSERT = 4, UPDATE = 8, ALL = 15;

    private Role grantedRole;
    private int grantedRight;
    private Table grantedTable;
    private RightOwner grantee;

    public Right(Database db, int id, RightOwner grantee, Role grantedRole) {
        super(db, id, "RIGHT_"+id, Trace.USER);
        this.grantee = grantee;
        this.grantedRole = grantedRole;
    }

    public Right(Database db, int id, RightOwner grantee, int grantedRight, Table grantedRightOnTable) {
        super(db, id, ""+id, Trace.USER);
        this.grantee = grantee;
        this.grantedRight = grantedRight;
        this.grantedTable = grantedRightOnTable;
    }

    private boolean appendRight(StringBuffer buff, int right, int mask, String name, boolean comma) {
        if ((right & mask) != 0) {
            if (comma) {
                buff.append(", ");
            }
            buff.append(name);
            return true;
        }
        return comma;
    }

    public String getRights() {
        StringBuffer buff = new StringBuffer();
        if (grantedRight == ALL) {
            buff.append("ALL");
        } else {
            boolean comma = false;
            comma = appendRight(buff, grantedRight, SELECT, "SELECT", comma);
            comma = appendRight(buff, grantedRight, DELETE, "DELETE", comma);
            comma = appendRight(buff, grantedRight, INSERT, "INSERT", comma);
            appendRight(buff, grantedRight, UPDATE, "UPDATE", comma);
        }
        return buff.toString();
    }

    public Role getGrantedRole() {
        return grantedRole;
    }

    public Table getGrantedTable() {
        return grantedTable;
    }

    public DbObject getGrantee() {
        return grantee;
    }

    public String getDropSQL() {
        return null;
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        StringBuffer buff = new StringBuffer();
        buff.append("GRANT ");
        if (grantedRole != null) {
            buff.append(grantedRole.getSQL());
        } else {
            buff.append(getRights());
            buff.append(" ON ");
            buff.append(table.getSQL());
        }
        buff.append(" TO ");
        // TODO rights: need role 'PUBLIC'
        buff.append(grantee.getSQL());
        return buff.toString();
    }

    public String getCreateSQL() {
        return getCreateSQLForCopy(grantedTable, null);
    }

    public int getType() {
        return DbObject.RIGHT;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        if (grantedTable != null) {
            grantee.revokeRight(grantedTable);
        } else {
            grantee.revokeRole(session, grantedRole);
        }
        database.removeMeta(session, getId());
        grantedRole = null;
        grantedTable = null;
        grantee = null;
        invalidate();
    }

    public void checkRename() throws SQLException {
        throw Message.getInternalError();
    }

    public void setRightMask(int rightMask) {
        grantedRight = rightMask;
    }

    public int getRightMask() {
        return grantedRight;
    }

}
