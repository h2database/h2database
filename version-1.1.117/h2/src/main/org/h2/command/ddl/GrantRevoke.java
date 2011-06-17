/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.RightOwner;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.table.Table;
import org.h2.util.ObjectArray;

/**
 * This class represents the statements
 * GRANT RIGHT,
 * GRANT ROLE,
 * REVOKE RIGHT,
 * REVOKE ROLE
 */
public class GrantRevoke extends DefineCommand {

    /**
     * The operation type to grant a right.
     */
    public static final int GRANT = 0;

    /**
     * The operation type to revoke a right.
     */
    public static final int REVOKE = 1;

    private ObjectArray<String> roleNames;
    private int operationType;
    private int rightMask;
    private ObjectArray<Table> tables = ObjectArray.newInstance();
    private RightOwner grantee;

    public GrantRevoke(Session session) {
        super(session);
    }

    public void setOperationType(int operationType) {
        this.operationType = operationType;
    }

    /**
     * Add the specified right bit to the rights bitmap.
     *
     * @param right the right bit
     */
    public void addRight(int right) {
        this.rightMask |= right;
    }

    /**
     * Add the specified role to the list of roles.
     *
     * @param roleName the role
     */
    public void addRoleName(String roleName) {
        if (roleNames == null) {
            roleNames = ObjectArray.newInstance();
        }
        roleNames.add(roleName);
    }

    public void setGranteeName(String granteeName) throws SQLException {
        Database db = session.getDatabase();
        grantee = db.findUser(granteeName);
        if (grantee == null) {
            grantee = db.findRole(granteeName);
            if (grantee == null) {
                throw Message.getSQLException(ErrorCode.USER_OR_ROLE_NOT_FOUND_1, granteeName);
            }
        }
    }

    public int update() throws SQLException {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        if (roleNames != null) {
            for (String name : roleNames) {
                Role grantedRole = db.findRole(name);
                if (grantedRole == null) {
                    throw Message.getSQLException(ErrorCode.ROLE_NOT_FOUND_1, name);
                }
                if (operationType == GRANT) {
                    grantRole(grantedRole);
                } else if (operationType == REVOKE) {
                    revokeRole(grantedRole);
                } else {
                    Message.throwInternalError("type=" + operationType);
                }
            }
        } else {
            if (operationType == GRANT) {
                grantRight();
            } else if (operationType == REVOKE) {
                revokeRight();
            } else {
                Message.throwInternalError("type=" + operationType);
            }
        }
        return 0;
    }

    private void grantRight() throws SQLException {
        Database db = session.getDatabase();
        for (Table table : tables) {
            Right right = grantee.getRightForTable(table);
            if (right == null) {
                int id = getObjectId(true, true);
                right = new Right(db, id, grantee, rightMask, table);
                grantee.grantRight(table, right);
                db.addDatabaseObject(session, right);
            } else {
                right.setRightMask(right.getRightMask() | rightMask);
            }
        }
    }

    private void grantRole(Role grantedRole) throws SQLException {
        if (grantedRole != grantee && grantee.isRoleGranted(grantedRole)) {
            return;
        }
        if (grantee instanceof Role) {
            Role granteeRole = (Role) grantee;
            if (grantedRole.isRoleGranted(granteeRole)) {
                // cyclic role grants are not allowed
                throw Message.getSQLException(ErrorCode.ROLE_ALREADY_GRANTED_1, grantedRole.getSQL());
            }
        }
        Database db = session.getDatabase();
        int id = getObjectId(true, true);
        Right right = new Right(db, id, grantee, grantedRole);
        db.addDatabaseObject(session, right);
        grantee.grantRole(grantedRole, right);
    }

    private void revokeRight() throws SQLException {
        for (Table table : tables) {
            Right right = grantee.getRightForTable(table);
            if (right == null) {
                continue;
            }
            int mask = right.getRightMask();
            int newRight = mask & ~rightMask;
            Database db = session.getDatabase();
            if (newRight == 0) {
                db.removeDatabaseObject(session, right);
            } else {
                right.setRightMask(newRight);
                db.update(session, right);
            }
        }
    }

    private void revokeRole(Role grantedRole) throws SQLException {
        Right right = grantee.getRightForRole(grantedRole);
        if (right == null) {
            return;
        }
        Database db = session.getDatabase();
        db.removeDatabaseObject(session, right);
    }

    public boolean isTransactional() {
        return false;
    }

    /**
     * Add the specified table to the list of tables.
     *
     * @param table the table
     */
    public void addTable(Table table) {
        tables.add(table);
    }

}
