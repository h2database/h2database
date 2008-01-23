/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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
import org.h2.jdbc.JdbcSQLException;
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

    public static final int GRANT = 0, REVOKE = 1;
    private ObjectArray roleNames;
    private int operationType;
    private int rightMask;
    private ObjectArray tables = new ObjectArray();
    private RightOwner grantee;

    public GrantRevoke(Session session) {
        super(session);
    }

    public void setOperationType(int operationType) {
        this.operationType = operationType;
    }

    public void addRight(int right) {
        this.rightMask |= right;
    }

    public void addRoleName(String roleName) {
        if (roleNames == null) {
            roleNames = new ObjectArray();
        }
        roleNames.add(roleName);
    }

    public void setGranteeName(String granteeName) throws JdbcSQLException {
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
            for (int i = 0; i < roleNames.size(); i++) {
                String name = (String) roleNames.get(i);
                Role grantedRole = db.findRole(name);
                if (grantedRole == null) {
                    throw Message.getSQLException(ErrorCode.ROLE_NOT_FOUND_1, name);
                }
                if (operationType == GRANT) {
                    grantRole(grantedRole);
                } else if (operationType == REVOKE) {
                    revokeRole(grantedRole);
                } else {
                    throw Message.getInternalError("type=" + operationType);
                }
            }
        } else {
            if (operationType == GRANT) {
                grantRight();
            } else if (operationType == REVOKE) {
                revokeRight();
            } else {
                throw Message.getInternalError("type=" + operationType);
            }
        }
        return 0;
    }

    private void grantRight() throws SQLException {
        Database db = session.getDatabase();
        for (int i = 0; i < tables.size(); i++) {
            Table table = (Table) tables.get(i);
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
        if (grantee.isRoleGranted(grantedRole)) {
            throw Message.getSQLException(ErrorCode.ROLE_ALREADY_GRANTED_1, grantedRole.getSQL());
        }
        if (grantee instanceof Role) {
            Role granteeRole = (Role) grantee;
            if (grantedRole.isRoleGranted(granteeRole)) {
                // TODO role: should be 'cyclic role grants are not allowed'
                throw Message.getSQLException(ErrorCode.ROLE_ALREADY_GRANTED_1, grantedRole.getSQL());
            }
        }
        Database db = session.getDatabase();
        int id = getObjectId(true, true);
        Right right = new Right(db, id, grantee, grantedRole);
        db.addDatabaseObject(session, right);
        grantee.grantRole(session, grantedRole, right);
    }

    private void revokeRight() throws SQLException {
        for (int i = 0; i < tables.size(); i++) {
            Table table = (Table) tables.get(i);
            Right right = grantee.getRightForTable(table);
            if (right == null) {
                throw Message.getSQLException(ErrorCode.RIGHT_NOT_FOUND);
            }
            int mask = right.getRightMask();
            if ((mask & rightMask) != rightMask) {
                throw Message.getSQLException(ErrorCode.RIGHT_NOT_FOUND);
            }
            int newRight = mask ^ rightMask;
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
            throw Message.getSQLException(ErrorCode.RIGHT_NOT_FOUND);
        }
        Database db = session.getDatabase();
        db.removeDatabaseObject(session, right);
    }

    public boolean isTransactional() {
        return false;
    }

    public void addTable(Table table) {
        tables.add(table);
    }

}
