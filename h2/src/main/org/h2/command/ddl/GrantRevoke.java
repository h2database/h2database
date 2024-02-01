/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.RightOwner;
import org.h2.engine.Role;
import org.h2.engine.SessionLocal;
import org.h2.engine.User;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.h2.util.Utils;

/**
 * This class represents the statements
 * GRANT RIGHT,
 * GRANT ROLE,
 * REVOKE RIGHT,
 * REVOKE ROLE
 */
public class GrantRevoke extends DefineCommand {

    private ArrayList<String> roleNames;
    private int operationType;
    private int rightMask;
    private final ArrayList<Table> tables = Utils.newSmallArrayList();
    private Schema schema;
    private RightOwner grantee;

    public GrantRevoke(SessionLocal session) {
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
            roleNames = Utils.newSmallArrayList();
        }
        roleNames.add(roleName);
    }

    public void setGranteeName(String granteeName) {
        Database db = getDatabase();
        grantee = db.findUserOrRole(granteeName);
        if (grantee == null) {
            throw DbException.get(ErrorCode.USER_OR_ROLE_NOT_FOUND_1, granteeName);
        }
    }

    @Override
    public long update() {
        Database db = getDatabase();
        User user = session.getUser();
        if (roleNames != null) {
            user.checkAdmin();
            for (String name : roleNames) {
                Role grantedRole = db.findRole(name);
                if (grantedRole == null) {
                    throw DbException.get(ErrorCode.ROLE_NOT_FOUND_1, name);
                }
                if (operationType == CommandInterface.GRANT) {
                    grantRole(grantedRole);
                } else if (operationType == CommandInterface.REVOKE) {
                    revokeRole(grantedRole);
                } else {
                    throw DbException.getInternalError("type=" + operationType);
                }
            }
        } else {
            if ((rightMask & Right.ALTER_ANY_SCHEMA) != 0) {
                user.checkAdmin();
            } else {
                if (schema != null) {
                    user.checkSchemaOwner(schema);
                }
                for (Table table : tables) {
                    user.checkSchemaOwner(table.getSchema());
                }
            }
            if (operationType == CommandInterface.GRANT) {
                grantRight();
            } else if (operationType == CommandInterface.REVOKE) {
                revokeRight();
            } else {
                throw DbException.getInternalError("type=" + operationType);
            }
        }
        return 0;
    }

    private void grantRight() {
        if (schema != null) {
            grantRight(schema);
        }
        for (Table table : tables) {
            grantRight(table);
        }
    }

    private void grantRight(DbObject object) {
        Database db = getDatabase();
        Right right = grantee.getRightForObject(object);
        if (right == null) {
            int id = getPersistedObjectId();
            if (id == 0) {
                id = getDatabase().allocateObjectId();
            }
            right = new Right(db, id, grantee, rightMask, object);
            grantee.grantRight(object, right);
            db.addDatabaseObject(session, right);
        } else {
            right.setRightMask(right.getRightMask() | rightMask);
            db.updateMeta(session, right);
        }
    }

    private void grantRole(Role grantedRole) {
        if (grantedRole != grantee && grantee.isRoleGranted(grantedRole)) {
            return;
        }
        if (grantee instanceof Role) {
            Role granteeRole = (Role) grantee;
            if (grantedRole.isRoleGranted(granteeRole)) {
                // cyclic role grants are not allowed
                throw DbException.get(ErrorCode.ROLE_ALREADY_GRANTED_1, grantedRole.getTraceSQL());
            }
        }
        Database db = getDatabase();
        int id = getObjectId();
        Right right = new Right(db, id, grantee, grantedRole);
        db.addDatabaseObject(session, right);
        grantee.grantRole(grantedRole, right);
    }

    private void revokeRight() {
        if (schema != null) {
            revokeRight(schema);
        }
        for (Table table : tables) {
            revokeRight(table);
        }
    }

    private void revokeRight(DbObject object) {
        Right right = grantee.getRightForObject(object);
        if (right == null) {
            return;
        }
        int mask = right.getRightMask();
        int newRight = mask & ~rightMask;
        Database db = getDatabase();
        if (newRight == 0) {
            db.removeDatabaseObject(session, right);
        } else {
            right.setRightMask(newRight);
            db.updateMeta(session, right);
        }
    }


    private void revokeRole(Role grantedRole) {
        Right right = grantee.getRightForRole(grantedRole);
        if (right == null) {
            return;
        }
        Database db = getDatabase();
        db.removeDatabaseObject(session, right);
    }

    @Override
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

    /**
     * Set the specified schema
     *
     * @param schema the schema
     */
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    public int getType() {
        return operationType;
    }

}
