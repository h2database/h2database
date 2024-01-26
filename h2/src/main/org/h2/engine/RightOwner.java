/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.h2.util.StringUtils;

/**
 * A right owner (sometimes called principal).
 */
public abstract class RightOwner extends DbObject {

    /**
     * The map of granted roles.
     */
    private HashMap<Role, Right> grantedRoles;

    /**
     * The map of granted rights.
     */
    private HashMap<DbObject, Right> grantedRights;

    protected RightOwner(Database database, int id, String name, int traceModuleId) {
        super(database, id, StringUtils.toUpperEnglish(name), traceModuleId);
    }

    @Override
    public void rename(String newName) {
        super.rename(StringUtils.toUpperEnglish(newName));
    }

    /**
     * Check if a role has been granted for this right owner.
     *
     * @param grantedRole the role
     * @return true if the role has been granted
     */
    public boolean isRoleGranted(Role grantedRole) {
        if (grantedRole == this) {
            return true;
        }
        if (grantedRoles != null) {
            for (Role role : grantedRoles.keySet()) {
                if (role == grantedRole) {
                    return true;
                }
                if (role.isRoleGranted(grantedRole)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks if a right is already granted to this object or to objects that
     * were granted to this object. The rights of schemas will be valid for
     * every each table in the related schema. The ALTER ANY SCHEMA right gives
     * all rights to all tables.
     *
     * @param table
     *            the table to check
     * @param rightMask
     *            the right mask to check
     * @return true if the right was already granted
     */
    final boolean isTableRightGrantedRecursive(Table table, int rightMask) {
        Schema schema = table.getSchema();
        if (schema.getOwner() == this) {
            return true;
        }
        if (grantedRights != null) {
            Right right = grantedRights.get(null);
            if (right != null && (right.getRightMask() & Right.ALTER_ANY_SCHEMA) == Right.ALTER_ANY_SCHEMA) {
                return true;
            }
            right = grantedRights.get(schema);
            if (right != null && (right.getRightMask() & rightMask) == rightMask) {
                return true;
            }
            right = grantedRights.get(table);
            if (right != null && (right.getRightMask() & rightMask) == rightMask) {
                return true;
            }
        }
        if (grantedRoles != null) {
            for (Role role : grantedRoles.keySet()) {
                if (role.isTableRightGrantedRecursive(table, rightMask)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks if a schema owner right is already granted to this object or to
     * objects that were granted to this object. The ALTER ANY SCHEMA right
     * gives rights to all schemas.
     *
     * @param schema
     *            the schema to check, or {@code null} to check for ALTER ANY
     *            SCHEMA right only
     * @return true if the right was already granted
     */
    final boolean isSchemaRightGrantedRecursive(Schema schema) {
        if (schema != null && schema.getOwner() == this) {
            return true;
        }
        if (grantedRights != null) {
            Right right = grantedRights.get(null);
            if (right != null && (right.getRightMask() & Right.ALTER_ANY_SCHEMA) == Right.ALTER_ANY_SCHEMA) {
                return true;
            }
        }
        if (grantedRoles != null) {
            for (Role role : grantedRoles.keySet()) {
                if (role.isSchemaRightGrantedRecursive(schema)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Grant a right for the given table. Only one right object per table is
     * supported.
     *
     * @param object the object (table or schema)
     * @param right the right
     */
    public void grantRight(DbObject object, Right right) {
        if (grantedRights == null) {
            grantedRights = new HashMap<>();
        }
        grantedRights.put(object, right);
    }

    /**
     * Revoke the right for the given object (table or schema).
     *
     * @param object the object
     */
    void revokeRight(DbObject object) {
        if (grantedRights == null) {
            return;
        }
        grantedRights.remove(object);
        if (grantedRights.size() == 0) {
            grantedRights = null;
        }
    }

    /**
     * Grant a role to this object.
     *
     * @param role the role
     * @param right the right to grant
     */
    public void grantRole(Role role, Right right) {
        if (grantedRoles == null) {
            grantedRoles = new HashMap<>();
        }
        grantedRoles.put(role, right);
    }

    /**
     * Remove the right for the given role.
     *
     * @param role the role to revoke
     */
    void revokeRole(Role role) {
        if (grantedRoles == null) {
            return;
        }
        Right right = grantedRoles.get(role);
        if (right == null) {
            return;
        }
        grantedRoles.remove(role);
        if (grantedRoles.size() == 0) {
            grantedRoles = null;
        }
    }

    /**
     * Remove all the temporary rights granted on roles
     */
    public void revokeTemporaryRightsOnRoles() {
        if (grantedRoles == null) {
            return;
        }
        List<Role> rolesToRemove= new ArrayList<>();
        for (Entry<Role,Right> currentEntry : grantedRoles.entrySet()) {
            if ( currentEntry.getValue().isTemporary() || !currentEntry.getValue().isValid()) {
                rolesToRemove.add(currentEntry.getKey());
            }
        }
        for (Role currentRoleToRemove : rolesToRemove) {
            revokeRole(currentRoleToRemove);
        }
    }



    /**
     * Get the 'grant schema' right of this object.
     *
     * @param object the granted object (table or schema)
     * @return the right or null if the right has not been granted
     */
    public Right getRightForObject(DbObject object) {
        if (grantedRights == null) {
            return null;
        }
        return grantedRights.get(object);
    }

    /**
     * Get the 'grant role' right of this object.
     *
     * @param role the granted role
     * @return the right or null if the right has not been granted
     */
    public Right getRightForRole(Role role) {
        if (grantedRoles == null) {
            return null;
        }
        return grantedRoles.get(role);
    }

    /**
     * Check that this right owner does not own any schema. An exception is
     * thrown if it owns one or more schemas.
     *
     * @throws DbException
     *             if this right owner owns a schema
     */
    public final void checkOwnsNoSchemas() {
        for (Schema s : database.getAllSchemas()) {
            if (this == s.getOwner()) {
                throw DbException.get(ErrorCode.CANNOT_DROP_2, getName(), s.getName());
            }
        }
    }

}
