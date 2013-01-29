/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.HashMap;

import org.h2.table.Table;
import org.h2.util.New;

/**
 * A right owner (sometimes called principal).
 */
public abstract class RightOwner extends DbObjectBase {

    /**
     * The map of granted roles.
     */
    private HashMap<Role, Right> grantedRoles;

    /**
     * The map of granted rights.
     */
    private HashMap<Table, Right> grantedRights;

    protected RightOwner(Database database, int id, String name, String traceModule) {
        initDbObjectBase(database, id, name, traceModule);
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
     * Check if a right is already granted to this object or to objects that
     * were granted to this object.
     *
     * @param table the table to check
     * @param rightMask the right mask to check
     * @return true if the right was already granted
     */
    boolean isRightGrantedRecursive(Table table, int rightMask) {
        Right right;
        if (grantedRights != null) {
            right = grantedRights.get(table);
            if (right != null) {
                if ((right.getRightMask() & rightMask) == rightMask) {
                    return true;
                }
            }
        }
        if (grantedRoles != null) {
            for (RightOwner role : grantedRoles.keySet()) {
                if (role.isRightGrantedRecursive(table, rightMask)) {
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
     * @param table the table
     * @param right the right
     */
    public void grantRight(Table table, Right right) {
        if (grantedRights == null) {
            grantedRights = New.hashMap();
        }
        grantedRights.put(table, right);
    }

    /**
     * Revoke the right for the given table.
     *
     * @param table the table
     */
    void revokeRight(Table table) {
        if (grantedRights == null) {
            return;
        }
        grantedRights.remove(table);
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
            grantedRoles = New.hashMap();
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
     * Get the 'grant table' right of this object.
     *
     * @param table the granted table
     * @return the right or null if the right has not been granted
     */
    public Right getRightForTable(Table table) {
        if (grantedRights == null) {
            return null;
        }
        return grantedRights.get(table);
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

}
