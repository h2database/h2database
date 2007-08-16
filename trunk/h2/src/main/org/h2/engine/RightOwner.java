/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.table.Table;

public abstract class RightOwner extends DbObjectBase {

    // key: role; value: right
    private HashMap grantedRoles;
    // key: table; value: right
    private HashMap grantedRights;
    
    protected RightOwner(Database database, int id, String name, String traceModule) {
        super(database, id, name, traceModule);
    }

    public boolean isRoleGranted(Role grantedRole) {
        if(grantedRoles != null) {
            Iterator it = grantedRoles.keySet().iterator();
            while(it.hasNext()) {
                Role role = (Role) it.next();
                if(role == grantedRole) {
                    return true;
                }
                if(role.isRoleGranted(grantedRole)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    protected boolean isRightGrantedRecursive(Table table, int rightMask) {
        Right right;
        if(grantedRights != null) {
            right = (Right) grantedRights.get(table);
            if(right != null) {
                if((right.getRightMask() & rightMask) == rightMask) {
                    return true;
                }
            }
        }
        if(grantedRoles != null) {
            Iterator it = grantedRoles.keySet().iterator();
            while(it.hasNext()) {
                RightOwner role = (RightOwner) it.next();
                if(role.isRightGrantedRecursive(table, rightMask)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    public void grantRight(Table table, Right right) {
        if(grantedRights == null) {
            grantedRights = new HashMap();
        }
        grantedRights.put(table, right);
    }
    
    public void revokeRight(Table table) {
        if(grantedRights == null) {
            return;
        }
        grantedRights.remove(table);
        if(grantedRights.size() == 0) {
            grantedRights = null;
        }
    }
    
    public void grantRole(Session session, Role role, Right right) {
        if(grantedRoles == null) {
            grantedRoles = new HashMap();
        }
        grantedRoles.put(role, right);
    }    
    
    public void revokeRole(Session session, Role role) throws SQLException {
        if(grantedRoles == null) {
            throw Message.getSQLException(ErrorCode.RIGHT_NOT_FOUND);
        }
        Right right = (Right) grantedRoles.get(role);
        if(right == null) {
            throw Message.getSQLException(ErrorCode.RIGHT_NOT_FOUND);
        }
        grantedRoles.remove(role);
        if(grantedRoles.size() == 0) {
            grantedRoles = null;
        }
    }
    
    public Right getRightForTable(Table table) {
        if(grantedRights == null) {
            return null;
        }
        return (Right) grantedRights.get(table);
    }
    
    public Right getRightForRole(Role role) {
        if(grantedRoles == null) {
            return null;
        }
        return (Right) grantedRoles.get(role);
    }    
    
}
