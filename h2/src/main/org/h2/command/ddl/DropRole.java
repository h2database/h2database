/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.Role;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;

/**
 * This class represents the statement
 * DROP ROLE
 */
public class DropRole extends DefineCommand {

    private String roleName;
    private boolean ifExists;

    public DropRole(SessionLocal session) {
        super(session);
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    @Override
    public long update() {
        session.getUser().checkAdmin();
        Database db = getDatabase();
        Role role = db.findRole(roleName);
        if (role == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.ROLE_NOT_FOUND_1, roleName);
            }
        } else {
            if (role == db.getPublicRole()) {
                throw DbException.get(ErrorCode.ROLE_CAN_NOT_BE_DROPPED_1, roleName);
            }
            role.checkOwnsNoSchemas();
            db.removeDatabaseObject(session, role);
        }
        return 0;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_ROLE;
    }

}
