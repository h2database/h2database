/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.ConstraintActionType;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.engine.UserDataType;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * This class represents the statement
 * DROP DOMAIN
 */
public class DropUserDataType extends DefineCommand {

    private String typeName;
    private boolean ifExists;
    private ConstraintActionType dropAction;

    public DropUserDataType(Session session) {
        super(session);
        dropAction = session.getDatabase().getSettings().dropRestrict ?
                ConstraintActionType.RESTRICT : ConstraintActionType.CASCADE;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public void setDropAction(ConstraintActionType dropAction) {
        this.dropAction = dropAction;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        UserDataType type = db.findUserDataType(typeName);
        if (type == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.USER_DATA_TYPE_NOT_FOUND_1, typeName);
            }
        } else {
            for (Table t : db.getAllTablesAndViews(false)) {
                boolean modified = false;
                for (Column c : t.getColumns()) {
                    UserDataType udt = c.getUserDataType();
                    if (udt != null && udt.getName().equals(typeName)) {
                        if (dropAction == ConstraintActionType.RESTRICT) {
                            throw DbException.get(ErrorCode.CANNOT_DROP_2, typeName, t.getCreateSQL());
                        }
                        c.setOriginalSQL(type.getColumn().getOriginalSQL());
                        c.setUserDataType(null);
                        modified = true;
                    }
                }
                if (modified) {
                    db.updateMeta(session, t);
                }
            }
            db.removeDatabaseObject(session, type);
        }
        return 0;
    }

    public void setTypeName(String name) {
        this.typeName = name;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_DOMAIN;
    }

}
