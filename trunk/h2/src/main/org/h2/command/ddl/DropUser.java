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
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.message.Message;

/**
 * This class represents the statement
 * DROP USER
 */
public class DropUser extends DefineCommand {

    private boolean ifExists;
    private String userName;

    public DropUser(Session session) {
        super(session);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int update() throws SQLException {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        User user = db.findUser(userName);
        if (user == null) {
            if (!ifExists) {
                throw Message.getSQLException(ErrorCode.USER_NOT_FOUND_1, userName);
            }
        } else {
            if (user == session.getUser()) {
                throw Message.getSQLException(ErrorCode.CANNOT_DROP_CURRENT_USER);
            }
            user.checkOwnsNoSchemas();
            db.removeDatabaseObject(session, user);
        }
        return 0;
    }

    public boolean isTransactional() {
        return false;
    }

}
