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
import org.h2.engine.UserAggregate;
import org.h2.message.Message;

/**
 * This class represents the statement
 * DROP AGGREGATE
 */
public class DropAggregate extends DefineCommand {

    private String name;
    private boolean ifExists;

    public DropAggregate(Session session) {
        super(session);
    }

    public int update() throws SQLException {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        UserAggregate aggregate = db.findAggregate(name);
        if (aggregate == null) {
            if (!ifExists) {
                throw Message.getSQLException(ErrorCode.AGGREGATE_NOT_FOUND_1, name);
            }
        } else {
            db.removeDatabaseObject(session, aggregate);
        }
        return 0;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

}
