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
import org.h2.engine.FunctionAlias;
import org.h2.engine.Session;
import org.h2.message.Message;

/**
 * This class represents the statement
 * CREATE ALIAS
 */
public class CreateFunctionAlias extends DefineCommand {

    private String aliasName;
    private String javaClassMethod;
    private boolean deterministic;
    private boolean ifNotExists;
    private boolean force;

    public CreateFunctionAlias(Session session) {
        super(session);
    }

    public int update() throws SQLException {
        session.commit(true);
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        if (db.findFunctionAlias(aliasName) != null) {
            if (!ifNotExists) {
                throw Message.getSQLException(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1, aliasName);
            }
        } else {
            int id = getObjectId(false, true);
            FunctionAlias functionAlias = new FunctionAlias(db, id, aliasName, javaClassMethod, force);
            functionAlias.setDeterministic(deterministic);
            db.addDatabaseObject(session, functionAlias);
        }
        return 0;
    }

    public void setAliasName(String name) {
        this.aliasName = name;
    }

    public void setJavaClassMethod(String string) {
        this.javaClassMethod = string;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public void setDeterministic(boolean deterministic) {
        this.deterministic = deterministic;
    }

}
