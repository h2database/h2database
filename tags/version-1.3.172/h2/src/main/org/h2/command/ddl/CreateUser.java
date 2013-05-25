/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.command.CommandInterface;
import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.security.SHA256;
import org.h2.util.StringUtils;

/**
 * This class represents the statement
 * CREATE USER
 */
public class CreateUser extends DefineCommand {

    private String userName;
    private boolean admin;
    private Expression password;
    private Expression salt;
    private Expression hash;
    private boolean ifNotExists;
    private String comment;

    public CreateUser(Session session) {
        super(session);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(Expression password) {
        this.password = password;
    }

    private char[] getCharArray(Expression e) {
        return e.optimize(session).getValue(session).getString().toCharArray();
    }

    private byte[] getByteArray(Expression e) {
        return StringUtils.convertHexToBytes(e.optimize(session).getValue(session).getString());
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        if (db.findRole(userName) != null) {
            throw DbException.get(ErrorCode.ROLE_ALREADY_EXISTS_1, userName);
        }
        if (db.findUser(userName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.USER_ALREADY_EXISTS_1, userName);
        }
        int id = getObjectId();
        User user = new User(db, id, userName, false);
        user.setAdmin(admin);
        user.setComment(comment);
        if (hash != null && salt != null) {
            user.setSaltAndHash(getByteArray(salt), getByteArray(hash));
        } else if (password != null) {
            char[] passwordChars = getCharArray(password);
            byte[] userPasswordHash;
            if (userName.length() == 0 && passwordChars.length == 0) {
                userPasswordHash = new byte[0];
            } else {
                userPasswordHash = SHA256.getKeyPasswordHash(userName, passwordChars);
            }
            user.setUserPasswordHash(userPasswordHash);
        } else {
            throw DbException.throwInternalError();
        }
        db.addDatabaseObject(session, user);
        return 0;
    }

    public void setSalt(Expression e) {
        salt = e;
    }

    public void setHash(Expression e) {
        hash = e;
    }

    public void setAdmin(boolean b) {
        admin = b;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_USER;
    }

}
