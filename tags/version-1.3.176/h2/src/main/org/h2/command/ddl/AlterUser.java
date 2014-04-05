/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.security.SHA256;
import org.h2.util.StringUtils;

/**
 * This class represents the statements
 * ALTER USER ADMIN,
 * ALTER USER RENAME,
 * ALTER USER SET PASSWORD
 */
public class AlterUser extends DefineCommand {

    private int type;
    private User user;
    private String newName;
    private Expression password;
    private Expression salt;
    private Expression hash;
    private boolean admin;

    public AlterUser(Session session) {
        super(session);
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setNewName(String newName) {
        this.newName = newName;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public void setSalt(Expression e) {
        salt = e;
    }

    public void setHash(Expression e) {
        hash = e;
    }

    public void setPassword(Expression password) {
        this.password = password;
    }

    private char[] getCharArray(Expression e) {
        return e.optimize(session).getValue(session).getString().toCharArray();
    }

    private byte[] getByteArray(Expression e) {
        return StringUtils.convertHexToBytes(
                e.optimize(session).getValue(session).getString());
    }

    @Override
    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        switch (type) {
        case CommandInterface.ALTER_USER_SET_PASSWORD:
            if (user != session.getUser()) {
                session.getUser().checkAdmin();
            }
            if (hash != null && salt != null) {
                user.setSaltAndHash(getByteArray(salt), getByteArray(hash));
            } else {
                String name = newName == null ? user.getName() : newName;
                char[] passwordChars = getCharArray(password);
                byte[] userPasswordHash = SHA256.getKeyPasswordHash(name, passwordChars);
                user.setUserPasswordHash(userPasswordHash);
            }
            break;
        case CommandInterface.ALTER_USER_RENAME:
            session.getUser().checkAdmin();
            if (db.findUser(newName) != null || newName.equals(user.getName())) {
                throw DbException.get(ErrorCode.USER_ALREADY_EXISTS_1, newName);
            }
            db.renameDatabaseObject(session, user, newName);
            break;
        case CommandInterface.ALTER_USER_ADMIN:
            session.getUser().checkAdmin();
            if (!admin) {
                user.checkOwnsNoSchemas();
            }
            user.setAdmin(admin);
            break;
        default:
            DbException.throwInternalError("type=" + type);
        }
        db.update(session, user);
        return 0;
    }

    @Override
    public int getType() {
        return type;
    }

}
