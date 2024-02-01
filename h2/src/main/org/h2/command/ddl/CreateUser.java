/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.RightOwner;
import org.h2.engine.SessionLocal;
import org.h2.engine.User;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.security.SHA256;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.Value;

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

    public CreateUser(SessionLocal session) {
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

    /**
     * Set the salt and hash for the given user.
     *
     * @param user the user
     * @param session the session
     * @param salt the salt
     * @param hash the hash
     */
    static void setSaltAndHash(User user, SessionLocal session, Expression salt, Expression hash) {
        user.setSaltAndHash(getByteArray(session, salt), getByteArray(session, hash));
    }

    private static byte[] getByteArray(SessionLocal session, Expression e) {
        Value value = e.optimize(session).getValue(session);
        if (DataType.isBinaryStringType(value.getValueType())) {
            byte[] b = value.getBytes();
            return b == null ? new byte[0] : b;
        }
        String s = value.getString();
        return s == null ? new byte[0] : StringUtils.convertHexToBytes(s);
    }

    /**
     * Set the password for the given user.
     *
     * @param user the user
     * @param session the session
     * @param password the password
     */
    static void setPassword(User user, SessionLocal session, Expression password) {
        String pwd = password.optimize(session).getValue(session).getString();
        char[] passwordChars = pwd == null ? new char[0] : pwd.toCharArray();
        byte[] userPasswordHash;
        String userName = user.getName();
        if (userName.isEmpty() && passwordChars.length == 0) {
            userPasswordHash = new byte[0];
        } else {
            userPasswordHash = SHA256.getKeyPasswordHash(userName, passwordChars);
        }
        user.setUserPasswordHash(userPasswordHash);
    }

    @Override
    public long update() {
        session.getUser().checkAdmin();
        Database db = getDatabase();
        RightOwner rightOwner = db.findUserOrRole(userName);
        if (rightOwner != null) {
            if (rightOwner instanceof User) {
                if (ifNotExists) {
                    return 0;
                }
                throw DbException.get(ErrorCode.USER_ALREADY_EXISTS_1, userName);
            }
            throw DbException.get(ErrorCode.ROLE_ALREADY_EXISTS_1, userName);
        }
        int id = getObjectId();
        User user = new User(db, id, userName, false);
        user.setAdmin(admin);
        user.setComment(comment);
        if (hash != null && salt != null) {
            setSaltAndHash(user, session, salt, hash);
        } else if (password != null) {
            setPassword(user, session, password);
        } else {
            throw DbException.getInternalError();
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
