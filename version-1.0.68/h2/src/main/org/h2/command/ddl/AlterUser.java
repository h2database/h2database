/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
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
import org.h2.security.SHA256;
import org.h2.util.ByteUtils;

/**
 * This class represents the statements
 * ALTER USER ADMIN,
 * ALTER USER RENAME,
 * ALTER USER SET PASSWORD
 */
public class AlterUser extends DefineCommand {

    /**
     * The command type to set the password.
     */
    public static final int SET_PASSWORD = 0;
    
    /**
     * The command type to rename the user.
     */
    public static final int RENAME = 1;
    
    /**
     * The command type to change the admin flag.
     */
    public static final int ADMIN = 2;
    
    private int type;
    private User user;
    private String newName;
    private byte[] userPasswordHash;
    private byte[] salt;
    private byte[] hash;
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

    public void setSalt(String s) throws SQLException {
        salt = ByteUtils.convertStringToBytes(s);
    }

    public void setHash(String s) throws SQLException {
        hash = ByteUtils.convertStringToBytes(s);
    }

    public void setPassword(String password) {
        SHA256 sha = new SHA256();
        String name = newName == null ? user.getName() : newName;
        this.userPasswordHash = sha.getKeyPasswordHash(name, password.toCharArray());
    }

    public int update() throws SQLException {
        session.commit(true);
        Database db = session.getDatabase();
        switch (type) {
        case SET_PASSWORD:
            if (user != session.getUser()) {
                session.getUser().checkAdmin();
            }
            if (hash != null && salt != null) {
                user.setSaltAndHash(salt, hash);
            } else {
                user.setUserPasswordHash(userPasswordHash);
            }
            break;
        case RENAME:
            session.getUser().checkAdmin();
            if (db.findUser(newName) != null || newName.equals(user.getName())) {
                throw Message.getSQLException(ErrorCode.USER_ALREADY_EXISTS_1, newName);
            }
            db.renameDatabaseObject(session, user, newName);
            break;
        case ADMIN:
            session.getUser().checkAdmin();
            if (!admin) {
                user.checkNoSchemas();
            }
            user.setAdmin(admin);
            break;
        default:
            throw Message.getInternalError("type=" + type);
        }
        db.update(session, user);
        return 0;
    }

}
