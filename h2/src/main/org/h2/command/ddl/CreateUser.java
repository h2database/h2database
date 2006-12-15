/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.message.Message;
import org.h2.security.SHA256;
import org.h2.util.ByteUtils;

public class CreateUser extends DefineCommand {

    private String userName;
    private boolean admin;
    private byte[] userPasswordHash;
    private byte[] salt;
    private byte[] hash;
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

    public void setPassword(String password) {
        SHA256 sha = new SHA256();
        this.userPasswordHash = sha.getKeyPasswordHash(userName, password.toCharArray());
    }

    public int update() throws SQLException {
        session.getUser().checkAdmin();
        session.commit();
        Database db = session.getDatabase();
        if(db.findUser(userName)!=null) {
            if (ifNotExists) {
                return 0;
            }
            throw Message.getSQLException(Message.USER_ALREADY_EXISTS_1, userName);
        }
        int id = getObjectId(false, true);
        User user = new User(db, id, userName, false);
        user.setAdmin(admin);
        user.setComment(comment);
        if(hash!=null && salt !=null) {
            user.setSaltAndHash(salt, hash);
        } else {
            user.setUserPasswordHash(userPasswordHash);
        }
        db.addDatabaseObject(session, user);
        return 0;
    }

    public void setSalt(String s) throws SQLException {
        salt = ByteUtils.convertStringToBytes(s);
    }

    public void setHash(String s) throws SQLException {
        hash = ByteUtils.convertStringToBytes(s);
    }

    public void setAdmin(boolean b) {
        admin = b;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

}
