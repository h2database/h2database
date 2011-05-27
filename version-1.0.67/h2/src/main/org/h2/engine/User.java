/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.schema.Schema;
import org.h2.security.SHA256;
import org.h2.table.MetaTable;
import org.h2.table.RangeTable;
import org.h2.table.Table;
import org.h2.table.TableView;
import org.h2.util.ByteUtils;
import org.h2.util.ObjectArray;
import org.h2.util.RandomUtils;
import org.h2.util.StringUtils;

/**
 * Represents a user object.
 */
public class User extends RightOwner {

    private final boolean systemUser;
    private byte[] salt;
    private byte[] passwordHash;
    private boolean admin;

    public User(Database database, int id, String userName, boolean systemUser) {
        super(database, id, userName, Trace.USER);
        this.systemUser = systemUser;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public boolean getAdmin() {
        return admin;
    }

    public void setSaltAndHash(byte[] salt, byte[] hash) {
        this.salt = salt;
        this.passwordHash = hash;
    }

    public void setUserPasswordHash(byte[] userPasswordHash) {
        if (userPasswordHash != null) {
            salt = RandomUtils.getSecureBytes(Constants.SALT_LEN);
            SHA256 sha = new SHA256();
            this.passwordHash = sha.getHashWithSalt(userPasswordHash, salt);
        }
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.getInternalError();
    }

    public String getCreateSQL() {
        return getCreateSQL(true, false);
    }

    public String getDropSQL() {
        return null;
    }

    public void checkRight(Table table, int rightMask) throws SQLException {
        if (rightMask != Right.SELECT && !systemUser) {
            database.checkWritingAllowed();
        }
        if (admin) {
            return;
        }
        Role publicRole = database.getPublicRole();
        if (publicRole.isRightGrantedRecursive(table, rightMask)) {
            return;
        }
        if (table instanceof MetaTable || table instanceof RangeTable) {
            // everybody has access to the metadata information
            return;
        }
        if (Table.VIEW.equals(table.getTableType())) {
            TableView v = (TableView) table;
            if (v.getOwner() == this) {
                // the owner of a view has access:
                // SELECT * FROM (SELECT * FROM ...)
                return;
            }
        }
        if (!isRightGrantedRecursive(table, rightMask)) {
            if (table.getTemporary() && !table.getGlobalTemporary()) {
                // the owner has all rights on local temporary tables
                return;
            }
            throw Message.getSQLException(ErrorCode.NOT_ENOUGH_RIGHTS_FOR_1, table.getSQL());
        }
    }

    public String getCreateSQL(boolean password, boolean ifNotExists) {
        StringBuffer buff = new StringBuffer();
        buff.append("CREATE USER ");
        if (ifNotExists) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ");
            buff.append(StringUtils.quoteStringSQL(comment));
        }
        if (password) {
            buff.append(" SALT '");
            buff.append(ByteUtils.convertBytesToString(salt));
            buff.append("' HASH '");
            buff.append(ByteUtils.convertBytesToString(passwordHash));
            buff.append("'");
        } else {
            buff.append(" PASSWORD ''");
        }
        if (admin) {
            buff.append(" ADMIN");
        }
        return buff.toString();
    }

    public void checkUserPasswordHash(byte[] buff, SQLException onError) throws SQLException {
        SHA256 sha = new SHA256();
        byte[] hash = sha.getHashWithSalt(buff, salt);
        if (!ByteUtils.compareSecure(hash, passwordHash)) {
            try {
                Thread.sleep(Constants.DELAY_WRONG_PASSWORD);
            } catch (InterruptedException e) {
                // ignore
            }
            throw onError;
        }
    }

    public void checkAdmin() throws SQLException {
        if (!admin) {
            throw Message.getSQLException(ErrorCode.ADMIN_RIGHTS_REQUIRED);
        }
    }

    public int getType() {
        return DbObject.USER;
    }

    public ObjectArray getChildren() {
        ObjectArray all = database.getAllRights();
        ObjectArray children = new ObjectArray();
        for (int i = 0; i < all.size(); i++) {
            Right right = (Right) all.get(i);
            if (right.getGrantee() == this) {
                children.add(right);
            }
        }
        all = database.getAllSchemas();
        for (int i = 0; i < all.size(); i++) {
            Schema schema = (Schema) all.get(i);
            if (schema.getOwner() == this) {
                children.add(schema);
            }
        }
        return children;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        ObjectArray rights = database.getAllRights();
        for (int i = 0; i < rights.size(); i++) {
            Right right = (Right) rights.get(i);
            if (right.getGrantee() == this) {
                database.removeDatabaseObject(session, right);
            }
        }
        database.removeMeta(session, getId());
        salt = null;
        ByteUtils.clear(passwordHash);
        passwordHash = null;
        invalidate();
    }

    public void checkRename() {
        // ok
    }

    public void checkNoSchemas() throws SQLException {
        ObjectArray schemas = database.getAllSchemas();
        for (int i = 0; i < schemas.size(); i++) {
            Schema s = (Schema) schemas.get(i);
            if (this == s.getOwner()) {
                throw Message.getSQLException(ErrorCode.CANNOT_DROP_2, new String[]{ getName(), s.getName() });
            }
        }
    }

}
