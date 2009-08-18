/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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

    public boolean isAdmin() {
        return admin;
    }

    /**
     * Set the salt and hash of the password for this user.
     *
     * @param salt the salt
     * @param hash the password hash
     */
    public void setSaltAndHash(byte[] salt, byte[] hash) {
        this.salt = salt;
        this.passwordHash = hash;
    }

    /**
     * Set the user name password hash. A random salt is generated as well.
     * The parameter is filled with zeros after use.
     *
     * @param userPasswordHash the user name password hash
     */
    public void setUserPasswordHash(byte[] userPasswordHash) {
        if (userPasswordHash != null) {
            salt = RandomUtils.getSecureBytes(Constants.SALT_LEN);
            SHA256 sha = new SHA256();
            this.passwordHash = sha.getHashWithSalt(userPasswordHash, salt);
        }
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.throwInternalError();
    }

    public String getCreateSQL() {
        return getCreateSQL(true, false);
    }

    public String getDropSQL() {
        return null;
    }

    /**
     * Checks that this user has the given rights for this database object.
     *
     * @param table the database object
     * @param rightMask the rights required
     * @throws SQLException if this user does not have the required rights
     */
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
        String tableType = table.getTableType();
        if (Table.VIEW.equals(tableType)) {
            TableView v = (TableView) table;
            if (v.getOwner() == this) {
                // the owner of a view has access:
                // SELECT * FROM (SELECT * FROM ...)
                return;
            }
        } else if (tableType == null) {
            // function table
            return;
        }
        if (!isRightGrantedRecursive(table, rightMask)) {
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                // the owner has all rights on local temporary tables
                return;
            }
            throw Message.getSQLException(ErrorCode.NOT_ENOUGH_RIGHTS_FOR_1, table.getSQL());
        }
    }

    /**
     * Get the CREATE SQL statement for this object.
     *
     * @param password true if the password (actually the salt and hash) should
     *            be returned
     * @param ifNotExists true if IF NOT EXISTS should be used
     * @return the SQL statement
     */
    public String getCreateSQL(boolean password, boolean ifNotExists) {
        StringBuilder buff = new StringBuilder("CREATE USER ");
        if (ifNotExists) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        if (password) {
            buff.append(" SALT '").
                append(ByteUtils.convertBytesToString(salt)).
                append("' HASH '").
                append(ByteUtils.convertBytesToString(passwordHash)).
                append('\'');
        } else {
            buff.append(" PASSWORD ''");
        }
        if (admin) {
            buff.append(" ADMIN");
        }
        return buff.toString();
    }

    /**
     * Check the password of this user.
     *
     * @param userPasswordHash the password data (the user password hash)
     * @return true if the user password hash is correct
     */
    public boolean validateUserPasswordHash(byte[] userPasswordHash) {
        SHA256 sha = new SHA256();
        byte[] hash = sha.getHashWithSalt(userPasswordHash, salt);
        return ByteUtils.compareSecure(hash, passwordHash);
    }

    /**
     * Check if this user has admin rights. An exception is thrown if he does
     * not have them.
     *
     * @throws SQLException if this user is not an admin
     */
    public void checkAdmin() throws SQLException {
        if (!admin) {
            throw Message.getSQLException(ErrorCode.ADMIN_RIGHTS_REQUIRED);
        }
    }

    public int getType() {
        return DbObject.USER;
    }

    public ObjectArray<DbObject> getChildren() {
        ObjectArray<DbObject> children = ObjectArray.newInstance();
        for (Right right : database.getAllRights()) {
            if (right.getGrantee() == this) {
                children.add(right);
            }
        }
        for (Schema schema : database.getAllSchemas()) {
            if (schema.getOwner() == this) {
                children.add(schema);
            }
        }
        return children;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        for (Right right : database.getAllRights()) {
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

    /**
     * Check that this user does not own any schema. An exception is thrown if he
     * owns one or more schemas.
     *
     * @throws SQLException if this user owns a schema
     */
    public void checkOwnsNoSchemas() throws SQLException {
        for (Schema s : database.getAllSchemas()) {
            if (this == s.getOwner()) {
                throw Message.getSQLException(ErrorCode.CANNOT_DROP_2, getName(), s.getName());
            }
        }
    }

}
