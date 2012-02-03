/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import java.util.Arrays;
import org.h2.constant.ErrorCode;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.schema.Schema;
import org.h2.security.SHA256;
import org.h2.table.MetaTable;
import org.h2.table.RangeTable;
import org.h2.table.Table;
import org.h2.table.TableView;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

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
            salt = new byte[Constants.SALT_LEN];
            MathUtils.randomBytes(salt);
            SHA256 sha = new SHA256();
            this.passwordHash = sha.getHashWithSalt(userPasswordHash, salt);
        }
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError();
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
    public void checkRight(Table table, int rightMask) {
        if (!hasRight(table, rightMask)) {
            throw DbException.get(ErrorCode.NOT_ENOUGH_RIGHTS_FOR_1, table.getSQL());
        }
    }

    /**
     * See if this user has the given rights for this database object.
     *
     * @param table the database object
     * @param rightMask the rights required
     * @return true if the user has the rights
     */
    public boolean hasRight(Table table, int rightMask) {
        if (rightMask != Right.SELECT && !systemUser) {
            table.checkWritingAllowed();
        }
        if (admin) {
            return true;
        }
        Role publicRole = database.getPublicRole();
        if (publicRole.isRightGrantedRecursive(table, rightMask)) {
            return true;
        }
        if (table instanceof MetaTable || table instanceof RangeTable) {
            // everybody has access to the metadata information
            return true;
        }
        String tableType = table.getTableType();
        if (Table.VIEW.equals(tableType)) {
            TableView v = (TableView) table;
            if (v.getOwner() == this) {
                // the owner of a view has access:
                // SELECT * FROM (SELECT * FROM ...)
                return true;
            }
        } else if (tableType == null) {
            // function table
            return true;
        }
        if (table.isTemporary() && !table.isGlobalTemporary()) {
            // the owner has all rights on local temporary tables
            return true;
        }
        if (isRightGrantedRecursive(table, rightMask)) {
            return true;
        }
        return false;
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
                append(Utils.convertBytesToString(salt)).
                append("' HASH '").
                append(Utils.convertBytesToString(passwordHash)).
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
        return Utils.compareSecure(hash, passwordHash);
    }

    /**
     * Check if this user has admin rights. An exception is thrown if he does
     * not have them.
     *
     * @throws SQLException if this user is not an admin
     */
    public void checkAdmin() {
        if (!admin) {
            throw DbException.get(ErrorCode.ADMIN_RIGHTS_REQUIRED);
        }
    }

    public int getType() {
        return DbObject.USER;
    }

    public ArrayList<DbObject> getChildren() {
        ArrayList<DbObject> children = New.arrayList();
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

    public void removeChildrenAndResources(Session session) {
        for (Right right : database.getAllRights()) {
            if (right.getGrantee() == this) {
                database.removeDatabaseObject(session, right);
            }
        }
        database.removeMeta(session, getId());
        salt = null;
        Arrays.fill(passwordHash, (byte) 0);
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
    public void checkOwnsNoSchemas() {
        for (Schema s : database.getAllSchemas()) {
            if (this == s.getOwner()) {
                throw DbException.get(ErrorCode.CANNOT_DROP_2, getName(), s.getName());
            }
        }
    }

}
