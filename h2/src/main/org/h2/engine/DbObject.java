/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import org.h2.table.Table;
import org.h2.util.ObjectArray;

/**
 * A database object such as a table, an index, or a user.
 */
public interface DbObject {
    int TABLE_OR_VIEW = 0;
    int INDEX = 1;
    int USER = 2;
    int SEQUENCE = 3;
    int TRIGGER = 4;
    int CONSTRAINT = 5;
    int SETTING = 6;
    int ROLE = 7;
    int RIGHT = 8;
    int FUNCTION_ALIAS = 9;
    int SCHEMA = 10;
    int CONSTANT = 11;
    int USER_DATATYPE = 12;
    int COMMENT = 13;
    int AGGREGATE = 14;

    /**
     * Tell the object that is was modified.
     */
    void setModified();

    /**
     * Get the last modification id.
     *
     * @return the modification id
     */
    long getModificationId();

    /**
     * Get the SQL name of this object (may be quoted).
     *
     * @return the SQL name
     */
    String getSQL();

    /**
     * Get the list of dependent children (for tables, this includes indexes and so on).
     *
     * @return the list of children
     */
    ObjectArray getChildren();

    /**
     * Get the database.
     *
     * @return the database
     */
    Database getDatabase();

    /**
     * Get the unique object id.
     *
     * @return the object id
     */
    int getId();

    /**
     * Get the name.
     *
     * @return the name
     */
    String getName();

    /**
     * Construct a CREATE ... SQL statement for this object when creating a copy of it.
     *
     * @param table the new table
     * @param quotedName the quoted name
     * @return the SQL statement
     */
    String getCreateSQLForCopy(Table table, String quotedName);

    /**
     * Construct the original CREATE ... SQL statement for this object.
     *
     * @return the SQL statement
     */
    String getCreateSQL();

    /**
     * Construct a DROP ... SQL statement for this object.
     *
     * @return the SQL statement
     */
    String getDropSQL();

    /**
     * Get the object type.
     *
     * @return the object type
     */
    int getType();

    /**
     * Delete all dependent children objects and resources of this object.
     *
     * @param session the session
     */
    void removeChildrenAndResources(Session session) throws SQLException;

    /**
     * Check if renaming is allowed. Does nothing when allowed.
     *
     * @throws SQLException if renaming is not allowed
     */
    void checkRename() throws SQLException;

    /**
     * Rename the object.
     *
     * @param newName the new name
     */
    void rename(String newName) throws SQLException;

    /**
     * Check if this object is temporary (for example, a temporary table).
     *
     * @return true if is temporary
     */
    boolean getTemporary();

    /**
     * Tell this object that it is temporary or not.
     *
     * @param temporary the new value
     */
    void setTemporary(boolean temporary);

    /**
     * Change the comment of this object.
     *
     * @param comment the new comment, or null for no comment
     */
    void setComment(String comment);

    /**
     * Get the current comment of this object.
     *
     * @return the comment, or null if not set
     */
    String getComment();

    /**
     * Get the position of the head record.
     *
     * @return the head position
     */
    int getHeadPos();
}
