/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;

import org.h2.command.ParserBase;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.table.Table;
import org.h2.util.HasSQL;
import org.h2.util.ParserUtil;

/**
 * A database object such as a table, an index, or a user.
 */
public abstract class DbObject implements HasSQL {

    /**
     * The object is of the type table or view.
     */
    public static final int TABLE_OR_VIEW = 0;

    /**
     * This object is an index.
     */
    public static final int INDEX = 1;

    /**
     * This object is a user.
     */
    public static final int USER = 2;

    /**
     * This object is a sequence.
     */
    public static final int SEQUENCE = 3;

    /**
     * This object is a trigger.
     */
    public static final int TRIGGER = 4;

    /**
     * This object is a constraint (check constraint, unique constraint, or
     * referential constraint).
     */
    public static final int CONSTRAINT = 5;

    /**
     * This object is a setting.
     */
    public static final int SETTING = 6;

    /**
     * This object is a role.
     */
    public static final int ROLE = 7;

    /**
     * This object is a right.
     */
    public static final int RIGHT = 8;

    /**
     * This object is an alias for a Java function.
     */
    public static final int FUNCTION_ALIAS = 9;

    /**
     * This object is a schema.
     */
    public static final int SCHEMA = 10;

    /**
     * This object is a constant.
     */
    public static final int CONSTANT = 11;

    /**
     * This object is a domain.
     */
    public static final int DOMAIN = 12;

    /**
     * This object is a comment.
     */
    public static final int COMMENT = 13;

    /**
     * This object is a user-defined aggregate function.
     */
    public static final int AGGREGATE = 14;

    /**
     * This object is a synonym.
     */
    public static final int SYNONYM = 15;

    /**
     * The database.
     */
    protected Database database;

    /**
     * The trace module.
     */
    protected Trace trace;

    /**
     * The comment (if set).
     */
    protected String comment;

    private int id;

    private String objectName;

    private long modificationId;

    private boolean temporary;

    /**
     * Initialize some attributes of this object.
     *
     * @param db the database
     * @param objectId the object id
     * @param name the name
     * @param traceModuleId the trace module id
     */
    protected DbObject(Database db, int objectId, String name, int traceModuleId) {
        this.database = db;
        this.trace = db.getTrace(traceModuleId);
        this.id = objectId;
        this.objectName = name;
        this.modificationId = db.getModificationMetaId();
    }

    /**
     * Tell the object that is was modified.
     */
    public final void setModified() {
        this.modificationId = database == null ? -1 : database.getNextModificationMetaId();
    }

    public final long getModificationId() {
        return modificationId;
    }

    protected final void setObjectName(String name) {
        objectName = name;
    }

    @Override
    public String getSQL(int sqlFlags) {
        return ParserBase.quoteIdentifier(objectName, sqlFlags);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return ParserUtil.quoteIdentifier(builder, objectName, sqlFlags);
    }

    /**
     * Get the list of dependent children (for tables, this includes indexes and
     * so on).
     *
     * @return the list of children, or {@code null}
     */
    public ArrayList<DbObject> getChildren() {
        return null;
    }

    /**
     * Get the database.
     *
     * @return the database
     */
    public final Database getDatabase() {
        return database;
    }

    /**
     * Get the unique object id.
     *
     * @return the object id
     */
    public final int getId() {
        return id;
    }

    /**
     * Get the name.
     *
     * @return the name
     */
    public final String getName() {
        return objectName;
    }

    /**
     * Set the main attributes to null to make sure the object is no longer
     * used.
     */
    protected void invalidate() {
        if (id == -1) {
            throw DbException.getInternalError();
        }
        setModified();
        id = -1;
        database = null;
        trace = null;
        objectName = null;
    }

    public final boolean isValid() {
        return id != -1;
    }

    /**
     * Build a SQL statement to re-create the object, or to create a copy of the
     * object with a different name or referencing a different table
     *
     * @param table the new table
     * @param quotedName the quoted name
     * @return the SQL statement
     */
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.getInternalError(toString());
    }

    /**
     * Construct the CREATE ... SQL statement for this object for meta table.
     *
     * @return the SQL statement
     */
    public String getCreateSQLForMeta() {
        return getCreateSQL();
    }

    /**
     * Construct the CREATE ... SQL statement for this object.
     *
     * @return the SQL statement
     */
    public abstract String getCreateSQL();

    /**
     * Construct a DROP ... SQL statement for this object.
     *
     * @return the SQL statement
     */
    public String getDropSQL() {
        return null;
    }

    /**
     * Get the object type.
     *
     * @return the object type
     */
    public abstract int getType();

    /**
     * Delete all dependent children objects and resources of this object.
     *
     * @param session the session
     */
    public abstract void removeChildrenAndResources(SessionLocal session);

    /**
     * Check if renaming is allowed. Does nothing when allowed.
     */
    public void checkRename() {
        // Allowed by default
    }

    /**
     * Rename the object.
     *
     * @param newName the new name
     */
    public void rename(String newName) {
        checkRename();
        objectName = newName;
        setModified();
    }

    /**
     * Check if this object is temporary (for example, a temporary table).
     *
     * @return true if is temporary
     */
    public boolean isTemporary() {
        return temporary;
    }

    /**
     * Tell this object that it is temporary or not.
     *
     * @param temporary the new value
     */
    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    /**
     * Change the comment of this object.
     *
     * @param comment the new comment, or null for no comment
     */
    public void setComment(String comment) {
        this.comment = comment != null && !comment.isEmpty() ? comment : null;
    }

    /**
     * Get the current comment of this object.
     *
     * @return the comment, or null if not set
     */
    public String getComment() {
        return comment;
    }

    @Override
    public String toString() {
        return objectName + ":" + id + ":" + super.toString();
    }

}
