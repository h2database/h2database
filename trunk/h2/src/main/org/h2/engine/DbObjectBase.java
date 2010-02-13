/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import org.h2.command.Parser;
import org.h2.message.Trace;
import org.h2.table.Table;

/**
 * The base class for all database objects.
 */
public abstract class DbObjectBase implements DbObject {

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
     * @param traceModule the trace module name
     */
    protected void initDbObjectBase(Database db, int objectId, String name, String traceModule) {
        this.database = db;
        this.trace = db.getTrace(traceModule);
        this.id = objectId;
        this.objectName = name;
        this.modificationId = db.getModificationMetaId();
    }

    /**
     * Build a SQL statement to re-create the object, or to create a copy of the
     * object with a different name or referencing a different table
     *
     * @param table
     *            the new table name
     * @param quotedName
     *            the new quoted name
     * @return the SQL statement
     */
    public abstract String getCreateSQLForCopy(Table table, String quotedName);

    /**
     * Build a SQL statement to re-create this object.
     *
     * @return the SQL statement
     */
    public abstract String getCreateSQL();

    /**
     * Build a SQL statement to drop this object.
     *
     * @return the SQL statement
     */
    public abstract String getDropSQL();

    /**
     * Get the object type.
     *
     * @return the object type
     */
    public abstract int getType();

    /**
     * Remove all dependent objects and free all resources (files, blocks in
     * files) of this object.
     *
     * @param session the session
     */
    public abstract void removeChildrenAndResources(Session session);

    /**
     * Check if this object can be renamed. System objects may not be renamed.
     */
    public abstract void checkRename();

    public void setModified() {
        this.modificationId = database == null ? -1 : database.getNextModificationMetaId();
    }

    public long getModificationId() {
        return modificationId;
    }

    protected void setObjectName(String name) {
        objectName = name;
    }

    public String getSQL() {
        return Parser.quoteIdentifier(objectName);
    }

    public ArrayList<DbObject> getChildren() {
        return null;
    }

    public Database getDatabase() {
        return database;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return objectName;
    }

    /**
     * Set the main attributes to null to make sure the object is no longer
     * used.
     */
    protected void invalidate() {
        setModified();
        id = -1;
        database = null;
        trace = null;
        objectName = null;
    }

    public void rename(String newName) {
        checkRename();
        objectName = newName;
        setModified();
    }

    public boolean isTemporary() {
        return temporary;
    }

    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    public String toString() {
        return objectName + ":" + id + ":" + super.toString();
    }

}
