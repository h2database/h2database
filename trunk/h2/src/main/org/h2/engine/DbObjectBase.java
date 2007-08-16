/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

import org.h2.command.Parser;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.table.Table;
import org.h2.util.ObjectArray;

/**
 * @author Thomas
 */

public abstract class DbObjectBase implements DbObject {
    
    private int id;
    protected Database database;
    protected Trace trace;
    private String objectName;
    private long modificationId;
    private boolean temporary;
    protected String comment;

    protected DbObjectBase(Database database, int id, String name, String traceModule) {
        this.database = database;
        this.trace = database.getTrace(traceModule);
        this.id = id;
        this.objectName = name;
        this.modificationId = database.getModificationMetaId();
    }

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

    public ObjectArray getChildren() {
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

    public abstract String getCreateSQLForCopy(Table table, String quotedName);

    public abstract String getCreateSQL();

    public abstract String getDropSQL();
    
    public abstract int getType();

    public abstract void removeChildrenAndResources(Session session) throws SQLException;

    public abstract void checkRename() throws SQLException;

    protected void invalidate() {
        setModified();
        id = -1;
        database = null;
        trace = null;
        objectName = null;
    }

    public int getHeadPos() {
        return 0;
    }

    public void rename(String newName) throws SQLException {
        checkRename();
        objectName = newName;
        setModified();
    }

    public boolean getTemporary() {
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

    static int getCreateOrder(int type) {
        switch(type) {
        case SETTING:
            return 0;
        case USER:
            return 1;
        case SCHEMA:
            return 2;
        case USER_DATATYPE:
            return 3;
        case SEQUENCE:
            return 4;
        case CONSTANT:
            return 5;
        case FUNCTION_ALIAS:
            return 6;
        case TABLE_OR_VIEW:
            return 7;
        case INDEX:
            return 8;
        case CONSTRAINT:
            return 9;
        case TRIGGER:
            return 10;
        case ROLE:
            return 11;
        case RIGHT:
            return 12;
        case COMMENT:
            return 13;
        default:
            throw Message.getInternalError("type="+type);
        }
    }

}
