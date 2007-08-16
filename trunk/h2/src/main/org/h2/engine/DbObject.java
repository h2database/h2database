/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import org.h2.table.Table;
import org.h2.util.ObjectArray;

public interface DbObject {
    public static final int INDEX = 1;
    public static final int SEQUENCE = 3;
    public static final int TABLE_OR_VIEW = 0;
    public static final int TRIGGER = 4;
    public static final int USER = 2;
    public static final int CONSTRAINT = 5;
    public static final int FUNCTION_ALIAS = 9;
    public static final int RIGHT = 8;
    public static final int ROLE = 7;
    public static final int SETTING = 6;
    public static final int CONSTANT = 11;
    public static final int SCHEMA = 10;
    public static final int COMMENT = 13;
    public static final int USER_DATATYPE = 12;

    public abstract void setModified();

    public abstract long getModificationId();

    public abstract String getSQL();

    public abstract ObjectArray getChildren();

    public abstract Database getDatabase();

    public abstract int getId();

    public abstract String getName();

    public abstract String getCreateSQLForCopy(Table table, String quotedName);

    public abstract String getCreateSQL();

    public abstract String getDropSQL();

    public abstract int getType();

    public abstract void removeChildrenAndResources(Session session)
            throws SQLException;

    public abstract void checkRename() throws SQLException;

    public abstract void rename(String newName) throws SQLException;

    public abstract boolean getTemporary();

    public abstract void setTemporary(boolean temporary);

    public abstract void setComment(String comment);

    public abstract String getComment();

    public abstract int getHeadPos();
}