/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import org.h2.table.Table;
import org.h2.util.ObjectArray;

public interface DbObject {
    int INDEX = 1;
    int SEQUENCE = 3;
    int TABLE_OR_VIEW = 0;
    int TRIGGER = 4;
    int USER = 2;
    int CONSTRAINT = 5;
    int FUNCTION_ALIAS = 9;
    int RIGHT = 8;
    int ROLE = 7;
    int SETTING = 6;
    int CONSTANT = 11;
    int SCHEMA = 10;
    int COMMENT = 13;
    int USER_DATATYPE = 12;
    int AGGREGATE = 14;

    void setModified();

    long getModificationId();

    String getSQL();

    ObjectArray getChildren();

    Database getDatabase();

    int getId();

    String getName();

    String getCreateSQLForCopy(Table table, String quotedName);

    String getCreateSQL();

    String getDropSQL();

    int getType();

    void removeChildrenAndResources(Session session) throws SQLException;

    void checkRename() throws SQLException;

    void rename(String newName) throws SQLException;

    boolean getTemporary();

    void setTemporary(boolean temporary);

    void setComment(String comment);

    String getComment();

    int getHeadPos();
}
