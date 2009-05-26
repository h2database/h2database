/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.table.Table;
import org.h2.util.StringUtils;

/**
 * Represents a database object comment.
 */
public class Comment extends DbObjectBase {

    private final int objectType;
    private final String objectName;
    private String commentText;

    public Comment(Database database, int id, DbObject obj) {
        initDbObjectBase(database, id,  getKey(obj), Trace.DATABASE);
        this.objectType = obj.getType();
        this.objectName = obj.getSQL();
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.throwInternalError();
    }

    private static String getTypeName(int type) {
        switch(type) {
        case DbObject.CONSTANT:
            return "CONSTANT";
        case DbObject.CONSTRAINT:
            return "CONSTRAINT";
        case DbObject.FUNCTION_ALIAS:
            return "ALIAS";
        case DbObject.INDEX:
            return "INDEX";
        case DbObject.ROLE:
            return "ROLE";
        case DbObject.SCHEMA:
            return "SCHEMA";
        case DbObject.SEQUENCE:
            return "SEQUENCE";
        case DbObject.TABLE_OR_VIEW:
            return "TABLE";
        case DbObject.TRIGGER:
            return "TRIGGER";
        case DbObject.USER:
            return "USER";
        case DbObject.USER_DATATYPE:
            return "DOMAIN";
        default:
            // not supported by parser, but required when trying to find a comment
            return "type" + type;
        }
    }

    public String getDropSQL() {
        return null;
    }

    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder("COMMENT ON ");
        buff.append(getTypeName(objectType)).append(' ').append(objectName).append(" IS ");
        if (commentText == null) {
            buff.append("NULL");
        } else {
            buff.append(StringUtils.quoteStringSQL(commentText));
        }
        return buff.toString();
    }

    public int getType() {
        return DbObject.COMMENT;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        database.removeMeta(session, getId());
    }

    public void checkRename() {
        Message.throwInternalError();
    }

    /**
     * Get the comment key name for the given database object. This key name is
     * used internally to associate the comment to the object.
     *
     * @param obj the object
     * @return the key name
     */
    public static String getKey(DbObject obj) {
        return getTypeName(obj.getType()) + " " + obj.getSQL();
    }

    /**
     * Set the comment text.
     *
     * @param comment the text
     */
    public void setCommentText(String comment) {
        this.commentText = comment;
    }

}
