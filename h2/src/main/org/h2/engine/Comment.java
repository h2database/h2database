/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.table.Table;
import org.h2.util.StringUtils;

public class Comment extends DbObjectBase {

    private final int objectType;
    private final String objectName;
    private String commentText;

    public Comment(Database database, int id, DbObject obj) {
        super(database, id,  getKey(obj), Trace.DATABASE);
        this.objectType = obj.getType();
        this.objectName = obj.getSQL();
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.getInternalError();
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
        StringBuffer buff = new StringBuffer();
        buff.append("COMMENT ON ");
        buff.append(getTypeName(objectType));
        buff.append(' ');
        buff.append(objectName);
        buff.append(" IS ");
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
    }

    public void checkRename() throws SQLException {
        throw Message.getInternalError();
    }

    public static String getKey(DbObject obj) {
        return getTypeName(obj.getType()) + " " + obj.getSQL();
    }

    public void setCommentText(String comment) {
        this.commentText = comment;
    }

}
