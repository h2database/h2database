/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * Represents a domain (user defined data type).
 */
public class UserDataType extends DbObjectBase {

    private Column column;

    public UserDataType(Database database, int id, String name) {
        super(database, id, name, Trace.DATABASE);
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.getInternalError();
    }

    public String getDropSQL() {
        return "DROP DOMAIN IF EXISTS " + getSQL();
    }

    public String getCreateSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append("CREATE DOMAIN ");
        buff.append(getSQL());
        buff.append(" AS ");
        buff.append(column.getCreateSQL());
        return buff.toString();
    }

    public Column getColumn() {
        return column;
    }

    public int getType() {
        return DbObject.USER_DATATYPE;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
    }

    public void checkRename() throws SQLException {
    }

    public void setColumn(Column column) {
        this.column = column;
    }

}
