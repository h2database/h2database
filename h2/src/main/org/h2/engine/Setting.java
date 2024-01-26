/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.message.DbException;
import org.h2.message.Trace;

/**
 * A persistent database setting.
 */
public final class Setting extends DbObject {

    private int intValue;
    private String stringValue;

    public Setting(Database database, int id, String settingName) {
        super(database, id, settingName, Trace.SETTING);
    }

    @Override
    public String getSQL(int sqlFlags) {
        return getName();
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return builder.append(getName());
    }

    public void setIntValue(int value) {
        intValue = value;
    }

    public int getIntValue() {
        return intValue;
    }

    public void setStringValue(String value) {
        stringValue = value;
    }

    public String getStringValue() {
        return stringValue;
    }

    @Override
    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder("SET ");
        getSQL(buff, DEFAULT_SQL_FLAGS).append(' ');
        if (stringValue != null) {
            buff.append(stringValue);
        } else {
            buff.append(intValue);
        }
        return buff.toString();
    }

    @Override
    public int getType() {
        return DbObject.SETTING;
    }

    @Override
    public void removeChildrenAndResources(SessionLocal session) {
        database.removeMeta(session, getId());
        invalidate();
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("RENAME");
    }

}
