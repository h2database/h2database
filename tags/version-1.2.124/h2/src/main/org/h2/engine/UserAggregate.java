/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

import org.h2.api.AggregateFunction;
import org.h2.command.Parser;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.table.Table;
import org.h2.util.ClassUtils;

/**
 * Represents a user-defined aggregate function.
 */
public class UserAggregate extends DbObjectBase {

    private String className;
    private Class< ? > javaClass;

    public UserAggregate(Database db, int id, String name, String className, boolean force) throws SQLException {
        initDbObjectBase(db, id, name, Trace.FUNCTION);
        this.className = className;
        if (!force) {
            getInstance();
        }
    }

    public AggregateFunction getInstance() throws SQLException {
        if (javaClass == null) {
            javaClass = ClassUtils.loadUserClass(className);
        }
        Object obj;
        try {
            obj = javaClass.newInstance();
            AggregateFunction agg = (AggregateFunction) obj;
            return agg;
        } catch (Exception e) {
            throw Message.convert(e);
        }
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.throwInternalError();
    }

    public String getDropSQL() {
        return "DROP AGGREGATE IF EXISTS " + getSQL();
    }

    public String getCreateSQL() {
        return "CREATE FORCE AGGREGATE " + getSQL() + " FOR " + Parser.quoteIdentifier(className);
    }

    public int getType() {
        return DbObject.AGGREGATE;
    }

    public synchronized void removeChildrenAndResources(Session session) throws SQLException {
        database.removeMeta(session, getId());
        className = null;
        javaClass = null;
        invalidate();
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException("AGGREGATE");
    }

    public String getJavaClassName() {
        return this.className;
    }

}
