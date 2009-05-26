/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import java.sql.SQLException;

import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.expression.ValueExpression;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.table.Table;
import org.h2.value.Value;

/**
 * A user-defined constant as created by the SQL statement
 * CREATE CONSTANT
 */
public class Constant extends SchemaObjectBase {

    private Value value;
    private ValueExpression expression;

    public Constant(Schema schema, int id, String name) {
        initSchemaObjectBase(schema, id, name, Trace.SCHEMA);
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.throwInternalError();
    }

    public String getDropSQL() {
        return null;
    }

    public String getCreateSQL() {
        return "CREATE CONSTANT " + getSQL() + " VALUE " + value.getSQL();
    }

    public int getType() {
        return DbObject.CONSTANT;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        database.removeMeta(session, getId());
        invalidate();
    }

    public void checkRename() {
        // ok
    }

    public void setValue(Value value) {
        this.value = value;
        expression = ValueExpression.get(value);
    }

    public ValueExpression getValue() {
        return expression;
    }

}
