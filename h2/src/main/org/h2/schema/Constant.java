/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.expression.ValueExpression;
import org.h2.message.Trace;
import org.h2.value.Value;

/**
 * A user-defined constant as created by the SQL statement
 * CREATE CONSTANT
 */
public final class Constant extends SchemaObject {

    private Value value;
    private ValueExpression expression;

    public Constant(Schema schema, int id, String name) {
        super(schema, id, name, Trace.SCHEMA);
    }

    @Override
    public String getCreateSQL() {
        StringBuilder builder = new StringBuilder("CREATE CONSTANT ");
        getSQL(builder, DEFAULT_SQL_FLAGS).append(" VALUE ");
        return value.getSQL(builder, DEFAULT_SQL_FLAGS).toString();
    }

    @Override
    public int getType() {
        return DbObject.CONSTANT;
    }

    @Override
    public void removeChildrenAndResources(SessionLocal session) {
        database.removeMeta(session, getId());
        invalidate();
    }

    public void setValue(Value value) {
        this.value = value;
        expression = ValueExpression.get(value);
    }

    public ValueExpression getValue() {
        return expression;
    }

}
