/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.SessionLocal;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueJson;

/**
 * A format clause such as FORMAT JSON.
 */
public final class Format extends Operation1 {

    /**
     * Supported formats.
     */
    public enum FormatEnum {
        /**
         * JSON.
         */
        JSON;
    }

    private final FormatEnum format;

    public Format(Expression arg, FormatEnum format) {
        super(arg);
        this.format = format;
    }

    @Override
    public Value getValue(SessionLocal session) {
        return getValue(arg.getValue(session));
    }

    /**
     * Returns the value with applied format.
     *
     * @param value
     *            the value
     * @return the value with applied format
     */
    public Value getValue(Value value) {
        switch (value.getValueType()) {
        case Value.NULL:
            return ValueJson.NULL;
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.CHAR:
        case Value.CLOB:
            return ValueJson.fromJson(value.getString());
        default:
            return value.convertTo(TypeInfo.TYPE_JSON);
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        arg = arg.optimize(session);
        if (arg.isConstant()) {
            return ValueExpression.get(getValue(session));
        }
        if (arg instanceof Format && format == ((Format) arg).format) {
            return arg;
        }
        type = TypeInfo.TYPE_JSON;
        return this;
    }

    @Override
    public boolean isIdentity() {
        return arg.isIdentity();
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return arg.getSQL(builder, sqlFlags, AUTO_PARENTHESES).append(" FORMAT ").append(format.name());
    }

    @Override
    public int getNullable() {
        return arg.getNullable();
    }

    @Override
    public String getTableName() {
        return arg.getTableName();
    }

    @Override
    public String getColumnName(SessionLocal session, int columnIndex) {
        return arg.getColumnName(session, columnIndex);
    }

}
