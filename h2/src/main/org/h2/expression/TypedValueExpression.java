/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * An expression representing a constant value with a type cast.
 */
public class TypedValueExpression extends ValueExpression {

    /**
     * The expression represents the SQL UNKNOWN value.
     */
    public static final TypedValueExpression UNKNOWN = new TypedValueExpression(ValueNull.INSTANCE,
            TypeInfo.TYPE_BOOLEAN);

    /**
     * Create a new expression with the given value and type.
     *
     * @param value
     *            the value
     * @param type
     *            the value type
     * @return the expression
     */
    public static TypedValueExpression get(Value value, TypeInfo type) {
        if (value == ValueNull.INSTANCE && type.getValueType() == Value.BOOLEAN) {
            return UNKNOWN;
        }
        return new TypedValueExpression(value, type);
    }

    private final TypeInfo type;

    private TypedValueExpression(Value value, TypeInfo type) {
        super(value);
        this.type = type;
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if (this == UNKNOWN) {
            builder.append("UNKNOWN");
        } else {
            value.getSQL(builder.append("CAST("), sqlFlags).append(" AS ");
            type.getSQL(builder).append(')');
        }
        return builder;
    }

    @Override
    public boolean isNullConstant() {
        return value == ValueNull.INSTANCE;
    }

}
