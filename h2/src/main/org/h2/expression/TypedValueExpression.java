/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.Objects;

import org.h2.value.DataType;
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
    public static ValueExpression get(Value value, TypeInfo type) {
        return getImpl(value, type, true);
    }

    /**
     * Create a new typed value expression with the given value and type if
     * value is {@code NULL}, or a plain value expression otherwise.
     *
     * @param value
     *            the value
     * @param type
     *            the value type
     * @return the expression
     */
    public static ValueExpression getTypedIfNull(Value value, TypeInfo type) {
        return getImpl(value, type, false);
    }

    private static ValueExpression getImpl(Value value, TypeInfo type, boolean preserveStrictType) {
        if (value == ValueNull.INSTANCE) {
            switch (type.getValueType()) {
            case Value.NULL:
                return ValueExpression.NULL;
            case Value.BOOLEAN:
                return UNKNOWN;
            }
            return new TypedValueExpression(value, type);
        }
        if (preserveStrictType) {
            DataType dt = DataType.getDataType(type.getValueType());
            TypeInfo vt = value.getType();
            if (dt.supportsPrecision && type.getPrecision() != vt.getPrecision()
                    || dt.supportsScale && type.getScale() != vt.getScale()
                    || !Objects.equals(type.getExtTypeInfo(), vt.getExtTypeInfo())) {
                return new TypedValueExpression(value, type);
            }
        }
        return ValueExpression.get(value);
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
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        if (this == UNKNOWN) {
            builder.append("UNKNOWN");
        } else {
            value.getSQL(builder.append("CAST("), sqlFlags | NO_CASTS).append(" AS ");
            type.getSQL(builder, sqlFlags).append(')');
        }
        return builder;
    }

    @Override
    public boolean isNullConstant() {
        return value == ValueNull.INSTANCE;
    }

}
