/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.table.Column;
import org.h2.util.DateTimeTemplate;
import org.h2.util.HasSQL;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * A cast specification.
 */
public final class CastSpecification extends Function1_2 {

    private Domain domain;

    public CastSpecification(Expression arg, Column column, Expression template) {
        super(arg, template);
        type = column.getType();
        domain = column.getDomain();
    }

    public CastSpecification(Expression arg, Column column) {
        super(arg, null);
        type = column.getType();
        domain = column.getDomain();
    }

    public CastSpecification(Expression arg, TypeInfo type) {
        super(arg, null);
        this.type = type;
    }

    @Override
    protected Value getValue(SessionLocal session, Value v1, Value v2) {
        if (v2 != null) {
            v1 = getValueWithTemplate(v1, v2, session);
        }
        v1 = v1.castTo(type, session);
        if (domain != null) {
            domain.checkConstraints(session, v1);
        }
        return v1;
    }

    private Value getValueWithTemplate(Value v, Value template, SessionLocal session) {
        if (v == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        int valueType = v.getValueType();
        if (DataType.isDateTimeType(valueType)) {
            if (DataType.isCharacterStringType(type.getValueType())) {
                return ValueVarchar.get(DateTimeTemplate.of(template.getString()).format(v), session);
            }
        } else if (DataType.isCharacterStringType(valueType)) {
            if (DataType.isDateTimeType(type.getValueType())) {
                return DateTimeTemplate.of(template.getString()).parse(v.getString(), type, session);
            }
        }
        throw DbException.getUnsupportedException(
                type.getSQL(v.getType().getSQL(new StringBuilder("CAST with template from "), HasSQL.TRACE_SQL_FLAGS)
                        .append(" to "), HasSQL.DEFAULT_SQL_FLAGS).toString());
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        if (right != null) {
            right = right.optimize(session);
        }
        if (left.isConstant() && (right == null || right.isConstant())) {
            Value v = getValue(session);
            if (v == ValueNull.INSTANCE || canOptimizeCast(left.getType().getValueType(), type.getValueType())) {
                return TypedValueExpression.get(v, type);
            }
        }
        return this;
    }

    @Override
    public TypeInfo getTypeIfStaticallyKnown(SessionLocal session) {
        return type;
    }

    @Override
    public boolean isConstant() {
        return left instanceof ValueExpression && (right == null || right.isConstant())
                && canOptimizeCast(left.getType().getValueType(), type.getValueType());
    }

    private static boolean canOptimizeCast(int src, int dst) {
        switch (src) {
        case Value.TIME:
            switch (dst) {
            case Value.TIME_TZ:
            case Value.TIMESTAMP:
            case Value.TIMESTAMP_TZ:
                return false;
            }
            break;
        case Value.TIME_TZ:
            switch (dst) {
            case Value.TIME:
            case Value.TIMESTAMP:
            case Value.TIMESTAMP_TZ:
                return false;
            }
            break;
        case Value.DATE:
            if (dst == Value.TIMESTAMP_TZ) {
                return false;
            }
            break;
        case Value.TIMESTAMP:
            switch (dst) {
            case Value.TIME_TZ:
            case Value.TIMESTAMP_TZ:
                return false;
            }
            break;
        case Value.TIMESTAMP_TZ:
            switch (dst) {
            case Value.TIME:
            case Value.DATE:
            case Value.TIMESTAMP:
                return false;
            }
        }
        return true;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        builder.append("CAST(");
        left.getUnenclosedSQL(builder, left instanceof ValueExpression ? sqlFlags | NO_CASTS : sqlFlags) //
                .append(" AS ");
        (domain != null ? domain : type).getSQL(builder, sqlFlags);
        if (right != null) {
            right.getSQL(builder.append(" FORMAT "), sqlFlags);
        }
        return builder.append(')');
    }

    @Override
    public String getName() {
        return "CAST";
    }

}
