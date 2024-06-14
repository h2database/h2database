/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.SessionLocal;
import org.h2.expression.condition.Comparison;
import org.h2.index.IndexCondition;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * An expression representing a constant value.
 */
public class ValueExpression extends Operation0 {

    /**
     * The expression represents ValueNull.INSTANCE.
     */
    public static final ValueExpression NULL = new ValueExpression(ValueNull.INSTANCE);

    /**
     * This special expression represents the default value. It is used for
     * UPDATE statements of the form SET COLUMN = DEFAULT. The value is
     * ValueNull.INSTANCE, but should never be accessed.
     */
    public static final ValueExpression DEFAULT = new ValueExpression(ValueNull.INSTANCE);

    /**
     * The expression represents ValueBoolean.TRUE.
     */
    public static final ValueExpression TRUE = new ValueExpression(ValueBoolean.TRUE);

    /**
     * The expression represents ValueBoolean.FALSE.
     */
    public static final ValueExpression FALSE = new ValueExpression(ValueBoolean.FALSE);

    /**
     * The value.
     */
    final Value value;

    ValueExpression(Value value) {
        this.value = value;
    }

    /**
     * Create a new expression with the given value.
     *
     * @param value the value
     * @return the expression
     */
    public static ValueExpression get(Value value) {
        if (value == ValueNull.INSTANCE) {
            return NULL;
        }
        if (value.getValueType() == Value.BOOLEAN) {
            return getBoolean(value.getBoolean());
        }
        return new ValueExpression(value);
    }

    /**
     * Create a new expression with the given boolean value.
     *
     * @param value the boolean value
     * @return the expression
     */
    public static ValueExpression getBoolean(Value value) {
        if (value == ValueNull.INSTANCE) {
            return TypedValueExpression.UNKNOWN;
        }
        return getBoolean(value.getBoolean());
    }

    /**
     * Create a new expression with the given boolean value.
     *
     * @param value the boolean value
     * @return the expression
     */
    public static ValueExpression getBoolean(boolean value) {
        return value ? TRUE : FALSE;
    }

    @Override
    public Value getValue(SessionLocal session) {
        return value;
    }

    @Override
    public TypeInfo getType() {
        return value.getType();
    }

    @Override
    public void createIndexConditions(SessionLocal session, TableFilter filter) {
        if (value.getValueType() == Value.BOOLEAN && !value.getBoolean()) {
            filter.addIndexCondition(IndexCondition.get(Comparison.FALSE, null, this));
        }
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        if (value == ValueNull.INSTANCE) {
            return TypedValueExpression.UNKNOWN;
        }
        return getBoolean(!value.getBoolean());
    }

    @Override
    public TypeInfo getTypeIfStaticallyKnown(SessionLocal session) {
        return value.getType();
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean isNullConstant() {
        return this == NULL;
    }

    @Override
    public boolean isValueSet() {
        return true;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        if (this == DEFAULT) {
            builder.append("DEFAULT");
        } else {
            value.getSQL(builder, sqlFlags);
        }
        return builder;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return true;
    }

    @Override
    public int getCost() {
        return 0;
    }

}
