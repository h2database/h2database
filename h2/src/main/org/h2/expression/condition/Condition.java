/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.function.CastSpecification;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * Represents a condition returning a boolean value, or NULL.
 */
abstract class Condition extends Expression {

    /**
     * Add a cast around the expression (if necessary) so that the type is boolean.
     *
     * @param session the session
     * @param expression the expression
     * @return the new expression
     */
    static Expression castToBoolean(SessionLocal session, Expression expression) {
        if (expression.getType().getValueType() == Value.BOOLEAN) {
            return expression;
        }
        return new CastSpecification(expression, TypeInfo.TYPE_BOOLEAN);
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_BOOLEAN;
    }

}
