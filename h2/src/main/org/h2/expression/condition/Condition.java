/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.function.Function;
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
    static Expression castToBoolean(Session session, Expression expression) {
        if (expression.getType().getValueType() == Value.BOOLEAN) {
            return expression;
        }
        Function f = Function.getFunctionWithArgs(session.getDatabase(), Function.CAST, expression);
        f.setDataType(TypeInfo.TYPE_BOOLEAN);
        return f;
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_BOOLEAN;
    }

}
