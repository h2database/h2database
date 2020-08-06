/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.expression.Expression;
import org.h2.expression.Operation2;

/**
 * Function with two arguments.
 */
public abstract class Function2 extends Operation2 implements NamedExpression {

    protected Function2(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        left.getUnenclosedSQL(builder.append(getName()).append('('), sqlFlags).append(", ");
        return right.getUnenclosedSQL(builder, sqlFlags).append(')');
    }

}
