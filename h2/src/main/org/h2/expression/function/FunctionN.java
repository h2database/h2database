/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.expression.Expression;
import org.h2.expression.OperationN;

/**
 * Function with many arguments.
 */
public abstract class FunctionN extends OperationN implements NamedExpression {

    protected FunctionN(Expression[] args) {
        super(args);
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return writeExpressions(builder.append(getName()).append('('), args, sqlFlags).append(')');
    }

}
