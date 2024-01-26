/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function.table;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.schema.FunctionAlias;

/**
 * This class wraps a user-defined function.
 */
public final class JavaTableFunction extends TableFunction {

    private final FunctionAlias functionAlias;
    private final FunctionAlias.JavaMethod javaMethod;

    public JavaTableFunction(FunctionAlias functionAlias, Expression[] args) {
        super(args);
        this.functionAlias = functionAlias;
        this.javaMethod = functionAlias.findJavaMethod(args);
        if (javaMethod.getDataType() != null) {
            throw DbException.get(ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, getName());
        }
    }

    @Override
    public ResultInterface getValue(SessionLocal session) {
        return javaMethod.getTableValue(session, args, false);
    }

    @Override
    public ResultInterface getValueTemplate(SessionLocal session) {
        return javaMethod.getTableValue(session, args, true);
    }

    @Override
    public void optimize(SessionLocal session) {
        super.optimize(session);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return Expression.writeExpressions(functionAlias.getSQL(builder, sqlFlags).append('('), args, sqlFlags)
                .append(')');
    }

    @Override
    public String getName() {
        return functionAlias.getName();
    }

    @Override
    public boolean isDeterministic() {
        return functionAlias.isDeterministic();
    }

}
