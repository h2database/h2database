/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;

/**
 * Function with one optional argument.
 */
public abstract class Function0_1 extends Expression implements NamedExpression {

    /**
     * The argument of the operation.
     */
    protected Expression arg;

    /**
     * The type of the result.
     */
    protected TypeInfo type;

    protected Function0_1(Expression arg) {
        this.arg = arg;
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        if (arg != null) {
            arg.mapColumns(resolver, level, state);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        if (arg != null) {
            arg.setEvaluatable(tableFilter, value);
        }
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        if (arg != null) {
            arg.updateAggregate(session, stage);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return arg == null || arg.isEverything(visitor);
    }

    @Override
    public int getCost() {
        int cost = 1;
        if (arg != null) {
            cost += arg.getCost();
        }
        return cost;
    }

    @Override
    public int getSubexpressionCount() {
        return arg != null ? 1 : 0;
    }

    @Override
    public Expression getSubexpression(int index) {
        if (index == 0 && arg != null) {
            return arg;
        }
        throw new IndexOutOfBoundsException();
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        builder.append(getName()).append('(');
        if (arg != null) {
            arg.getUnenclosedSQL(builder, sqlFlags);
        }
        return builder.append(')');
    }

}
