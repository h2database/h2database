/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.command.Parser;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Session;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StatementBuilder;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueResultSet;

/**
 * This class wraps a user-defined function.
 */
public class JavaFunction extends Expression implements FunctionCall {

    private FunctionAlias functionAlias;
    private FunctionAlias.JavaMethod javaMethod;
    private Expression[] args;

    public JavaFunction(FunctionAlias functionAlias, Expression[] args) throws SQLException {
        this.functionAlias = functionAlias;
        this.javaMethod = functionAlias.findJavaMethod(args);
        this.args = args;
    }

    public Value getValue(Session session) throws SQLException {
        return javaMethod.getValue(session, args, false);
    }

    public int getType() {
        return javaMethod.getDataType();
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        for (Expression e : args) {
            e.mapColumns(resolver, level);
        }
    }

    public Expression optimize(Session session) throws SQLException {
        boolean allConst = isDeterministic();
        for (int i = 0; i < args.length; i++) {
            Expression e = args[i].optimize(session);
            args[i] = e;
            allConst &= e.isConstant();
        }
        if (allConst) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : args) {
            if (e != null) {
                e.setEvaluatable(tableFilter, b);
            }
        }
    }

    public int getScale() {
        return DataType.getDataType(getType()).defaultScale;
    }

    public long getPrecision() {
        return Integer.MAX_VALUE;
    }

    public int getDisplaySize() {
        return Integer.MAX_VALUE;
    }

    public String getSQL() {
        StatementBuilder buff = new StatementBuilder();
        buff.append(Parser.quoteIdentifier(functionAlias.getName())).append('(');
        for (Expression e : args) {
            buff.appendExceptFirst(", ");
            buff.append(e.getSQL());
        }
        return buff.append(')').toString();
    }

    public void updateAggregate(Session session) throws SQLException {
        for (Expression e : args) {
            if (e != null) {
                e.updateAggregate(session);
            }
        }
    }

    public String getName() {
        return functionAlias.getName();
    }

    public int getParameterCount() {
        return javaMethod.getParameterCount();
    }

    public ValueResultSet getValueForColumnList(Session session, Expression[] args) throws SQLException {
        Value v = javaMethod.getValue(session, args, true);
        return v == ValueNull.INSTANCE ? null : (ValueResultSet) v;
    }

    public Expression[] getArgs() {
        return args;
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch(visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
            if (!isDeterministic()) {
                return false;
            }
            // only if all parameters are deterministic as well
            break;
        case ExpressionVisitor.GET_DEPENDENCIES:
            visitor.addDependency(functionAlias);
            break;
        default:
        }
        for (Expression e : args) {
            if (e != null && !e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    public int getCost() {
        int cost = javaMethod.hasConnectionParam() ? 25 : 5;
        for (Expression e : args) {
            cost += e.getCost();
        }
        return cost;
    }

    public boolean isDeterministic() {
        return functionAlias.isDeterministic();
    }

}
