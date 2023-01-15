/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.h2.api.ErrorCode;
import org.h2.command.query.Query;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.ExtTypeInfoRow;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * A query returning a single value.
 * Subqueries are used inside other statements.
 */
public final class Subquery extends Expression {

    private final Query query;

    private Expression expression;

    private Value nullValue;

    private HashSet<ColumnResolver> outerResolvers = new HashSet<>();

    public Subquery(Query query) {
        this.query = query;
    }

    @Override
    public Value getValue(SessionLocal session) {
        query.setSession(session);
        try (ResultInterface result = query.query(2)) {
            Value v;
            if (!result.next()) {
                return nullValue;
            } else {
                v = readRow(result);
                if (result.hasNext()) {
                    throw DbException.get(ErrorCode.SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW);
                }
            }
            return v;
        }
    }

    /**
     * Evaluates and returns all rows of the subquery.
     *
     * @param session
     *            the session
     * @return values in all rows
     */
    public ArrayList<Value> getAllRows(SessionLocal session) {
        ArrayList<Value> list = new ArrayList<>();
        query.setSession(session);
        try (ResultInterface result = query.query(Integer.MAX_VALUE)) {
            while (result.next()) {
                list.add(readRow(result));
            }
        }
        return list;
    }

    private Value readRow(ResultInterface result) {
        Value[] values = result.currentRow();
        int visible = result.getVisibleColumnCount();
        return visible == 1 ? values[0]
                : ValueRow.get(getType(), visible == values.length ? values : Arrays.copyOf(values, visible));
    }

    @Override
    public TypeInfo getType() {
        return expression.getType();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        outerResolvers.add(resolver);
        query.mapColumns(resolver, level + 1);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        query.prepare();
        if (query.isConstantQuery()) {
            setType();
            return ValueExpression.get(getValue(session));
        }
        if (outerResolvers != null && session.getDatabase().getSettings().optimizeSimpleSingleRowSubqueries) {
            Expression e = query.getIfSingleRow();
            if (e != null && e.isEverything(ExpressionVisitor.getDecrementQueryLevelVisitor(outerResolvers, 0))) {
                e.isEverything(ExpressionVisitor.getDecrementQueryLevelVisitor(outerResolvers, 1));
                return e.optimize(session);
            }
        }
        outerResolvers = null;
        setType();
        return this;
    }

    private void setType() {
        ArrayList<Expression> expressions = query.getExpressions();
        int columnCount = query.getColumnCount();
        if (columnCount == 1) {
            expression = expressions.get(0);
            nullValue = ValueNull.INSTANCE;
        } else {
            Expression[] list = new Expression[columnCount];
            Value[] nulls = new Value[columnCount];
            for (int i = 0; i < columnCount; i++) {
                list[i] = expressions.get(i);
                nulls[i] = ValueNull.INSTANCE;
            }
            ExpressionList expressionList = new ExpressionList(list, false);
            expressionList.initializeType();
            expression = expressionList;
            nullValue = ValueRow.get(new ExtTypeInfoRow(list), nulls);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        query.setEvaluatable(tableFilter, b);
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return builder.append('(').append(query.getPlanSQL(sqlFlags)).append(')');
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        query.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return query.isEverything(visitor);
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public int getCost() {
        return query.getCostAsExpression();
    }

    @Override
    public boolean isConstant() {
        return query.isConstantQuery();
    }

}
