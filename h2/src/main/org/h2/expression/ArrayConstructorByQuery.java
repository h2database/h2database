/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.query.Query;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 * Array value constructor by query.
 */
public final class ArrayConstructorByQuery extends Expression {

    /**
     * The subquery.
     */
    private final Query query;

    private TypeInfo componentType, type;

    /**
     * Creates new instance of array value constructor by query.
     *
     * @param query
     *            the query
     */
    public ArrayConstructorByQuery(Query query) {
        this.query = query;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return StringUtils.indent(builder.append("ARRAY ("), query.getPlanSQL(sqlFlags), 4, false).append(')');
    }

    @Override
    public Value getValue(SessionLocal session) {
        query.setSession(session);
        ArrayList<Value> values = new ArrayList<>();
        try (ResultInterface result = query.query(0)) {
            while (result.next()) {
                values.add(result.currentRow()[0]);
            }
        }
        return ValueArray.get(componentType, values.toArray(new Value[0]), session);
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        query.mapColumns(resolver, level + 1);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        query.prepare();
        if (query.getColumnCount() != 1) {
            throw DbException.get(ErrorCode.SUBQUERY_IS_NOT_SINGLE_COLUMN);
        }
        componentType = query.getExpressions().get(0).getType();
        type = TypeInfo.getTypeInfo(Value.ARRAY, -1L, -1, componentType);
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        query.setEvaluatable(tableFilter, value);
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        query.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return query.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return query.getCostAsExpression();
    }

}
