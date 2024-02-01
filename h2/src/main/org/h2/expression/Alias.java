/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.SessionLocal;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.ParserUtil;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * A column alias as in SELECT 'Hello' AS NAME ...
 */
public final class Alias extends Expression {

    private final String alias;
    private Expression expr;
    private final boolean aliasColumnName;

    public Alias(Expression expression, String alias, boolean aliasColumnName) {
        this.expr = expression;
        this.alias = alias;
        this.aliasColumnName = aliasColumnName;
    }

    @Override
    public Expression getNonAliasExpression() {
        return expr;
    }

    @Override
    public Value getValue(SessionLocal session) {
        return expr.getValue(session);
    }

    @Override
    public TypeInfo getType() {
        return expr.getType();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        expr.mapColumns(resolver, level, state);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        expr = expr.optimize(session);
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        expr.setEvaluatable(tableFilter, b);
    }

    @Override
    public boolean isIdentity() {
        return expr.isIdentity();
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        expr.getUnenclosedSQL(builder, sqlFlags).append(" AS ");
        return ParserUtil.quoteIdentifier(builder, alias, sqlFlags);
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        expr.updateAggregate(session, stage);
    }

    @Override
    public String getAlias(SessionLocal session, int columnIndex) {
        return alias;
    }

    @Override
    public String getColumnNameForView(SessionLocal session, int columnIndex) {
        return alias;
    }

    @Override
    public int getNullable() {
        return expr.getNullable();
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return expr.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return expr.getCost();
    }

    @Override
    public String getSchemaName() {
        if (aliasColumnName) {
            return null;
        }
        return expr.getSchemaName();
    }

    @Override
    public String getTableName() {
        if (aliasColumnName) {
            return null;
        }
        return expr.getTableName();
    }

    @Override
    public String getColumnName(SessionLocal session, int columnIndex) {
        if (!(expr instanceof ExpressionColumn) || aliasColumnName) {
            return alias;
        }
        return expr.getColumnName(session, columnIndex);
    }

}
