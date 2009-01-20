/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;

/**
 * A column alias as in SELECT 'Hello' AS NAME ...
 */
public class Alias extends Expression {

    private final String alias;
    private Expression expr;
    private boolean aliasColumnName;

    public Alias(Expression expression, String alias, boolean aliasColumnName) {
        this.expr = expression;
        this.alias = alias;
        this.aliasColumnName = aliasColumnName;
    }

    public Expression getNonAliasExpression() {
        return expr;
    }

    public Value getValue(Session session) throws SQLException {
        return expr.getValue(session);
    }

    public int getType() {
        return expr.getType();
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        expr.mapColumns(resolver, level);
    }

    public Expression optimize(Session session) throws SQLException {
        expr = expr.optimize(session);
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        expr.setEvaluatable(tableFilter, b);
    }

    public int getScale() {
        return expr.getScale();
    }

    public long getPrecision() {
        return expr.getPrecision();
    }

    public int getDisplaySize() {
        return expr.getDisplaySize();
    }

    public boolean isAutoIncrement() {
        return expr.isAutoIncrement();
    }

    public String getSQL() {
        return expr.getSQL() + " AS " + Parser.quoteIdentifier(alias);
    }

    public void updateAggregate(Session session) throws SQLException {
        expr.updateAggregate(session);
    }

    public String getAlias() {
        return alias;
    }

    public int getNullable() {
        return expr.getNullable();
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        return expr.isEverything(visitor);
    }

    public int getCost() {
        return expr.getCost();
    }

    public String getTableName() {
        if (aliasColumnName) {
            return super.getTableName();
        }
        return expr.getTableName();
    }

    public String getColumnName() {
        if (!(expr instanceof ExpressionColumn) || aliasColumnName) {
            return super.getColumnName();
        }
        return expr.getColumnName();
    }

}
