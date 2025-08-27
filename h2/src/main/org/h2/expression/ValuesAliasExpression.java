/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.SessionLocal;
import org.h2.mode.ValuesAliasResolver;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.HasSQL;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Expression that represents a column reference through VALUES alias.
 * Used in MySQL 8.0.19+ style INSERT ... VALUES ... AS alias ON DUPLICATE KEY UPDATE syntax.
 */
public final class ValuesAliasExpression extends Expression {

    private final ValuesAliasResolver resolver;
    private final Column column;

    /**
     * Creates a new VALUES alias expression.
     *
     * @param resolver the VALUES alias resolver
     * @param column the column
     */
    public ValuesAliasExpression(ValuesAliasResolver resolver, Column column) {
        this.resolver = resolver;
        this.column = column;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value value = resolver.getValue(column);
        if (value == null) {
            // Return NULL if value is not available
            return ValueNull.INSTANCE;
        }
        return value;
    }

    @Override
    public TypeInfo getType() {
        return column.getType();
    }

    @Override
    public void mapColumns(ColumnResolver columnResolver, int level, int state) {
        // Already resolved, no mapping needed
    }

    @Override
    public Expression optimize(SessionLocal session) {
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        // Not applicable for VALUES alias expressions
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        // Not applicable for VALUES alias expressions
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        // Convert alias.column to VALUES(column) for SQL generation
        return column.getSQL(builder.append("VALUES("), sqlFlags).append(')');
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
            return false;
        }
        return true;
    }

    @Override
    public int getCost() {
        return 1;
    }

    /**
     * Get the VALUES alias resolver.
     *
     * @return the resolver
     */
    public ValuesAliasResolver getResolver() {
        return resolver;
    }

    /**
     * Get the column.
     *
     * @return the column
     */
    public Column getColumn() {
        return column;
    }

}
