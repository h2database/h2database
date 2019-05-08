/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Session;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueJson;

/**
 * A format clause such as FORMAT JSON.
 */
public class Format extends Expression {

    /**
     * Supported formats.
     */
    public enum FormatEnum {
        /**
         * JSON.
         */
        JSON;
    }

    private Expression expr;
    private final FormatEnum format;

    public Format(Expression expression, FormatEnum format) {
        this.expr = expression;
        this.format = format;
    }

    @Override
    public Value getValue(Session session) {
        return getValue(expr.getValue(session));
    }

    /**
     * Returns the value with applied format.
     *
     * @param value
     *            the value
     * @return the value with applied format
     */
    public Value getValue(Value value) {
        switch (value.getValueType()) {
        case Value.NULL:
            return ValueJson.NULL;
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
        case Value.CLOB:
            return ValueJson.fromJson(value.getString());
        default:
            return value.convertTo(Value.JSON);
        }
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_JSON;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        expr.mapColumns(resolver, level, state);
    }

    @Override
    public Expression optimize(Session session) {
        expr = expr.optimize(session);
        if (expr.isConstant()) {
            return ValueExpression.get(getValue(session));
        }
        if (expr instanceof Format && format == ((Format) expr).format) {
            return expr;
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        expr.setEvaluatable(tableFilter, b);
    }

    @Override
    public boolean isAutoIncrement() {
        return expr.isAutoIncrement();
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        return expr.getSQL(builder, alwaysQuote).append(" FORMAT ").append(format.name());
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        expr.updateAggregate(session, stage);
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
    public String getTableName() {
        return expr.getTableName();
    }

    @Override
    public String getColumnName() {
        return expr.getColumnName();
    }

    @Override
    public int getSubexpressionCount() {
        return 1;
    }

    @Override
    public Expression getSubexpression(int index) {
        if (index != 0) {
            throw new IndexOutOfBoundsException();
        }
        return expr;
    }

}
