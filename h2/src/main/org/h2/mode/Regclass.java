/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ValueExpression;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;

/**
 * A ::regclass expression.
 */
public class Regclass extends Expression {

    private Expression expr;

    public Regclass(Expression expression) {
        this.expr = expression;
    }

    @Override
    public Value getValue(Session session) {
        Value value = expr.getValue(session);
        if (value == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        int valueType = value.getValueType();
        if (valueType >= Value.TINYINT && valueType <= Value.INTEGER) {
            return value.convertToInt(null);
        }
        if (valueType == Value.BIGINT) {
            return ValueInteger.get((int) value.getLong());
        }
        String name = value.getString();
        ArrayList<Table> tables = session.getDatabase().getAllTablesAndViews(true);
        ArrayList<Table> tempTables = session.getLocalTempTables();
        tables.addAll(tempTables);
        for (Table table : tables) {
            if (table.isHidden()) {
                continue;
            }
            if (table.getName().equals(name)) {
                return ValueInteger.get(table.getId());
            }
            ArrayList<Index> indexes = table.getIndexes();
            if (indexes != null) {
                for (Index index : indexes) {
                    if (index.getCreateSQL() == null) {
                        continue;
                    }
                    if (index.getName().equals(name)) {
                        return ValueInteger.get(index.getId());
                    }
                }
            }
        }
        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, name);
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_INTEGER;
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
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        expr.setEvaluatable(tableFilter, b);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return expr.getSQL(builder, sqlFlags).append("::REGCLASS");
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        expr.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return expr.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return expr.getCost() + 100;
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
