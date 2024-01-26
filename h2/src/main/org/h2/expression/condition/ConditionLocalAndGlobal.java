/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * A global condition or combination of local and global conditions. May be used
 * only as a top-level expression in a WHERE, HAVING, or QUALIFY clause of a
 * SELECT.
 */
public class ConditionLocalAndGlobal extends Condition {

    private Expression local, global;

    public ConditionLocalAndGlobal(Expression local, Expression global) {
        if (global == null) {
            throw DbException.getInternalError();
        }
        this.local = local;
        this.global = global;
    }

    @Override
    public boolean needParentheses() {
        return local != null || global.needParentheses();
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        if (local == null) {
            return global.getUnenclosedSQL(builder, sqlFlags);
        }
        local.getSQL(builder, sqlFlags, AUTO_PARENTHESES);
        builder.append("\n    _LOCAL_AND_GLOBAL_ ");
        return global.getSQL(builder, sqlFlags, AUTO_PARENTHESES);
    }

    @Override
    public void createIndexConditions(SessionLocal session, TableFilter filter) {
        if (local != null) {
            local.createIndexConditions(session, filter);
        }
        global.createIndexConditions(session, filter);
    }

    @Override
    public Value getValue(SessionLocal session) {
        if (local == null) {
            return global.getValue(session);
        }
        Value l = local.getValue(session), r;
        if (l.isFalse() || (r = global.getValue(session)).isFalse()) {
            return ValueBoolean.FALSE;
        }
        if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.TRUE;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        global = global.optimize(session);
        if (local != null) {
            local = local.optimize(session);
            Expression e = ConditionAndOr.optimizeIfConstant(session, ConditionAndOr.AND, local, global);
            if (e != null) {
                return e;
            }
        }
        return this;
    }

    @Override
    public void addFilterConditions(TableFilter filter) {
        if (local != null) {
            local.addFilterConditions(filter);
        }
        global.addFilterConditions(filter);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        if (local != null) {
            local.mapColumns(resolver, level, state);
        }
        global.mapColumns(resolver, level, state);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (local != null) {
            local.setEvaluatable(tableFilter, b);
        }
        global.setEvaluatable(tableFilter, b);
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        if (local != null) {
            local.updateAggregate(session, stage);
        }
        global.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return (local == null || local.isEverything(visitor)) && global.isEverything(visitor);
    }

    @Override
    public int getCost() {
        int cost = global.getCost();
        if (local != null) {
            cost += local.getCost();
        }
        return cost;
    }

    @Override
    public int getSubexpressionCount() {
        return local == null ? 1 : 2;
    }

    @Override
    public Expression getSubexpression(int index) {
        switch (index) {
        case 0:
            return local != null ? local : global;
        case 1:
            if (local != null) {
                return global;
            }
            //$FALL-THROUGH$
        default:
            throw new IndexOutOfBoundsException();
        }
    }

}
