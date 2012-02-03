/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.command.dml.Query;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.IndexCondition;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * An 'in' condition with a subquery, as in WHERE ID IN(SELECT ...)
 */
public class ConditionInSelect extends Condition {
    private Database database;
    private Expression left;
    private Query query;
    private boolean all;
    private int compareType;
    private int queryLevel;

    public ConditionInSelect(Database database, Expression left, Query query, boolean all, int compareType) {
        this.database = database;
        this.left = left;
        this.query = query;
        this.all = all;
        this.compareType = compareType;
    }

    public Value getValue(Session session) {
        query.setSession(session);
        ResultInterface rows = query.query(0);
        session.addTemporaryResult(rows);
        boolean hasNull = false;
        boolean result = all;
        Value l = left.getValue(session);
        boolean hasRow = false;
        while (rows.next()) {
            if (!hasRow) {
                if (l == ValueNull.INSTANCE) {
                    return l;
                }
                hasRow = true;
            }
            boolean value;
            Value r = rows.currentRow()[0];
            if (r == ValueNull.INSTANCE) {
                value = false;
                hasNull = true;
            } else {
                value = Comparison.compareNotNull(database, l, r, compareType);
            }
            if (!value && all) {
                result = false;
                break;
            } else if (value && !all) {
                result = true;
                break;
            }
        }
        if (!hasRow) {
            return ValueBoolean.get(false);
        }
        if (!result && hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(result);
    }

    public void mapColumns(ColumnResolver resolver, int level) {
        left.mapColumns(resolver, level);
        query.mapColumns(resolver, level + 1);
        this.queryLevel = Math.max(level, this.queryLevel);
    }

    public Expression optimize(Session session) {
        left = left.optimize(session);
        query.setDistinct(true);
        query.prepare();
        if (query.getColumnCount() != 1) {
            throw DbException.get(ErrorCode.SUBQUERY_IS_NOT_SINGLE_COLUMN);
        }
        // Can not optimize: the data may change
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        query.setEvaluatable(tableFilter, b);
    }

    public String getSQL() {
        return "(" + left.getSQL() + " IN(" + query.getPlanSQL() + "))";
    }

    public void updateAggregate(Session session) {
        // TODO exists: is it allowed that the subquery contains aggregates?
        // probably not
        // select id from test group by id having 1 in (select * from test2
        // where id=count(test.id))
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && query.isEverything(visitor);
    }

    public int getCost() {
        return left.getCost() + 10 + (int) (10 * query.getCost());
    }

    public void createIndexConditions(Session session, TableFilter filter) {
        if (!SysProperties.OPTIMIZE_IN_LIST) {
            return;
        }
        if (!(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        ExpressionVisitor visitor = ExpressionVisitor.get(ExpressionVisitor.NOT_FROM_RESOLVER);
        visitor.setResolver(filter);
        if (!query.isEverything(visitor)) {
            return;
        }
        filter.addIndexCondition(IndexCondition.getInQuery(l, query));
    }

}
