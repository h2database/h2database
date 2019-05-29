/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ValueExpression;
import org.h2.index.IndexCondition;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * Null predicate (IS [NOT] NULL).
 */
public class NullPredicate extends Predicate {

    public NullPredicate(Expression left, boolean not) {
        super(left, not);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        return left.getSQL(builder.append('('), alwaysQuote).append(not ? " IS NOT NULL)" : " IS NULL)");
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        return ValueBoolean.get(l == ValueNull.INSTANCE ^ not);
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        return new NullPredicate(left, !not);
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        if (!filter.getTable().isQueryComparable()) {
            return;
        }
        ExpressionColumn l = null;
        if (left instanceof ExpressionColumn) {
            l = (ExpressionColumn) left;
            if (filter != l.getTableFilter()) {
                l = null;
            }
        }
        if (l != null && !not) {
            filter.addIndexCondition(IndexCondition.get(Comparison.EQUAL_NULL_SAFE, l, ValueExpression.getNull()));
        }
    }

    @Override
    public void addFilterConditions(TableFilter filter, boolean outerJoin) {
        if (!not && outerJoin) {
            // can not optimize:
            // select * from test t1 left join test t2 on t1.id = t2.id
            // where t2.id is null
            // to
            // select * from test t1 left join test t2
            // on t1.id = t2.id and t2.id is null
            return;
        }
        super.addFilterConditions(filter, outerJoin);
    }

}
