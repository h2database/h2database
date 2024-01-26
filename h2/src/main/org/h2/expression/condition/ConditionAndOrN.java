/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (https://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.expression.condition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
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
 * An 'and' or 'or' condition as in WHERE ID=1 AND NAME=? with N operands.
 * Mostly useful for optimisation and preventing stack overflow where generated
 * SQL has tons of conditions.
 */
public class ConditionAndOrN extends Condition {

    private final int andOrType;
    /**
     * Use an ArrayDeque because we primarily insert at the front.
     */
    private final List<Expression> expressions;

    /**
     * Additional conditions for index only.
     */
    private List<Expression> added;

    public ConditionAndOrN(int andOrType, Expression expr1, Expression expr2, Expression expr3) {
        this.andOrType = andOrType;
        this.expressions = new ArrayList<>(3);
        expressions.add(expr1);
        expressions.add(expr2);
        expressions.add(expr3);
    }

    public ConditionAndOrN(int andOrType, List<Expression> expressions) {
        this.andOrType = andOrType;
        this.expressions = expressions;
    }

    int getAndOrType() {
        return andOrType;
    }

    /**
     * Add the expression at the beginning of the list.
     *
     * @param e the expression
     */
    void addFirst(Expression e) {
        expressions.add(0, e);
    }

    @Override
    public boolean needParentheses() {
        return true;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        Iterator<Expression> it = expressions.iterator();
        it.next().getSQL(builder, sqlFlags, AUTO_PARENTHESES);
        while (it.hasNext()) {
            switch (andOrType) {
            case ConditionAndOr.AND:
                builder.append("\n    AND ");
                break;
            case ConditionAndOr.OR:
                builder.append("\n    OR ");
                break;
            default:
                throw DbException.getInternalError("andOrType=" + andOrType);
            }
            it.next().getSQL(builder, sqlFlags, AUTO_PARENTHESES);
        }
        return builder;
    }

    @Override
    public void createIndexConditions(SessionLocal session, TableFilter filter) {
        if (andOrType == ConditionAndOr.AND) {
            for (Expression e : expressions) {
                e.createIndexConditions(session, filter);
            }
            if (added != null) {
                for (Expression e : added) {
                    e.createIndexConditions(session, filter);
                }
            }
        }
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        // (NOT (A OR B)): (NOT(A) AND NOT(B))
        // (NOT (A AND B)): (NOT(A) OR NOT(B))
        final ArrayList<Expression> newList = new ArrayList<>(expressions.size());
        for (Expression e : expressions) {
            Expression l = e.getNotIfPossible(session);
            if (l == null) {
                l = new ConditionNot(e);
            }
            newList.add(l);
        }
        int reversed = andOrType == ConditionAndOr.AND ? ConditionAndOr.OR : ConditionAndOr.AND;
        return new ConditionAndOrN(reversed, newList);
    }

    @Override
    public Value getValue(SessionLocal session) {
        boolean hasNull = false;
        switch (andOrType) {
        case ConditionAndOr.AND: {
            for (Expression e : expressions) {
                Value v = e.getValue(session);
                if (v == ValueNull.INSTANCE) {
                    hasNull = true;
                } else if (!v.getBoolean()) {
                    return ValueBoolean.FALSE;
                }
            }
            return hasNull ? ValueNull.INSTANCE : ValueBoolean.TRUE;
        }
        case ConditionAndOr.OR: {
            for (Expression e : expressions) {
                Value v = e.getValue(session);
                if (v == ValueNull.INSTANCE) {
                    hasNull = true;
                } else if (v.getBoolean()) {
                    return ValueBoolean.TRUE;
                }
            }
            return hasNull ? ValueNull.INSTANCE : ValueBoolean.FALSE;
        }
        default:
            throw DbException.getInternalError("type=" + andOrType);
        }
    }

    private static final Comparator<Expression> COMPARE_BY_COST = new Comparator<Expression>() {
        @Override
        public int compare(Expression lhs, Expression rhs) {
            return lhs.getCost() - rhs.getCost();
        }

    };

    @Override
    public Expression optimize(SessionLocal session) {
        // NULL handling: see wikipedia,
        // http://www-cs-students.stanford.edu/~wlam/compsci/sqlnulls

        // first pass, optimize individual sub-expressions
        for (int i = 0; i < expressions.size(); i++ ) {
            expressions.set(i, expressions.get(i).optimize(session));
        }

        Collections.sort(expressions, COMPARE_BY_COST);

        // TODO we're only matching pairs so that are next to each other, so in complex expressions
        //   we will miss opportunities

        // second pass, optimize combinations
        optimizeMerge(0);
        for (int i = 1; i < expressions.size(); ) {
            Expression left = expressions.get(i-1);
            Expression right = expressions.get(i);
            switch (andOrType) {
            case ConditionAndOr.AND:
                if (!session.getDatabase().getSettings().optimizeTwoEquals) {
                    break;
                }
                // this optimization does not work in the following case,
                // but NOT is optimized before:
                // CREATE TABLE TEST(A INT, B INT);
                // INSERT INTO TEST VALUES(1, NULL);
                // SELECT * FROM TEST WHERE NOT (B=A AND B=0); // no rows
                // SELECT * FROM TEST WHERE NOT (B=A AND B=0 AND A=0); // 1,
                // NULL
                // try to add conditions (A=B AND B=1: add A=1)
                if (left instanceof Comparison && right instanceof Comparison) {
                    // try to add conditions (A=B AND B=1: add A=1)
                    Expression added = ((Comparison) left).getAdditionalAnd(session, (Comparison) right);
                    if (added != null) {
                        if (this.added == null) {
                            this.added = new ArrayList<>();
                        }
                        this.added.add(added.optimize(session));
                    }
                }
                break;
            case ConditionAndOr.OR:
                if (!session.getDatabase().getSettings().optimizeOr) {
                    break;
                }
                Expression reduced;
                if (left instanceof Comparison && right instanceof Comparison) {
                    reduced = ((Comparison) left).optimizeOr(session, (Comparison) right);
                } else if (left instanceof ConditionIn && right instanceof Comparison) {
                    reduced = ((ConditionIn) left).getAdditional((Comparison) right);
                } else if (right instanceof ConditionIn && left instanceof Comparison) {
                    reduced = ((ConditionIn) right).getAdditional((Comparison) left);
                } else if (left instanceof ConditionInConstantSet && right instanceof Comparison) {
                    reduced = ((ConditionInConstantSet) left).getAdditional(session, (Comparison) right);
                } else if (right instanceof ConditionInConstantSet && left instanceof Comparison) {
                    reduced = ((ConditionInConstantSet) right).getAdditional(session, (Comparison) left);
                } else if (left instanceof ConditionAndOr && right instanceof ConditionAndOr) {
                    reduced = ConditionAndOr.optimizeConditionAndOr((ConditionAndOr) left, (ConditionAndOr) right);
                } else {
                    // TODO optimization: convert .. OR .. to UNION if the cost
                    // is lower
                    break;
                }
                if (reduced != null) {
                    expressions.remove(i);
                    expressions.set(i - 1, reduced.optimize(session));
                    continue; // because we don't want to increment, we want to compare the new pair exposed
                }
            }

            Expression e = ConditionAndOr.optimizeIfConstant(session, andOrType, left, right);
            if (e != null) {
                expressions.remove(i);
                expressions.set(i-1, e);
                continue; // because we don't want to increment, we want to compare the new pair exposed
            }

            if (optimizeMerge(i)) {
                continue;
            }

            i++;
        }

        Collections.sort(expressions, COMPARE_BY_COST);

        if (expressions.size() == 1) {
            return Condition.castToBoolean(session, expressions.get(0));
        }
        return this;
    }


    private boolean optimizeMerge(int i) {
        Expression e = expressions.get(i);
        // If we have a ConditionAndOrN as a sub-expression, see if we can merge it
        // into this one.
        if (e instanceof ConditionAndOrN) {
            ConditionAndOrN rightCondition = (ConditionAndOrN) e;
            if (this.andOrType == rightCondition.andOrType) {
                expressions.remove(i);
                expressions.addAll(i, rightCondition.expressions);
                return true;
            }
        }
        else if (e instanceof ConditionAndOr) {
            ConditionAndOr rightCondition = (ConditionAndOr) e;
            if (this.andOrType == rightCondition.getAndOrType()) {
                expressions.set(i, rightCondition.getSubexpression(0));
                expressions.add(i+1, rightCondition.getSubexpression(1));
                return true;
            }
        }
        return false;
    }

    @Override
    public void addFilterConditions(TableFilter filter) {
        if (andOrType == ConditionAndOr.AND) {
            for (Expression e : expressions) {
                e.addFilterConditions(filter);
            }
        } else {
            super.addFilterConditions(filter);
        }
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        for (Expression e : expressions) {
            e.mapColumns(resolver, level, state);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : expressions) {
            e.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        for (Expression e : expressions) {
            e.updateAggregate(session, stage);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        for (Expression e : expressions) {
            if (!e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = 0;
        for (Expression e : expressions) {
            cost += e.getCost();
        }
        return cost;
    }

    @Override
    public int getSubexpressionCount() {
        return expressions.size();
    }

    @Override
    public Expression getSubexpression(int index) {
        return expressions.get(index);
    }

}
