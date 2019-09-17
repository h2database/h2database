/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * An 'and' or 'or' condition as in WHERE ID=1 AND NAME=?
 */
public class ConditionAndOr extends Condition {

    /**
     * The AND condition type as in ID=1 AND NAME='Hello'.
     */
    public static final int AND = 0;

    /**
     * The OR condition type as in ID=1 OR NAME='Hello'.
     */
    public static final int OR = 1;

    private final int andOrType;
    private Expression left, right;

    /**
     * Additional condition for index only.
     */
    private Expression added;

    public ConditionAndOr(int andOrType, Expression left, Expression right) {
        if (left == null || right == null) {
            DbException.throwInternalError(left + " " + right);
        }
        this.andOrType = andOrType;
        this.left = left;
        this.right = right;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        builder.append('(');
        left.getSQL(builder, alwaysQuote);
        switch (andOrType) {
        case AND:
            builder.append("\n    AND ");
            break;
        case OR:
            builder.append("\n    OR ");
            break;
        default:
            throw DbException.throwInternalError("andOrType=" + andOrType);
        }
        return right.getSQL(builder, alwaysQuote).append(')');
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        if (andOrType == AND) {
            left.createIndexConditions(session, filter);
            right.createIndexConditions(session, filter);
            if (added != null) {
                added.createIndexConditions(session, filter);
            }
        }
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        // (NOT (A OR B)): (NOT(A) AND NOT(B))
        // (NOT (A AND B)): (NOT(A) OR NOT(B))
        Expression l = left.getNotIfPossible(session);
        if (l == null) {
            l = new ConditionNot(left);
        }
        Expression r = right.getNotIfPossible(session);
        if (r == null) {
            r = new ConditionNot(right);
        }
        int reversed = andOrType == AND ? OR : AND;
        return new ConditionAndOr(reversed, l, r);
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        Value r;
        switch (andOrType) {
        case AND: {
            if (l != ValueNull.INSTANCE && !l.getBoolean()) {
                return ValueBoolean.FALSE;
            }
            r = right.getValue(session);
            if (r != ValueNull.INSTANCE && !r.getBoolean()) {
                return ValueBoolean.FALSE;
            }
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            return ValueBoolean.TRUE;
        }
        case OR: {
            if (l.getBoolean()) {
                return ValueBoolean.TRUE;
            }
            r = right.getValue(session);
            if (r.getBoolean()) {
                return ValueBoolean.TRUE;
            }
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            return ValueBoolean.FALSE;
        }
        default:
            throw DbException.throwInternalError("type=" + andOrType);
        }
    }

    @Override
    public Expression optimize(Session session) {
        // NULL handling: see wikipedia,
        // http://www-cs-students.stanford.edu/~wlam/compsci/sqlnulls
        left = left.optimize(session);
        right = right.optimize(session);
        int lc = left.getCost(), rc = right.getCost();
        if (rc < lc) {
            Expression t = left;
            left = right;
            right = t;
        }
        switch (andOrType) {
        case AND:
            if (!session.getDatabase().getSettings().optimizeTwoEquals) {
                break;
            }
            // this optimization does not work in the following case,
            // but NOT is optimized before:
            // CREATE TABLE TEST(A INT, B INT);
            // INSERT INTO TEST VALUES(1, NULL);
            // SELECT * FROM TEST WHERE NOT (B=A AND B=0); // no rows
            // SELECT * FROM TEST WHERE NOT (B=A AND B=0 AND A=0); // 1, NULL
            // try to add conditions (A=B AND B=1: add A=1)
            if (left instanceof Comparison && right instanceof Comparison) {
                // try to add conditions (A=B AND B=1: add A=1)
                Expression added = ((Comparison) left).getAdditionalAnd(session, (Comparison) right);
                if (added != null) {
                    this.added = added.optimize(session);
                }
            }
            break;
        case OR:
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
                reduced = optimizeConditionAndOr((ConditionAndOr)left, (ConditionAndOr)right);
            } else {
                // TODO optimization: convert .. OR .. to UNION if the cost is lower
                break;
            }
            if (reduced != null) {
                return reduced.optimize(session);
            }
        }
        return optimizeConstant(session, this, andOrType, left, right);
    }

    /**
     * Optimize the expression if at least one part is constant.
     *
     * @param session the session
     * @param condition the condition
     * @param andOrType the type
     * @param left the left part of the condition
     * @param right the right part of the condition
     * @return the optimized expression
     */
    static Expression optimizeConstant(Session session, Expression condition, int andOrType, Expression left,
            Expression right) {
        Value l = left.isConstant() ? left.getValue(session) : null;
        Value r = right.isConstant() ? right.getValue(session) : null;
        if (l == null && r == null) {
            return condition;
        }
        if (l != null && r != null) {
            return ValueExpression.getBoolean(condition.getValue(session));
        }
        switch (andOrType) {
        case AND:
            if (l != null) {
                if (l != ValueNull.INSTANCE && !l.getBoolean()) {
                    return ValueExpression.getBoolean(false);
                } else if (l.getBoolean()) {
                    return castToBoolean(session, right);
                }
            } else if (r != null) {
                if (r != ValueNull.INSTANCE && !r.getBoolean()) {
                    return ValueExpression.getBoolean(false);
                } else if (r.getBoolean()) {
                    return castToBoolean(session, left);
                }
            }
            break;
        case OR:
            if (l != null) {
                if (l.getBoolean()) {
                    return ValueExpression.getBoolean(true);
                } else if (l != ValueNull.INSTANCE) {
                    return castToBoolean(session, right);
                }
            } else if (r != null) {
                if (r.getBoolean()) {
                    return ValueExpression.getBoolean(true);
                } else if (r != ValueNull.INSTANCE) {
                    return castToBoolean(session, left);
                }
            }
            break;
        default:
            DbException.throwInternalError("type=" + andOrType);
        }
        return condition;
    }

    @Override
    public void addFilterConditions(TableFilter filter) {
        if (andOrType == AND) {
            left.addFilterConditions(filter);
            right.addFilterConditions(filter);
        } else {
            super.addFilterConditions(filter);
        }
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        right.mapColumns(resolver, level, state);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        left.updateAggregate(session, stage);
        right.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + right.getCost();
    }

    @Override
    public int getSubexpressionCount() {
        return 2;
    }

    @Override
    public Expression getSubexpression(int index) {
        switch (index) {
        case 0:
            return left;
        case 1:
            return right;
        default:
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * Optimize query according to the given condition. Example:
     * (A AND B) OR (C AND B), the new condition B AND (A OR C) is returned
     *
     * @param left the session
     * @param right the second condition
     * @return null or the third condition
     */
    private static Expression optimizeConditionAndOr(ConditionAndOr left, ConditionAndOr right) {
        if (left.andOrType != AND || right.andOrType != AND) {
            return null;
        }
        Expression leftLeft = left.getSubexpression(0), leftRight = left.getSubexpression(1);
        Expression rightLeft = right.getSubexpression(0), rightRight = right.getSubexpression(1);
        String leftLeftSQL = leftLeft.getSQL(true), rightLeftSQL = rightLeft.getSQL(true);
        Expression combinedExpression;
        if (leftLeftSQL.equals(rightLeftSQL)) {
            combinedExpression = new ConditionAndOr(OR, leftRight, rightRight);
            return new ConditionAndOr(AND, leftLeft, combinedExpression);
        }
        String rightRightSQL = rightRight.getSQL(true);
        if (leftLeftSQL.equals(rightRightSQL)) {
            combinedExpression = new ConditionAndOr(OR, leftRight, rightLeft);
            return new ConditionAndOr(AND, leftLeft, combinedExpression);
        }
        String leftRightSQL = leftRight.getSQL(true);
        if (leftRightSQL.equals(rightLeftSQL)) {
            combinedExpression = new ConditionAndOr(OR, leftLeft, rightRight);
            return new ConditionAndOr(AND, leftRight, combinedExpression);
        } else if (leftRightSQL.equals(rightRightSQL)) {
            combinedExpression = new ConditionAndOr(OR, leftLeft, rightLeft);
            return new ConditionAndOr(AND, leftRight, combinedExpression);
        }
        return null;
    }
}
