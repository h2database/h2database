/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * @author Thomas
 */
public class ConditionAndOr extends Condition {

    // TODO optimization: we could extend (ID=1 AND ID=B) to (ID=1 AND ID=B AND
    // B=1)

    public static final int AND = 0, OR = 1;

    private final int andOrType;
    private Expression left, right;

    public ConditionAndOr(int andOrType, Expression left, Expression right) {
        this.andOrType = andOrType;
        this.left = left;
        this.right = right;
        if (SysProperties.CHECK && (left == null || right == null)) {
            throw Message.getInternalError();
        }
    }

    public String getSQL() {
        String sql;
        switch (andOrType) {
        case AND:
            sql = left.getSQL() + " AND " + right.getSQL();
            break;
        case OR:
            sql = left.getSQL() + " OR " + right.getSQL();
            break;
        default:
            throw Message.getInternalError("andOrType=" + andOrType);
        }
        return "(" + sql + ")";
    }

    public void createIndexConditions(Session session, TableFilter filter) throws SQLException {
        if (andOrType == AND) {
            left.createIndexConditions(session, filter);
            right.createIndexConditions(session, filter);
        }
    }

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

    public Value getValue(Session session) throws SQLException {
        Value l = left.getValue(session);
        Value r;
        switch (andOrType) {
        case AND: {
            if (Boolean.FALSE.equals(l.getBoolean())) {
                return l;
            }
            r = right.getValue(session);
            if (Boolean.FALSE.equals(r.getBoolean())) {
                return r;
            }
            if (l == ValueNull.INSTANCE) {
                return l;
            }
            if (r == ValueNull.INSTANCE) {
                return r;
            }
            return ValueBoolean.get(true);
        }
        case OR: {
            if (Boolean.TRUE.equals(l.getBoolean())) {
                return l;
            }
            r = right.getValue(session);
            if (Boolean.TRUE.equals(r.getBoolean())) {
                return r;
            }
            if (l == ValueNull.INSTANCE) {
                return l;
            }
            if (r == ValueNull.INSTANCE) {
                return r;
            }
            return ValueBoolean.get(false);
        }
        default:
            throw Message.getInternalError("type=" + andOrType);
        }
    }

    public Expression optimize(Session session) throws SQLException {
        // TODO NULL: see wikipedia, http://www-cs-students.stanford.edu/~wlam/compsci/sqlnulls
        // TODO test if all optimizations are switched off against all on (including performance)
        left = left.optimize(session);
        right = right.optimize(session);
        int lc = left.getCost(), rc = right.getCost();
        if (rc < lc) {
            Expression t = left;
            left = right;
            right = t;
        }
        // TODO optimization: convert ((A=1 AND B=2) OR (A=1 AND B=3)) to (A=1 AND (B=2 OR B=3))
        // this optimization does not work in the following case, but NOT is optimized before:
        // CREATE TABLE TEST(A INT, B INT); 
        // INSERT INTO TEST VALUES(1, NULL);
        // SELECT * FROM TEST WHERE NOT (B=A AND B=0); // no rows
        // SELECT * FROM TEST WHERE NOT (B=A AND B=0 AND A=0); // 1, NULL
        if (SysProperties.OPTIMIZE_NOT && SysProperties.OPTIMIZE_TWO_EQUALS && andOrType == AND) {
            // try to add conditions (A=B AND B=1: add A=1)
            if (left instanceof Comparison && right instanceof Comparison) {
                Comparison compLeft = (Comparison) left;
                Comparison compRight = (Comparison) right;
                Expression added = compLeft.getAdditional(session, compRight);
                if (added != null) {
                    added = added.optimize(session);
                    ConditionAndOr a = new ConditionAndOr(AND, this, added);
                    return a;
                }
            }
        }
        Value l = left.isConstant() ? left.getValue(session) : null;
        Value r = right.isConstant() ? right.getValue(session) : null;
        if (l == null && r == null) {
            return this;
        }
        if (l != null && r != null) {
            return ValueExpression.get(getValue(session));
        }
        switch (andOrType) {
        case AND:
            if (l != null) {
                if (Boolean.FALSE.equals(l.getBoolean())) {
                    return ValueExpression.get(l);
                } else if (Boolean.TRUE.equals(l.getBoolean())) {
                    return right;
                }
            } else if (r != null) {
                if (Boolean.FALSE.equals(r.getBoolean())) {
                    return ValueExpression.get(r);
                } else if (Boolean.TRUE.equals(r.getBoolean())) {
                    return left;
                }
            }
            break;
        case OR:
            if (l != null) {
                if (Boolean.TRUE.equals(l.getBoolean())) {
                    return ValueExpression.get(l);
                } else if (Boolean.FALSE.equals(l.getBoolean())) {
                    return right;
                }
            } else if (r != null) {
                if (Boolean.TRUE.equals(r.getBoolean())) {
                    return ValueExpression.get(r);
                } else if (Boolean.FALSE.equals(r.getBoolean())) {
                    return left;
                }
            }
            break;
        default:
            throw Message.getInternalError("type=" + andOrType);
        }
        return this;
    }

    public void addFilterConditions(TableFilter filter, boolean outerJoin) {
        if (andOrType == AND) {
            left.addFilterConditions(filter, outerJoin);
            right.addFilterConditions(filter, outerJoin);
        } else {
            super.addFilterConditions(filter, outerJoin);
        }
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        left.mapColumns(resolver, level);
        right.mapColumns(resolver, level);
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
    }

    public void updateAggregate(Session session) throws SQLException {
        left.updateAggregate(session);
        right.updateAggregate(session);
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor);
    }

    public int getCost() {
        return left.getCost() + right.getCost();
    }

}
