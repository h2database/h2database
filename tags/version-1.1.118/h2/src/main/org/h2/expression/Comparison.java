/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.IndexCondition;
import org.h2.message.Message;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * Example comparison expressions are ID=1, NAME=NAME, NAME IS NULL.
 */
public class Comparison extends Condition {

    /**
     * The comparison type meaning = as in ID=1.
     */
    public static final int EQUAL = 0;

    /**
     * The comparison type meaning &gt;= as in ID&gt;=1.
     */
    public static final int BIGGER_EQUAL = 1;

    /**
     * The comparison type meaning &gt; as in ID&gt;1.
     */
    public static final int BIGGER = 2;

    /**
     * The comparison type meaning &lt;= as in ID&lt;=1.
     */
    public static final int SMALLER_EQUAL = 3;

    /**
     * The comparison type meaning &lt; as in ID&lt;1.
     */
    public static final int SMALLER = 4;

    /**
     * The comparison type meaning &lt;&gt; as in ID&lt;&gt;1.
     */
    public static final int NOT_EQUAL = 5;

    /**
     * The comparison type meaning IS NULL as in NAME IS NULL.
     */
    public static final int IS_NULL = 6;

    /**
     * The comparison type meaning IS NOT NULL as in NAME IS NOT NULL.
     */
    public static final int IS_NOT_NULL = 7;

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means the comparison will always yield FALSE. Example: 1=0.
     */
    public static final int FALSE = 8;

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means equals any value of a list. Example: IN(1, 2, 3).
     */
    public static final int IN_LIST = 9;

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means equals any value of a list. Example: IN(SELECT ...).
     */
    public static final int IN_QUERY = 10;

    private final Database database;
    private final int compareType;
    private Expression left;
    private Expression right;
    private int dataType = -2;

    public Comparison(Session session, int compareType, Expression left, Expression right) {
        this.database = session.getDatabase();
        this.left = left;
        this.right = right;
        this.compareType = compareType;
    }

    public String getSQL() {
        String sql;
        switch (compareType) {
        case EQUAL:
            sql = left.getSQL() + " = " + right.getSQL();
            break;
        case BIGGER_EQUAL:
            sql = left.getSQL() + " >= " + right.getSQL();
            break;
        case BIGGER:
            sql = left.getSQL() + " > " + right.getSQL();
            break;
        case SMALLER_EQUAL:
            sql = left.getSQL() + " <= " + right.getSQL();
            break;
        case SMALLER:
            sql = left.getSQL() + " < " + right.getSQL();
            break;
        case NOT_EQUAL:
            sql = left.getSQL() + " <> " + right.getSQL();
            break;
        case IS_NULL:
            sql = left.getSQL() + " IS NULL";
            break;
        case IS_NOT_NULL:
            sql = left.getSQL() + " IS NOT NULL";
            break;
        default:
            throw Message.throwInternalError("compareType=" + compareType);
        }
        return "(" + sql + ")";
    }

    private Expression getCast(Expression expr, int targetDataType, long precision, int scale, int displaySize, Session session)
            throws SQLException {
        if (expr == ValueExpression.getNull()) {
            return expr;
        }
        Function function = Function.getFunction(session.getDatabase(), "CAST");
        function.setParameter(0, expr);
        function.setDataType(targetDataType, precision, scale, displaySize);
        function.doneWithParameters();
        return function.optimize(session);
    }

    public Expression optimize(Session session) throws SQLException {
        left = left.optimize(session);
        if (right == null) {
            dataType = left.getType();
        } else {
            right = right.optimize(session);
            try {
                if (left instanceof ExpressionColumn) {
                    if (right.isConstant()) {
                        right = getCast(right, left.getType(), left.getPrecision(), left.getScale(), left.getDisplaySize(), session);
                    } else if (right instanceof Parameter) {
                        ((Parameter) right).setColumn(((ExpressionColumn) left).getColumn());
                    }
                } else if (right instanceof ExpressionColumn) {
                    if (left.isConstant()) {
                        left = getCast(left, right.getType(), right.getPrecision(), right.getScale(), right.getDisplaySize(), session);
                    } else if (left instanceof Parameter) {
                        ((Parameter) left).setColumn(((ExpressionColumn) right).getColumn());
                    }
                }
            } catch (SQLException e) {
                int code = e.getErrorCode();
                switch(code) {
                case ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE:
                    // WHERE ID=100000000000
                    return ValueExpression.get(ValueBoolean.get(false));
                default:
                    throw e;
                }
            }
            int lt = left.getType(), rt = right.getType();
            if (lt == rt) {
                if (lt == Value.UNKNOWN) {
                    throw Message.getSQLException(ErrorCode.UNKNOWN_DATA_TYPE_1, getSQL());
                }
                dataType = lt;
            } else {
                dataType = Value.getHigherOrder(left.getType(), right.getType());
                long precision = Math.max(left.getPrecision(), right.getPrecision());
                int scale = Math.max(left.getScale(), right.getScale());
                int displaySize = Math.max(left.getDisplaySize(), right.getDisplaySize());
                if (dataType != lt) {
                    left = getCast(left, dataType, precision, scale, displaySize, session);
                }
                if (dataType != rt) {
                    right = getCast(right, dataType, precision, scale, displaySize, session);
                }
            }
        }
        if (compareType == IS_NULL || compareType == IS_NOT_NULL) {
            if (left.isConstant()) {
                return ValueExpression.get(getValue(session));
            }
        } else {
            if (SysProperties.CHECK && (left == null || right == null)) {
                Message.throwInternalError();
            }
            if (left == ValueExpression.getNull() || right == ValueExpression.getNull()) {
                // TODO NULL handling: maybe issue a warning when comparing with
                // a NULL constants
                return ValueExpression.getNull();
            }
            if (left.isConstant() && right.isConstant()) {
                return ValueExpression.get(getValue(session));
            }
        }
        return this;
    }

    public Value getValue(Session session) throws SQLException {
        Value l = left.getValue(session);
        if (right == null) {
            boolean result;
            switch (compareType) {
            case IS_NULL:
                result = l == ValueNull.INSTANCE;
                break;
            case IS_NOT_NULL:
                result = !(l == ValueNull.INSTANCE);
                break;
            default:
                throw Message.throwInternalError("type=" + compareType);
            }
            return ValueBoolean.get(result);
        }
        l = l.convertTo(dataType);
        Value r = right.getValue(session).convertTo(dataType);
        if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        boolean result = compareNotNull(database, l, r, compareType);
        return ValueBoolean.get(result);
    }

    /**
     * Compare two values, given the values are not NULL.
     *
     * @param database the database
     * @param l the first value
     * @param r the second value
     * @param compareType the compare type
     * @return the result of the comparison (1 if the first value is bigger, -1
     *         if smaller, 0 if both are equal)
     */
    static boolean compareNotNull(Database database, Value l, Value r, int compareType) throws SQLException {
        boolean result;
        switch (compareType) {
        case EQUAL:
            result = database.areEqual(l, r);
            break;
        case NOT_EQUAL:
            result = !database.areEqual(l, r);
            break;
        case BIGGER_EQUAL:
            result = database.compare(l, r) >= 0;
            break;
        case BIGGER:
            result = database.compare(l, r) > 0;
            break;
        case SMALLER_EQUAL:
            result = database.compare(l, r) <= 0;
            break;
        case SMALLER:
            result = database.compare(l, r) < 0;
            break;
        default:
            throw Message.throwInternalError("type=" + compareType);
        }
        return result;
    }

    private int getReversedCompareType(int type) {
        switch (compareType) {
        case EQUAL:
        case NOT_EQUAL:
            return type;
        case BIGGER_EQUAL:
            return SMALLER_EQUAL;
        case BIGGER:
            return SMALLER;
        case SMALLER_EQUAL:
            return BIGGER_EQUAL;
        case SMALLER:
            return BIGGER;
        default:
            throw Message.throwInternalError("type=" + compareType);
        }
    }

    private int getNotCompareType() {
        switch (compareType) {
        case EQUAL:
            return NOT_EQUAL;
        case NOT_EQUAL:
            return EQUAL;
        case BIGGER_EQUAL:
            return SMALLER;
        case BIGGER:
            return SMALLER_EQUAL;
        case SMALLER_EQUAL:
            return BIGGER;
        case SMALLER:
            return BIGGER_EQUAL;
        case IS_NULL:
            return IS_NOT_NULL;
        case IS_NOT_NULL:
            return IS_NULL;
        default:
            throw Message.throwInternalError("type=" + compareType);
        }
    }

    public Expression getNotIfPossible(Session session) {
        int type = getNotCompareType();
        return new Comparison(session, type, left, right);
    }

    public void createIndexConditions(Session session, TableFilter filter) {
        if (right == null) {
            // TODO index usage: IS [NOT] NULL index usage is possible
            return;
        }
        ExpressionColumn l = null;
        if (left instanceof ExpressionColumn) {
            l = (ExpressionColumn) left;
            if (filter != l.getTableFilter()) {
                l = null;
            }
        }
        ExpressionColumn r = null;
        if (right instanceof ExpressionColumn) {
            r = (ExpressionColumn) right;
            if (filter != r.getTableFilter()) {
                r = null;
            }
        }
        // one side must be from the current filter
        if (l == null && r == null) {
            return;
        }
        if (l != null && r != null) {
            return;
        }
        if (l == null) {
            ExpressionVisitor visitor = ExpressionVisitor.get(ExpressionVisitor.NOT_FROM_RESOLVER);
            visitor.setResolver(filter);
            if (!left.isEverything(visitor)) {
                return;
            }
        } else if (r == null) {
            ExpressionVisitor visitor = ExpressionVisitor.get(ExpressionVisitor.NOT_FROM_RESOLVER);
            visitor.setResolver(filter);
            if (!right.isEverything(visitor)) {
                return;
            }
        } else {
            // if both sides are part of the same filter, it can't be used for
            // index lookup
            return;
        }
        boolean addIndex;
        switch (compareType) {
        case NOT_EQUAL:
            addIndex = false;
            break;
        case EQUAL:
        case BIGGER:
        case BIGGER_EQUAL:
        case SMALLER_EQUAL:
        case SMALLER:
            addIndex = true;
            break;
        default:
            throw Message.throwInternalError("type=" + compareType);
        }
        if (addIndex) {
            if (l != null) {
                filter.addIndexCondition(IndexCondition.get(compareType, l, right));
            } else if (r != null) {
                int compareRev = getReversedCompareType(compareType);
                filter.addIndexCondition(IndexCondition.get(compareRev, r, left));
            }
        }
        return;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        if (right != null) {
            right.setEvaluatable(tableFilter, b);
        }
    }

    public void updateAggregate(Session session) throws SQLException {
        left.updateAggregate(session);
        if (right != null) {
            right.updateAggregate(session);
        }
    }

    public void addFilterConditions(TableFilter filter, boolean outerJoin) {
        if (compareType == IS_NULL && outerJoin) {
            // can not optimize:
            // select * from test t1 left join test t2 on t1.id = t2.id where t2.id is null
            // to
            // select * from test t1 left join test t2 on t1.id = t2.id and t2.id is null
            return;
        }
        super.addFilterConditions(filter, outerJoin);
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        left.mapColumns(resolver, level);
        if (right != null) {
            right.mapColumns(resolver, level);
        }
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && (right == null || right.isEverything(visitor));
    }

    public int getCost() {
        return left.getCost() + (right == null ? 0 : right.getCost()) + 1;
    }

    /**
     * Get the other expression if this is an equals comparison and the other
     * expression matches.
     *
     * @param match the expression that should match
     * @return null if no match, the other expression if there is a match
     */
    Expression getIfEquals(Expression match) {
        if (compareType == EQUAL) {
            String sql = match.getSQL();
            if (left.getSQL().equals(sql)) {
                return right;
            } else if (right.getSQL().equals(sql)) {
                return left;
            }
        }
        return null;
    }

    /**
     * Get an additional condition if possible. Example: given two conditions
     * A=B AND B=C, the new condition A=C is returned. Given the two conditions
     * A=1 OR A=2, the new condition A IN(1, 2) is returned.
     *
     * @param session the session
     * @param other the second condition
     * @param add true for AND, false for OR
     * @return null or the third condition
     */
    Expression getAdditional(Session session, Comparison other, boolean and) {
        if (compareType == other.compareType && compareType == EQUAL) {
            boolean lc = left.isConstant(), rc = right.isConstant();
            boolean l2c = other.left.isConstant(), r2c = other.right.isConstant();
            String l = left.getSQL();
            String l2 = other.left.getSQL();
            String r = right.getSQL();
            String r2 = other.right.getSQL();
            if (and) {
                // a=b AND a=c
                // must not compare constants. example: NOT(B=2 AND B=3)
                if (!(rc && r2c) && l.equals(l2)) {
                    return new Comparison(session, EQUAL, right, other.right);
                } else if (!(rc && l2c) && l.equals(r2)) {
                    return new Comparison(session, EQUAL, right, other.left);
                } else if (!(lc && r2c) && r.equals(l2)) {
                    return new Comparison(session, EQUAL, left, other.right);
                } else if (!(lc && l2c) && r.equals(r2)) {
                    return new Comparison(session, EQUAL, left, other.left);
                }
            } else {
                // a=b OR a=c
                Database db = session.getDatabase();
                if (rc && r2c && l.equals(l2)) {
                    return new ConditionIn(db, left, ObjectArray.newInstance(right, other.right));
                } else if (rc && l2c && l.equals(r2)) {
                    return new ConditionIn(db, left, ObjectArray.newInstance(right, other.left));
                } else if (lc && r2c && r.equals(l2)) {
                    return new ConditionIn(db, right, ObjectArray.newInstance(left, other.right));
                } else if (lc && l2c && r.equals(r2)) {
                    return new ConditionIn(db, right, ObjectArray.newInstance(left, other.left));
                }
            }
        }
        return null;
    }

    /**
     * Get the left or the right sub-expression of this condition.
     *
     * @param getLeft true to get the left sub-expression, false to get the right
     *            sub-expression.
     * @return the sub-expression
     */
    public Expression getExpression(boolean getLeft) {
        return getLeft ? this.left : right;
    }

}
