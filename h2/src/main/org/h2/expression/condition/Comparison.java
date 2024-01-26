/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import java.util.ArrayList;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.TypedValueExpression;
import org.h2.expression.ValueExpression;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.aggregate.AggregateType;
import org.h2.index.IndexCondition;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * Example comparison expressions are ID=1, NAME=NAME, NAME IS NULL.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public final class Comparison extends Condition {

    /**
     * The comparison type meaning = as in ID=1.
     */
    public static final int EQUAL = 0;

    /**
     * The comparison type meaning &lt;&gt; as in ID&lt;&gt;1.
     */
    public static final int NOT_EQUAL = 1;

    /**
     * The comparison type meaning &lt; as in ID&lt;1.
     */
    public static final int SMALLER = 2;

    /**
     * The comparison type meaning &gt; as in ID&gt;1.
     */
    public static final int BIGGER = 3;

    /**
     * The comparison type meaning &lt;= as in ID&lt;=1.
     */
    public static final int SMALLER_EQUAL = 4;

    /**
     * The comparison type meaning &gt;= as in ID&gt;=1.
     */
    public static final int BIGGER_EQUAL = 5;

    /**
     * The comparison type meaning ID IS NOT DISTINCT FROM 1.
     */
    public static final int EQUAL_NULL_SAFE = 6;

    /**
     * The comparison type meaning ID IS DISTINCT FROM 1.
     */
    public static final int NOT_EQUAL_NULL_SAFE = 7;

    /**
     * This is a comparison type that is only used for spatial index
     * conditions (operator "&amp;&amp;").
     */
    public static final int SPATIAL_INTERSECTS = 8;

    static final String[] COMPARE_TYPES = { "=", "<>", "<", ">", "<=", ">=", //
            "IS NOT DISTINCT FROM", "IS DISTINCT FROM", //
            "&&" };

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means the comparison will always yield FALSE. Example: 1=0.
     */
    public static final int FALSE = 9;

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means equals any value of a list. Example: IN(1, 2, 3).
     */
    public static final int IN_LIST = 10;

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means equals any value of an ARRAY. Example: ARRAY[1, 2, 3].
     */
    public static final int IN_ARRAY = 11;

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means equals any value of a list. Example: IN(SELECT ...).
     */
    public static final int IN_QUERY = 12;

    private int compareType;
    private Expression left;
    private Expression right;
    private final boolean whenOperand;

    public Comparison(int compareType, Expression left, Expression right, boolean whenOperand) {
        this.left = left;
        this.right = right;
        this.compareType = compareType;
        this.whenOperand = whenOperand;
    }

    @Override
    public boolean needParentheses() {
        return true;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return getWhenSQL(left.getSQL(builder, sqlFlags, AUTO_PARENTHESES), sqlFlags);
    }

    @Override
    public StringBuilder getWhenSQL(StringBuilder builder, int sqlFlags) {
        builder.append(' ').append(COMPARE_TYPES[compareType]).append(' ');
        return right.getSQL(builder, sqlFlags,
                right instanceof Aggregate && ((Aggregate) right).getAggregateType() == AggregateType.ANY
                        ? WITH_PARENTHESES
                        : AUTO_PARENTHESES);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        right = right.optimize(session);
        check: {
            TypeInfo leftType = left.getType(), rightType = right.getType();
            if (session.getMode().numericWithBooleanComparison) {
                switch (compareType) {
                case EQUAL:
                case NOT_EQUAL:
                case EQUAL_NULL_SAFE:
                case NOT_EQUAL_NULL_SAFE:
                    int lValueType = leftType.getValueType();
                    if (lValueType == Value.BOOLEAN) {
                        if (DataType.isNumericType(rightType.getValueType())) {
                            break check;
                        }
                    } else if (DataType.isNumericType(lValueType) && rightType.getValueType() == Value.BOOLEAN) {
                        break check;
                    }
                }
            }
            TypeInfo.checkComparable(leftType, rightType);
        }
        if (whenOperand) {
            return this;
        }
        if (right instanceof ExpressionColumn) {
            if (left.isConstant() || left instanceof Parameter) {
                Expression temp = left;
                left = right;
                right = temp;
                compareType = getReversedCompareType(compareType);
            }
        }
        if (left instanceof ExpressionColumn) {
            if (right.isConstant()) {
                Value r = right.getValue(session);
                if (r == ValueNull.INSTANCE) {
                    if ((compareType & ~1) != EQUAL_NULL_SAFE) {
                        return TypedValueExpression.UNKNOWN;
                    }
                }
                TypeInfo colType = left.getType(), constType = r.getType();
                int constValueType = constType.getValueType();
                if (constValueType != colType.getValueType() || constValueType >= Value.ARRAY) {
                    TypeInfo resType = TypeInfo.getHigherType(colType, constType);
                    // If not, the column values will need to be promoted
                    // to constant type, but vise versa, then let's do this here
                    // once.
                    if (constValueType != resType.getValueType() || constValueType >= Value.ARRAY) {
                        Column column = ((ExpressionColumn) left).getColumn();
                        right = ValueExpression.get(r.convertTo(resType, session, column));
                    }
                }
            } else if (right instanceof Parameter) {
                ((Parameter) right).setColumn(((ExpressionColumn) left).getColumn());
            }
        }
        if (left.isConstant() && right.isConstant()) {
            return ValueExpression.getBoolean(getValue(session));
        }
        if (left.isNullConstant() || right.isNullConstant()) {
            // TODO NULL handling: maybe issue a warning when comparing with
            // a NULL constants
            if ((compareType & ~1) != EQUAL_NULL_SAFE) {
                return TypedValueExpression.UNKNOWN;
            } else {
                Expression e = left.isNullConstant() ? right : left;
                int type = e.getType().getValueType();
                if (type != Value.UNKNOWN && type != Value.ROW) {
                    return new NullPredicate(e, compareType == NOT_EQUAL_NULL_SAFE, false);
                }
            }
        }
        return this;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value l = left.getValue(session);
        // Optimization: do not evaluate right if not necessary
        if (l == ValueNull.INSTANCE && (compareType & ~1) != EQUAL_NULL_SAFE) {
            return ValueNull.INSTANCE;
        }
        return compare(session, l, right.getValue(session), compareType);
    }

    @Override
    public boolean getWhenValue(SessionLocal session, Value left) {
        if (!whenOperand) {
            return super.getWhenValue(session, left);
        }
        // Optimization: do not evaluate right if not necessary
        if (left == ValueNull.INSTANCE && (compareType & ~1) != EQUAL_NULL_SAFE) {
            return false;
        }
        return compare(session, left, right.getValue(session), compareType).isTrue();
    }

    /**
     * Compare two values.
     *
     * @param session the session
     * @param l the first value
     * @param r the second value
     * @param compareType the compare type
     * @return result of comparison, either TRUE, FALSE, or NULL
     */
    static Value compare(SessionLocal session, Value l, Value r, int compareType) {
        Value result;
        switch (compareType) {
        case EQUAL: {
            int cmp = session.compareWithNull(l, r, true);
            if (cmp == 0) {
                result = ValueBoolean.TRUE;
            } else if (cmp == Integer.MIN_VALUE) {
                result = ValueNull.INSTANCE;
            } else {
                result = ValueBoolean.FALSE;
            }
            break;
        }
        case EQUAL_NULL_SAFE:
            result = ValueBoolean.get(session.areEqual(l, r));
            break;
        case NOT_EQUAL: {
            int cmp = session.compareWithNull(l, r, true);
            if (cmp == 0) {
                result = ValueBoolean.FALSE;
            } else if (cmp == Integer.MIN_VALUE) {
                result = ValueNull.INSTANCE;
            } else {
                result = ValueBoolean.TRUE;
            }
            break;
        }
        case NOT_EQUAL_NULL_SAFE:
            result = ValueBoolean.get(!session.areEqual(l, r));
            break;
        case BIGGER_EQUAL: {
            int cmp = session.compareWithNull(l, r, false);
            if (cmp >= 0) {
                result = ValueBoolean.TRUE;
            } else if (cmp == Integer.MIN_VALUE) {
                result = ValueNull.INSTANCE;
            } else {
                result = ValueBoolean.FALSE;
            }
            break;
        }
        case BIGGER: {
            int cmp = session.compareWithNull(l, r, false);
            if (cmp > 0) {
                result = ValueBoolean.TRUE;
            } else if (cmp == Integer.MIN_VALUE) {
                result = ValueNull.INSTANCE;
            } else {
                result = ValueBoolean.FALSE;
            }
            break;
        }
        case SMALLER_EQUAL: {
            int cmp = session.compareWithNull(l, r, false);
            if (cmp == Integer.MIN_VALUE) {
                result = ValueNull.INSTANCE;
            } else {
                result = ValueBoolean.get(cmp <= 0);
            }
            break;
        }
        case SMALLER: {
            int cmp = session.compareWithNull(l, r, false);
            if (cmp == Integer.MIN_VALUE) {
                result = ValueNull.INSTANCE;
            } else {
                result = ValueBoolean.get(cmp < 0);
            }
            break;
        }
        case SPATIAL_INTERSECTS: {
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                result = ValueNull.INSTANCE;
            } else {
                result = ValueBoolean.get(l.convertToGeometry(null).intersectsBoundingBox(r.convertToGeometry(null)));
            }
            break;
        }
        default:
            throw DbException.getInternalError("type=" + compareType);
        }
        return result;
    }

    @Override
    public boolean isWhenConditionOperand() {
        return whenOperand;
    }

    private static int getReversedCompareType(int type) {
        switch (type) {
        case EQUAL:
        case EQUAL_NULL_SAFE:
        case NOT_EQUAL:
        case NOT_EQUAL_NULL_SAFE:
        case SPATIAL_INTERSECTS:
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
            throw DbException.getInternalError("type=" + type);
        }
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        if (compareType == SPATIAL_INTERSECTS || whenOperand) {
            return null;
        }
        return new Comparison(getNotCompareType(compareType), left, right, false);
    }

    static int getNotCompareType(int type) {
        switch (type) {
        case EQUAL:
            return NOT_EQUAL;
        case EQUAL_NULL_SAFE:
            return NOT_EQUAL_NULL_SAFE;
        case NOT_EQUAL:
            return EQUAL;
        case NOT_EQUAL_NULL_SAFE:
            return EQUAL_NULL_SAFE;
        case BIGGER_EQUAL:
            return SMALLER;
        case BIGGER:
            return SMALLER_EQUAL;
        case SMALLER_EQUAL:
            return BIGGER;
        case SMALLER:
            return BIGGER_EQUAL;
        default:
            throw DbException.getInternalError("type=" + type);
        }
    }

    @Override
    public void createIndexConditions(SessionLocal session, TableFilter filter) {
        if (!whenOperand) {
            createIndexConditions(filter, left, right, compareType);
        }
    }

    static void createIndexConditions(TableFilter filter, Expression left, Expression right, int compareType) {
        if (compareType == NOT_EQUAL || compareType == NOT_EQUAL_NULL_SAFE) {
            return;
        }
        if (!filter.getTable().isQueryComparable()) {
            return;
        }
        if (compareType != SPATIAL_INTERSECTS) {
            boolean lIsList = left instanceof ExpressionList, rIsList = right instanceof ExpressionList;
            if (lIsList) {
                if (rIsList) {
                    createIndexConditions(filter, (ExpressionList) left, (ExpressionList) right, compareType);
                } else if (right instanceof ValueExpression) {
                    createIndexConditions(filter, (ExpressionList) left, (ValueExpression) right, compareType);
                }
            } else if (rIsList && left instanceof ValueExpression) {
                createIndexConditions(filter, (ExpressionList) right, (ValueExpression) left,
                        getReversedCompareType(compareType));
                return;
            }
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
        if ((l == null) == (r == null)) {
            return;
        }
        if (l == null) {
            if (!left.isEverything(ExpressionVisitor.getNotFromResolverVisitor(filter))) {
                return;
            }
        } else { // r == null
            if (!right.isEverything(ExpressionVisitor.getNotFromResolverVisitor(filter))) {
                return;
            }
        }
        switch (compareType) {
        case EQUAL:
        case EQUAL_NULL_SAFE:
        case BIGGER:
        case BIGGER_EQUAL:
        case SMALLER_EQUAL:
        case SMALLER:
        case SPATIAL_INTERSECTS:
            if (l != null) {
                TypeInfo colType = l.getType();
                if (TypeInfo.haveSameOrdering(colType, TypeInfo.getHigherType(colType, right.getType()))) {
                    filter.addIndexCondition(IndexCondition.get(compareType, l, right));
                }
            } else {
                @SuppressWarnings("null")
                TypeInfo colType = r.getType();
                if (TypeInfo.haveSameOrdering(colType, TypeInfo.getHigherType(colType, left.getType()))) {
                    filter.addIndexCondition(IndexCondition.get(getReversedCompareType(compareType), r, left));
                }
            }
            break;
        default:
            throw DbException.getInternalError("type=" + compareType);
        }
    }

    private static void createIndexConditions(TableFilter filter, ExpressionList left, ExpressionList right,
            int compareType) {
        int c = left.getSubexpressionCount();
        if (c == 0 || c != right.getSubexpressionCount()) {
            return;
        }
        if (compareType != EQUAL && compareType != EQUAL_NULL_SAFE) {
            if (c > 1) {
                if (compareType == BIGGER) {
                    compareType = BIGGER_EQUAL;
                } else if (compareType == SMALLER) {
                    compareType = SMALLER_EQUAL;
                }
            }
            c = 1;
        }
        for (int i = 0; i < c; i++) {
            createIndexConditions(filter, left.getSubexpression(i), right.getSubexpression(i), compareType);
        }
    }

    private static void createIndexConditions(TableFilter filter, ExpressionList left, ValueExpression right,
            int compareType) {
        int c = left.getSubexpressionCount();
        if (c == 0) {
            return;
        } else if (c == 1) {
            createIndexConditions(filter, left.getSubexpression(0), right, compareType);
        } else if (c > 1) {
            Value v = right.getValue(null);
            if (v.getValueType() == Value.ROW) {
                Value[] values = ((ValueRow) v).getList();
                if (c != values.length) {
                    return;
                }
                if (compareType != EQUAL && compareType != EQUAL_NULL_SAFE) {
                    if (compareType == BIGGER) {
                        compareType = BIGGER_EQUAL;
                    } else if (compareType == SMALLER) {
                        compareType = SMALLER_EQUAL;
                    }
                    c = 1;
                }
                for (int i = 0; i < c; i++) {
                    createIndexConditions(filter, left.getSubexpression(i), ValueExpression.get(values[i]),
                            compareType);
                }
            }
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        if (right != null) {
            right.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        left.updateAggregate(session, stage);
        if (right != null) {
            right.updateAggregate(session, stage);
        }
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        right.mapColumns(resolver, level, state);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + right.getCost() + 1;
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
            String sql = match.getSQL(DEFAULT_SQL_FLAGS);
            if (left.getSQL(DEFAULT_SQL_FLAGS).equals(sql)) {
                return right;
            } else if (right.getSQL(DEFAULT_SQL_FLAGS).equals(sql)) {
                return left;
            }
        }
        return null;
    }

    /**
     * Get an additional condition if possible. Example: given two conditions
     * A=B AND B=C, the new condition A=C is returned.
     *
     * @param session the session
     * @param other the second condition
     * @return null or the third condition for indexes
     */
    Expression getAdditionalAnd(SessionLocal session, Comparison other) {
        if (compareType == EQUAL && other.compareType == EQUAL && !whenOperand) {
            boolean lc = left.isConstant();
            boolean rc = right.isConstant();
            boolean l2c = other.left.isConstant();
            boolean r2c = other.right.isConstant();
            String l = left.getSQL(DEFAULT_SQL_FLAGS);
            String l2 = other.left.getSQL(DEFAULT_SQL_FLAGS);
            String r = right.getSQL(DEFAULT_SQL_FLAGS);
            String r2 = other.right.getSQL(DEFAULT_SQL_FLAGS);
            // a=b AND a=c
            // must not compare constants. example: NOT(B=2 AND B=3)
            if (!(rc && r2c) && l.equals(l2)) {
                return new Comparison(EQUAL, right, other.right, false);
            } else if (!(rc && l2c) && l.equals(r2)) {
                return new Comparison(EQUAL, right, other.left, false);
            } else if (!(lc && r2c) && r.equals(l2)) {
                return new Comparison(EQUAL, left, other.right, false);
            } else if (!(lc && l2c) && r.equals(r2)) {
                return new Comparison(EQUAL, left, other.left, false);
            }
        }
        return null;
    }

    /**
     * Replace the OR condition with IN condition if possible. Example: given
     * the two conditions A=1 OR A=2, the new condition A IN(1, 2) is returned.
     *
     * @param session the session
     * @param other the second condition
     * @return null or the joined IN condition
     */
    Expression optimizeOr(SessionLocal session, Comparison other) {
        if (compareType == EQUAL && other.compareType == EQUAL) {
            Expression left2 = other.left;
            Expression right2 = other.right;
            String l2 = left2.getSQL(DEFAULT_SQL_FLAGS);
            String r2 = right2.getSQL(DEFAULT_SQL_FLAGS);
            if (left.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR)) {
                String l = left.getSQL(DEFAULT_SQL_FLAGS);
                if (l.equals(l2)) {
                    return getConditionIn(left, right, right2);
                } else if (l.equals(r2)) {
                    return getConditionIn(left, right, left2);
                }
            }
            if (right.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR)) {
                String r = right.getSQL(DEFAULT_SQL_FLAGS);
                if (r.equals(l2)) {
                    return getConditionIn(right, left, right2);
                } else if (r.equals(r2)) {
                    return getConditionIn(right, left, left2);
                }
            }
        }
        return null;
    }

    private static ConditionIn getConditionIn(Expression left, Expression value1,
            Expression value2) {
        ArrayList<Expression> right = new ArrayList<>(2);
        right.add(value1);
        right.add(value2);
        return new ConditionIn(left, false, false, right);
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

}
