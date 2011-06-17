/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import org.h2.command.dml.Query;
import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.table.Column;
import org.h2.util.ObjectArray;
import org.h2.util.StatementBuilder;
import org.h2.value.CompareMode;
import org.h2.value.Value;

/**
 * A index condition object is made for each condition that can potentially use
 * an index. This class does not extend expression, but in general there is one
 * expression that maps to each index condition.
 */
public class IndexCondition {

    /**
     * A bit of a search mask meaning 'equal'.
     */
    public static final int EQUALITY = 1;

    /**
     * A bit of a search mask meaning 'larger or equal'.
     */
    public static final int START = 2;

    /**
     * A bit of a search mask meaning 'smaller or equal'.
     */
    public static final int END = 4;

    /**
     * A search mask meaning 'between'.
     */
    public static final int RANGE = START | END;

    /**
     * A bit of a search mask meaning 'the condition is always false'.
     */
    public static final int ALWAYS_FALSE = 8;

    private Column column;
    private int compareType;

    private Expression expression;
    private ObjectArray<Expression> expressionList;
    private Query expressionQuery;

    private IndexCondition(int compareType, ExpressionColumn column, Expression expression) {
        this.compareType = compareType;
        this.column = column == null ? null : column.getColumn();
        this.expression = expression;
    }

    /**
     * Create an index condition with the given parameters.
     *
     * @param compareType the comparison type
     * @param column the column
     * @param expression the expression
     * @return the index condition
     */
    public static IndexCondition get(int compareType, ExpressionColumn column, Expression expression) {
        return new IndexCondition(compareType, column, expression);
    }

    /**
     * Create an index condition with the compare type IN_LIST and with the
     * given parameters.
     *
     * @param column the column
     * @param list the expression list
     * @return the index condition
     */
    public static IndexCondition getInList(ExpressionColumn column, ObjectArray<Expression> list) {
        IndexCondition cond = new IndexCondition(Comparison.IN_LIST, column, null);
        cond.expressionList = list;
        return cond;
    }

    /**
     * Create an index condition with the compare type IN_QUERY and with the
     * given parameters.
     *
     * @param column the column
     * @param query the select statement
     * @return the index condition
     */
    public static IndexCondition getInQuery(ExpressionColumn column, Query query) {
        IndexCondition cond = new IndexCondition(Comparison.IN_QUERY, column, null);
        cond.expressionQuery = query;
        return cond;
    }

    /**
     * Get the current value of the expression.
     *
     * @param session the session
     * @return the value
     */
    public Value getCurrentValue(Session session) throws SQLException {
        return expression.getValue(session);
    }

    /**
     * Get the current value list of the expression. The value list is of the
     * same type as the column, distinct, and sorted.
     *
     * @param session the session
     * @return the value list
     */
    public Value[] getCurrentValueList(Session session) throws SQLException {
        HashSet<Value> valueSet = new HashSet<Value>();
        int dataType = column.getType();
        for (Expression e : expressionList) {
            Value v = e.getValue(session);
            v = v.convertTo(dataType);
            valueSet.add(v);
        }
        Value[] array = new Value[valueSet.size()];
        valueSet.toArray(array);
        final CompareMode mode = session.getDatabase().getCompareMode();
        Arrays.sort(array, new Comparator<Value>() {
            public int compare(Value o1, Value o2) {
                try {
                    return o1.compareTo(o2, mode);
                } catch (SQLException e) {
                    throw Message.convertToInternal(e);
                }
            }
        });
        return array;
    }

    /**
     * Get the current result of the expression. The rows may not be of the same
     * type, therefore the rows may not be unique.
     *
     * @param session the session
     * @return the result
     */
    public LocalResult getCurrentResult(Session session) throws SQLException {
        return expressionQuery.query(0);
    }

    /**
     * Get the SQL snippet of this comparison.
     *
     * @return the SQL snippet
     */
    public String getSQL() {
        if (compareType == Comparison.FALSE) {
            return "FALSE";
        }
        StatementBuilder buff = new StatementBuilder();
        buff.append(column.getSQL());
        switch(compareType) {
        case Comparison.EQUAL:
            buff.append(" = ");
            break;
        case Comparison.BIGGER_EQUAL:
            buff.append(" >= ");
            break;
        case Comparison.BIGGER:
            buff.append(" > ");
            break;
        case Comparison.SMALLER_EQUAL:
            buff.append(" <= ");
            break;
        case Comparison.SMALLER:
            buff.append(" < ");
            break;
        case Comparison.IN_LIST:
            buff.append(" IN(");
            for (Expression e : expressionList) {
                buff.appendExceptFirst(", ");
                buff.append(e.getSQL());
            }
            buff.append(')');
            break;
        case Comparison.IN_QUERY:
            buff.append(" IN(");
            buff.append(expressionQuery.getPlanSQL());
            buff.append(')');
            break;
        default:
            Message.throwInternalError("type="+compareType);
        }
        if (expression != null) {
            buff.append(expression.getSQL());
        }
        return buff.toString();
    }

    /**
     * Get the comparison bit mask.
     *
     * @return the mask
     */
    public int getMask() {
        switch (compareType) {
        case Comparison.FALSE:
            return ALWAYS_FALSE;
        case Comparison.EQUAL:
        case Comparison.IN_LIST:
        case Comparison.IN_QUERY:
            return EQUALITY;
        case Comparison.BIGGER_EQUAL:
        case Comparison.BIGGER:
            return START;
        case Comparison.SMALLER_EQUAL:
        case Comparison.SMALLER:
            return END;
        default:
            throw Message.throwInternalError("type=" + compareType);
        }
    }

    /**
     * Check if the result is always false.
     *
     * @return true if the result will always be false
     */
    public boolean isAlwaysFalse() {
        return compareType == Comparison.FALSE;
    }

    /**
     * Check if this index condition is of the type column larger or equal to
     * value.
     *
     * @return true if this is a start condition
     */
    public boolean isStart() {
        switch (compareType) {
        case Comparison.EQUAL:
        case Comparison.BIGGER_EQUAL:
        case Comparison.BIGGER:
            return true;
        default:
            return false;
        }
    }

    /**
     * Check if this index condition is of the type column smaller or equal to
     * value.
     *
     * @return true if this is a end condition
     */
    public boolean isEnd() {
        switch (compareType) {
        case Comparison.EQUAL:
        case Comparison.SMALLER_EQUAL:
        case Comparison.SMALLER:
            return true;
        default:
            return false;
        }
    }

    public int getCompareType() {
        return compareType;
    }

    /**
     * Get the referenced column.
     *
     * @return the column
     */
    public Column getColumn() {
        return column;
    }

    /**
     * Check if the expression can be evaluated.
     *
     * @return true if it can be evaluated
     */
    public boolean isEvaluatable() {
        if (expression != null) {
            return expression.isEverything(ExpressionVisitor.EVALUATABLE);
        }
        if (expressionList != null) {
            for (Expression e : expressionList) {
                if (!e.isEverything(ExpressionVisitor.EVALUATABLE)) {
                    return false;
                }
            }
            return true;
        }
        return expressionQuery.isEverything(ExpressionVisitor.EVALUATABLE);
    }

}
