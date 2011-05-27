/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.Message;
import org.h2.table.Column;
import org.h2.value.Value;

/**
 * A index condition object is made for each condition that can potentially use an index.
 * This class does not extend expression, but in general there is one expression that maps to each index condition.
 */
public class IndexCondition {
    public static final int EQUALITY = 1, START = 2, END = 4, RANGE = START | END, ALWAYS_FALSE = 8;
    private Column column;
    private Expression expression;
    private int compareType;

    public IndexCondition(int compareType, ExpressionColumn column, Expression expression) {
        this.compareType = compareType;
        this.column = column == null ? null : column.getColumn();
        this.expression = expression;
    }

    public Value getCurrentValue(Session session) throws SQLException {
        return expression.getValue(session);
    }

    public String getSQL() {
        if (compareType == Comparison.FALSE) {
            return "FALSE";
        }
        StringBuffer buff = new StringBuffer();
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
        default:
            throw Message.getInternalError("type="+compareType);
        }
        buff.append(expression.getSQL());
        return buff.toString();
    }

    public int getMask() {
        switch (compareType) {
        case Comparison.FALSE:
            return ALWAYS_FALSE;
        case Comparison.EQUAL:
            return EQUALITY;
        case Comparison.BIGGER_EQUAL:
        case Comparison.BIGGER:
            return START;
        case Comparison.SMALLER_EQUAL:
        case Comparison.SMALLER:
            return END;
        default:
            throw Message.getInternalError("type=" + compareType);
        }
    }

    public boolean isAlwaysFalse() {
        return compareType == Comparison.FALSE;
    }

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

    public Column getColumn() {
        return column;
    }

    public boolean isEvaluatable() {
        return expression.isEverything(ExpressionVisitor.EVALUATABLE);
    }
    
    public Expression getExpression() {
        return expression;
    }

}
