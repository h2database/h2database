/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

public class Operation extends Expression {
    public static final int CONCAT = 0, PLUS = 1, MINUS = 2, MULTIPLY = 3, DIVIDE = 4, NEGATE = 5;
    private int opType;
    private Expression left, right;
    private int dataType;

    public Operation(int opType, Expression left, Expression right) {
        this.opType = opType;
        this.left = left;
        this.right = right;
    }

    public String getSQL() {
        String sql;
        switch (opType) {
        case NEGATE:
            // don't remove the space, otherwise it might end up some thing line
            // --1 which is a remark
            // TODO need to () everything correctly, but avoiding double (())
            sql = "- " + left.getSQL();
            break;
        case CONCAT:
            sql = left.getSQL() + " || " + right.getSQL();
            break;
        case PLUS:
            sql = left.getSQL() + " + " + right.getSQL();
            break;
        case MINUS:
            sql = left.getSQL() + " - " + right.getSQL();
            break;
        case MULTIPLY:
            sql = left.getSQL() + " * " + right.getSQL();
            break;
        case DIVIDE:
            sql = left.getSQL() + " / " + right.getSQL();
            break;
        default:
            throw Message.getInternalError("opType=" + opType);
        }
        return "(" + sql + ")";
    }

    public Value getValue(Session session) throws SQLException {
        Value l = left.getValue(session).convertTo(dataType);
        Value r = right == null ? null : right.getValue(session).convertTo(dataType);

        switch (opType) {
        case NEGATE:
            return l == ValueNull.INSTANCE ? l : l.negate();
        case CONCAT: {
            if (l == ValueNull.INSTANCE) {
                if (Mode.getCurrentMode().nullConcatIsNull) {
                    return ValueNull.INSTANCE;
                } else {
                    return r;
                }
            } else if (r == ValueNull.INSTANCE) {
                if (Mode.getCurrentMode().nullConcatIsNull) {
                    return ValueNull.INSTANCE;
                } else {
                    return l;
                }
            }
            String s1 = l.getString(), s2 = r.getString();
            StringBuffer buff = new StringBuffer(s1.length() + s2.length());
            buff.append(s1);
            buff.append(s2);
            return ValueString.get(buff.toString());
        }
        case PLUS:
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            return l.add(r);
        case MINUS:
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            return l.subtract(r);
        case MULTIPLY:
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            return l.multiply(r);
        case DIVIDE:
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            return l.divide(r);
        default:
            throw Message.getInternalError("type=" + opType);
        }
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        left.mapColumns(resolver, level);
        if (right != null) {
            right.mapColumns(resolver, level);
        }
    }

    public Expression optimize(Session session) throws SQLException {
        left = left.optimize(session);
        switch (opType) {
        case NEGATE:
            dataType = left.getType();
            break;
        case CONCAT:
            right = right.optimize(session);
            dataType = Value.STRING;
            if (left.isConstant() && right.isConstant()) {
                return ValueExpression.get(getValue(session));
            }
            break;
        case PLUS:
        case MINUS:
        case MULTIPLY:
        case DIVIDE:
            right = right.optimize(session);
            int l = left.getType();
            int r = right.getType();
            if (l == Value.NULL && r == Value.NULL) {
                // example: (? + ?) - the most safe data type is probably
                // decimal
                dataType = Value.DECIMAL;
            } else if (l == Value.DATE || l == Value.TIMESTAMP) {
                if (r == Value.INT && (opType == PLUS || opType == MINUS)) {
                    // Oracle date add
                    Function f = Function.getFunction(session.getDatabase(), "DATEADD");
                    f.setParameter(0, ValueExpression.get(ValueString.get("DAY")));
                    if (opType == MINUS) {
                        right = new Operation(NEGATE, right, null);
                        right = right.optimize(session);
                    }
                    f.setParameter(1, right);
                    f.setParameter(2, left);
                    f.doneWithParameters();
                    return f.optimize(session);
                } else if (opType == MINUS && (l == Value.DATE || l == Value.TIMESTAMP)) {
                    // Oracle date subtract
                    Function f = Function.getFunction(session.getDatabase(), "DATEDIFF");
                    f.setParameter(0, ValueExpression.get(ValueString.get("DAY")));
                    f.setParameter(1, right);
                    f.setParameter(2, left);
                    f.doneWithParameters();
                    return f.optimize(session);
                }
            } else {
                dataType = Value.getHigherOrder(l, r);
            }
            break;
        default:
            throw Message.getInternalError("type=" + opType);
        }
        if (left.isConstant() && (right == null || right.isConstant())) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        if (right != null) {
            right.setEvaluatable(tableFilter, b);
        }
    }

    public int getType() {
        return dataType;
    }

    public long getPrecision() {
        if (right != null) {
            switch (opType) {
                case CONCAT:
                    return left.getPrecision() + right.getPrecision();
                default:
                    return Math.max(left.getPrecision(), right.getPrecision());
            }
        }
        return left.getPrecision();
    }

    public int getScale() {
        if (right != null) {
            return Math.max(left.getScale(), right.getScale());
        }
        return left.getScale();
    }

    public void updateAggregate(Session session) throws SQLException {
        left.updateAggregate(session);
        if (right != null) {
            right.updateAggregate(session);
        }
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && (right == null || right.isEverything(visitor));
    }

    public int getCost() {
        return left.getCost() + 1 + (right == null ? 0 : right.getCost());
    }

}
