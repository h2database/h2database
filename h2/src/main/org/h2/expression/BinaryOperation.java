/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.expression.IntervalOperation.IntervalOpType;
import org.h2.expression.function.DateTimeFunction;
import org.h2.message.DbException;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;

/**
 * A mathematical expression, or string concatenation.
 */
public class BinaryOperation extends Operation2 {

    public enum OpType {
        /**
         * This operation represents an addition as in 1 + 2.
         */
        PLUS,

        /**
         * This operation represents a subtraction as in 2 - 1.
         */
        MINUS,

        /**
         * This operation represents a multiplication as in 2 * 3.
         */
        MULTIPLY,

        /**
         * This operation represents a division as in 4 / 2.
         */
        DIVIDE
    }

    private OpType opType;
    private TypeInfo forcedType;
    private boolean convertRight = true;

    public BinaryOperation(OpType opType, Expression left, Expression right) {
        super(left, right);
        this.opType = opType;
    }

    /**
     * Sets a forced data type of a datetime minus datetime operation.
     *
     * @param forcedType the forced data type
     */
    public void setForcedType(TypeInfo forcedType) {
        if (opType != OpType.MINUS) {
            throw getUnexpectedForcedTypeException();
        }
        this.forcedType = forcedType;
    }

    @Override
    public boolean needParentheses() {
        return true;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        // don't remove the space, otherwise it might end up some thing like
        // --1 which is a line remark
        left.getSQL(builder, sqlFlags, AUTO_PARENTHESES).append(' ').append(getOperationToken()).append(' ');
        return right.getSQL(builder, sqlFlags, AUTO_PARENTHESES);
    }

    private String getOperationToken() {
        switch (opType) {
        case PLUS:
            return "+";
        case MINUS:
            return "-";
        case MULTIPLY:
            return "*";
        case DIVIDE:
            return "/";
        default:
            throw DbException.getInternalError("opType=" + opType);
        }
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value l = left.getValue(session).convertTo(type, session);
        Value r = right.getValue(session);
        if (convertRight) {
            r = r.convertTo(type, session);
        }
        switch (opType) {
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
            return l.divide(r, type);
        default:
            throw DbException.getInternalError("type=" + opType);
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        right = right.optimize(session);
        TypeInfo leftType = left.getType(), rightType = right.getType();
        int l = leftType.getValueType(), r = rightType.getValueType();
        if ((l == Value.NULL && r == Value.NULL) || (l == Value.UNKNOWN && r == Value.UNKNOWN)) {
            // (? + ?) - use decimal by default (the most safe data type) or
            // string when text concatenation with + is enabled
            if (opType == OpType.PLUS && session.getDatabase().getMode().allowPlusForStringConcat) {
                return new ConcatenationOperation(left, right).optimize(session);
            } else {
                type = TypeInfo.TYPE_NUMERIC_FLOATING_POINT;
            }
        } else if (DataType.isIntervalType(l) || DataType.isIntervalType(r)) {
            if (forcedType != null) {
                throw getUnexpectedForcedTypeException();
            }
            return optimizeInterval(l, r);
        } else if (DataType.isDateTimeType(l) || DataType.isDateTimeType(r)) {
            return optimizeDateTime(session, l, r);
        } else if (forcedType != null) {
            throw getUnexpectedForcedTypeException();
        } else {
            int dataType = Value.getHigherOrder(l, r);
            if (dataType == Value.NUMERIC) {
                optimizeNumeric(leftType, rightType);
            } else if (dataType == Value.DECFLOAT) {
                optimizeDecfloat(leftType, rightType);
            } else if (dataType == Value.ENUM) {
                type = TypeInfo.TYPE_INTEGER;
            } else if (DataType.isCharacterStringType(dataType)
                    && opType == OpType.PLUS && session.getDatabase().getMode().allowPlusForStringConcat) {
                return new ConcatenationOperation(left, right).optimize(session);
            } else {
                type = TypeInfo.getTypeInfo(dataType);
            }
        }
        if (left.isConstant() && right.isConstant()) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    private void optimizeNumeric(TypeInfo leftType, TypeInfo rightType) {
        leftType = leftType.toNumericType();
        rightType = rightType.toNumericType();
        long leftPrecision = leftType.getPrecision(), rightPrecision = rightType.getPrecision();
        int leftScale = leftType.getScale(), rightScale = rightType.getScale();
        long precision;
        int scale;
        switch (opType) {
        case PLUS:
        case MINUS:
            // Precision is implementation-defined.
            // Scale must be max(leftScale, rightScale).
            // Choose the largest scale and adjust the precision of other
            // argument.
            if (leftScale < rightScale) {
                leftPrecision += rightScale - leftScale;
                scale = rightScale;
            } else {
                rightPrecision += leftScale - rightScale;
                scale = leftScale;
            }
            // Add one extra digit to the largest precision.
            precision = Math.max(leftPrecision, rightPrecision) + 1;
            break;
        case MULTIPLY:
            // Precision is implementation-defined.
            // Scale must be leftScale + rightScale.
            // Use sum of precisions.
            precision = leftPrecision + rightPrecision;
            scale = leftScale + rightScale;
            break;
        case DIVIDE: {
            // Precision and scale are implementation-defined.
            long scaleAsLong = leftScale - rightScale + rightPrecision * 2;
            if (scaleAsLong >= ValueNumeric.MAXIMUM_SCALE) {
                scale = ValueNumeric.MAXIMUM_SCALE;
            } else if (scaleAsLong <= 0) {
                scale = 0;
            } else {
                scale = (int) scaleAsLong;
            }
            // Divider can be effectively multiplied by no more than
            // 10^rightScale, so add rightScale to its precision and adjust the
            // result to the changes in scale.
            precision = leftPrecision + rightScale - leftScale + scale;
            // If precision is too large, reduce it together with scale
            if (precision > Constants.MAX_NUMERIC_PRECISION) {
                long sub = Math.min(precision - Constants.MAX_NUMERIC_PRECISION, scale);
                precision -= sub;
                scale -= sub;
            }
            break;
        }
        default:
            throw DbException.getInternalError("type=" + opType);
        }
        type = TypeInfo.getTypeInfo(Value.NUMERIC, precision, scale, null);
    }

    private void optimizeDecfloat(TypeInfo leftType, TypeInfo rightType) {
        leftType = leftType.toDecfloatType();
        rightType = rightType.toDecfloatType();
        long leftPrecision = leftType.getPrecision(), rightPrecision = rightType.getPrecision();
        long precision;
        switch (opType) {
        case PLUS:
        case MINUS:
        case DIVIDE:
            // Add one extra digit to the largest precision.
            precision = Math.max(leftPrecision, rightPrecision) + 1;
            break;
        case MULTIPLY:
            // Use sum of precisions.
            precision = leftPrecision + rightPrecision;
            break;
        default:
            throw DbException.getInternalError("type=" + opType);
        }
        type = TypeInfo.getTypeInfo(Value.DECFLOAT, precision, 0, null);
    }

    private Expression optimizeInterval(int l, int r) {
        boolean lInterval = false, lNumeric = false, lDateTime = false;
        if (DataType.isIntervalType(l)) {
            lInterval = true;
        } else if (DataType.isNumericType(l)) {
            lNumeric = true;
        } else if (DataType.isDateTimeType(l)) {
            lDateTime = true;
        } else {
            throw getUnsupported(l, r);
        }
        boolean rInterval = false, rNumeric = false, rDateTime = false;
        if (DataType.isIntervalType(r)) {
            rInterval = true;
        } else if (DataType.isNumericType(r)) {
            rNumeric = true;
        } else if (DataType.isDateTimeType(r)) {
            rDateTime = true;
        } else {
            throw getUnsupported(l, r);
        }
        switch (opType) {
        case PLUS:
            if (lInterval && rInterval) {
                if (DataType.isYearMonthIntervalType(l) == DataType.isYearMonthIntervalType(r)) {
                    return new IntervalOperation(IntervalOpType.INTERVAL_PLUS_INTERVAL, left, right);
                }
            } else if (lInterval && rDateTime) {
                if (r == Value.TIME && DataType.isYearMonthIntervalType(l)) {
                    break;
                }
                return new IntervalOperation(IntervalOpType.DATETIME_PLUS_INTERVAL, right, left);
            } else if (lDateTime && rInterval) {
                if (l == Value.TIME && DataType.isYearMonthIntervalType(r)) {
                    break;
                }
                return new IntervalOperation(IntervalOpType.DATETIME_PLUS_INTERVAL, left, right);
            }
            break;
        case MINUS:
            if (lInterval && rInterval) {
                if (DataType.isYearMonthIntervalType(l) == DataType.isYearMonthIntervalType(r)) {
                    return new IntervalOperation(IntervalOpType.INTERVAL_MINUS_INTERVAL, left, right);
                }
            } else if (lDateTime && rInterval) {
                if (l == Value.TIME && DataType.isYearMonthIntervalType(r)) {
                    break;
                }
                return new IntervalOperation(IntervalOpType.DATETIME_MINUS_INTERVAL, left, right);
            }
            break;
        case MULTIPLY:
            if (lInterval && rNumeric) {
                return new IntervalOperation(IntervalOpType.INTERVAL_MULTIPLY_NUMERIC, left, right);
            } else if (lNumeric && rInterval) {
                return new IntervalOperation(IntervalOpType.INTERVAL_MULTIPLY_NUMERIC, right, left);
            }
            break;
        case DIVIDE:
            if (lInterval) {
                if (rNumeric) {
                    return new IntervalOperation(IntervalOpType.INTERVAL_DIVIDE_NUMERIC, left, right);
                } else if (rInterval && DataType.isYearMonthIntervalType(l) == DataType.isYearMonthIntervalType(r)) {
                    // Non-standard
                    return new IntervalOperation(IntervalOpType.INTERVAL_DIVIDE_INTERVAL, left, right);
                }
            }
            break;
        default:
        }
        throw getUnsupported(l, r);
    }

    private Expression optimizeDateTime(SessionLocal session, int l, int r) {
        switch (opType) {
        case PLUS: {
            if (DataType.isDateTimeType(l)) {
                if (DataType.isDateTimeType(r)) {
                    if (l > r) {
                        swap();
                        int t = l;
                        l = r;
                        r = t;
                    }
                    return new CompatibilityDatePlusTimeOperation(right, left).optimize(session);
                }
                swap();
                int t = l;
                l = r;
                r = t;
            }
            switch (l) {
            case Value.INTEGER:
                // Oracle date add
                return new DateTimeFunction(DateTimeFunction.DATEADD, DateTimeFunction.DAY, left, right)
                        .optimize(session);
            case Value.NUMERIC:
            case Value.REAL:
            case Value.DOUBLE:
            case Value.DECFLOAT:
                // Oracle date add
                return new DateTimeFunction(DateTimeFunction.DATEADD, DateTimeFunction.SECOND,
                        new BinaryOperation(OpType.MULTIPLY, ValueExpression.get(ValueInteger.get(60 * 60 * 24)),
                                left), right).optimize(session);
            }
            break;
        }
        case MINUS:
            switch (l) {
            case Value.DATE:
            case Value.TIMESTAMP:
            case Value.TIMESTAMP_TZ:
                switch (r) {
                case Value.INTEGER: {
                    if (forcedType != null) {
                        throw getUnexpectedForcedTypeException();
                    }
                    // Oracle date subtract
                    return new DateTimeFunction(DateTimeFunction.DATEADD, DateTimeFunction.DAY,
                            new UnaryOperation(right), left).optimize(session);
                }
                case Value.NUMERIC:
                case Value.REAL:
                case Value.DOUBLE:
                case Value.DECFLOAT: {
                    if (forcedType != null) {
                        throw getUnexpectedForcedTypeException();
                    }
                    // Oracle date subtract
                    return new DateTimeFunction(DateTimeFunction.DATEADD, DateTimeFunction.SECOND,
                            new BinaryOperation(OpType.MULTIPLY, ValueExpression.get(ValueInteger.get(-60 * 60 * 24)),
                                    right), left).optimize(session);
                }
                case Value.TIME:
                case Value.TIME_TZ:
                case Value.DATE:
                case Value.TIMESTAMP:
                case Value.TIMESTAMP_TZ:
                    return new IntervalOperation(IntervalOpType.DATETIME_MINUS_DATETIME, left, right, forcedType);
                }
                break;
            case Value.TIME:
            case Value.TIME_TZ:
                if (DataType.isDateTimeType(r)) {
                    return new IntervalOperation(IntervalOpType.DATETIME_MINUS_DATETIME, left, right, forcedType);
                }
                break;
            }
            break;
        case MULTIPLY:
            if (l == Value.TIME) {
                type = TypeInfo.TYPE_TIME;
                convertRight = false;
                return this;
            } else if (r == Value.TIME) {
                swap();
                type = TypeInfo.TYPE_TIME;
                convertRight = false;
                return this;
            }
            break;
        case DIVIDE:
            if (l == Value.TIME) {
                type = TypeInfo.TYPE_TIME;
                convertRight = false;
                return this;
            }
            break;
        default:
        }
        throw getUnsupported(l, r);
    }

    private DbException getUnsupported(int l, int r) {
        return DbException.getUnsupportedException(
                Value.getTypeName(l) + ' ' + getOperationToken() + ' ' + Value.getTypeName(r));
    }

    private DbException getUnexpectedForcedTypeException() {
        StringBuilder builder = getUnenclosedSQL(new StringBuilder(), TRACE_SQL_FLAGS);
        int index = builder.length();
        return DbException.getSyntaxError(
                IntervalOperation.getForcedTypeSQL(builder.append(' '), forcedType).toString(), index, "");
    }

    private void swap() {
        Expression temp = left;
        left = right;
        right = temp;
    }

    /**
     * Returns the type of this binary operation.
     *
     * @return the type of this binary operation
     */
    public OpType getOperationType() {
        return opType;
    }

}
