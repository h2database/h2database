/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.mvstore.db.Store;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueDecfloat;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueReal;

/**
 * A math function.
 */
public final class MathFunction extends Function1_2 {

    /**
     * ABS().
     */
    public static final int ABS = 0;

    /**
     * MOD().
     */
    public static final int MOD = ABS + 1;

    /**
     * FLOOR().
     */
    public static final int FLOOR = MOD + 1;

    /**
     * CEIL() or CEILING().
     */
    public static final int CEIL = FLOOR + 1;

    /**
     * ROUND() (non-standard)
     */
    public static final int ROUND = CEIL + 1;

    /**
     * ROUNDMAGIC() (non-standard)
     */
    public static final int ROUNDMAGIC = ROUND + 1;

    /**
     * SIGN() (non-standard)
     */
    public static final int SIGN = ROUNDMAGIC + 1;

    /**
     * TRUNC() (non-standard)
     */
    public static final int TRUNC = SIGN + 1;

    private static final String[] NAMES = { //
            "ABS", "MOD", "FLOOR", "CEIL", "ROUND", "ROUNDMAGIC", "SIGN", "TRUNC" //
    };

    private final int function;

    private TypeInfo commonType;

    public MathFunction(Expression arg1, Expression arg2, int function) {
        super(arg1, arg2);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session, Value v1, Value v2) {
        switch (function) {
        case ABS:
            if (v1.getSignum() < 0) {
                v1 = v1.negate();
            }
            break;
        case MOD:
            v1 = v1.convertTo(commonType, session).modulus(v2.convertTo(commonType, session)).convertTo(type, session);
            break;
        case FLOOR:
            v1 = round(v1, v2, RoundingMode.FLOOR);
            break;
        case CEIL:
            v1 = round(v1, v2, RoundingMode.CEILING);
            break;
        case ROUND:
            v1 = round(v1, v2, RoundingMode.HALF_UP);
            break;
        case ROUNDMAGIC:
            v1 = ValueDouble.get(roundMagic(v1.getDouble()));
            break;
        case SIGN:
            v1 = ValueInteger.get(v1.getSignum());
            break;
        case TRUNC:
            v1 = round(v1, v2, RoundingMode.DOWN);
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v1;
    }

    @SuppressWarnings("incomplete-switch")
    private Value round(Value v1, Value v2, RoundingMode roundingMode) {
        int scale = v2 != null ? checkScale(v2) : 0;
        int t = type.getValueType();
        c: switch (t) {
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT: {
            if (scale < 0) {
                long original = v1.getLong();
                long scaled = scale < -18 ? 0L
                        : Value.convertToLong(BigDecimal.valueOf(original).setScale(scale, roundingMode), null);
                if (original != scaled) {
                    v1 = ValueBigint.get(scaled).convertTo(type);
                }
            }
            break;
        }
        case Value.NUMERIC: {
            int targetScale = type.getScale();
            BigDecimal bd = v1.getBigDecimal();
            if (scale < targetScale) {
                bd = bd.setScale(scale, roundingMode);
            }
            v1 = ValueNumeric.get(bd.setScale(targetScale, roundingMode));
            break;
        }
        case Value.REAL:
        case Value.DOUBLE: {
            l: if (scale == 0) {
                double d;
                switch (roundingMode) {
                case DOWN:
                    d = v1.getDouble();
                    d = d < 0 ? Math.ceil(d) : Math.floor(d);
                    break;
                case CEILING:
                    d = Math.ceil(v1.getDouble());
                    break;
                case FLOOR:
                    d = Math.floor(v1.getDouble());
                    break;
                default:
                    break l;
                }
                v1 = t == Value.REAL ? ValueReal.get((float) d) : ValueDouble.get(d);
                break c;
            }
            BigDecimal bd = v1.getBigDecimal().setScale(scale, roundingMode);
            v1 = t == Value.REAL ? ValueReal.get(bd.floatValue()) : ValueDouble.get(bd.doubleValue());
            break;
        }
        case Value.DECFLOAT:
            v1 = ValueDecfloat.get(v1.getBigDecimal().setScale(scale, roundingMode));
        }
        return v1;
    }

    private static double roundMagic(double d) {
        if ((d < 0.000_000_000_000_1) && (d > -0.000_000_000_000_1)) {
            return 0.0;
        }
        if ((d > 1_000_000_000_000d) || (d < -1_000_000_000_000d)) {
            return d;
        }
        StringBuilder s = new StringBuilder();
        s.append(d);
        if (s.toString().indexOf('E') >= 0) {
            return d;
        }
        int len = s.length();
        if (len < 16) {
            return d;
        }
        if (s.toString().indexOf('.') > len - 3) {
            return d;
        }
        s.delete(len - 2, len);
        len -= 2;
        char c1 = s.charAt(len - 2);
        char c2 = s.charAt(len - 3);
        char c3 = s.charAt(len - 4);
        if ((c1 == '0') && (c2 == '0') && (c3 == '0')) {
            s.setCharAt(len - 1, '0');
        } else if ((c1 == '9') && (c2 == '9') && (c3 == '9')) {
            s.setCharAt(len - 1, '9');
            s.append('9');
            s.append('9');
            s.append('9');
        }
        return Double.parseDouble(s.toString());
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        if (right != null) {
            right = right.optimize(session);
        }
        switch (function) {
        case ABS:
            type = left.getType();
            if (type.getValueType() == Value.NULL) {
                type = TypeInfo.TYPE_NUMERIC_FLOATING_POINT;
            }
            break;
        case FLOOR:
        case CEIL: {
            Expression e = optimizeRound(0, true, false, true);
            if (e != null) {
                return e;
            }
            break;
        }
        case MOD:
            TypeInfo divisorType = right.getType();
            commonType = TypeInfo.getHigherType(left.getType(), divisorType);
            int valueType = commonType.getValueType();
            if (valueType == Value.NULL) {
                commonType = TypeInfo.TYPE_BIGINT;
            } else if (!DataType.isNumericType(valueType)) {
                throw Store.getInvalidExpressionTypeException("MOD argument",
                        DataType.isNumericType(left.getType().getValueType()) ? right : left);
            }
            type = DataType.isNumericType(divisorType.getValueType()) ? divisorType : commonType;
            break;
        case ROUND: {
            Expression e = optimizeRoundWithScale(session, true);
            if (e != null) {
                return e;
            }
            break;
        }
        case ROUNDMAGIC:
            type = TypeInfo.TYPE_DOUBLE;
            break;
        case SIGN:
            type = TypeInfo.TYPE_INTEGER;
            break;
        case TRUNC:
            switch (left.getType().getValueType()) {
            case Value.VARCHAR:
                left = new CastSpecification(left, TypeInfo.getTypeInfo(Value.TIMESTAMP, -1L, 0, null))
                        .optimize(session);
                //$FALL-THROUGH$
            case Value.TIMESTAMP:
            case Value.TIMESTAMP_TZ:
                if (right != null) {
                    throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, "TRUNC", "1");
                }
                return new DateTimeFunction(DateTimeFunction.DATE_TRUNC, DateTimeFunction.DAY, left, null)
                        .optimize(session);
            case Value.DATE:
                if (right != null) {
                    throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, "TRUNC", "1");
                }
                return new CastSpecification(left, TypeInfo.getTypeInfo(Value.TIMESTAMP, -1L, 0, null))
                        .optimize(session);
            default: {
                Expression e = optimizeRoundWithScale(session, false);
                if (e != null) {
                    return e;
                }
            }
            }
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        if (left.isConstant() && (right == null || right.isConstant())) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    private Expression optimizeRoundWithScale(SessionLocal session, boolean possibleRoundUp) {
        int scale;
        boolean scaleIsKnown = false, scaleIsNull = false;
        if (right != null) {
            if (right.isConstant()) {
                Value scaleValue = right.getValue(session);
                scaleIsKnown = true;
                if (scaleValue != ValueNull.INSTANCE) {
                    scale = checkScale(scaleValue);
                } else {
                    scale = -1;
                    scaleIsNull = true;
                }
            } else {
                scale = -1;
            }
        } else {
            scale = 0;
            scaleIsKnown = true;
        }
        return optimizeRound(scale, scaleIsKnown, scaleIsNull, possibleRoundUp);
    }

    private static int checkScale(Value v) {
        int scale = v.getInt();
        if (scale < -ValueNumeric.MAXIMUM_SCALE || scale > ValueNumeric.MAXIMUM_SCALE) {
            throw DbException.getInvalidValueException("scale", scale);
        }
        return scale;
    }

    /**
     * Optimizes rounding and truncation functions.
     *
     * @param scale
     *            the scale, if known
     * @param scaleIsKnown
     *            whether scale is known
     * @param scaleIsNull
     *            whether scale is {@code NULL}
     * @param possibleRoundUp
     *            {@code true} if result of rounding can have larger precision
     *            than precision of argument, {@code false} otherwise
     * @return the optimized expression or {@code null} if this function should
     *         be used
     */
    private Expression optimizeRound(int scale, boolean scaleIsKnown, boolean scaleIsNull, boolean possibleRoundUp) {
        TypeInfo leftType = left.getType();
        switch (leftType.getValueType()) {
        case Value.NULL:
            type = TypeInfo.TYPE_NUMERIC_SCALE_0;
            break;
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT:
            if (scaleIsKnown && scale >= 0) {
                return left;
            }
            type = leftType;
            break;
        case Value.REAL:
        case Value.DOUBLE:
        case Value.DECFLOAT:
            type = leftType;
            break;
        case Value.NUMERIC: {
            long precision;
            int originalScale = leftType.getScale();
            if (scaleIsKnown) {
                if (originalScale <= scale) {
                    return left;
                } else {
                    if (scale < 0) {
                        scale = 0;
                    } else if (scale > ValueNumeric.MAXIMUM_SCALE) {
                        scale = ValueNumeric.MAXIMUM_SCALE;
                    }
                    precision = leftType.getPrecision() - originalScale + scale;
                    if (possibleRoundUp) {
                        precision++;
                    }
                }
            } else {
                precision = leftType.getPrecision();
                if (possibleRoundUp) {
                    precision++;
                }
                scale = originalScale;
            }
            type = TypeInfo.getTypeInfo(Value.NUMERIC, precision, scale, null);
            break;
        }
        default:
            throw Store.getInvalidExpressionTypeException(getName() + " argument", left);
        }
        if (scaleIsNull) {
            return TypedValueExpression.get(ValueNull.INSTANCE, type);
        }
        return null;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
