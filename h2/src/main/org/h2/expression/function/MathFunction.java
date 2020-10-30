/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
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
        case CEIL:
            int t = v1.getValueType();
            switch (t) {
            case Value.NUMERIC:
                v1 = ValueNumeric.get(
                        v1.getBigDecimal().setScale(0, function == FLOOR ? RoundingMode.FLOOR : RoundingMode.CEILING));
                break;
            case Value.DECFLOAT: {
                BigDecimal bd = v1.getBigDecimal();
                if (bd.scale() > 0) {
                    v1 = ValueDecfloat.get(
                            bd.setScale(0, function == FLOOR ? RoundingMode.FLOOR : RoundingMode.CEILING));
                }
                break;
            }
            default:
                double v = v1.getDouble();
                v = function == FLOOR ? Math.floor(v) : Math.ceil(v);
                v1 = t == Value.DOUBLE ? ValueDouble.get(v) : ValueReal.get((float) v);
                break;
            }
            break;
        case ROUND:
            v1 = round(session, v1);
            break;
        case ROUNDMAGIC:
            v1 = ValueDouble.get(roundMagic(v1.getDouble()));
            break;
        case SIGN:
            v1 = ValueInteger.get(v1.getSignum());
            break;
        case TRUNC:
            v1 = trunc(session, v1);
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v1;
    }

    private Value round(SessionLocal session, Value v1) {
        int scale;
        if (right != null) {
            Value v2 = right.getValue(session);
            if (v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            scale = checkScale(v2);
        } else {
            scale = 0;
        }
        BigDecimal bd = v1.getBigDecimal().setScale(scale, RoundingMode.HALF_UP);
        switch (type.getValueType()) {
        case Value.DOUBLE:
            v1 = ValueDouble.get(bd.doubleValue());
            break;
        case Value.REAL:
            v1 = ValueReal.get(bd.floatValue());
            break;
        case Value.DECFLOAT:
            v1 = ValueDecfloat.get(bd);
            break;
        default:
            v1 = ValueNumeric.get(bd.setScale(type.getScale(), RoundingMode.HALF_UP));
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

    private Value trunc(SessionLocal session, Value v1) {
        int scale;
        if (right != null) {
            Value v2 = right.getValue(session);
            if (v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            scale = checkScale(v2);
        } else {
            scale = 0;
        }
        int t = v1.getValueType();
        switch (t) {
        case Value.DOUBLE:
        case Value.REAL:
            double d = v1.getDouble();
            if (scale == 0) {
                d = d < 0 ? Math.ceil(d) : Math.floor(d);
            } else {
                double f = Math.pow(10, scale);
                d *= f;
                d = (d < 0 ? Math.ceil(d) : Math.floor(d)) / f;
            }
            v1 = t == Value.DOUBLE ? ValueDouble.get(d) : ValueReal.get((float) d);
            break;
        case Value.DECFLOAT:
            v1 = ValueDecfloat.get(v1.getBigDecimal().setScale(scale, RoundingMode.DOWN));
            break;
        default:
            v1 = ValueNumeric.get(v1.getBigDecimal().setScale(scale, RoundingMode.DOWN));
            break;
        }
        return v1;
    }

    private static int checkScale(Value v) {
        int scale;
        scale = v.getInt();
        if (scale < 0 || scale > ValueNumeric.MAXIMUM_SCALE) {
            throw DbException.getInvalidValueException("digits", scale);
        }
        return scale;
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
                type = TypeInfo.TYPE_NUMERIC;
            }
            break;
        case FLOOR:
        case CEIL: {
            type = left.getType();
            int valueType = type.getValueType();
            switch (valueType) {
            case Value.NULL:
                type = TypeInfo.TYPE_NUMERIC_SCALE_0;
                break;
            case Value.TINYINT:
            case Value.SMALLINT:
            case Value.INTEGER:
            case Value.BIGINT:
                return left;
            case Value.REAL:
            case Value.DOUBLE:
            case Value.DECFLOAT:
                break;
            case Value.NUMERIC:
                if (type.getScale() > 0) {
                    type = TypeInfo.getTypeInfo(Value.NUMERIC, type.getPrecision(), 0, null);
                }
                break;
            default:
                throw DbException.getInvalidValueException("numeric", type.getTraceSQL());
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
                throw DbException.getInvalidValueException("numeric", commonType.getTraceSQL());
            }
            type = DataType.isNumericType(divisorType.getValueType()) ? divisorType : commonType;
            break;
        case ROUND:
            switch (left.getType().getValueType()) {
            case Value.DOUBLE:
            case Value.REAL:
            case Value.DECFLOAT:
                type = left.getType();
                break;
            default:
                type = getRoundNumericType(session);
            }
            break;
        case ROUNDMAGIC:
            type = TypeInfo.TYPE_DOUBLE;
            break;
        case SIGN:
            type = TypeInfo.TYPE_INTEGER;
            break;
        case TRUNC:
            switch (left.getType().getValueType()) {
            case Value.DOUBLE:
            case Value.REAL:
            case Value.DECFLOAT:
                type = left.getType();
                break;
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
            default:
                type = getRoundNumericType(session);
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

    private TypeInfo getRoundNumericType(SessionLocal session) {
        int scale = 0;
        if (right != null) {
            if (right.isConstant()) {
                Value scaleValue = right.getValue(session);
                if (scaleValue != ValueNull.INSTANCE) {
                    scale = scaleValue.getInt();
                    if (scale < 0) {
                        scale = 0;
                    }
                }
            } else {
                scale = ValueNumeric.MAXIMUM_SCALE;
            }
        }
        return TypeInfo.getTypeInfo(Value.NUMERIC, Integer.MAX_VALUE, scale, null);
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
