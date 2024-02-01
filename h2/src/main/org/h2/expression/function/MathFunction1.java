/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueNull;

/**
 * A math function with one argument and DOUBLE PRECISION result.
 */
public final class MathFunction1 extends Function1 {

    // Trigonometric functions

    /**
     * SIN().
     */
    public static final int SIN = 0;

    /**
     * COS().
     */
    public static final int COS = SIN + 1;

    /**
     * TAN().
     */
    public static final int TAN = COS + 1;

    /**
     * COT() (non-standard).
     */
    public static final int COT = TAN + 1;

    /**
     * SINH().
     */
    public static final int SINH = COT + 1;

    /**
     * COSH().
     */
    public static final int COSH = SINH + 1;

    /**
     * TANH().
     */
    public static final int TANH = COSH + 1;

    /**
     * ASIN().
     */
    public static final int ASIN = TANH + 1;

    /**
     * ACOS().
     */
    public static final int ACOS = ASIN + 1;

    /**
     * ATAN().
     */
    public static final int ATAN = ACOS + 1;

    // Logarithm functions

    /**
     * LOG10().
     */
    public static final int LOG10 = ATAN + 1;

    /**
     * LN().
     */
    public static final int LN = LOG10 + 1;

    // Exponential function

    /**
     * EXP().
     */
    public static final int EXP = LN + 1;

    // Square root

    /**
     * SQRT().
     */
    public static final int SQRT = EXP + 1;

    // Other non-standard

    /**
     * DEGREES() (non-standard).
     */
    public static final int DEGREES = SQRT + 1;

    /**
     * RADIANS() (non-standard).
     */
    public static final int RADIANS = DEGREES + 1;

    private static final String[] NAMES = { //
            "SIN", "COS", "TAN", "COT", "SINH", "COSH", "TANH", "ASIN", "ACOS", "ATAN", //
            "LOG10", "LN", "EXP", "SQRT", "DEGREES", "RADIANS" //
    };

    private final int function;

    public MathFunction1(Expression arg, int function) {
        super(arg);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v = arg.getValue(session);
        if (v == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        double d = v.getDouble();
        switch (function) {
        case SIN:
            d = Math.sin(d);
            break;
        case COS:
            d = Math.cos(d);
            break;
        case TAN:
            d = Math.tan(d);
            break;
        case COT:
            d = Math.tan(d);
            if (d == 0.0) {
                throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
            }
            d = 1d / d;
            break;
        case SINH:
            d = Math.sinh(d);
            break;
        case COSH:
            d = Math.cosh(d);
            break;
        case TANH:
            d = Math.tanh(d);
            break;
        case ASIN:
            if (d < -1d || d > 1d) {
                throw DbException.getInvalidValueException("ASIN() argument", d);
            }
            d = Math.asin(d);
            break;
        case ACOS:
            if (d < -1d || d > 1d) {
                throw DbException.getInvalidValueException("ACOS() argument", d);
            }
            d = Math.acos(d);
            break;
        case ATAN:
            d = Math.atan(d);
            break;
        case LOG10:
            if (d <= 0) {
                throw DbException.getInvalidValueException("LOG10() argument", d);
            }
            d = Math.log10(d);
            break;
        case LN:
            if (d <= 0) {
                throw DbException.getInvalidValueException("LN() argument", d);
            }
            d = Math.log(d);
            break;
        case EXP:
            d = Math.exp(d);
            break;
        case SQRT:
            d = Math.sqrt(d);
            break;
        case DEGREES:
            d = Math.toDegrees(d);
            break;
        case RADIANS:
            d = Math.toRadians(d);
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return ValueDouble.get(d);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        arg = arg.optimize(session);
        type = TypeInfo.TYPE_DOUBLE;
        if (arg.isConstant()) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
