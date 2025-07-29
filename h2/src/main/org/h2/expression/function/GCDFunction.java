/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.math.BigInteger;

import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;

/**
 * GCD and LCM functions.
 */
public class GCDFunction extends FunctionN {

    /**
     * GCD() (non-standard).
     */
    public static final int GCD = 0;

    /**
     * LCM() (non-standard).
     */
    public static final int LCM = GCD + 1;

    private static final String[] NAMES = { //
            "GCD", "LCM" //
    };

    public static final int MAX_BIT_LENGTH = (int) Math.ceil(Constants.MAX_NUMERIC_PRECISION / Math.log10(2));

    private final int function;

    public GCDFunction(int function) {
        super(new Expression[2]);
        this.function = function;
        type = TypeInfo.TYPE_NUMERIC_SCALE_0;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v1 = args[0].getValue(session);
        if (v1 == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        Value v2 = args[1].getValue(session);
        if (v2 == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        BigInteger a = v1.getBigInteger(), b = v2.getBigInteger();
        switch (function) {
        case GCD:
            return gcd(session, a, b);
        case LCM:
            return lcm(session, a, b);
        default:
            throw DbException.getInternalError("function=" + function);
        }
    }

    private Value gcd(SessionLocal session, BigInteger a, BigInteger b) {
        a = a.gcd(b);
        int count = args.length;
        if (count > 2) {
            boolean one = a.equals(BigInteger.ONE);
            for (int i = 2; i < count; i++) {
                Value v = args[i].getValue(session);
                if (v == ValueNull.INSTANCE) {
                    return ValueNull.INSTANCE;
                }
                if (!one) {
                    b = v.getBigInteger();
                    if (b.signum() != 0) {
                        a = a.gcd(b);
                        one = a.equals(BigInteger.ONE);
                    }
                }
            }
        }
        return ValueNumeric.get(a);
    }

    private Value lcm(SessionLocal session, BigInteger a, BigInteger b) {
        boolean zero = a.signum() == 0 || b.signum() == 0;
        a = zero ? BigInteger.ZERO : a.multiply(b).abs().divide(a.gcd(b));
        int count = args.length;
        if (count > 2) {
            boolean overflow = !zero && a.bitLength() > MAX_BIT_LENGTH;
            for (int i = 2; i < count; i++) {
                Value v = args[i].getValue(session);
                if (v == ValueNull.INSTANCE) {
                    return ValueNull.INSTANCE;
                }
                if (!zero) {
                    b = v.getBigInteger();
                    if (b.signum() == 0) {
                        a = BigInteger.ZERO;
                        zero = true;
                        overflow = false;
                    } else if (!overflow) {
                        a = a.multiply(b).abs().divide(a.gcd(b));
                        overflow = a.bitLength() > MAX_BIT_LENGTH;
                    }
                }
            }
            if (overflow) {
                throw DbException.getValueTooLongException("NUMERIC", "unknown least common multiple", -1);
            }
        }
        return ValueNumeric.get(a);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        boolean allConst = true, wasNull = false;
        for (int i = 0, l = args.length; i < l; i++) {
            Expression e = args[i].optimize(session);
            wasNull |= checkType(e, getName());
            args[i] = e;
            if (allConst && !e.isConstant()) {
                allConst = false;
            }
        }
        if (allConst) {
            return TypedValueExpression.getTypedIfNull(getValue(session), TypeInfo.TYPE_NUMERIC_SCALE_0);
        } else if (wasNull) {
            return TypedValueExpression.get(ValueNull.INSTANCE, TypeInfo.TYPE_NUMERIC_SCALE_0);
        }
        inlineSubexpressions(t -> t instanceof GCDFunction && ((GCDFunction) t).function == function);
        return this;
    }

    /**
     * Checks type of GCD, LCM, GCD_AGG, or LCM_AGG argument.
     *
     * @param e
     *            the argument
     * @param name
     *            the name of the function
     * @return {@code true} if argument has NULL data type, {@code false}
     *         otherwise
     */
    public static boolean checkType(Expression e, String name) {
        TypeInfo argType = e.getType();
        switch (argType.getValueType()) {
        case Value.NULL:
            return true;
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT:
            break;
        case Value.NUMERIC:
            if (argType.getScale() == 0) {
                break;
            }
            //$FALL-THROUGH$
        default:
            throw DbException.getInvalidValueException(name + " argument", e.getTraceSQL());
        }
        return false;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
