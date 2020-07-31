/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.Operation1_2;
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
public final class MathFunction extends Operation1_2 implements NamedExpression {

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
     * SIGN() (non-standard)
     */
    public static final int SIGN = CEIL + 1;

    private static final String[] NAMES = { //
            "ABS", "MOD", "FLOOR", "CEIL", "SIGN" //
    };

    private final int function;

    private TypeInfo commonType;

    public MathFunction(Expression arg1, Expression arg2, int function) {
        super(arg1, arg2);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v1 = left.getValue(session);
        if (v1 == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        switch (function) {
        case ABS:
            if (v1.getSignum() < 0) {
                v1 = v1.negate();
            }
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
        case SIGN:
            v1 = ValueInteger.get(v1.getSignum());
            break;
        default:
            Value v2 = right.getValue(session);
            if (v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            switch (function) {
            case MOD:
                v1 = v1.convertTo(commonType, session).modulus(v2.convertTo(commonType, session)).convertTo(type,
                        session);
                break;
            default:
                throw DbException.throwInternalError("function=" + function);
            }
            break;
        }
        return v1;
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
                throw DbException.getInvalidValueException("numeric", commonType.getTraceSQL());
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
        case SIGN:
            type = TypeInfo.TYPE_INTEGER;
            break;
        default:
            type = TypeInfo.TYPE_DOUBLE;
            break;
        }
        if (left.isConstant() && (right == null || right.isConstant())) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        left.getUnenclosedSQL(builder.append(getName()).append('('), sqlFlags);
        if (right != null) {
            right.getUnenclosedSQL(builder.append(", "), sqlFlags);
        }
        return builder.append(')');
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
