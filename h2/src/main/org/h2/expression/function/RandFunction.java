/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.util.Random;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.DbException;
import org.h2.util.MathUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueNull;
import org.h2.value.ValueUuid;
import org.h2.value.ValueVarbinary;

/**
 * A RAND, SECURE_RAND, or RANDOM_UUID function.
 */
public final class RandFunction extends Function0_1 {

    /**
     * RAND() (non-standard).
     */
    public static final int RAND = 0;

    /**
     * SECURE_RAND() (non-standard).
     */
    public static final int SECURE_RAND = RAND + 1;

    /**
     * RANDOM_UUID() (non-standard).
     */
    public static final int RANDOM_UUID = SECURE_RAND + 1;

    private static final String[] NAMES = { //
            "RAND", "SECURE_RAND", "RANDOM_UUID" //
    };

    private final int function;

    public RandFunction(Expression arg, int function) {
        super(arg);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v;
        if (arg != null) {
            v = arg.getValue(session);
            if (v == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
        } else {
            v = null;
        }
        switch (function) {
        case RAND: {
            Random random = session.getRandom();
            if (v != null) {
                random.setSeed(v.getInt());
            }
            v = ValueDouble.get(random.nextDouble());
            break;
        }
        case SECURE_RAND:
            v = ValueVarbinary.getNoCopy(MathUtils.secureRandomBytes(v.getInt()));
            break;
        case RANDOM_UUID:
            v = ValueUuid.getNewRandom(v != null ? v.getInt() : 4);
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        if (arg != null) {
            arg = arg.optimize(session);
        }
        switch (function) {
        case RAND:
            type = TypeInfo.TYPE_DOUBLE;
            break;
        case SECURE_RAND: {
            Value v;
            type = arg.isConstant() && (v = arg.getValue(session)) != ValueNull.INSTANCE
                    ? TypeInfo.getTypeInfo(Value.VARBINARY, Math.max(v.getInt(), 1), 0, null)
                    : TypeInfo.TYPE_VARBINARY;
            break;
        }
        case RANDOM_UUID:
            type = TypeInfo.TYPE_UUID;
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return this;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
            return false;
        }
        return super.isEverything(visitor);
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
