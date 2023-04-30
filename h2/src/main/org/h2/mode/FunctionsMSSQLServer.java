/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import java.util.HashMap;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.expression.function.CoalesceFunction;
import org.h2.expression.function.CurrentDateTimeValueFunction;
import org.h2.expression.function.RandFunction;
import org.h2.expression.function.StringFunction;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueNull;

/**
 * Functions for {@link org.h2.engine.Mode.ModeEnum#MSSQLServer} compatibility
 * mode.
 */
public final class FunctionsMSSQLServer extends ModeFunction {

    private static final HashMap<String, FunctionInfo> FUNCTIONS = new HashMap<>();

    private static final int CHARINDEX = 4001;

    private static final int GETDATE = CHARINDEX + 1;

    private static final int ISNULL = GETDATE + 1;

    private static final int LEN = ISNULL + 1;

    private static final int NEWID = LEN + 1;

    private static final int NEWSEQUENTIALID = NEWID + 1;

    private static final int SCOPE_IDENTITY = NEWSEQUENTIALID + 1;

    private static final TypeInfo SCOPE_IDENTITY_TYPE = TypeInfo.getTypeInfo(Value.NUMERIC, 38, 0, null);

    static {
        FUNCTIONS.put("CHARINDEX", new FunctionInfo("CHARINDEX", CHARINDEX, VAR_ARGS, Value.INTEGER, true, true));
        FUNCTIONS.put("GETDATE", new FunctionInfo("GETDATE", GETDATE, 0, Value.TIMESTAMP, false, true));
        FUNCTIONS.put("LEN", new FunctionInfo("LEN", LEN, 1, Value.INTEGER, true, true));
        FUNCTIONS.put("NEWID", new FunctionInfo("NEWID", NEWID, 0, Value.UUID, true, false));
        FUNCTIONS.put("NEWSEQUENTIALID",
                new FunctionInfo("NEWSEQUENTIALID", NEWSEQUENTIALID, 0, Value.UUID, true, false));
        FUNCTIONS.put("ISNULL", new FunctionInfo("ISNULL", ISNULL, 2, Value.NULL, false, true));
        FUNCTIONS.put("SCOPE_IDENTITY",
                new FunctionInfo("SCOPE_IDENTITY", SCOPE_IDENTITY, 0, Value.NUMERIC, true, false));
    }

    /**
     * Returns mode-specific function for a given name, or {@code null}.
     *
     * @param upperName
     *            the upper-case name of a function
     * @return the function with specified name or {@code null}
     */
    public static FunctionsMSSQLServer getFunction(String upperName) {
        FunctionInfo info = FUNCTIONS.get(upperName);
        if (info != null) {
            return new FunctionsMSSQLServer(info);
        }
        return null;
    }

    private FunctionsMSSQLServer(FunctionInfo info) {
        super(info);
    }

    @Override
    protected void checkParameterCount(int len) {
        int min, max;
        switch (info.type) {
        case CHARINDEX:
            min = 2;
            max = 3;
            break;
        default:
            throw DbException.getInternalError("type=" + info.type);
        }
        if (len < min || len > max) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, min + ".." + max);
        }
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value[] values = getArgumentsValues(session, args);
        if (values == null) {
            return ValueNull.INSTANCE;
        }
        Value v0 = getNullOrValue(session, args, values, 0);
        switch (info.type) {
        case LEN: {
            long len;
            if (v0.getValueType() == Value.CHAR) {
                String s = v0.getString();
                int l = s.length();
                while (l > 0 && s.charAt(l - 1) == ' ') {
                    l--;
                }
                len = l;
            } else {
                len = v0.charLength();
            }
            return ValueBigint.get(len);
        }
        case SCOPE_IDENTITY:
            return session.getLastIdentity().convertTo(type);
        default:
            throw DbException.getInternalError("type=" + info.type);
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        switch (info.type) {
        case CHARINDEX:
            return new StringFunction(args, StringFunction.LOCATE).optimize(session);
        case GETDATE:
            return new CurrentDateTimeValueFunction(CurrentDateTimeValueFunction.LOCALTIMESTAMP, 3).optimize(session);
        case ISNULL:
            return new CoalesceFunction(CoalesceFunction.COALESCE, args).optimize(session);
        case NEWID:
        case NEWSEQUENTIALID:
            return new RandFunction(null, RandFunction.RANDOM_UUID).optimize(session);
        case SCOPE_IDENTITY:
            type = SCOPE_IDENTITY_TYPE;
            break;
        default:
            type = TypeInfo.getTypeInfo(info.returnDataType);
            if (optimizeArguments(session)) {
                return TypedValueExpression.getTypedIfNull(getValue(session), type);
            }
        }
        return this;
    }

}
