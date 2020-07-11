/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import java.util.HashMap;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.expression.function.CoalesceFunction;
import org.h2.expression.function.CurrentDateTimeValueFunction;
import org.h2.expression.function.Function;
import org.h2.expression.function.FunctionInfo;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueNull;

/**
 * Functions for {@link org.h2.engine.Mode.ModeEnum#MSSQLServer} compatibility
 * mode.
 */
public final class FunctionsMSSQLServer extends FunctionsBase {

    private static final HashMap<String, FunctionInfo> FUNCTIONS = new HashMap<>();

    private static final int GETDATE = 4001;

    private static final int LEN = GETDATE + 1;

    private static final int ISNULL = LEN + 1;

    static {
        copyFunction(FUNCTIONS, "LOCATE", "CHARINDEX");
        FUNCTIONS.put("GETDATE", new FunctionInfo("GETDATE", GETDATE, 0, Value.TIMESTAMP, false, true));
        FUNCTIONS.put("LEN", new FunctionInfo("LEN", LEN, 1, Value.INTEGER, true, true));
        FUNCTIONS.put("ISNULL", new FunctionInfo("ISNULL", ISNULL, 2, Value.NULL, false, true));
        copyFunction(FUNCTIONS, "RANDOM_UUID", "NEWID");
    }

    /**
     * Returns mode-specific function for a given name, or {@code null}.
     *
     * @param upperName
     *            the upper-case name of a function
     * @return the function with specified name or {@code null}
     */
    public static Function getFunction(String upperName) {
        FunctionInfo info = FUNCTIONS.get(upperName);
        if (info != null) {
            if (info.type > 4000) {
                return new FunctionsMSSQLServer(info);
            }
            return new Function(info);
        }
        return null;
    }

    private FunctionsMSSQLServer(FunctionInfo info) {
        super(info);
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
                len = Function.length(v0);
            }
            return ValueBigint.get(len);
        }
        default:
            throw DbException.throwInternalError("type=" + info.type);
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        switch (info.type) {
        case GETDATE:
            return new CurrentDateTimeValueFunction(CurrentDateTimeValueFunction.LOCALTIMESTAMP, 3).optimize(session);
        case ISNULL:
            return new CoalesceFunction(CoalesceFunction.COALESCE, args).optimize(session);
        default:
            type = TypeInfo.getTypeInfo(info.returnDataType);
            if (optimizeArguments(session)) {
                return TypedValueExpression.getTypedIfNull(getValue(session), type);
            }
            return this;
        }
    }

}
