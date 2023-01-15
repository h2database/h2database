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
import org.h2.expression.ValueExpression;
import org.h2.expression.function.DateTimeFunction;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueUuid;

/**
 * Functions for {@link org.h2.engine.Mode.ModeEnum#Oracle} compatibility mode.
 */
public final class FunctionsOracle extends ModeFunction {

    private static final int ADD_MONTHS = 2001;

    private static final int SYS_GUID = ADD_MONTHS + 1;

    private static final int TO_DATE = SYS_GUID + 1;

    private static final int TO_TIMESTAMP = TO_DATE + 1;

    private static final int TO_TIMESTAMP_TZ = TO_TIMESTAMP + 1;

    private static final HashMap<String, FunctionInfo> FUNCTIONS = new HashMap<>();

    static {
        FUNCTIONS.put("ADD_MONTHS",
                new FunctionInfo("ADD_MONTHS", ADD_MONTHS, 2, Value.TIMESTAMP, true, true));
        FUNCTIONS.put("SYS_GUID",
                new FunctionInfo("SYS_GUID", SYS_GUID, 0, Value.VARBINARY, false, false));
        FUNCTIONS.put("TO_DATE",
                new FunctionInfo("TO_DATE", TO_DATE, VAR_ARGS, Value.TIMESTAMP, true, true));
        FUNCTIONS.put("TO_TIMESTAMP",
                new FunctionInfo("TO_TIMESTAMP", TO_TIMESTAMP, VAR_ARGS, Value.TIMESTAMP, true, true));
        FUNCTIONS.put("TO_TIMESTAMP_TZ",
                new FunctionInfo("TO_TIMESTAMP_TZ", TO_TIMESTAMP_TZ, VAR_ARGS, Value.TIMESTAMP_TZ, true, true));
    }

    /**
     * Returns mode-specific function for a given name, or {@code null}.
     *
     * @param upperName
     *            the upper-case name of a function
     * @return the function with specified name or {@code null}
     */
    public static FunctionsOracle getFunction(String upperName) {
        FunctionInfo info = FUNCTIONS.get(upperName);
        return info != null ? new FunctionsOracle(info) : null;
    }

    private FunctionsOracle(FunctionInfo info) {
        super(info);
    }

    @Override
    protected void checkParameterCount(int len) {
        int min = 0, max = Integer.MAX_VALUE;
        switch (info.type) {
        case TO_TIMESTAMP:
        case TO_TIMESTAMP_TZ:
            min = 1;
            max = 2;
            break;
        case TO_DATE:
            min = 1;
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
    public Expression optimize(SessionLocal session) {
        boolean allConst = optimizeArguments(session);
        switch (info.type) {
        case SYS_GUID:
            type = TypeInfo.getTypeInfo(Value.VARBINARY, 16, 0, null);
            break;
        default:
            type = TypeInfo.getTypeInfo(info.returnDataType);
        }
        if (allConst) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value[] values = getArgumentsValues(session, args);
        if (values == null) {
            return ValueNull.INSTANCE;
        }
        Value v0 = getNullOrValue(session, args, values, 0);
        Value v1 = getNullOrValue(session, args, values, 1);
        Value result;
        switch (info.type) {
        case ADD_MONTHS:
            result = DateTimeFunction.dateadd(session, DateTimeFunction.MONTH, v1.getInt(), v0);
            break;
        case SYS_GUID:
            result = ValueUuid.getNewRandom().convertTo(TypeInfo.TYPE_VARBINARY);
            break;
        case TO_DATE:
            result = ToDateParser.toDate(session, v0.getString(), v1 == null ? null : v1.getString());
            break;
        case TO_TIMESTAMP:
            result = ToDateParser.toTimestamp(session, v0.getString(), v1 == null ? null : v1.getString());
            break;
        case TO_TIMESTAMP_TZ:
            result = ToDateParser.toTimestampTz(session, v0.getString(), v1 == null ? null : v1.getString());
            break;
        default:
            throw DbException.getInternalError("type=" + info.type);
        }
        return result;
    }

}
