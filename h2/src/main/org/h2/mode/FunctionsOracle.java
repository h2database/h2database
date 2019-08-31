/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import java.util.HashMap;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.function.Function;
import org.h2.expression.function.FunctionInfo;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueUuid;

/**
 * Functions for {@link org.h2.engine.Mode.ModeEnum#Oracle} compatibility mode.
 */
public final class FunctionsOracle extends FunctionsBase {

    private static final int SYS_GUID = 2001;

    private static final HashMap<String, FunctionInfo> FUNCTIONS = new HashMap<>();

    static {
        FUNCTIONS.put("SYS_GUID", new FunctionInfo("SYS_GUID", SYS_GUID, 0, Value.BYTES, false, false, true, false));
    }

    /**
     * Returns mode-specific function for a given name, or {@code null}.
     *
     * @param database
     *            the database
     * @param upperName
     *            the upper-case name of a function
     * @return the function with specified name or {@code null}
     */
    public static Function getFunction(Database database, String upperName) {
        FunctionInfo info = FUNCTIONS.get(upperName);
        return info != null ? new FunctionsOracle(database, info) : null;
    }

    private FunctionsOracle(Database database, FunctionInfo info) {
        super(database, info);
    }

    @Override
    public Expression optimize(Session session) {
        switch (info.type) {
        case SYS_GUID:
            type = TypeInfo.getTypeInfo(Value.BYTES, 16, 0, null);
            break;
        default:
            type = TypeInfo.getTypeInfo(info.returnDataType);
        }
        return this;
    }

    @Override
    protected Value getValueWithArgs(Session session, Expression[] args) {
        Value result;
        switch (info.type) {
        case SYS_GUID:
            result = ValueUuid.getNewRandom().convertTo(Value.BYTES);
            break;
        default:
            throw DbException.throwInternalError("type=" + info.type);
        }
        return result;
    }

}
