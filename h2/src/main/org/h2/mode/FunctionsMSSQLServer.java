/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import java.util.HashMap;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.function.CurrentDateTimeValueFunction;
import org.h2.expression.function.Function;
import org.h2.expression.function.FunctionInfo;
import org.h2.message.DbException;
import org.h2.value.Value;

/**
 * Functions for {@link org.h2.engine.Mode.ModeEnum#MSSQLServer} compatibility
 * mode.
 */
public final class FunctionsMSSQLServer extends FunctionsBase {

    private static final HashMap<String, FunctionInfo> FUNCTIONS = new HashMap<>();

    private static final int GETDATE = 4001;

    static {
        copyFunction(FUNCTIONS, "LOCATE", "CHARINDEX");
        FUNCTIONS.put("GETDATE", new FunctionInfo("GETDATE", GETDATE, 0, Value.TIMESTAMP, false, true, true, false));
        FUNCTIONS.put("ISNULL", new FunctionInfo("ISNULL", Function.COALESCE,
                2, Value.NULL, false, true, true, false));
        copyFunction(FUNCTIONS, "LENGTH", "LEN");
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
    public Expression optimize(Session session) {
        switch (info.type) {
        case GETDATE:
            return new CurrentDateTimeValueFunction(CurrentDateTimeValueFunction.LOCALTIMESTAMP, 3).optimize(session);
        default:
            throw DbException.throwInternalError("type=" + info.type);
        }
    }

}
