/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import java.util.HashMap;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.function.CompatibilityIdentityFunction;
import org.h2.expression.function.Function;
import org.h2.expression.function.FunctionInfo;
import org.h2.message.DbException;
import org.h2.value.Value;

/**
 * Functions for {@link org.h2.engine.Mode.ModeEnum#DB2} and
 * {@link org.h2.engine.Mode.ModeEnum#Derby} compatibility modes.
 */
public final class FunctionsDB2Derby extends FunctionsBase {

    private static final int IDENTITY_VAL_LOCAL = 5001;

    private static final HashMap<String, FunctionInfo> FUNCTIONS = new HashMap<>();

    static {
        FUNCTIONS.put("IDENTITY_VAL_LOCAL",
                new FunctionInfo("IDENTITY_VAL_LOCAL", IDENTITY_VAL_LOCAL, 0, Value.BIGINT, true, false));
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
        return info != null ? new FunctionsDB2Derby(info) : null;
    }

    private FunctionsDB2Derby(FunctionInfo info) {
        super(info);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        switch (info.type) {
        case IDENTITY_VAL_LOCAL:
            return new CompatibilityIdentityFunction(false).optimize(session);
        default:
            throw DbException.throwInternalError("type=" + info.type);
        }
    }

}
