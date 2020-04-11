/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import java.util.HashMap;

import org.h2.expression.function.Function;
import org.h2.expression.function.FunctionInfo;

/**
 * Functions for {@link org.h2.engine.Mode.ModeEnum#DB2} and
 * {@link org.h2.engine.Mode.ModeEnum#Derby} compatibility modes.
 */
public final class FunctionsDB2Derby extends FunctionsBase {

    private static final HashMap<String, FunctionInfo> FUNCTIONS = new HashMap<>();

    static {
        copyFunction(FUNCTIONS, "IDENTITY", "IDENTITY_VAL_LOCAL");
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
        return info != null ? new Function(info) : null;
    }

    private FunctionsDB2Derby(FunctionInfo info) {
        super(info);
    }

}
