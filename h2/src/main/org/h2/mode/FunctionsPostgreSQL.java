/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import java.util.HashMap;

import org.h2.engine.Database;
import org.h2.expression.function.Function;
import org.h2.expression.function.FunctionInfo;

/**
 * Functions for {@link org.h2.engine.Mode.ModeEnum#PostgreSQL} compatibility
 * mode.
 */
public final class FunctionsPostgreSQL extends FunctionsBase {

    private static final HashMap<String, FunctionInfo> FUNCTIONS = new HashMap<>();

    static {
        copyFunction(FUNCTIONS, "CURRENT_CATALOG", "CURRENT_DATABASE");
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
        return info != null ? new Function(database, info) : null;
    }

    private FunctionsPostgreSQL(Database database, FunctionInfo info) {
        super(database, info);
    }

}
