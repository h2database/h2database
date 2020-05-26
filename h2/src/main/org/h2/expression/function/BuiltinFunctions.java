/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.util.HashSet;

import org.h2.engine.Database;

/**
 * Maintains the list of built-in functions.
 */
public final class BuiltinFunctions {

    private static final HashSet<String> FUNCTIONS;

    static {
        String[] names = { //
                // MathFunction
                "ABS", "MOD", "FLOOR", "CEIL",
                // MathFunction1
                "SIN", "COS", "TAN", "COT", "SINH", "COSH", "TANH", "ASIN", "ACOS", "ATAN", //
                "LOG10", "LN", "EXP", "SQRT", "DEGREES", "RADIANS",
                // MathFunction2
                "ATAN2", "LOG", "POWER",
                // BitFunction
                "BITAND", "BITOR", "BITXOR", "BITNOT", "BITGET", "LSHIFT", "RSHIFT",
                // DateTimeFunction
                "EXTRACT", "DATE_TRUNC", "DATEADD", "DATEDIFF", //
                "TIMESTAMPADD", "TIMESTAMPDIFF",
                // DateTimeFormatFunction
                "FORMATDATETIME", "PARSEDATETIME",
                // DayMonthNameFunction
                "DAYNAME", "MONTHNAME",
                // CardinalityExpression
                "CARDINALITY", "ARRAY_MAX_CARDINALITY",
                // JsonConstructorFunction
                "JSON_OBJECT", "JSON_ARRAY",
                // CompatibilityIdentityFunction
                "IDENTITY", "SCOPE_IDENTITY",
                // CompatibilitySequenceValueFunction
                "CURRVAL", "NEXTVAL",
                //
        };
        HashSet<String> set = new HashSet<>(64);
        for (String n : names) {
            set.add(n);
        }
        FUNCTIONS = set;
    }

    /**
     * Returns whether specified function is a non-keyword built-in function.
     *
     * @param database
     *            the database
     * @param upperName
     *            the name of the function in upper case
     * @return {@code true} if it is
     */
    public static boolean isBuiltinFunction(Database database, String upperName) {
        return FUNCTIONS.contains(upperName) || Function.getFunction(database, upperName) != null;
    }

    private BuiltinFunctions() {
    }

}
