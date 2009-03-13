/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.value.Value;
import org.h2.value.ValueResultSet;

/**
 * This interface is used by the built-in functions,
 * as well as the user-defined functions.
 */
public interface FunctionCall {

    /**
     * Get the name of the function.
     *
     * @return the name
     */
    String getName();

    /**
     * Get the number of parameters.
     *
     * @return the number of parameters
     */
    int getParameterCount() throws SQLException;

    /**
     * Get an empty result set with the column names set.
     *
     * @param session the session
     * @param nullArgs the argument list (some arguments may be null)
     * @return the empty result set
     */
    ValueResultSet getValueForColumnList(Session session, Expression[] nullArgs) throws SQLException;

    /**
     * Get the data type.
     *
     * @return the data type
     */
    int getType();

    /**
     * Optimize the function if possible.
     *
     * @param session the session
     * @return the optimized expression
     */
    Expression optimize(Session session) throws SQLException;

    /**
     * Calculate the result.
     *
     * @param session the session
     * @return the result
     */
    Value getValue(Session session) throws SQLException;

    /**
     * Get the function arguments.
     *
     * @return argument list
     */
    Expression[] getArgs();

    /**
     * Get the SQL snippet of the function (including arguments).
     *
     * @return the SQL snippet.
     */
    String getSQL();

    /**
     * Whether the function always returns the same result for the same parameters.
     *
     * @return true if it does
     */
    boolean isDeterministic();
}
