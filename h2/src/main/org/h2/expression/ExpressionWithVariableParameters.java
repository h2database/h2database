/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.message.DbException;

/**
 * An expression with variable number of parameters.
 */
public interface ExpressionWithVariableParameters {

    /**
     * Adds the parameter expression.
     *
     * @param param
     *            the expression
     */
    void addParameter(Expression param);

    /**
     * This method must be called after all the parameters have been set. It
     * checks if the parameter count is correct when required by the
     * implementation.
     *
     * @throws DbException
     *             if the parameter count is incorrect.
     */
    void doneWithParameters() throws DbException;

}
