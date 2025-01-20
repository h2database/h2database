/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function.json;

import org.h2.expression.Expression;

/**
 * Interface for JSON functions with ON EMPTY and ON ERROR clauses.
 */
public interface JsonEmptyErrorBehavior {

    /**
     * Sets ON EMPTY clause.
     *
     * @param onEmpty
     *            ON EMPTY clause
     */
    void setOnEmpty(Expression onEmpty);

    /**
     * Sets ON ERROR clause.
     *
     * @param onError
     *            ON ERROR clause
     */
    void setOnError(Expression onError);

}
