/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.schema.Domain;

/**
 * Column or domain.
 */
public interface ColumnTemplate {

    Domain getDomain();

    void setDomain(Domain domain);

    /**
     * Set the default expression.
     *
     * @param session
     *            the session
     * @param defaultExpression
     *            the default expression
     */
    void setDefaultExpression(SessionLocal session, Expression defaultExpression);

    Expression getDefaultExpression();

    Expression getEffectiveDefaultExpression();

    String getDefaultSQL();

    /**
     * Set the on update expression.
     *
     * @param session
     *            the session
     * @param onUpdateExpression
     *            the on update expression
     */
    void setOnUpdateExpression(SessionLocal session, Expression onUpdateExpression);

    Expression getOnUpdateExpression();

    Expression getEffectiveOnUpdateExpression();

    String getOnUpdateSQL();

    /**
     * Prepare all expressions of this column or domain.
     *
     * @param session
     *            the session
     */
    void prepareExpressions(SessionLocal session);

}
