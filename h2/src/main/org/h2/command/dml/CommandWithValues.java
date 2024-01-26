/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.util.Utils;

/**
 * Command that supports VALUES clause.
 */
public abstract class CommandWithValues extends DataChangeStatement {

    /**
     * Expression data for the VALUES clause.
     */
    protected final ArrayList<Expression[]> valuesExpressionList = Utils.newSmallArrayList();

    /**
     * Creates new instance of command with VALUES clause.
     *
     * @param session
     *            the session
     */
    protected CommandWithValues(SessionLocal session) {
        super(session);
    }

    /**
     * Add a row to this command.
     *
     * @param expr
     *            the list of values
     */
    public void addRow(Expression[] expr) {
        valuesExpressionList.add(expr);
    }

}
