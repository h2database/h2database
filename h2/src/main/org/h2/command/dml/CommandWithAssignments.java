/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.expression.Expression;
import org.h2.table.Column;

/**
 * Command with column-expression assignments.
 */
public interface CommandWithAssignments {

    /**
     * Add an assignment of the form column = expression.
     *
     * @param column the column
     * @param expression the expression
     */
    void setAssignment(Column column, Expression expression);

}
