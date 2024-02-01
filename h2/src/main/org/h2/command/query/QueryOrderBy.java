/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.query;

import org.h2.expression.Expression;
import org.h2.result.SortOrder;

/**
 * Describes one element of the ORDER BY clause of a query.
 */
public class QueryOrderBy {

    /**
     * The order by expression.
     */
    public Expression expression;

    /**
     * The column index expression. This can be a column index number (1 meaning
     * the first column of the select list) or a parameter (the parameter is a
     * number representing the column index number).
     */
    public Expression columnIndexExpr;

    /**
     * Sort type for this column.
     */
    public int sortType;

    /**
     * Appends the order by expression to the specified builder.
     *
     * @param builder the string builder
     * @param sqlFlags formatting flags
     */
    public void getSQL(StringBuilder builder, int sqlFlags) {
        (expression != null ? expression : columnIndexExpr).getUnenclosedSQL(builder, sqlFlags);
        SortOrder.typeToString(builder, sortType);
    }

}
