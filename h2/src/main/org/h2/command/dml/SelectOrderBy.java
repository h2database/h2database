/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.expression.Expression;
import org.h2.result.SortOrder;

/**
 * Describes one element of the ORDER BY clause of a query.
 */
public class SelectOrderBy {

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

    public String getSQL() {
        StringBuilder buff = new StringBuilder();
        if (expression != null) {
            buff.append('=');
            expression.getSQL(buff);
        } else {
            columnIndexExpr.getSQL(buff);
        }
        SortOrder.typeToString(buff, sortType);
        return buff.toString();
    }

}
