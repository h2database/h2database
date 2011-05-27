/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.expression.Expression;

/**
 * Describes the ORDER BY clause of a query.
 */
public class SelectOrderBy {
    public Expression expression;
    public Expression columnIndexExpr;
    public boolean descending;
    public boolean nullsFirst;
    public boolean nullsLast;
}
