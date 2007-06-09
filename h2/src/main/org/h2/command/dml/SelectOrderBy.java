/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.expression.Expression;

/**
 * @author Thomas
 */
public class SelectOrderBy {
    public Expression expression;
    public Expression columnIndexExpr;
    public boolean descending;
    public boolean nullsFirst;
    public boolean nullsLast;
}
