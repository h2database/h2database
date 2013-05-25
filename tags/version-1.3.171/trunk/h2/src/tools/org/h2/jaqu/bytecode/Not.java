/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu.bytecode;

import org.h2.jaqu.Query;
import org.h2.jaqu.SQLStatement;
import org.h2.jaqu.Token;

/**
 * A NOT condition.
 */
public class Not implements Token {

    private final Token expr;

    private Not(Token expr) {
        this.expr = expr;
    }

    static Token get(Token expr) {
        if (expr instanceof Not) {
            return ((Not) expr).expr;
        } else if (expr instanceof Operation) {
            return ((Operation) expr).reverse();
        }
        return new Not(expr);
    }

    Token not() {
        return expr;
    }

    @Override
    public <T> void appendSQL(SQLStatement stat, Query<T> query) {
        // untested
        stat.appendSQL("NOT(");
        expr.appendSQL(stat, query);
        stat.appendSQL(")");
    }

}
