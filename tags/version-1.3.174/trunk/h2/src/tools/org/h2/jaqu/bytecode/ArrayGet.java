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
 * An array access operation.
 */
public class ArrayGet implements Token {

    private final Token variable;
    private final Token index;

    private ArrayGet(Token variable, Token index) {
        this.variable = variable;
        this.index = index;
    }

    static ArrayGet get(Token variable, Token index) {
        return new ArrayGet(variable, index);
    }

    @Override
    public <T> void appendSQL(SQLStatement stat, Query<T> query) {
        // untested
        variable.appendSQL(stat, query);
        stat.appendSQL("[");
        index.appendSQL(stat, query);
        stat.appendSQL("]");
    }

}
