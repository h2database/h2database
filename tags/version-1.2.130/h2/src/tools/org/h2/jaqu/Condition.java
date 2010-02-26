/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * A condition contains one or two operands and a compare operation.
 *
 * @param <A> the operand type
 */
//## Java 1.5 begin ##
class Condition<A> implements Token {
    CompareType compareType;
    A x, y;

    Condition(A x, A y, CompareType compareType) {
        this.compareType = compareType;
        this.x = x;
        this.y = y;
    }

    public <T> void appendSQL(SQLStatement stat, Query<T> query) {
        query.appendSQL(stat, x);
        stat.appendSQL(" ");
        stat.appendSQL(compareType.getString());
        if (compareType.hasRightExpression()) {
            stat.appendSQL(" ");
            query.appendSQL(stat, y);
        }
    }
}
//## Java 1.5 end ##
