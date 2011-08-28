/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu;

/**
 * This class represents "SET column = (column + x)" in an UPDATE statement.
 *
 * @param <T> the query type
 * @param <A> the new value data type
 */
public class UpdateColumnIncrement<T, A> implements UpdateColumn {

    private Query<T> query;
    private A x;
    private A y;

    UpdateColumnIncrement(Query<T> query, A x) {
        this.query = query;
        this.x = x;
    }

    public Query<T> by(A y) {
        query.addUpdateColumnDeclaration(this);
        this.y = y;
        return query;
    }

    public void appendSQL(SQLStatement stat) {
        query.appendSQL(stat, x);
        stat.appendSQL("=(");
        query.appendSQL(stat, x);
        stat.appendSQL("+");
        query.appendSQL(stat, y);
        stat.appendSQL(")");
    }

}
