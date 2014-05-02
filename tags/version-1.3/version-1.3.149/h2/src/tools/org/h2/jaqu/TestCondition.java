/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

import org.h2.jaqu.util.Utils;

/**
 * This class represents an incomplete condition.
 *
 * @param <A> the incomplete condition data type
 */
//## Java 1.5 begin ##
public class TestCondition<A> {

    private A x;

    public TestCondition(A x) {
        this.x = x;
    }

    public Boolean is(A y) {
        Boolean o = Utils.newObject(Boolean.class);
        return Db.registerToken(o, new Function("=", x, y) {
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" = ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

    public Boolean bigger(A y) {
        Boolean o = Utils.newObject(Boolean.class);
        return Db.registerToken(o, new Function(">", x, y) {
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" > ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

    public Boolean biggerEqual(A y) {
        Boolean o = Utils.newObject(Boolean.class);
        return Db.registerToken(o, new Function(">=", x, y) {
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" >= ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

    public Boolean smaller(A y) {
        Boolean o = Utils.newObject(Boolean.class);
        return Db.registerToken(o, new Function("<", x, y) {
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" < ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

    public Boolean smallerEqual(A y) {
        Boolean o = Utils.newObject(Boolean.class);
        return Db.registerToken(o, new Function("<=", x, y) {
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" <= ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

    public Boolean like(A pattern) {
        Boolean o = Utils.newObject(Boolean.class);
        return Db.registerToken(o, new Function("LIKE", x, pattern) {
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" LIKE ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

}
//## Java 1.5 end ##
