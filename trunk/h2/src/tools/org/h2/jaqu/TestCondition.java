/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
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
            public String getString(Query query) {
                return "(" + query.getString(x[0]) + " = " + query.getString(x[1]) + ")";
            }
        });
    }

    public Boolean bigger(A y) {
        Boolean o = Utils.newObject(Boolean.class);
        return Db.registerToken(o, new Function(">", x, y) {
            public String getString(Query query) {
                return "(" + query.getString(x[0]) + " > " + query.getString(x[1]) + ")";
            }
        });
    }

    public Boolean biggerEqual(A y) {
        Boolean o = Utils.newObject(Boolean.class);
        return Db.registerToken(o, new Function(">=", x, y) {
            public String getString(Query query) {
                return "(" + query.getString(x[0]) + " >= " + query.getString(x[1]) + ")";
            }
        });
    }

    public Boolean smaller(A y) {
        Boolean o = Utils.newObject(Boolean.class);
        return Db.registerToken(o, new Function("<", x, y) {
            public String getString(Query query) {
                return "(" + query.getString(x[0]) + " < " + query.getString(x[1]) + ")";
            }
        });
    }

    public Boolean smallerEqual(A y) {
        Boolean o = Utils.newObject(Boolean.class);
        return Db.registerToken(o, new Function("<=", x, y) {
            public String getString(Query query) {
                return "(" + query.getString(x[0]) + " <= " + query.getString(x[1]) + ")";
            }
        });
    }

    public Boolean like(A pattern) {
        Boolean o = Utils.newObject(Boolean.class);
        return Db.registerToken(o, new Function("LIKE", x, pattern) {
            public String getString(Query query) {
                return "(" + query.getString(x[0]) + " LIKE " + query.getString(x[1]) + ")";
            }
        });
    }

}
//## Java 1.5 end ##
