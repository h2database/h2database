/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

//## Java 1.5 begin ##
import org.h2.jaqu.util.Utils;
//## Java 1.5 end ##

/**
 * This class provides static methods that represents common SQL functions.
 */
public class Function implements Token {
//## Java 1.5 begin ##
    
    private static final Long COUNT_STAR = new Long(0);

    protected Object[] x;
    private String name;

    protected Function(String name, Object... x) {
        this.name = name;
        this.x = x;
    }

    public String getString(Query query) {
        StringBuilder buff = new StringBuilder();
        buff.append(name).append('(');
        for (int i = 0; i < x.length; i++) {
            if (i > 0) {
                buff.append(',');
            }
            buff.append(query.getString(x[i]));
        }
        buff.append(')');
        return buff.toString();
    }
    
    public static Long count() {
        return COUNT_STAR;
    }
    
    public static Integer length(Object x) {
        return Db.registerToken(
            Utils.newObject(Integer.class), new Function("LENGTH", x));
    }

    public static <T extends Number> T sum(T x) {
        return (T) Db.registerToken(
            Utils.newObject(x.getClass()), new Function("SUM", x));
    }

    public static Long count(Object x) {
        return Db.registerToken(
            Utils.newObject(Long.class), new Function("COUNT", x));
    }

    public static Boolean isNull(Object x) {
        return Db.registerToken(
            Utils.newObject(Boolean.class), new Function("", x) {
                public String getString(Query query) {
                    return query.getString(x[0]) + " IS NULL";
                }
            });
    }

    public static Boolean isNotNull(Object x) {
        return Db.registerToken(
            Utils.newObject(Boolean.class), new Function("", x) {
                public String getString(Query query) {
                    return query.getString(x[0]) + " IS NOT NULL";
                }
            });
    }
    
    public static Boolean not(Boolean x) {
        return Db.registerToken(
            Utils.newObject(Boolean.class), new Function("", x) {
                public String getString(Query query) {
                    return "NOT " + query.getString(x[0]);
                }
            });
    }

    public static Boolean or(Boolean... x) {
        return Db.registerToken(Utils.newObject(Boolean.class), new Function("", (Object[]) x) {
            public String getString(Query query) {
                StringBuilder buff = new StringBuilder();
                for (int i = 0; i < x.length; i++) {
                    if (i > 0) {
                        buff.append(" OR ");
                    }
                    buff.append(query.getString(x[i]));
                }
                return buff.toString();
            }
        });
    }

    public static Boolean and(Boolean... x) {
        return Db.registerToken(Utils.newObject(Boolean.class), new Function("", (Object[]) x) {
            public String getString(Query query) {
                StringBuilder buff = new StringBuilder();
                for (int i = 0; i < x.length; i++) {
                    if (i > 0) {
                        buff.append(" AND ");
                    }
                    buff.append(query.getString(x[i]));
                }
                return buff.toString();
            }
        });
    }

    public static <X> X min(X x) {
        Class<X> clazz = (Class<X>) x.getClass();
        X o = Utils.newObject(clazz);
        return Db.registerToken(o, new Function("MIN", x));
    }
    
    public static <X> X max(X x) {
        Class<X> clazz = (Class<X>) x.getClass();
        X o = Utils.newObject(clazz);
        return Db.registerToken(o, new Function("MAX", x));
    }
    
    public static Boolean like(String x, String pattern) {
        Boolean o = Utils.newObject(Boolean.class);
        return Db.registerToken(o, new Function("LIKE", x, pattern) {
            public String getString(Query query) {
                return "(" + query.getString(x[0]) + " LIKE " + query.getString(x[1]) + ")";
            }
        });
    }

//## Java 1.5 end ##
}
