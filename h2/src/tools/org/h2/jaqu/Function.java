/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
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

    public void appendSQL(SqlStatement stat, Query query) {
        stat.appendSQL(name);
        stat.appendSQL("(");
        for (int i = 0; i < x.length; i++) {
            if (i > 0) {
                stat.appendSQL(",");
            }
            query.appendSQL(stat, x[i]);
        }
        stat.appendSQL(")");
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
                public void appendSQL(SqlStatement stat, Query query) {
                    query.appendSQL(stat, x[0]);
                    stat.appendSQL(" IS NULL");
                }
            });
    }

    public static Boolean isNotNull(Object x) {
        return Db.registerToken(
            Utils.newObject(Boolean.class), new Function("", x) {
                public void appendSQL(SqlStatement stat, Query query) {
                    query.appendSQL(stat, x[0]);
                    stat.appendSQL(" IS NOT NULL");
                }
            });
    }

    public static Boolean not(Boolean x) {
        return Db.registerToken(
            Utils.newObject(Boolean.class), new Function("", x) {
                public void appendSQL(SqlStatement stat, Query query) {
                    stat.appendSQL("NOT ");
                    query.appendSQL(stat, x[0]);
                }
            });
    }

    public static Boolean or(Boolean... x) {
        return Db.registerToken(
                Utils.newObject(Boolean.class),
                new Function("", (Object[]) x) {
            public void appendSQL(SqlStatement stat, Query query) {
                for (int i = 0; i < x.length; i++) {
                    if (i > 0) {
                        stat.appendSQL(" OR ");
                    }
                    query.appendSQL(stat, x[i]);
                }
            }
        });
    }

    public static Boolean and(Boolean... x) {
        return Db.registerToken(
                Utils.newObject(Boolean.class),
                new Function("", (Object[]) x) {
            public void appendSQL(SqlStatement stat, Query query) {
                for (int i = 0; i < x.length; i++) {
                    if (i > 0) {
                        stat.appendSQL(" AND ");
                    }
                    query.appendSQL(stat, x[i]);
                }
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
            public void appendSQL(SqlStatement stat, Query query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" LIKE ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

//## Java 1.5 end ##
}
