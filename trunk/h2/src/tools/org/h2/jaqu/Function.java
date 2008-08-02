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

    private String name;
    private Object x;

    private Function(String name, Object x) {
        this.name = name;
        this.x = x;
    }
        
    public String getString(Query query) {
        return name + "(" + query.getString(x) + ")";
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

//## Java 1.5 end ##
}
