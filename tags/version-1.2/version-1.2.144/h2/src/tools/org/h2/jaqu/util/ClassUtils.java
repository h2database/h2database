/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu.util;


/**
 * This utility class contains functions related to class loading.
 * There is a mechanism to restrict class loading.
 */
public class ClassUtils {

    private ClassUtils() {
        // utility class
    }

//## Java 1.5 begin ##

    @SuppressWarnings("unchecked")
    public static <X> Class<X> getClass(X x) {
        return (Class<X>) x.getClass();
    }

    public static Class< ? > loadClass(String className) {
        try {
            return Class.forName(className);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
//## Java 1.5 end ##
}
