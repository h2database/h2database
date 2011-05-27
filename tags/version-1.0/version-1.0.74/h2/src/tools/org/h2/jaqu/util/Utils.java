/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu.util;

//## Java 1.6 begin ##
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.h2.util.StringUtils;
//## Java 1.6 end ##

/**
 * Generic utility methods.
 */
//## Java 1.6 begin ##
public class Utils {
    
    private static volatile long counter;
    
    private static final boolean MAKE_ACCESSIBLE = true;
    
    public static <T> ArrayList<T> newArrayList() {
        return new ArrayList<T>();
    }

    public static <A, B> HashMap<A, B> newHashMap() {
        return new HashMap<A, B>();
    }

    public static <A, B> Map<A, B> newSynchronizedHashMap() {
        HashMap<A, B> map = newHashMap();
        return Collections.synchronizedMap(map);
    }

    public static <A, B> WeakIdentityHashMap<A, B> newWeakIdentityHashMap() {
        return new WeakIdentityHashMap<A, B>();
    }
    
    public static <A, B> IdentityHashMap<A, B> newIdentityHashMap() {
        return new IdentityHashMap<A, B>();
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T newObject(Class<T> clazz) {
        if (clazz == Integer.class) {
            return (T) new Integer((int) counter++);
        } else if (clazz == String.class) {
            return (T) ("" + counter++);
        } else if (clazz == Long.class) {
            return (T) new Long(counter++);
        } else if (clazz == Short.class) {
            return (T) new Short((short) counter++);
        } else if (clazz == Byte.class) {
            return (T) new Byte((byte) counter++);
        } else if (clazz == Float.class) {
            return (T) new Float(counter++);
        } else if (clazz == Double.class) {
            return (T) new Double(counter++);
        } else if (clazz == Boolean.class) {
            return (T) Boolean.FALSE;
        } else if (clazz == BigDecimal.class) {
            return (T) new BigDecimal(counter++);
        } else if (clazz == BigInteger.class) {
            return (T) new BigInteger("" + counter++);
        } else if (clazz == List.class) {
            return (T) new ArrayList();
        }
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            if (MAKE_ACCESSIBLE) {
                Constructor[] constructors = clazz.getDeclaredConstructors();
                // try 0 length constructors
                for (Constructor c : constructors) {
                    if (c.getParameterTypes().length == 0) {
                        c.setAccessible(true);
                        try {
                            return clazz.newInstance();
                        } catch (Exception e2) {
                            // ignore
                        }
                    }
                }
                // try 1 length constructors
                for (Constructor c : constructors) {
                    if (c.getParameterTypes().length == 1) {
                        c.setAccessible(true);
                        try {
                            return (T) c.newInstance(new Object[1]);
                        } catch (Exception e2) {
                            // ignore
                        }
                    }
                }
            }
            throw new RuntimeException("Exception trying to create " + 
                    clazz.getName() + ": " + e, e);
        }
    }

    public static String quoteSQL(Object x) {
        if (x == null) {
            return "NULL";
        }
        if (x instanceof String) {
            return StringUtils.quoteStringSQL((String) x);
        }
        return x.toString();
    }

    public static <T> boolean isSimpleType(Class<T> clazz) {
        if (Number.class.isAssignableFrom(clazz)) {
            return true;
        } else if (clazz == String.class) {
            return true;
        }
        return false;
    }

}
//## Java 1.6 end ##
