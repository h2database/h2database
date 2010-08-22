/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu.util;

//## Java 1.5 begin ##
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
//## Java 1.5 end ##

/**
 * Generic utility methods.
 */
public class Utils {
//## Java 1.5 begin ##

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
        // must create new instances
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
            return (T) new Boolean(false);
        } else if (clazz == BigDecimal.class) {
            return (T) new BigDecimal(counter++);
        } else if (clazz == BigInteger.class) {
            return (T) new BigInteger("" + counter++);
        } else if (clazz == java.sql.Date.class) {
            return (T) new java.sql.Date(counter++);
        } else if (clazz == java.sql.Time.class) {
            return (T) new java.sql.Time(counter++);
        } else if (clazz == java.sql.Timestamp.class) {
            return (T) new java.sql.Timestamp(counter++);
        } else if (clazz == java.util.Date.class) {
            return (T) new java.util.Date(counter++);
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

    public static <T> boolean isSimpleType(Class<T> clazz) {
        if (Number.class.isAssignableFrom(clazz)) {
            return true;
        } else if (clazz == String.class) {
            return true;
        }
        return false;
    }

    public static Object convert(Object o, Class< ? > targetType) {
        if (o == null) {
            return null;
        }
        Class< ? > currentType = o.getClass();
        if (targetType.isAssignableFrom(currentType)) {
            return o;
        }
        if (targetType == String.class) {
            return o.toString();
        }
        if (Number.class.isAssignableFrom(currentType)) {
            Number n = (Number) o;
            if (targetType == Integer.class) {
                return n.intValue();
            } else if (targetType == Long.class) {
                return n.longValue();
            } else if (targetType == Double.class) {
                return n.doubleValue();
            } else if (targetType == Float.class) {
                return n.floatValue();
            }
        }
        throw new RuntimeException("Can not convert the value " + o +
                " from " + currentType + " to " + targetType);
    }

//## Java 1.5 end ##
}
