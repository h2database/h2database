/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu.util;

//## Java 1.5 begin ##
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generic utility methods.
 */
public class Utils {
//## Java 1.5 begin ##

    private static final AtomicLong counter = new AtomicLong(0);

    private static final boolean MAKE_ACCESSIBLE = true;
    
    private static final int BUFFER_BLOCK_SIZE = 4 * 1024;

    public static <T> ArrayList<T> newArrayList() {
        return new ArrayList<T>();
    }
    
    public static <T> ArrayList<T> newArrayList(Collection<T> c) {
        return new ArrayList<T>(c);
    }
    
    public static <T> HashSet<T> newHashSet() {
        return new HashSet<T>();
    }
    
    public static <T> HashSet<T> newHashSet(Collection<T> list) {
        return new HashSet<T>(list);
    }
    
    public static <T> Set<T> newConcurrentHashSet() {
        return Collections.newSetFromMap(new ConcurrentHashMap<T,Boolean>());
    }

    public static <A, B> HashMap<A, B> newHashMap() {
        return new HashMap<A, B>();
    }

    public static <A, B> Map<A, B> newSynchronizedHashMap() {
        HashMap<A, B> map = newHashMap();
        return Collections.synchronizedMap(map);
    }

    public static <A, B> IdentityHashMap<A, B> newIdentityHashMap() {
        return new IdentityHashMap<A, B>();
    }

    @SuppressWarnings("unchecked")
    public static <T> T newObject(Class<T> clazz) {
        // must create new instances
        if (clazz == Integer.class) {
            return (T) new Integer((int) counter.incrementAndGet());
        } else if (clazz == String.class) {
            return (T) ("" + counter.incrementAndGet());
        } else if (clazz == Long.class) {
            return (T) new Long(counter.incrementAndGet());
        } else if (clazz == Short.class) {
            return (T) new Short((short) counter.incrementAndGet());
        } else if (clazz == Byte.class) {
            return (T) new Byte((byte) counter.incrementAndGet());
        } else if (clazz == Float.class) {
            return (T) new Float(counter.incrementAndGet());
        } else if (clazz == Double.class) {
            return (T) new Double(counter.incrementAndGet());
        } else if (clazz == Boolean.class) {
            return (T) new Boolean(false);
        } else if (clazz == BigDecimal.class) {
            return (T) new BigDecimal(counter.incrementAndGet());
        } else if (clazz == BigInteger.class) {
            return (T) new BigInteger("" + counter.incrementAndGet());
        } else if (clazz == java.sql.Date.class) {
            return (T) new java.sql.Date(counter.incrementAndGet());
        } else if (clazz == java.sql.Time.class) {
            return (T) new java.sql.Time(counter.incrementAndGet());
        } else if (clazz == java.sql.Timestamp.class) {
            return (T) new java.sql.Timestamp(counter.incrementAndGet());
        } else if (clazz == java.util.Date.class) {
            return (T) new java.util.Date(counter.incrementAndGet());
        } else if (clazz == List.class) {
            return (T) newArrayList();
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

    public static Object convert(Object o, Class<?> targetType) {
        if (o == null) {
            return null;
        }
        Class<?> currentType = o.getClass();
        if (targetType.isAssignableFrom(currentType)) {
            return o;
        }
        if (targetType == String.class) {
            if (Clob.class.isAssignableFrom(currentType)) {
                Clob c = (Clob) o;
                try {
                    Reader r = c.getCharacterStream();
                    return readStringAndClose(r, -1);
                } catch (Exception e) {
                    throw new RuntimeException("Error converting CLOB to String: " + e.toString(), e);
                }
            }
            return o.toString();
        }
        if (Number.class.isAssignableFrom(currentType)) {
            Number n = (Number) o;
            if (targetType == Byte.class) {
                return n.byteValue();
            } else if (targetType == Short.class) {
                return n.shortValue();
            } else if (targetType == Integer.class) {
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
    
    /**
     * Read a number of characters from a reader and close it.
     *
     * @param in the reader
     * @param length the maximum number of characters to read, or -1 to read
     *            until the end of file
     * @return the string read
     */
    public static String readStringAndClose(Reader in, int length) throws IOException {
        try {
            if (length <= 0) {
                length = Integer.MAX_VALUE;
            }
            int block = Math.min(BUFFER_BLOCK_SIZE, length);
            StringWriter out = new StringWriter(length == Integer.MAX_VALUE ? block : length);
            char[] buff = new char[block];
            while (length > 0) {
                int len = Math.min(block, length);
                len = in.read(buff, 0, len);
                if (len < 0) {
                    break;
                }
                out.write(buff, 0, len);
                length -= len;
            }
            return out.toString();
        } finally {
            in.close();
        }
    }
    
//## Java 1.5 end ##
}
