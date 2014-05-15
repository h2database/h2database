/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Utility to detect AB-BA deadlocks.
 */
public class AbbaDetector {
    
    private static final boolean TRACE = false;
    
    private static final ThreadLocal<Deque<Object>> STACK =
            new ThreadLocal<Deque<Object>>() {
                @Override protected Deque<Object> initialValue() {
                    return new ArrayDeque<Object>();
            }
        };
        
    private static final Map<Object, Map<Object, Exception>> ORDER = 
            new WeakHashMap<Object, Map<Object, Exception>>();
    
    private static final Set<String> KNOWN = new HashSet<String>();
    
    public static Object begin(Object o) {
        if (o == null) {
            o = new SecurityManager() {
                Class<?> clazz = getClassContext()[2];
            }.clazz;
        }
        Deque<Object> stack = STACK.get();
        if (!stack.isEmpty()) {
            if (stack.contains(o)) {
                // already synchronized on this
                return o;
            }
            while (!stack.isEmpty()) {
                Object last = stack.peek();
                if (Thread.holdsLock(last)) {
                    break;
                }
                stack.pop();
            }
        }
        if (TRACE) { 
            String thread = "[thread " + Thread.currentThread().getId() + "]";
            String ident = new String(new char[stack.size() * 2]).replace((char) 0, ' ');
            System.out.println(thread + " " + ident + 
                    "sync " + getName(o));
        }
        if (stack.size() > 0) {
            markHigher(o, stack);
        }
        stack.push(o);
        return o;
    }
    
    private static Object getTest(Object o) {
        // return o.getClass();
        return o;
    }
    
    private static String getName(Object o) {
        return o.getClass().getSimpleName() + ":" + System.identityHashCode(o);
    }
    
    public static synchronized void markHigher(Object o, Deque<Object> older) {
        Object test = getTest(o);
        Map<Object, Exception> map = ORDER.get(test);
        if (map == null) {
            map = new WeakHashMap<Object, Exception>();
            ORDER.put(test, map);
        }
        Exception oldException = null;
        for (Object old : older) {
            Object oldTest = getTest(old);
            if (oldTest == test) {
                continue;
            }
            Map<Object, Exception> oldMap = ORDER.get(oldTest);
            if (oldMap != null) {
                Exception e = oldMap.get(test);
                if (e != null) {
                    String deadlockType = test.getClass() + " " + oldTest.getClass();
                    if (!KNOWN.contains(deadlockType)) {
                        String message = getName(test) +
                                " synchronized after \n " + getName(oldTest) +
                                ", but in the past before";
                        RuntimeException ex = new RuntimeException(message);
                        ex.initCause(e);
                        ex.printStackTrace(System.out);
                        // throw ex;
                        KNOWN.add(deadlockType);
                    }
                }
            }
            if (!map.containsKey(oldTest)) {
                if (oldException == null) {
                    oldException = new Exception("Before");
                }
                map.put(oldTest, oldException);
            }
        }
    }
    
    public static void main(String... args) {
        Integer a = 1;
        Float b = 2.0f;
        synchronized (a) {
            synchronized (b) {
                System.out.println("a, then b");
            }
        }
        synchronized (b) {
            synchronized (a) {
                System.out.println("b, then a");
            }
        }
    }
    
}
