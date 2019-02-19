/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A utility class that allows to verify access to a resource is synchronized.
 */
public class SynchronizedVerifier {

    private static volatile boolean enabled;
    private static final ConcurrentHashMap<Class<?>, AtomicBoolean> DETECT = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Object, Object> CURRENT = new ConcurrentHashMap<>();

    /**
     * Enable or disable detection for a given class.
     *
     * @param clazz the class
     * @param value the new value (true means detection is enabled)
     */
    public static void setDetect(Class<?> clazz, boolean value) {
        if (value) {
            DETECT.put(clazz, new AtomicBoolean());
        } else {
            AtomicBoolean b = DETECT.remove(clazz);
            if (b == null) {
                throw new AssertionError("Detection was not enabled");
            } else if (!b.get()) {
                throw new AssertionError("No object of this class was tested");
            }
        }
        enabled = DETECT.size() > 0;
    }

    /**
     * Verify the object is not accessed concurrently.
     *
     * @param o the object
     */
    public static void check(Object o) {
        if (enabled) {
            detectConcurrentAccess(o);
        }
    }

    private static void detectConcurrentAccess(Object o) {
        AtomicBoolean value = DETECT.get(o.getClass());
        if (value != null) {
            value.set(true);
            if (CURRENT.remove(o) != null) {
                throw new AssertionError("Concurrent access");
            }
            CURRENT.put(o, o);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // ignore
            }
            Object old = CURRENT.remove(o);
            if (old == null) {
                throw new AssertionError("Concurrent access");
            }
        }
    }

}
