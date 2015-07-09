/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Similar to ThreadLocal, except that it allows it's data to be read from other
 * threads - useful for debugging info.
 *
 * @param <T> the type
 */
public class DebuggingThreadLocal<T> {

    private final ConcurrentHashMap<Long, T> map = new ConcurrentHashMap<Long, T>();

    public void set(T value) {
        map.put(Thread.currentThread().getId(), value);
    }

    public void remove() {
        map.remove(Thread.currentThread().getId());
    }

    public T get() {
        return map.get(Thread.currentThread().getId());
    }

    /**
     * @return a HashMap containing a mapping from thread-id to value
     */
    public HashMap<Long, T> getSnapshotOfAllThreads() {
        return new HashMap<Long, T>(map);
    }

    public T deepCopy(T value) {
        return value;
    }

}
