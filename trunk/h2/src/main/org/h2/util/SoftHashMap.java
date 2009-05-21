/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Map which stores items using SoftReference. Items can be garbage collected
 * and removed. It is not a general purpose cache, as it doesn't implement some
 * methods, and others not according to the map definition, to improve speed.
 */
public class SoftHashMap extends AbstractMap {

    private Map map;
    private ReferenceQueue queue = new ReferenceQueue();

    public SoftHashMap() {
        map = new HashMap();
    }

    private void processQueue() {
        while (true) {
            Object o = queue.poll();
            if (o == null) {
                return;
            }
            Object key = ((SoftValue) o).key;
            map.remove(key);
        }
    }

    public Object get(Object key) {
        processQueue();
        Object o = map.get(key);
        if (o == null) {
            return null;
        }
        return ((SoftReference) o).get();
    }

    /**
     * Store the object. The return value of this method is null or a SoftReference.
     *
     * @param key the key
     * @param value the value
     * @return null or a SoftReference that points to the old object.
     */
    public Object put(Object key, Object value) {
        processQueue();
        return map.put(key, new SoftValue(value, queue, key));
    }

    /**
     * Remove an object. The return value of this method is null or a SoftReference.
     *
     * @param key the key
     * @return null or a SoftReference that points to the old object.
     */
    public Object remove(Object key) {
        processQueue();
        return map.remove(key);
    }

    public void clear() {
        processQueue();
        map.clear();
    }

    public Set entrySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * A soft reference that has a hard reference to the key.
     */
    private static class SoftValue extends SoftReference {
        private final Object key;

        public SoftValue(Object ref, ReferenceQueue q, Object key) {
            super(ref, q);
            this.key = key;
        }

    }

}
