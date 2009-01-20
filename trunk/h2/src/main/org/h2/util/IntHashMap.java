/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.Collection;
import java.util.HashMap;

/**
 * A hash map with int keys and object values.
 */
public class IntHashMap {
    private final HashMap map = new HashMap();

    /**
     * Get the value for the given key. This method returns null if the
     * entry has not been found.
     *
     * @param key the key
     * @return the value or null
     */
    public Object get(int key) {
        return map.get(ObjectUtils.getInteger(key));
    }

    /**
     * Store the given key-value pair. The value is overwritten or added.
     *
     * @param key the key
     * @param value the value
     */
    public void put(int key, Object value) {
        map.put(ObjectUtils.getInteger(key), value);
    }

    /**
     * Remove the key-value pair with the given key.
     *
     * @param key the key
     */
    public void remove(int key) {
        map.remove(ObjectUtils.getInteger(key));
    }

    /**
     * Remove all entries from the map.
     */
    public void clear() {
        map.clear();
    }

    /**
     * Get all values  from the map.
     *
     * @return the values
     */
    public Collection values() {
        return map.values();
    }
}
