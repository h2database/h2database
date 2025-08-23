/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class implements a small LRU object cache.
 *
 * @param <K> the key
 * @param <V> the value
 */
public class SmallLRUCache<K, V> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = 1L;

    /**
     * Maximum possible capacity of HashMap.
     */
    private static final int HASH_MAP_MAX_CAPACITY = 1 << 30;

    /**
     * Number one less than threshold of HashMap with previous capacity of 1^29.
     */
    private static final int MAX_UNDER_SIZE = (HASH_MAP_MAX_CAPACITY >> 3) * 3 - 1;

    private int size;

    private SmallLRUCache(int size) {
        // HashMap can hold up to ceil(capacity * loadFactor) values.
        // Actual capacities of HashMap are powers of 2.
        // Maximum capacity of HashMap is 1^30.
        // Before removeEldestEntry() map can contain size + 1 values.
        super(size <= MAX_UNDER_SIZE ? (size << 2) / 3 + 2 : HASH_MAP_MAX_CAPACITY, (float) 0.75, true);
        this.size = size;
    }

    /**
     * Create a new object with all elements of the given collection.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param size the number of elements
     * @return the object
     * @implNote with current implementation, optimal sizes are 11, 23, 35, 47,
     *           95, 191, etc. ({@code 2^N * 0.75 - 1})
     */
    public static <K, V> SmallLRUCache<K, V> newInstance(int size) {
        return new SmallLRUCache<>(size);
    }

    public void setMaxSize(int size) {
        this.size = size;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > size;
    }

}
