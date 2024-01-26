/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.HashMap;
import org.h2.util.StringUtils;

/**
 * A hash map with a case-insensitive string key.
 *
 * @param <V> the value type
 */
public class CaseInsensitiveMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = 1L;

    /**
     * Creates new instance of case-insensitive map.
     */
    public CaseInsensitiveMap() {
    }

    /**
     * Creates new instance of case-insensitive map with specified initial
     * capacity.
     *
     * @param initialCapacity the initial capacity
     */
    public CaseInsensitiveMap(int initialCapacity) {
        super(initialCapacity);
    }

    @Override
    public V get(Object key) {
        return super.get(StringUtils.toUpperEnglish((String) key));
    }

    @Override
    public V put(String key, V value) {
        return super.put(StringUtils.toUpperEnglish(key), value);
    }

    @Override
    public V putIfAbsent(String key, V value) {
        return super.putIfAbsent(StringUtils.toUpperEnglish(key), value);
    }

    @Override
    public boolean containsKey(Object key) {
        return super.containsKey(StringUtils.toUpperEnglish((String) key));
    }

    @Override
    public V remove(Object key) {
        return super.remove(StringUtils.toUpperEnglish((String) key));
    }

}
