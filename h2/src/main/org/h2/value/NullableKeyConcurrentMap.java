/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.concurrent.ConcurrentHashMap;

import org.h2.util.StringUtils;

/**
 * A concurrent hash map with string keys that allows null keys.
 *
 * @param <V> the value type
 */
public class NullableKeyConcurrentMap<V> extends ConcurrentHashMap<String, V> {

    private static final long serialVersionUID = 1L;
    private static final String NULL = new String();

    private final boolean toUpper;

    /**
     * Create new instance of map.
     *
     * @param toUpper
     *                    whether keys should be converted to upper case
     */
    public NullableKeyConcurrentMap(boolean toUpper) {
        this.toUpper = toUpper;
    }

    @Override
    public V get(Object key) {
        return super.get(toUpper(key));
    }

    @Override
    public V put(String key, V value) {
        return super.put(toUpper(key), value);
    }

    @Override
    public boolean containsKey(Object key) {
        return super.containsKey(toUpper(key));
    }

    @Override
    public V remove(Object key) {
        return super.remove(toUpper(key));
    }

    private String toUpper(Object key) {
        if (key == null) {
            return NULL;
        }
        String s = key.toString();
        return toUpper ? StringUtils.toUpperEnglish(s) : s;
    }

}
