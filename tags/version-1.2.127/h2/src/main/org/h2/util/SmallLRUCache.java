/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

//## Java 1.4 begin ##
import java.util.LinkedHashMap;
//## Java 1.4 end ##
/*## Java 1.3 only begin ##
import java.util.HashMap;
## Java 1.3 only end ##*/
import java.util.Map;

/**
 * This class implements a small LRU object cache.
 *
 * @param <K> the key
 * @param <V> the value
 */
public class SmallLRUCache<K, V>
//## Java 1.4 begin ##
extends LinkedHashMap<K, V>
//## Java 1.4 end ##
/*## Java 1.3 only begin ##
extends HashMap
## Java 1.3 only end ##*/
{

    private static final long serialVersionUID = 1L;
    private int size;

    private SmallLRUCache(int size) {
        super(size, (float) 0.75, true);
        this.size = size;
    }

    /**
     * Create a new object with all elements of the given collection.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param size the number of elements
     * @return the object
     */
    public static <K, V> SmallLRUCache<K, V> newInstance(int size) {
        return new SmallLRUCache<K, V>(size);
    }

//## Java 1.4 begin ##
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > size;
    }
//## Java 1.4 end ##
}
