/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;

/**
 * A stored map. Read operations can happen concurrently with all other
 * operations, without risk of corruption.
 * <p>
 * Write operations first read the relevant area from disk to memory
 * concurrently, and only then modify the data. The in-memory part of write
 * operations is synchronized. For scalable concurrent in-memory write
 * operations, the map should be split into multiple smaller sub-maps that are
 * then synchronized independently.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class MVMapConcurrent<K, V> extends MVMap<K, V> {

    public MVMapConcurrent(DataType keyType, DataType valueType) {
        super(keyType, valueType);
    }

    public static <K, V> MVMap<K, V> create() {
        return new MVMapConcurrent<K, V>(new ObjectDataType(), new ObjectDataType());
    }

    protected Page copyOnWrite(Page p, long writeVersion) {
        return p.copy(writeVersion);
    }

    @SuppressWarnings("unchecked")
    public V put(K key, V value) {
        checkWrite();
        V result = get(key);
        if (value.equals(result)) {
            return result;
        }
        long writeVersion = store.getCurrentVersion();
        synchronized (this) {
            Page p = copyOnWrite(root, writeVersion);
            p = splitRootIfNeeded(p, writeVersion);
            result = (V) put(p, writeVersion, key, value);
            newRoot(p);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        checkWrite();
        V result = get(key);
        if (result == null) {
            return null;
        }
        long writeVersion = store.getCurrentVersion();
        synchronized (this) {
            Page p = copyOnWrite(root, writeVersion);
            result = (V) remove(p, writeVersion, key);
            newRoot(p);
        }
        return result;
    }

}
