/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;
import org.h2.dev.store.btree.MVMap;
import org.h2.dev.store.btree.MVStore;
import org.h2.dev.store.btree.DataType;

/**
 * A custom map returning the values 1 .. 10.
 *
 * @param <K> the key type
 * @param <V> the key type
 */
public class SequenceMap<K, V> extends MVMap<K, V> {

    /**
     * The minimum value.
     */
    int min = 1;

    /**
     * The maximum value.
     */
    int max = 10;

    SequenceMap(MVStore store, int id, String name, DataType keyType,
            DataType valueType, long createVersion) {
        super(store, id, name, keyType, valueType, createVersion);
        setReadOnly(true);
    }

    public Set<K> keySet() {
        return new AbstractSet<K>() {

            @Override
            public Iterator<K> iterator() {
                return new Iterator<K>() {

                    int x = min;

                    @Override
                    public boolean hasNext() {
                        return x <= max;
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public K next() {
                        return (K) Integer.valueOf(x++);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }

                };
            }

            @Override
            public int size() {
                return max - min + 1;
            }
        };
    }

}
