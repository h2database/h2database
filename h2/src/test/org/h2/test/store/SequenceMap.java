/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.AbstractSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import org.h2.dev.store.btree.MVMap;
import org.h2.dev.store.btree.MVStore;

/**
 * A custom map returning the keys and values values 1 .. 10.
 */
public class SequenceMap extends MVMap<Integer, String> {

    /**
     * The minimum value.
     */
    int min = 1;

    /**
     * The maximum value.
     */
    int max = 10;

    public SequenceMap() {
        super(IntegerType.INSTANCE, IntegerType.INSTANCE);
    }

    public void open(MVStore store, HashMap<String, String> config) {
        super.open(store, config);
        setReadOnly(true);
    }

    public Set<Integer> keySet() {
        return new AbstractSet<Integer>() {

            @Override
            public Iterator<Integer> iterator() {
                return new Iterator<Integer>() {

                    int x = min;

                    @Override
                    public boolean hasNext() {
                        return x <= max;
                    }

                    @Override
                    public Integer next() {
                        return Integer.valueOf(x++);
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
