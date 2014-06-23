/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.AbstractSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

/**
 * A custom map returning the keys and values values 1 .. 10.
 */
public class SequenceMap extends MVMap<Long, Long> {

    /**
     * The minimum value.
     */
    int min = 1;

    /**
     * The maximum value.
     */
    int max = 10;

    public SequenceMap() {
        super(null, null);
    }

    @Override
    public void init(MVStore store, HashMap<String, Object> config) {
        super.init(store, config);
    }

    @Override
    public Set<Long> keySet() {
        return new AbstractSet<Long>() {

            @Override
            public Iterator<Long> iterator() {
                return new Iterator<Long>() {

                    long x = min;

                    @Override
                    public boolean hasNext() {
                        return x <= max;
                    }

                    @Override
                    public Long next() {
                        return Long.valueOf(x++);
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

    /**
     * A builder for this class.
     */
    public static class Builder implements MapBuilder<SequenceMap, Long, Long> {

        /**
         * Create a new builder.
         */
        public Builder() {
            // ignore
        }

        @Override
        public SequenceMap create() {
            return new SequenceMap();
        }

    }

}
