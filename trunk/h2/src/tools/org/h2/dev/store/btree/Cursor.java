/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * A cursor to iterate over elements in ascending order.
 *
 * @param <K> the key type
 */
class Cursor<K> implements Iterator<K> {

    private ArrayList<CursorPos> parents = new ArrayList<CursorPos>();
    private K current;

    Cursor(Page root, K from) {
        Page.min(root, parents, from);
        fetchNext();
    }

    public K next() {
        K c = current;
        if (c != null) {
            fetchNext();
        }
        return c == null ? null : c;
    }

    @SuppressWarnings("unchecked")
    private void fetchNext() {
        current = (K) Page.nextKey(parents);
    }

    public boolean hasNext() {
        return current != null;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

}

