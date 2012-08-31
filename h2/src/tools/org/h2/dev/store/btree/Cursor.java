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
 * @param <V> the value type
 */
public class Cursor<K, V> implements Iterator<K> {

    protected final MVMap<K, V> map;
    protected final ArrayList<CursorPos> parents = new ArrayList<CursorPos>();
    protected CursorPos currentPos;
    protected K current;

    Cursor(MVMap<K, V> map) {
        this.map = map;
    }

    void start(Page root, K from) {
        currentPos = min(root, from);
        if (currentPos != null) {
            fetchNext();
        }
    }

    public K next() {
        K c = current;
        if (c != null) {
            fetchNext();
        }
        return c == null ? null : c;
    }

    @SuppressWarnings("unchecked")
    protected void fetchNext() {
        current = (K) map.nextKey(currentPos, this);
    }

    public boolean hasNext() {
        return current != null;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void push(CursorPos p) {
        parents.add(p);
    }

    public CursorPos pop() {
        int size = parents.size();
        return size == 0 ? null : parents.remove(size - 1);
    }

    public CursorPos min(Page p, K from) {
        return map.min(p, this, from);
    }

    public CursorPos visitChild(Page p, int childIndex) {
        p = p.getChildPage(childIndex);
        currentPos = min(p, null);
        return currentPos;
    }

}

