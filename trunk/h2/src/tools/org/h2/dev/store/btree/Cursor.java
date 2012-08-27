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

    private final BtreeMap<K, V> map;
    private final ArrayList<CursorPos> parents = new ArrayList<CursorPos>();
    private CursorPos currentPos;
    private K current;

    Cursor(BtreeMap<K, V> map, Page root, K from) {
        this.map = map;
        currentPos = map.min(root, this, from);
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
    private void fetchNext() {
        current = (K) map.nextKey(currentPos, this);
    }

    public boolean hasNext() {
        return current != null;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void setCurrentPos(CursorPos p) {
        currentPos = p;
    }

    public void push(CursorPos p) {
        parents.add(p);
    }

    public CursorPos pop() {
        int size = parents.size();
        return size == 0 ? null : parents.remove(size - 1);
    }

}

