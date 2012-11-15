/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.util.Iterator;

/**
 * A cursor to iterate over elements in ascending order.
 *
 * @param <K> the key type
 */
public class Cursor<K> implements Iterator<K> {

    private final MVMap<K, ?> map;
    private final K from;
    private final Page root;
    private boolean initialized;
    private CursorPos pos;
    private K current;

    Cursor(MVMap<K, ?> map, Page root, K from) {
        this.map = map;
        this.root = root;
        this.from = from;
    }

    public K next() {
        hasNext();
        K c = current;
        fetchNext();
        return c;
    }

    public boolean hasNext() {
        if (!initialized) {
            min(root, from);
            initialized = true;
            fetchNext();
        }
        return current != null;
    }

    public void skip(long n) {
        if (!hasNext()) {
            return;
        }
        if (n < 10) {
            while (n-- > 0) {
                fetchNext();
            }
            return;
        }
        long index = map.getKeyIndex(current);
        K k = map.getKey(index + n);
        min(root, k);
        fetchNext();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    private void min(Page p, K from) {
        while (true) {
            if (p.isLeaf()) {
                int x = from == null ? 0 : p.binarySearch(from);
                if (x < 0) {
                    x = -x - 1;
                }
                pos = new CursorPos(p, x, pos);
                break;
            }
            int x = from == null ? -1 : p.binarySearch(from);
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            pos = new CursorPos(p, x + 1, pos);
            p = p.getChildPage(x);
        }
    }

    @SuppressWarnings("unchecked")
    private void fetchNext() {
        while (pos != null) {
            if (pos.index < pos.page.getKeyCount()) {
                current = (K) pos.page.getKey(pos.index++);
                return;
            }
            pos = pos.parent;
            if (pos == null) {
                break;
            }
            if (pos.index < map.getChildPageCount(pos.page)) {
                min(pos.page.getChildPage(pos.index++), null);
            }
        }
        current = null;
    }

}
