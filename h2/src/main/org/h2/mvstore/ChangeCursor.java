/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.Iterator;

/**
 * A cursor to iterate over all keys in new pages.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class ChangeCursor<K, V> implements Iterator<K> {

    private final MVMap<K, V> map;
    private final Page root1, root2;

    /**
     * The state of this cursor.
     * 0: not initialized
     * 1: reading from root1
     * 2: reading from root2
     * 3: closed
     */
    private int state;
    private CursorPos pos1, pos2;
    private K current;

    ChangeCursor(MVMap<K, V> map, Page root1, Page root2) {
        this.map = map;
        this.root1 = root1;
        this.root2 = root2;
    }

    public K next() {
        K c = current;
        fetchNext();
        return c;
    }

    public boolean hasNext() {
        if (state == 0) {
            pos1 = new CursorPos(root1, 0, null);
            pos1 = min(pos1);
            state = 1;
            fetchNext();
        }
        return current != null;
    }

    public void remove() {
        throw DataUtils.newUnsupportedOperationException(
                "Removing is not supported");
    }

    private void fetchNext() {
        while (fetchNextKey()) {
            if (pos1 == null || pos2 == null) {
                break;
            }
            @SuppressWarnings("unchecked")
            V v1 = (V) map.binarySearch(root1, current);
            @SuppressWarnings("unchecked")
            V v2 = (V) map.binarySearch(root2, current);
            if (!v1.equals(v2)) {
                break;
            }
        }
    }

    private boolean fetchNextKey() {
        while (true) {
            if (state == 3) {
                return false;
            }
            if (state == 1) {
                // read from root1
                pos1 = fetchNext(pos1);
                if (pos1 == null) {
                    // reached the end of pos1
                    state = 2;
                    pos2 = null;
                    continue;
                }
                pos2 = find(root2, current);
                if (pos2 == null) {
                    // not found in root2
                    return true;
                }
                if (!pos1.page.equals(pos2.page)) {
                    // the page is different,
                    // so the entry has possibly changed
                    return true;
                }
                while (true) {
                    pos1 = pos1.parent;
                    if (pos1 == null) {
                        // reached end of pos1
                        state = 2;
                        pos2 = null;
                        break;
                    }
                    pos2 = pos2.parent;
                    if (pos2 == null || !pos1.page.equals(pos2.page)) {
                        if (pos1.index + 1 < map.getChildPageCount(pos1.page)) {
                            pos1 = new CursorPos(pos1.page.getChildPage(++pos1.index), 0, pos1);
                            pos1 = min(pos1);
                            break;
                        }
                    }
                }
            }
            if (state == 2) {
                if (pos2 == null) {
                    // init reading from root2
                    pos2 = new CursorPos(root2, 0, null);
                    pos2 = min(pos2);
                }
                // read from root2
                pos2 = fetchNext(pos2);
                if (pos2 == null) {
                    // reached the end of pos2
                    state = 3;
                    current = null;
                    continue;
                }
                pos1 = find(root1, current);
                if (pos1 != null) {
                    // found a corresponding record
                    // so it was not deleted
                    // but now we may need to skip pages
                    if (!pos1.page.equals(pos2.page)) {
                        // the page is different
                        pos1 = null;
                        continue;
                    }
                    while (true) {
                        pos2 = pos2.parent;
                        if (pos2 == null) {
                            // reached end of pos1
                            state = 3;
                            current = null;
                            pos1 = null;
                            break;
                        }
                        pos1 = pos1.parent;
                        if (pos1 == null || !pos2.page.equals(pos1.page)) {
                            if (pos2.index + 1 < map.getChildPageCount(pos2.page)) {
                                pos2 = new CursorPos(pos2.page.getChildPage(++pos2.index), 0, pos2);
                                pos2 = min(pos2);
                                break;
                            }
                        }
                    }
                    pos1 = null;
                    continue;
                }
                // found no corresponding record
                // so it was deleted
                return true;
            }
        }
    }

    private CursorPos find(Page p, K key) {
        // TODO combine with RangeCursor.min
        // possibly move to MVMap
        CursorPos pos = null;
        while (true) {
            if (p.isLeaf()) {
                int x = key == null ? 0 : p.binarySearch(key);
                if (x < 0) {
                    return null;
                }
                return new CursorPos(p, x, pos);
            }
            int x = key == null ? -1 : p.binarySearch(key);
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            pos = new CursorPos(p, x, pos);
            p = p.getChildPage(x);
        }
    }

    @SuppressWarnings("unchecked")
    private CursorPos fetchNext(CursorPos p) {
        while (p != null) {
            if (p.index < p.page.getKeyCount()) {
                current = (K) p.page.getKey(p.index++);
                return p;
            }
            p = p.parent;
            if (p == null) {
                break;
            }
            if (p.index + 1 < map.getChildPageCount(p.page)) {
                p = new CursorPos(p.page.getChildPage(++p.index), 0, p);
                p = min(p);
            }
        }
        current = null;
        return p;
    }

    private static CursorPos min(CursorPos p) {
        while (true) {
            if (p.page.isLeaf()) {
                return p;
            }
            Page c = p.page.getChildPage(0);
            p = new CursorPos(c, 0, p);
        }
    }

}
