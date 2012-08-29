/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.ArrayList;
import org.h2.dev.store.btree.BtreeMap;
import org.h2.dev.store.btree.BtreeMapStore;
import org.h2.dev.store.btree.Cursor;
import org.h2.dev.store.btree.CursorPos;
import org.h2.dev.store.btree.DataType;
import org.h2.dev.store.btree.Page;
import org.h2.util.New;

/**
 * An r-tree implementation. It uses the quadratic split algorithm.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class RtreeMap<K, V> extends BtreeMap<K, V> {

    private final SpatialType keyType;
    private boolean quadraticSplit;

    RtreeMap(BtreeMapStore store, int id, String name, DataType keyType,
            DataType valueType, long createVersion) {
        super(store, id, name, keyType, valueType, createVersion);
        this.keyType = (SpatialType) keyType;
    }

    @SuppressWarnings("unchecked")
    public V get(Object key) {
        checkOpen();
        if (root == null) {
            return null;
        }
        return (V) get(root, key);
    }

    private boolean contains(Page p, int index, Object key) {
        return keyType.contains(p.getKey(index), key);
    }

    /**
     * Get the object for the given key. An exact match is required.
     *
     * @param p the page
     * @param key the key
     * @return the value, or null if not found
     */
    protected Object get(Page p, Object key) {
        if (!p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    Object o = get(p.getChildPage(i), key);
                    if (o != null) {
                        return o;
                    }
                }
            }
        } else {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    return p.getValue(i);
                }
            }
        }
        return null;
    }

    protected Page getPage(K key) {
        if (root == null) {
            return null;
        }
        return getPage(root, key);
    }

    protected Page getPage(Page p, Object key) {
        if (!p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    Page x = getPage(p.getChildPage(i), key);
                    if (x != null) {
                        return x;
                    }
                }
            }
        } else {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    return p;
                }
            }
        }
        return null;
    }

    protected Page remove(Page p, long writeVersion, Object key) {
        if (!p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    Page c = p.getChildPage(i);
                    long oldSize = c.getTotalSize();
                    Page c2 = remove(c, writeVersion, key);
                    if (c2 == null) {
                        // this child was deleted
                        if (p.getKeyCount() == 1) {
                            removePage(p);
                            return null;
                        }
                        p = p.copyOnWrite(writeVersion);
                        p.remove(i);
                    } else if (oldSize != c2.getTotalSize()) {
                        p = p.copyOnWrite(writeVersion);
                        Object oldBounds = p.getKey(i);
                        if (!keyType.isInside(key, oldBounds)) {
                            p.setKey(i, getBounds(c));
                        }
                        p.setChild(i, c2);
                        break;
                    }
                }
            }
        } else {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    if (p.getKeyCount() == 1) {
                        removePage(p);
                        return null;
                    }
                    p = p.copyOnWrite(writeVersion);
                    p.remove(i);
                    break;
                }
            }
        }
        return p;
    }

    private Object getBounds(Page x) {
        Object bounds = keyType.createBoundingBox(x.getKey(0));
        for (int i = 1; i < x.getKeyCount(); i++) {
            keyType.increaseBounds(bounds, x.getKey(i));
        }
        return bounds;
    }

    public void put(K key, V data) {
        checkWrite();
        Page oldRoot = root;
        if (get(key) != null) {
            root = set(root, store.getCurrentVersion(), key, data);
        } else {
            root = add(root, store.getCurrentVersion(), key, data, store.getMaxPageSize());
        }
        markChanged(oldRoot);
    }

    protected Page set(Page p, long writeVersion, Object key, Object value) {
        if (!p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    Page c = p.getChildPage(i);
                    Page c2 = set(c, writeVersion, key, value);
                    if (c != c2) {
                        p = p.copyOnWrite(writeVersion);
                        p.setChild(i, c2);
                        break;
                    }
                }
            }
        } else {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    p = p.copyOnWrite(writeVersion);
                    p.setValue(i, value);
                    break;
                }
            }
        }
        return p;
    }

    public void add(K key, V data) {
        checkWrite();
        Page oldRoot = root;
        root = add(root, store.getCurrentVersion(), key, data, store.getMaxPageSize());
        markChanged(oldRoot);
    }

    protected Page add(Page p, long writeVersion, Object key, Object value, int maxPageSize) {
        if (p == null) {
            Object[] keys = { key };
            Object[] values = { value };
            p = Page.create(this, writeVersion, 1,
                    keys, values, null, null, 1, 0);
            return p;
        }
        if (p.getKeyCount() > maxPageSize) {
            // only possible if this is the root, else we would have split earlier
            // (this requires maxPageSize is fixed)
            p = p.copyOnWrite(writeVersion);
            long totalSize = p.getTotalSize();
            Page split = split(p, writeVersion);
            Object[] keys = { getBounds(p), getBounds(split) };
            long[] children = { p.getPos(), split.getPos(), 0 };
            long[] childrenSize = { p.getTotalSize(), split.getTotalSize(), 0 };
            p = Page.create(this, writeVersion, 2,
                    keys, null, children, childrenSize,
                    totalSize, 0);
            // now p is a node; continues
        } else if (p.isLeaf()) {
            p = p.copyOnWrite(writeVersion);
            p.insertLeaf(p.getKeyCount(), key, value);
            return p;
        }
        // p is a node
        int index = -1;
        for (int i = 0; i < p.getKeyCount(); i++) {
            if (contains(p, i, key)) {
                index = i;
                break;
            }
        }
        if (index < 0) {
            // a new entry, we don't know where to add yet
            float min = Float.MAX_VALUE;
            for (int i = 0; i < p.getKeyCount(); i++) {
                Object k = p.getKey(i);
                float areaIncrease = keyType.getAreaIncrease(k, key);
                if (areaIncrease < min) {
                    index = i;
                    min = areaIncrease;
                }
            }
        }
        Page c = p.getChildPage(index);
        if (c.getKeyCount() >= maxPageSize) {
            // split on the way down
            c = c.copyOnWrite(writeVersion);
            Page split = split(c, writeVersion);
            p = p.copyOnWrite(writeVersion);
            p.setKey(index, getBounds(c));
            p.setChild(index, c);
            p.insertNode(index, getBounds(split), split.getPos(), split.getTotalSize());
            // now we are not sure where to add
            return add(p, writeVersion, key, value, maxPageSize);
        }
        Page c2 = add(c, writeVersion, key, value, maxPageSize);
        p = p.copyOnWrite(writeVersion);
        Object bounds = p.getKey(index);
        keyType.increaseBounds(bounds, key);
        p.setKey(index, bounds);
        p.setChild(index, c2);
        return p;
    }

    private Page split(Page p, long writeVersion) {
        return quadraticSplit ?
                splitQuadratic(p, writeVersion) :
                splitLinear(p, writeVersion);
    }

    private Page splitLinear(Page p, long writeVersion) {
        ArrayList<Object> keys = New.arrayList();
        for (int i = 0; i < p.getKeyCount(); i++) {
            keys.add(p.getKey(i));
        }
        int[] extremes = keyType.getExtremes(keys);
        if (extremes == null) {
            return splitQuadratic(p, writeVersion);
        }
        Page splitA = newPage(p.isLeaf(), writeVersion);
        Page splitB = newPage(p.isLeaf(), writeVersion);
        move(p, splitA, extremes[0]);
        if (extremes[1] > extremes[0]) {
            extremes[1]--;
        }
        move(p, splitB, extremes[1]);
        Object boundsA = keyType.createBoundingBox(splitA.getKey(0));
        Object boundsB = keyType.createBoundingBox(splitB.getKey(0));
        while (p.getKeyCount() > 0) {
            Object o = p.getKey(0);
            float a = keyType.getAreaIncrease(boundsA, o);
            float b = keyType.getAreaIncrease(boundsB, o);
            if (a < b) {
                keyType.increaseBounds(boundsA, o);
                move(p, splitA, 0);
            } else {
                keyType.increaseBounds(boundsB, o);
                move(p, splitB, 0);
            }
        }
        while (splitB.getKeyCount() > 0) {
            move(splitB, p, 0);
        }
        return splitA;
    }

    private Page splitQuadratic(Page p, long writeVersion) {
        Page splitA = newPage(p.isLeaf(), writeVersion);
        Page splitB = newPage(p.isLeaf(), writeVersion);
        float largest = Float.MIN_VALUE;
        int ia = 0, ib = 0;
        for (int a = 0; a < p.getKeyCount(); a++) {
            Object objA = p.getKey(a);
            for (int b = 0; b < p.getKeyCount(); b++) {
                if (a == b) {
                    continue;
                }
                Object objB = p.getKey(b);
                float area = keyType.getCombinedArea(objA, objB);
                if (area > largest) {
                    largest = area;
                    ia = a;
                    ib = b;
                }
            }
        }
        move(p, splitA, ia);
        if (ia < ib) {
            ib--;
        }
        move(p, splitB, ib);
        Object boundsA = keyType.createBoundingBox(splitA.getKey(0));
        Object boundsB = keyType.createBoundingBox(splitB.getKey(0));
        while (p.getKeyCount() > 0) {
            float diff = 0, bestA = 0, bestB = 0;
            int best = 0;
            for (int i = 0; i < p.getKeyCount(); i++) {
                Object o = p.getKey(i);
                float incA = keyType.getAreaIncrease(boundsA, o);
                float incB = keyType.getAreaIncrease(boundsB, o);
                float d = Math.abs(incA - incB);
                if (d > diff) {
                    diff = d;
                    bestA = incA;
                    bestB = incB;
                    best = i;
                }
            }
            if (bestA < bestB) {
                keyType.increaseBounds(boundsA, p.getKey(best));
                move(p, splitA, best);
            } else {
                keyType.increaseBounds(boundsB, p.getKey(best));
                move(p, splitB, best);
            }
        }
        while (splitB.getKeyCount() > 0) {
            move(splitB, p, 0);
        }
        return splitA;
    }

    private Page newPage(boolean leaf, long writeVersion) {
        Object[] values = leaf ? new Object[4] : null;
        long[] c = leaf ? null : new long[1];
        return Page.create(this, writeVersion, 0,
                new Object[4], values, c, c, 0, 0);
    }

    private static void move(Page source, Page target, int sourceIndex) {
        Object k = source.getKey(sourceIndex);
        if (source.isLeaf()) {
            Object v = source.getValue(sourceIndex);
            target.insertLeaf(0, k, v);
        } else {
            long c = source.getChildPage(sourceIndex).getPos();
            long count = source.getCounts(sourceIndex);
            target.insertNode(0, k, c, count);
        }
        source.remove(sourceIndex);
    }

    @SuppressWarnings("unchecked")
    public void addNodeKeys(ArrayList<K> list, Page p) {
        if (p != null && !p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                list.add((K) p.getKey(i));
                addNodeKeys(list, p.getChildPage(i));
            }
        }
    }

    /**
     * Go to the first element for the given key.
     *
     * @param p the current page
     * @param parents the stack of parent page positions
     * @param key the key
     */
    protected CursorPos min(Page p, Cursor<K, V> cursor, Object key) {
        if (p == null) {
            return null;
        }
        while (true) {
            CursorPos c = new CursorPos();
            c.page = p;
            if (p.isLeaf()) {
                return c;
            }
            cursor.push(c);
            p = p.getChildPage(0);
        }
    }

    /**
     * Get the next key.
     *
     * @param parents the stack of parent page positions
     * @return the next key
     */
    protected Object nextKey(CursorPos p, Cursor<K, V> cursor) {
        while (true) {
            int index = p.index++;
            Page x = p.page;
            if (index < x.getKeyCount()) {
                return x.getKey(index);
            }
            while (true) {
                p = cursor.pop();
                if (p == null) {
                    return null;
                }
                index = ++p.index;
                x = p.page;
                if (index < x.getKeyCount()) {
                    cursor.push(p);
                    x = x.getChildPage(index);
                    p = min(x, cursor, null);
                    cursor.setCurrentPos(p);
                    break;
                }
            }
        }
    }

    public boolean isQuadraticSplit() {
        return quadraticSplit;
    }

    public void setQuadraticSplit(boolean quadraticSplit) {
        this.quadraticSplit = quadraticSplit;
    }

}
