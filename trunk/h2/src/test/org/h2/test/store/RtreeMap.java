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
import org.h2.dev.store.btree.CursorPos;
import org.h2.dev.store.btree.DataType;
import org.h2.dev.store.btree.Page;

/**
 * An r-tree implementation. It uses the quadratic split algorithm.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class RtreeMap<K, V> extends BtreeMap<K, V> {

    private final SpatialType keyType;

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
        return (V) getSpatial(root, key);
    }

    private boolean contains(Page p, int index, Object key) {
        return keyType.contains(p.getKey(index), key);
    }

    private float getAreaIncrease(Page p, int index, Object key) {
        return keyType.getAreaIncrease(p.getKey(index), key);
    }

    /**
     * Get the object for the given key. An exact match is required.
     *
     * @param p the page
     * @param key the key
     * @return the value, or null if not found
     */
    protected Object getSpatial(Page p, Object key) {
        if (!p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    Object o = getSpatial(p.getChildPage(i), key);
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
        return getPageSpatial(root, key);
    }

    protected Page getPageSpatial(Page p, Object key) {
        if (!p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    Page x = getPageSpatial(p.getChildPage(i), key);
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

    protected Page set(Page p, long writeVersion, Object key, Object value) {
        if (p == null) {
            throw KEY_NOT_FOUND;
        }
        if (!p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    Page c = p.getChildPage(i);
                    Page c2 = set(c, writeVersion, key, value);
                    if (c != c2) {
                        p = p.copyOnWrite(writeVersion);
                        setChildUpdateBox(p,  i, c2);
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

    protected Page removeExisting(Page p, long writeVersion, Object key) {
        if (p == null) {
            throw KEY_NOT_FOUND;
        }
        if (!p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    Page c = p.getChildPage(i);
                    Page c2 = removeExisting(c, writeVersion, key);
                    if (c != c2) {
                        p = p.copyOnWrite(writeVersion);
                        setChildUpdateBox(p, i, c2);
                        break;
                    }
                }
            }
        } else {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    p = p.copyOnWrite(writeVersion);
                    p.remove(i);
                    break;
                }
            }
        }
        return p;
    }

    /**
     * Set the child and update the bounding box.
     *
     * @param p the parent (this page is changed)
     * @param index the child index
     * @param c the child page
     */
    private void setChildUpdateBox(Page p, int index, Page c) {
        p.setKey(index, getBounds(c));
        p.setChild(index, c);
    }

    private SpatialKey getBounds(Page x) {
        SpatialKey bounds = keyType.copy((SpatialKey) x.getKey(0));
        for (int i = 1; i < x.getKeyCount(); i++) {
            keyType.increase(bounds, (SpatialKey) x.getKey(i));
        }
        return bounds;
    }

    protected Page add(Page p, long writeVersion, Object key, Object value) {
        if (p == null) {
            Object[] keys = { key };
            Object[] values = { value };
            p = Page.create(this, writeVersion, keys, values, null, null, 1);
            return p;
        }
        if (p.getKeyCount() >= store.getMaxPageSize()) {
            // only possible if this is the root,
            // otherwise we would have split earlier
            p = p.copyOnWrite(writeVersion);
            long totalSize = p.getTotalSize();
            Page split = split(p, writeVersion);
            Object[] keys = { getBounds(p), getBounds(split) };
            long[] children = { p.getPos(), split.getPos(), 0 };
            long[] childrenSize = { p.getTotalSize(), split.getTotalSize(), 0 };
            p = Page.create(this, writeVersion, keys, null, children, childrenSize,
                    totalSize);
            // now p is a node; insert continues
        } else if (p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    throw KEY_ALREADY_EXISTS;
                }
            }
            p = p.copyOnWrite(writeVersion);
            p.insert(p.getKeyCount(), key, value, 0, 0);
            return p;
        }
        // p is a node
        float min = Float.MAX_VALUE;
        int index = 0;
        for (int i = 0; i < p.getKeyCount(); i++) {
            float areaIncrease = getAreaIncrease(p, i, key);
            if (areaIncrease < min) {
                index = i;
                min = areaIncrease;
            }
        }
        Page c = p.getChildPage(index);
        if (c.getKeyCount() >= store.getMaxPageSize()) {
            // split on the way down
            c = c.copyOnWrite(writeVersion);
            Page split = split(c, writeVersion);
            p = p.copyOnWrite(writeVersion);
            setChildUpdateBox(p, index, c);
            p.insert(index, getBounds(split), null, split.getPos(), split.getTotalSize());
            // now we are not sure where to add
            return add(p, writeVersion, key, value);
        }
        Page c2 = add(c, writeVersion, key, value);
        p = p.copyOnWrite(writeVersion);
        // the child might be the same, but not the size
        setChildUpdateBox(p, index, c2);
        return p;
    }

    private Page split(Page p, long writeVersion) {
        // quadratic algorithm
        Object[] values = p.isLeaf() ? new Object[0] : null;
        long[] c = p.isLeaf() ? null : new long[1];
        Page split = Page.create(this, writeVersion, new Object[0],
                values, c, c, 0);
        Page newP = Page.create(this, writeVersion, new Object[0],
                values, c, c, 0);
        float largest = Float.MIN_VALUE;
        int iBest = 0, jBest = 0;
        for (int i = 0; i < p.getKeyCount(); i++) {
            Object oi = p.getKey(i);
            for (int j = 0; j < p.getKeyCount(); j++) {
                if (i == j) {
                    continue;
                }
                Object oj = p.getKey(j);
                float area = keyType.getCombinedArea(oi, oj);
                if (area > largest) {
                    largest = area;
                    iBest = i;
                    jBest = j;
                }
            }
        }
        move(p, newP, iBest);
        if (iBest < jBest) {
            jBest--;
        }
        move(p, split, jBest);
        while (p.getKeyCount() > 0) {
            float diff = 0, bestA = 0, bestB = 0;
            int best = 0;
            SpatialKey ba = getBounds(newP);
            SpatialKey bb = getBounds(split);
            for (int i = 0; i < p.getKeyCount(); i++) {
                Object o = p.getKey(i);
                float a = keyType.getAreaIncrease(ba, o);
                float b = keyType.getAreaIncrease(bb, o);
                float d = Math.abs(a - b);
                if (d > diff) {
                    diff = d;
                    bestA = a;
                    bestB = b;
                    best = i;
                }
            }
            if (bestA < bestB) {
                move(p, newP, best);
            } else {
                move(p, split, best);
            }
        }
        while (newP.getKeyCount() > 0) {
            move(newP, p, 0);
        }
        return split;
    }

    private static void move(Page source, Page target, int sourceIndex) {
        Object k = source.getKey(sourceIndex);
        Object v = source.isLeaf() ? source.getValue(sourceIndex) : null;
        long c = source.isLeaf() ? 0 : source.getChildPage(sourceIndex).getPos();
        long count = source.isLeaf() ? 0 : source.getCounts(sourceIndex);
        target.insert(0, k, v, c, count);
        source.remove(sourceIndex);
    }

    public void addNodeKeys(ArrayList<SpatialKey> list, Page p) {
        if (p != null && !p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                list.add((SpatialKey) p.getKey(i));
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
    protected void min(Page p, ArrayList<CursorPos> parents, Object key) {
        while (p != null) {
            CursorPos c = new CursorPos();
            c.page = p;
            parents.add(c);
            if (p.isLeaf()) {
                return;
            }
            p = p.getChildPage(0);
        }
    }

    /**
     * Get the next key.
     *
     * @param parents the stack of parent page positions
     * @return the next key
     */
    protected Object nextKey(ArrayList<CursorPos> parents) {
        if (parents.size() == 0) {
            return null;
        }
        while (true) {
            // TODO performance: avoid remove/add pairs if possible
            CursorPos p = parents.remove(parents.size() - 1);
            int index = p.index++;
            if (index < p.page.getKeyCount()) {
                parents.add(p);
                return p.page.getKey(index);
            }
            while (true) {
                if (parents.size() == 0) {
                    return null;
                }
                p = parents.remove(parents.size() - 1);
                index = ++p.index;
                if (index < p.page.getKeyCount()) {
                    parents.add(p);
                    Page x = p.page;
                     x = x.getChildPage(index);
                    min(x, parents, null);
                    break;
                }
            }
        }
    }

}
