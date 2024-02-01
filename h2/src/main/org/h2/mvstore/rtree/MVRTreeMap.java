/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.rtree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.h2.mvstore.CursorPos;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.Page;
import org.h2.mvstore.RootReference;
import org.h2.mvstore.type.DataType;

/**
 * An r-tree implementation. It supports both the linear and the quadratic split
 * algorithm.
 *
 * @param <V> the value class
 */
public final class MVRTreeMap<V> extends MVMap<Spatial, V> {

    /**
     * The spatial key type.
     */
    private final SpatialDataType keyType;

    private boolean quadraticSplit;

    public MVRTreeMap(Map<String, Object> config, SpatialDataType keyType, DataType<V> valueType) {
        super(config, keyType, valueType);
        this.keyType = keyType;
        quadraticSplit = Boolean.parseBoolean(String.valueOf(config.get("quadraticSplit")));
    }

    private MVRTreeMap(MVRTreeMap<V> source) {
        super(source);
        this.keyType = source.keyType;
        this.quadraticSplit = source.quadraticSplit;
    }

    @Override
    public MVRTreeMap<V> cloneIt() {
        return new MVRTreeMap<>(this);
    }

    /**
     * Iterate over all keys that have an intersection with the given rectangle.
     *
     * @param x the rectangle
     * @return the iterator
     */
    public RTreeCursor<V> findIntersectingKeys(Spatial x) {
        return new IntersectsRTreeCursor<>(getRootPage(), x, keyType);
    }

    /**
     * Iterate over all keys that are fully contained within the given
     * rectangle.
     *
     * @param x the rectangle
     * @return the iterator
     */
    public RTreeCursor<V> findContainedKeys(Spatial x) {
        return new ContainsRTreeCursor<>(getRootPage(), x, keyType);
    }

    private boolean contains(Page<Spatial,V> p, int index, Spatial key) {
        return keyType.contains(p.getKey(index), key);
    }

    /**
     * Get the object for the given key. An exact match is required.
     *
     * @param p the page
     * @param key the key
     * @return the value, or null if not found
     */
    @Override
    public V get(Page<Spatial,V> p, Spatial key) {
        int keyCount = p.getKeyCount();
        if (!p.isLeaf()) {
            for (int i = 0; i < keyCount; i++) {
                if (contains(p, i, key)) {
                    V o = get(p.getChildPage(i), key);
                    if (o != null) {
                        return o;
                    }
                }
            }
        } else {
            for (int i = 0; i < keyCount; i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    return p.getValue(i);
                }
            }
        }
        return null;
    }

    /**
     * Remove a key-value pair, if the key exists.
     *
     * @param key the key (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    @Override
    public V remove(Object key) {
        return operate((Spatial) key, null, DecisionMaker.REMOVE);
    }

    @Override
    public V operate(Spatial key, V value, DecisionMaker<? super V> decisionMaker) {
        int attempt = 0;
        final Collection<Page<Spatial,V>> removedPages = isPersistent() ? new ArrayList<>() : null;
        while(true) {
            RootReference<Spatial,V> rootReference = flushAndGetRoot();
            if (attempt++ == 0 && !rootReference.isLockedByCurrentThread()) {
                beforeWrite();
            }
            Page<Spatial,V> p = rootReference.root;
            if (removedPages != null && p.getTotalCount() > 0) {
                removedPages.add(p);
            }
            p = p.copy();
            V result = operate(p, key, value, decisionMaker, removedPages);
            if (!p.isLeaf() && p.getTotalCount() == 0) {
                if (removedPages != null) {
                    removedPages.add(p);
                }
                p = createEmptyLeaf();
            } else if (p.getKeyCount() > store.getKeysPerPage() || p.getMemory() > store.getMaxPageSize()
                                                                && p.getKeyCount() > 3) {
                // only possible if this is the root, else we would have
                // split earlier (this requires pageSplitSize is fixed)
                long totalCount = p.getTotalCount();
                Page<Spatial,V> split = split(p);
                Spatial k1 = getBounds(p);
                Spatial k2 = getBounds(split);
                Spatial[] keys = p.createKeyStorage(2);
                keys[0] = k1;
                keys[1] = k2;
                Page.PageReference<Spatial,V>[] children = Page.createRefStorage(3);
                children[0] = new Page.PageReference<>(p);
                children[1] = new Page.PageReference<>(split);
                children[2] = Page.PageReference.empty();
                p = Page.createNode(this, keys, children, totalCount, 0);
                registerUnsavedMemory(p.getMemory());
            }

            if (removedPages == null) {
                if (updateRoot(rootReference, p, attempt)) {
                    return result;
                }
            } else {
                RootReference<Spatial,V> lockedRootReference = tryLock(rootReference, attempt);
                if (lockedRootReference != null) {
                    try {
                        long version = lockedRootReference.version;
                        int unsavedMemory = 0;
                        for (Page<Spatial,V> page : removedPages) {
                            if (!page.isRemoved()) {
                                unsavedMemory += page.removePage(version);
                            }
                        }
                        registerUnsavedMemory(unsavedMemory);
                    } finally {
                        unlockRoot(p);
                    }
                    return result;
                }
                removedPages.clear();
            }
            decisionMaker.reset();
        }
    }

    private V operate(Page<Spatial,V> p, Spatial key, V value, DecisionMaker<? super V> decisionMaker,
                        Collection<Page<Spatial,V>> removedPages) {
        V result;
        if (p.isLeaf()) {
            int index = -1;
            int keyCount = p.getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    index = i;
                }
            }
            result = index < 0 ? null : p.getValue(index);
            Decision decision = decisionMaker.decide(result, value);
            switch (decision) {
                case REPEAT:
                case ABORT:
                    break;
                case REMOVE:
                    if(index >= 0) {
                        p.remove(index);
                    }
                    break;
                case PUT:
                    value = decisionMaker.selectValue(result, value);
                    if(index < 0) {
                        p.insertLeaf(p.getKeyCount(), key, value);
                    } else {
                        p.setKey(index, key);
                        p.setValue(index, value);
                    }
                    break;
            }
            return result;
        }

        // p is an internal node
        int index = -1;
        for (int i = 0; i < p.getKeyCount(); i++) {
            if (contains(p, i, key)) {
                Page<Spatial,V> c = p.getChildPage(i);
                if(get(c, key) != null) {
                    index = i;
                    break;
                }
                if(index < 0) {
                    index = i;
                }
            }
        }
        if (index < 0) {
            // a new entry, we don't know where to add yet
            float min = Float.MAX_VALUE;
            for (int i = 0; i < p.getKeyCount(); i++) {
                Spatial k = p.getKey(i);
                float areaIncrease = keyType.getAreaIncrease(k, key);
                if (areaIncrease < min) {
                    index = i;
                    min = areaIncrease;
                }
            }
        }
        Page<Spatial,V> c = p.getChildPage(index);
        if (removedPages != null) {
            removedPages.add(c);
        }
        c = c.copy();
        if (c.getKeyCount() > store.getKeysPerPage() || c.getMemory() > store.getMaxPageSize()
                && c.getKeyCount() > 4) {
            // split on the way down
            Page<Spatial,V> split = split(c);
            p.setKey(index, getBounds(c));
            p.setChild(index, c);
            p.insertNode(index, getBounds(split), split);
            // now we are not sure where to add
            result = operate(p, key, value, decisionMaker, removedPages);
        } else {
            result = operate(c, key, value, decisionMaker, removedPages);
            Spatial bounds = p.getKey(index);
            if (!keyType.contains(bounds, key)) {
                bounds = keyType.createBoundingBox(bounds);
                keyType.increaseBounds(bounds, key);
                p.setKey(index, bounds);
            }
            if (c.getTotalCount() > 0) {
                p.setChild(index, c);
            } else {
                p.remove(index);
            }
        }
        return result;
    }

    private Spatial getBounds(Page<Spatial,V> x) {
        Spatial bounds = keyType.createBoundingBox(x.getKey(0));
        int keyCount = x.getKeyCount();
        for (int i = 1; i < keyCount; i++) {
            keyType.increaseBounds(bounds, x.getKey(i));
        }
        return bounds;
    }

    @Override
    public V put(Spatial key, V value) {
        return operate(key, value, DecisionMaker.PUT);
    }

    /**
     * Add a given key-value pair. The key should not exist (if it exists, the
     * result is undefined).
     *
     * @param key the key
     * @param value the value
     */
    public void add(Spatial key, V value) {
        operate(key, value, DecisionMaker.PUT);
    }

    private Page<Spatial,V> split(Page<Spatial,V> p) {
        return quadraticSplit ?
                splitQuadratic(p) :
                splitLinear(p);
    }

    private Page<Spatial,V> splitLinear(Page<Spatial,V> p) {
        int keyCount = p.getKeyCount();
        ArrayList<Spatial> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add(p.getKey(i));
        }
        int[] extremes = keyType.getExtremes(keys);
        if (extremes == null) {
            return splitQuadratic(p);
        }
        Page<Spatial,V> splitA = newPage(p.isLeaf());
        Page<Spatial,V> splitB = newPage(p.isLeaf());
        move(p, splitA, extremes[0]);
        if (extremes[1] > extremes[0]) {
            extremes[1]--;
        }
        move(p, splitB, extremes[1]);
        Spatial boundsA = keyType.createBoundingBox(splitA.getKey(0));
        Spatial boundsB = keyType.createBoundingBox(splitB.getKey(0));
        while (p.getKeyCount() > 0) {
            Spatial o = p.getKey(0);
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

    private Page<Spatial,V> splitQuadratic(Page<Spatial,V> p) {
        Page<Spatial,V> splitA = newPage(p.isLeaf());
        Page<Spatial,V> splitB = newPage(p.isLeaf());
        float largest = Float.MIN_VALUE;
        int ia = 0, ib = 0;
        int keyCount = p.getKeyCount();
        for (int a = 0; a < keyCount; a++) {
            Spatial objA = p.getKey(a);
            for (int b = 0; b < keyCount; b++) {
                if (a == b) {
                    continue;
                }
                Spatial objB = p.getKey(b);
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
        Spatial boundsA = keyType.createBoundingBox(splitA.getKey(0));
        Spatial boundsB = keyType.createBoundingBox(splitB.getKey(0));
        while (p.getKeyCount() > 0) {
            float diff = 0, bestA = 0, bestB = 0;
            int best = 0;
            keyCount = p.getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                Spatial o = p.getKey(i);
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

    private Page<Spatial,V> newPage(boolean leaf) {
        Page<Spatial,V> page = leaf ? createEmptyLeaf() : createEmptyNode();
        registerUnsavedMemory(page.getMemory());
        return page;
    }

    private static <V> void move(Page<Spatial,V> source, Page<Spatial,V> target, int sourceIndex) {
        Spatial k = source.getKey(sourceIndex);
        if (source.isLeaf()) {
            V v = source.getValue(sourceIndex);
            target.insertLeaf(0, k, v);
        } else {
            Page<Spatial,V> c = source.getChildPage(sourceIndex);
            target.insertNode(0, k, c);
        }
        source.remove(sourceIndex);
    }

    /**
     * Add all node keys (including internal bounds) to the given list.
     * This is mainly used to visualize the internal splits.
     *
     * @param list the list
     * @param p the root page
     */
    public void addNodeKeys(ArrayList<Spatial> list, Page<Spatial,V> p) {
        if (p != null && !p.isLeaf()) {
            int keyCount = p.getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                list.add(p.getKey(i));
                addNodeKeys(list, p.getChildPage(i));
            }
        }
    }

    @SuppressWarnings("unused")
    public boolean isQuadraticSplit() {
        return quadraticSplit;
    }

    public void setQuadraticSplit(boolean quadraticSplit) {
        this.quadraticSplit = quadraticSplit;
    }

    @Override
    protected int getChildPageCount(Page<Spatial,V> p) {
        return p.getRawChildPageCount() - 1;
    }

    /**
     * A cursor to iterate over a subset of the keys.
     */
    public abstract static class RTreeCursor<V> implements Iterator<Spatial> {

        private final Spatial filter;
        private CursorPos<Spatial,V> pos;
        private Spatial current;
        private final Page<Spatial,V> root;
        private boolean initialized;

        protected RTreeCursor(Page<Spatial,V> root, Spatial filter) {
            this.root = root;
            this.filter = filter;
        }

        @Override
        public boolean hasNext() {
            if (!initialized) {
                // init
                pos = new CursorPos<>(root, 0, null);
                fetchNext();
                initialized = true;
            }
            return current != null;
        }

        /**
         * Skip over that many entries. This method is relatively fast (for this
         * map implementation) even if many entries need to be skipped.
         *
         * @param n the number of entries to skip
         */
        public void skip(long n) {
            while (hasNext() && n-- > 0) {
                fetchNext();
            }
        }

        @Override
        public Spatial next() {
            if (!hasNext()) {
                return null;
            }
            Spatial c = current;
            fetchNext();
            return c;
        }

        /**
         * Fetch the next entry if there is one.
         */
        void fetchNext() {
            while (pos != null) {
                Page<Spatial,V> p = pos.page;
                if (p.isLeaf()) {
                    while (pos.index < p.getKeyCount()) {
                        Spatial c = p.getKey(pos.index++);
                        if (filter == null || check(true, c, filter)) {
                            current = c;
                            return;
                        }
                    }
                } else {
                    boolean found = false;
                    while (pos.index < p.getKeyCount()) {
                        int index = pos.index++;
                        Spatial c = p.getKey(index);
                        if (filter == null || check(false, c, filter)) {
                            Page<Spatial,V> child = pos.page.getChildPage(index);
                            pos = new CursorPos<>(child, 0, pos);
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        continue;
                    }
                }
                // parent
                pos = pos.parent;
            }
            current = null;
        }

        /**
         * Check a given key.
         *
         * @param leaf if the key is from a leaf page
         * @param key the stored key
         * @param test the user-supplied test key
         * @return true if there is a match
         */
        protected abstract boolean check(boolean leaf, Spatial key, Spatial test);
    }

    private static final class IntersectsRTreeCursor<V> extends RTreeCursor<V> {
        private final SpatialDataType keyType;

        public IntersectsRTreeCursor(Page<Spatial,V> root, Spatial filter, SpatialDataType keyType) {
            super(root, filter);
            this.keyType = keyType;
        }

        @Override
        protected boolean check(boolean leaf, Spatial key,
                                Spatial test) {
            return keyType.isOverlap(key, test);
        }
    }

    private static final class ContainsRTreeCursor<V> extends RTreeCursor<V> {
        private final SpatialDataType keyType;

        public ContainsRTreeCursor(Page<Spatial,V> root, Spatial filter, SpatialDataType keyType) {
            super(root, filter);
            this.keyType = keyType;
        }

        @Override
        protected boolean check(boolean leaf, Spatial key, Spatial test) {
            return leaf ?
                keyType.isInside(key, test) :
                keyType.isOverlap(key, test);
        }
    }

    @Override
    public String getType() {
        return "rtree";
    }

    /**
     * A builder for this class.
     *
     * @param <V> the value type
     */
    public static class Builder<V> extends MVMap.BasicBuilder<MVRTreeMap<V>, Spatial, V> {

        private int dimensions = 2;

        /**
         * Create a new builder for maps with 2 dimensions.
         */
        public Builder() {
            setKeyType(new SpatialDataType(dimensions));
        }

        /**
         * Set the dimensions.
         *
         * @param dimensions the dimensions to use
         * @return this
         */
        public Builder<V> dimensions(int dimensions) {
            this.dimensions = dimensions;
            setKeyType(new SpatialDataType(dimensions));
            return this;
        }

        /**
         * Set the key data type.
         *
         * @param valueType the key type
         * @return this
         */
        @Override
        public Builder<V> valueType(DataType<? super V> valueType) {
            setValueType(valueType);
            return this;
        }

        @Override
        public MVRTreeMap<V> create(Map<String, Object> config) {
            return new MVRTreeMap<>(config, (SpatialDataType)getKeyType(), getValueType());
        }
    }
}
