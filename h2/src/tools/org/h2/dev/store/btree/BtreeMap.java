/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

/**
 * A stored map.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class BtreeMap<K, V> {

    protected final BtreeMapStore store;
    protected Page root;

    private final int id;
    private final String name;
    private final DataType keyType;
    private final DataType valueType;
    private final long createVersion;
    /**
     * The map of old roots. The key is the new version, the value is the root
     * before this version.
     */
    private final TreeMap<Long, Page> oldRoots = new TreeMap<Long, Page>();

    private boolean closed;
    private boolean readOnly;

    protected BtreeMap(BtreeMapStore store, int id, String name,
            DataType keyType, DataType valueType, long createVersion) {
        this.store = store;
        this.id = id;
        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
        this.createVersion = createVersion;
    }

    /**
     * Store a key-value pair.
     *
     * @param key the key
     * @param data the value
     */
    public void put(K key, V data) {
        checkWrite();
        Page oldRoot = root;
        root = put(oldRoot, store.getCurrentVersion(), key, data, store.getMaxPageSize());
        markChanged(oldRoot);
    }

    /**
     * Add or update a key-value pair.
     *
     * @param map the map
     * @param p the page (may be null)
     * @param writeVersion the write version
     * @param key the key
     * @param value the value (may not be null)
     * @param maxPageSize the maximum page size
     * @return the root page
     */
    protected Page put(Page p, long writeVersion, Object key, Object value, int maxPageSize) {
        if (p == null) {
            Object[] keys = { key };
            Object[] values = { value };
            p = Page.create(this, writeVersion, 1,
                    keys, values, null, null, null, 1, 0);
            return p;
        }
        if (p.getKeyCount() > maxPageSize) {
            // only possible if this is the root, else we would have split earlier
            // (this requires maxPageSize is fixed)
            p = p.copyOnWrite(writeVersion);
            int at = p.getKeyCount() / 2;
            long totalCount = p.getTotalCount();
            Object k = p.getKey(at);
            Page split = p.split(at);
            Object[] keys = { k };
            long[] children = { p.getPos(), split.getPos() };
            Page[] childrenPages = { p, split };
            long[] counts = { p.getTotalCount(), split.getTotalCount() };
            p = Page.create(this, writeVersion, 1,
                    keys, null, children, childrenPages, counts, totalCount, 0);
            // now p is a node; insert continues
        } else if (p.isLeaf()) {
            int index = p.binarySearch(key);
            p = p.copyOnWrite(writeVersion);
            if (index < 0) {
                index = -index - 1;
                p.insertLeaf(index, key, value);
            } else {
                p.setValue(index, value);
            }
            return p;
        }
        // p is a node
        int index = p.binarySearch(key);
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        Page c = p.getChildPage(index);
        if (c.getKeyCount() >= maxPageSize) {
            // split on the way down
            c = c.copyOnWrite(writeVersion);
            int at = c.getKeyCount() / 2;
            Object k = c.getKey(at);
            Page split = c.split(at);
            p = p.copyOnWrite(writeVersion);
            p.setChild(index, split);
            p.insertNode(index, k, c);
            // now we are not sure where to add
            return put(p, writeVersion, key, value, maxPageSize);
        }
        long oldSize = c.getTotalCount();
        Page c2 = put(c, writeVersion, key, value, maxPageSize);
        if (c != c2 || oldSize != c2.getTotalCount()) {
            p = p.copyOnWrite(writeVersion);
            p.setChild(index, c2);
        }
        return p;
    }

    /**
     * Get a value.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        checkOpen();
        if (root == null) {
            return null;
        }
        return (V) binarySearch(root, key);
    }

    /**
     * Go to the first element for the given key.
     *
     * @param p the current page
     * @param cursor the cursor
     * @param key the key
     */
    protected CursorPos min(Page p, Cursor<K, V> cursor, Object key) {
        while (p != null) {
            if (p.isLeaf()) {
                int x = key == null ? 0 : p.binarySearch(key);
                if (x < 0) {
                    x = -x - 1;
                }
                CursorPos c = new CursorPos();
                c.page = p;
                c.index = x;
                return c;
            }
            int x = key == null ? -1 : p.binarySearch(key);
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            CursorPos c = new CursorPos();
            c.page = p;
            c.index = x;
            cursor.push(c);
            p = p.getChildPage(x);
        }
        return null;
    }

    /**
     * Get the next key.
     *
     * @param p the cursor position
     * @param cursor the cursor
     * @return the next key
     */
    protected Object nextKey(CursorPos p, Cursor<K, V> cursor) {
        while (p != null) {
            int index = p.index++;
            Page x = p.page;
            if (index < x.getKeyCount()) {
                return x.getKey(index);
            }
            while (true) {
                p = cursor.pop();
                if (p == null) {
                    break;
                }
                index = ++p.index;
                x = p.page;
                if (index <= x.getKeyCount()) {
                    cursor.push(p);
                    p = cursor.visitChild(x, index);
                    if (p != null) {
                        break;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Get the value for the given key, or null if not found.
     *
     * @param key the key
     * @return the value or null
     */
    protected Object binarySearch(Page p, Object key) {
        int x = p.binarySearch(key);
        if (!p.isLeaf()) {
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            p = p.getChildPage(x);
            return binarySearch(p, key);
        }
        if (x >= 0) {
            return p.getValue(x);
        }
        return null;
    }

    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    /**
     * Get the page for the given value.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    protected Page getPage(K key) {
        if (root == null) {
            return null;
        }
        return binarySearchPage(root, key);
    }

    /**
     * Get the value for the given key, or null if not found.
     *
     * @param p the parent page
     * @param key the key
     * @return the page or null
     */
    protected Page binarySearchPage(Page p, Object key) {
        int x = p.binarySearch(key);
        if (!p.isLeaf()) {
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            p = p.getChildPage(x);
            return binarySearchPage(p, key);
        }
        if (x >= 0) {
            return p;
        }
        return null;
    }

    /**
     * Remove all entries.
     */
    public void clear() {
        checkWrite();
        if (root != null) {
            Page oldRoot = root;
            root.removeAllRecursive();
            root = null;
            markChanged(oldRoot);
        }
    }

    /**
     * Remove all entries, and close the map.
     */
    public void remove() {
        checkWrite();
        if (root != null) {
            root.removeAllRecursive();
        }
        store.removeMap(name);
        close();
    }

    public void close() {
        closed = true;
        readOnly = true;
        oldRoots.clear();
        root = null;
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * Remove a key-value pair, if the key exists.
     *
     * @param key the key
     */
    public void remove(K key) {
        checkWrite();
        Page oldRoot = root;
        if (oldRoot != null) {
            root = remove(oldRoot, store.getCurrentVersion(), key);
            markChanged(oldRoot);
        }
    }

    /**
     * Remove a key-value pair.
     *
     * @param p the page (may not be null)
     * @param writeVersion the write version
     * @param key the key
     * @return the new root page (null if empty)
     */
    protected Page remove(Page p, long writeVersion, Object key) {
        int index = p.binarySearch(key);
        if (p.isLeaf()) {
            if (index >= 0) {
                if (p.getKeyCount() == 1) {
                    removePage(p);
                    return null;
                }
                p = p.copyOnWrite(writeVersion);
                p.remove(index);
            }
            return p;
        }
        // node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        Page c = p.getChildPage(index);
        long oldCount = c.getTotalCount();
        Page c2 = remove(c, writeVersion, key);
        if (c2 == null) {
            // this child was deleted
            p = p.copyOnWrite(writeVersion);
            p.remove(index);
            if (p.getKeyCount() == 0) {
                removePage(p);
                p = p.getChildPage(0);
            }
        } else if (oldCount != c2.getTotalCount()) {
            p = p.copyOnWrite(writeVersion);
            p.setChild(index, c2);
        }
        return p;
    }

    protected void markChanged(Page oldRoot) {
        if (oldRoot != root) {
            long v = store.getCurrentVersion();
            if (!oldRoots.containsKey(v)) {
                oldRoots.put(v, oldRoot);
            }
            store.markChanged(this);
        }
    }

    public boolean hasUnsavedChanges() {
        return oldRoots.size() > 0;
    }

    /**
     * Compare two keys.
     *
     * @param a the first key
     * @param b the second key
     * @return -1 if the first key is smaller, 1 if bigger, 0 if equal
     */
    int compare(Object a, Object b) {
        return keyType.compare(a, b);
    }

    /**
     * Get the key type.
     *
     * @return the key type
     */
    DataType getKeyType() {
        return keyType;
    }

    /**
     * Get the value type.
     *
     * @return the value type
     */
    DataType getValueType() {
        return valueType;
    }

    /**
     * Read a page.
     *
     * @param pos the position of the page
     * @return the page
     */
    Page readPage(long pos) {
        return store.readPage(this, pos);
    }

    /**
     * Set the position of the root page.
     *
     * @param rootPos the position, 0 for empty
     */
    void setRootPos(long rootPos) {
        root = rootPos == 0 ? null : readPage(rootPos);
    }

    /**
     * Iterate over all keys.
     *
     * @param from the first key to return
     * @return the iterator
     */
    public Iterator<K> keyIterator(K from) {
        checkOpen();
        Cursor<K, V> c = new Cursor<K, V>(this);
        c.start(root, from);
        return c;
    }

    /**
     * Iterate over all keys in changed pages.
     *
     * @param minVersion the minimum version
     * @return the iterator
     */
    public Iterator<K> changeIterator(long minVersion) {
        checkOpen();
        Cursor<K, V> c = new ChangeCursor<K, V>(this, minVersion);
        c.start(root, null);
        return c;
    }

    public Set<K> keySet() {
        checkOpen();
        final Page root = this.root;
        return new AbstractSet<K>() {

            @Override
            public Iterator<K> iterator() {
                Cursor<K, V> c = new Cursor<K, V>(BtreeMap.this);
                c.start(root, null);
                return c;
            }

            @Override
            public int size() {
                return BtreeMap.this.size();
            }

            @Override
            public boolean contains(Object o) {
                return BtreeMap.this.containsKey(o);
            }

        };
    }

    /**
     * Get the root page.
     *
     * @return the root page
     */
    public Page getRoot() {
        return root;
    }

    /**
     * Get the map name.
     *
     * @return the name
     */
    String getName() {
        return name;
    }

    BtreeMapStore getStore() {
        return store;
    }

    int getId() {
        return id;
    }

    void rollbackTo(long version) {
        checkWrite();
        if (version < createVersion) {
            remove();
        } else {
            // iterating in ascending order, and pick the last version -
            // this is not terribly efficient if there are many versions
            // but it is a simple algorithm
            Long newestOldVersion = null;
            for (Iterator<Long> it = oldRoots.keySet().iterator(); it.hasNext();) {
                Long x = it.next();
                if (x > version) {
                    if (newestOldVersion == null) {
                        newestOldVersion = x;
                        root = oldRoots.get(x);
                    }
                    it.remove();
                }
            }
        }
    }

    void revertTemp() {
        oldRoots.clear();
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    protected void checkOpen() {
        if (closed) {
            throw new IllegalStateException("This map is closed");
        }
    }

    protected void checkWrite() {
        if (readOnly) {
            checkOpen();
            throw new IllegalStateException("This map is read-only");
        }
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("map:").append(name);
        if (readOnly) {
            buff.append(" readOnly");
        }
        if (closed) {
            buff.append(" closed");
        }
        return buff.toString();
    }

    public int hashCode() {
        return id;
    }

    public int size() {
        long size = getSize();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    public long getSize() {
        return root == null ? 0 : root.getTotalCount();
    }

    public boolean equals(Object o) {
        return this == o;
    }

    long getCreateVersion() {
        return createVersion;
    }

    protected void removePage(Page p) {
        store.removePage(p.getPos());
    }

    public BtreeMap<K, V> openVersion(long version) {
        if (version < createVersion) {
            throw new IllegalArgumentException("Unknown version");
        }
        if (!oldRoots.containsKey(version)) {
            return store.openMapVersion(version, name);
        }
        Page root = oldRoots.get(version);
        BtreeMap<K, V> m = new BtreeMap<K, V>(store, id, name, keyType, valueType, createVersion);
        m.readOnly = true;
        m.root = root;
        return m;
    }

}
