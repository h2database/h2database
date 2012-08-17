/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.util.AbstractSet;
import java.util.ArrayList;
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

    static final IllegalArgumentException KEY_NOT_FOUND = new IllegalArgumentException(
            "Key not found");
    static final IllegalArgumentException KEY_ALREADY_EXISTS = new IllegalArgumentException(
            "Key already exists");

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
    private BtreeMapStore store;
    private Page root;
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
        if (containsKey(key)) {
            root = set(root, store.getCurrentVersion(), key, data);
        } else {
            root = add(root, store.getCurrentVersion(), key, data);
        }
        markChanged(oldRoot);
    }

    /**
     * Update a value for an existing key.
     *
     * @param map the map
     * @param p the page (may not be null)
     * @param writeVersion the write version
     * @param key the key
     * @param value the value
     * @return the root page
     * @throws InvalidArgumentException if this key does not exist (without
     *         stack trace)
     */
    private Page set(Page p, long writeVersion, Object key,
            Object value) {
        if (p == null) {
            throw KEY_NOT_FOUND;
        }
        int index = p.binarySearch(key);
        if (p.isLeaf()) {
            if (index < 0) {
                throw KEY_NOT_FOUND;
            }
            p = p.copyOnWrite(writeVersion);
            p.setValue(index, value);
            return p;
        }
        // it is a node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        Page c = p.getChildPage(index);
        Page c2 = set(c, writeVersion, key, value);
        if (c != c2) {
            p = p.copyOnWrite(writeVersion);
            p.setChild(index, c2.getPos(), c2.getPos());
        }
        return p;
    }

    /**
     * Add a new key-value pair.
     *
     * @param map the map
     * @param p the page (may be null)
     * @param writeVersion the write version
     * @param key the key
     * @param value the value
     * @return the root page
     * @throws InvalidArgumentException if this key already exists (without
     *         stack trace)
     */
    private Page add(Page p, long writeVersion, Object key,
            Object value) {
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
            int at = p.getKeyCount() / 2;
            long totalSize = p.getTotalSize();
            Object k = p.getKey(at);
            Page split = p.split(at);
            Object[] keys = { k };
            long[] children = { p.getPos(), split.getPos() };
            long[] childrenSize = { p.getTotalSize(), split.getTotalSize() };
            p = Page.create(this, writeVersion, keys, null, children, childrenSize,
                    totalSize);
            // now p is a node; insert continues
        } else if (p.isLeaf()) {
            int index = p.binarySearch(key);
            if (index >= 0) {
                throw KEY_ALREADY_EXISTS;
            }
            index = -index - 1;
            p = p.copyOnWrite(writeVersion);
            p.insert(index, key, value, 0, 0);
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
        if (c.getKeyCount() >= store.getMaxPageSize()) {
            // split on the way down
            c = c.copyOnWrite(writeVersion);
            int at = c.getKeyCount() / 2;
            Object k = c.getKey(at);
            Page split = c.split(at);
            p = p.copyOnWrite(writeVersion);
            p.setChild(index, c.getPos(), c.getTotalSize());
            p.insert(index, k, null, split.getPos(), split.getTotalSize());
            // now we are not sure where to add
            return add(p, writeVersion, key, value);
        }
        Page c2 = add(c, writeVersion, key, value);
        p = p.copyOnWrite(writeVersion);
        // the child might be the same, but not the size
        p.setChild(index, c2.getPos(), c2.getTotalSize());
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
     * @param parents the stack of parent page positions
     * @param key the key
     */
    void min(Page p, ArrayList<CursorPos> parents, Object key) {
        while (p != null) {
            if (!p.isLeaf()) {
                int x = key == null ? -1 : p.binarySearch(key);
                if (x < 0) {
                    x = -x - 1;
                } else {
                    x++;
                }
                CursorPos c = new CursorPos();
                c.page = p;
                c.index = x;
                parents.add(c);
                p = p.getChildPage(x);
            } else {
                int x = key == null ? 0 : p.binarySearch(key);
                if (x < 0) {
                    x = -x - 1;
                }
                CursorPos c = new CursorPos();
                c.page = p;
                c.index = x;
                parents.add(c);
                return;
            }
        }
    }

    /**
     * Get the next key.
     *
     * @param parents the stack of parent page positions
     * @return the next key
     */
    Object nextKey(ArrayList<CursorPos> parents) {
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
                if (index <= p.page.getKeyCount()) {
                    parents.add(p);
                    Page x = p.page;
                     x = x.getChildPage(index);
                    min(x, parents, null);
                    break;
                }
            }
        }
    }

    /**
     * Get the value for the given key, or null if not found.
     *
     * @param key the key
     * @return the value or null
     */
    private Object binarySearch(Page p, Object key) {
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
    Page getPage(K key) {
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
    private Page binarySearchPage(Page p, Object key) {
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
        readOnly = true;
        store = null;
        oldRoots.clear();
        root = null;
    }

    public boolean isClosed() {
        return store == null;
    }

    /**
     * Remove a key-value pair, if the key exists.
     *
     * @param key the key
     */
    public void remove(K key) {
        checkWrite();
        if (containsKey(key)) {
            Page oldRoot = root;
            root = removeExisting(root, store.getCurrentVersion(), key);
            markChanged(oldRoot);
        }
    }


    /**
     * Remove an existing key-value pair.
     *
     * @param p the page (may not be null)
     * @param writeVersion the write version
     * @param key the key
     * @return the new root page (null if empty)
     * @throws InvalidArgumentException if not found (without stack trace)
     */
    private Page removeExisting(Page p, long writeVersion, Object key) {
        if (p == null) {
            throw KEY_NOT_FOUND;
        }
        int index = p.binarySearch(key);
        if (p.isLeaf()) {
            if (index >= 0) {
                if (p.getKeyCount() == 1) {
                    store.removePage(p.getPos());
                    return null;
                }
                p = p.copyOnWrite(writeVersion);
                p.remove(index);
            } else {
                throw KEY_NOT_FOUND;
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
        Page c2 = removeExisting(c, writeVersion, key);
        p = p.copyOnWrite(writeVersion);
        if (c2 == null) {
            // this child was deleted
            p.remove(index);
            if (p.getKeyCount() == 0) {
                store.removePage(p.getPos());
                p = p.getChildPage(0);
            }
        } else {
            p.setChild(index, c2.getPos(), c2.getTotalSize());
        }
        return p;
    }

    private void markChanged(Page oldRoot) {
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
        return new Cursor<K, V>(this, root, from);
    }

    public Set<K> keySet() {
        checkOpen();
        final Page root = this.root;
        return new AbstractSet<K>() {

            @Override
            public Iterator<K> iterator() {
                return new Cursor<K, V>(BtreeMap.this, root, null);
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
    Page getRoot() {
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

    private void checkOpen() {
        if (store == null) {
            throw new IllegalStateException("This map is closed");
        }
    }

    private void checkWrite() {
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
        if (store == null) {
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
        return root == null ? 0 : root.getTotalSize();
    }

    public boolean equals(Object o) {
        return this == o;
    }

    long getCreateVersion() {
        return createVersion;
    }

    @SuppressWarnings("unchecked")
    public <M> M cast() {
        return (M) this;
    }

}
