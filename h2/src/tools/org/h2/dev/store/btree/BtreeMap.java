/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.util.Iterator;
import java.util.TreeMap;

/**
 * A stored map.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class BtreeMap<K, V> {

    private final int id;
    private final String name;
    private final DataType keyType;
    private final DataType valueType;
    private final long createVersion;
    private final TreeMap<Long, Page> oldRoots = new TreeMap<Long, Page>();
    private BtreeMapStore store;
    private Page root;
    private boolean readOnly;

    BtreeMap(BtreeMapStore store, int id, String name, DataType keyType, DataType valueType, long createVersion) {
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
        root = Page.put(this, root, store.getCurrentVersion(), key, data);
        markChanged(oldRoot);
    }

    /**
     * Get a value.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    @SuppressWarnings("unchecked")
    public V get(K key) {
        checkOpen();
        if (root == null) {
            return null;
        }
        return (V) root.find(key);
    }

    public boolean containsKey(K key) {
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
        return root.findPage(key);
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
        store.removeMap(id);
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
     * Remove a key-value pair.
     *
     * @param key the key
     */
    public void remove(K key) {
        checkWrite();
        if (root != null) {
            Page oldRoot = root;
            root = Page.remove(root, store.getCurrentVersion(), key);
            markChanged(oldRoot);
        }
    }

    private void markChanged(Page oldRoot) {
        if (oldRoot != root) {
            oldRoots.put(store.getCurrentVersion(), oldRoot);
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
        return new Cursor<K>(root, from);
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
        if (version <= createVersion) {
            remove();
        } else {
            // iterating in ascending order, and pick the last version -
            // this is not terribly efficient if there are many versions
            // but it is a simple algorithm
            Long newestOldVersion = null;
            for (Iterator<Long> it = oldRoots.keySet().iterator(); it.hasNext();) {
                Long x = it.next();
                if (x >= version) {
                    if (newestOldVersion == null) {
                        newestOldVersion = x;
                        root = oldRoots.get(x);
                    }
                    it.remove();
                }
            }
        }
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

    public boolean equals(Object o) {
        return this == o;
    }

    long getCreatedVersion() {
        return createVersion;
    }

}
