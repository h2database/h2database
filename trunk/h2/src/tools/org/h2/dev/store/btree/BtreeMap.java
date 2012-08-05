/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.util.Iterator;

/**
 * A stored map.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class BtreeMap<K, V> {

    private final BtreeMapStore store;
    private final int id;
    private final String name;
    private final DataType keyType;
    private final DataType valueType;
    private Page root;

    private BtreeMap(BtreeMapStore store, int id, String name, DataType keyType, DataType valueType) {
        this.store = store;
        this.id = id;
        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    /**
     * Open a map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param store the tree store
     * @param id the map id
     * @param name the name of the map
     * @param keyClass the key class
     * @param valueClass the value class
     * @return the map
     */
    static <K, V> BtreeMap<K, V> open(BtreeMapStore store, int id, String name, DataType keyType, DataType valueType) {
        return new BtreeMap<K, V>(store, id, name, keyType, valueType);
    }

    /**
     * Store a key-value pair.
     *
     * @param key the key
     * @param data the value
     */
    public void put(K key, V data) {
        markChanged();
        root = Page.put(this, root, key, data);
    }

    /**
     * Get a value.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    @SuppressWarnings("unchecked")
    public V get(K key) {
        if (root == null) {
            return null;
        }
        return (V) root.find(key);
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
        if (root != null) {
            markChanged();
            root.removeAllRecursive();
            root = null;
        }
    }

    /**
     * Remove all entries, and remove the map.
     */
    public void remove() {
        clear();
        store.removeMap(id);
    }

    /**
     * Remove a key-value pair.
     *
     * @param key the key
     */
    public void remove(K key) {
        if (root != null) {
            markChanged();
            root = Page.remove(root, key);
        }
    }

    /**
     * Was this map changed.
     *
     * @return true if yes
     */
    boolean isChanged() {
        return root != null && root.getPos() < 0;
    }

    private void markChanged() {
        if (!isChanged()) {
            store.markChanged(name, this);
        }
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

    long getTransaction() {
        return store.getTransaction();
    }

    /**
     * Register a page and get the next temporary page id.
     *
     * @param p the new page
     * @return the page id
     */
    long registerTempPage(Page p) {
        return store.registerTempPage(p);
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
     * Remove a page.
     *
     * @param pos the position of the page
     */
    void removePage(long pos) {
        store.removePage(pos);
    }

    /**
     * Set the position of the root page.
     *
     * @param rootPos the position
     */
    void setRootPos(long rootPos) {
        root = readPage(rootPos);
    }

    /**
     * Iterate over all keys.
     *
     * @param from the first key to return
     * @return the iterator
     */
    public Iterator<K> keyIterator(K from) {
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

}
