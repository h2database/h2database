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
    private final String name;
    private final DataType keyType;
    private final DataType valueType;
    private Page root;

    private BtreeMap(BtreeMapStore store, String name, Class<K> keyClass, Class<V> valueClass) {
        this.store = store;
        this.name = name;
        if (keyClass == Integer.class) {
            keyType = new IntegerType();
        } else if (keyClass == String.class) {
            keyType = new StringType();
        } else {
            throw new RuntimeException("Unsupported key class " + keyClass.toString());
        }
        if (valueClass == Integer.class) {
            valueType = new IntegerType();
        } else if (valueClass == String.class) {
            valueType = new StringType();
        } else {
            throw new RuntimeException("Unsupported value class " + keyClass.toString());
        }
    }

    /**
     * Get the class with the given tag name.
     *
     * @param name the tag name
     * @return the class
     */
    static Class<?> getClass(String name) {
        if (name.equals("i")) {
            return Integer.class;
        } else if (name.equals("s")) {
            return String.class;
        }
        throw new RuntimeException("Unknown class name " + name);
    }

    /**
     * Open a map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param store the tree store
     * @param name the name of the map
     * @param keyClass the key class
     * @param valueClass the value class
     * @return the map
     */
    static <K, V> BtreeMap<K, V> open(BtreeMapStore store, String name, Class<K> keyClass, Class<V> valueClass) {
        return new BtreeMap<K, V>(store, name, keyClass, valueClass);
    }

    /**
     * Store a key-value pair.
     *
     * @param key the key
     * @param data the value
     */
    public void put(K key, V data) {
        if (!isChanged()) {
            store.markChanged(name, this);
        }
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
    public Page getPage(K key) {
        if (root == null) {
            return null;
        }
        return root.findPage(key);
    }

    /**
     * Remove a key-value pair.
     *
     * @param key the key
     */
    public void remove(K key) {
        if (!isChanged()) {
            store.markChanged(name, this);
        }
        if (root != null) {
            root = Page.remove(root, key);
        }
    }

    /**
     * Was this map changed.
     *
     * @return true if yes
     */
    boolean isChanged() {
        return root != null && root.getId() < 0;
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
     * Read a node.
     *
     * @param id the node id
     * @return the node
     */
    Page readPage(long id) {
        return store.readPage(this, id);
    }

    /**
     * Remove a node.
     *
     * @param id the node id
     */
    void removePage(long id) {
        store.removePage(id);
    }

    /**
     * Set the position of the root page.
     *
     * @param rootPos the position
     */
    void setRoot(long rootPos) {
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
     * Get the root node.
     *
     * @return the root node
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

}
