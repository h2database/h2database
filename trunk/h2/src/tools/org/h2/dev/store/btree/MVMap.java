/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * A stored map.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class MVMap<K, V> extends AbstractMap<K, V>
        implements ConcurrentMap<K, V> {

    /**
     * The store.
     */
    protected final MVStore store;

    /**
     * The current root page (may not be null).
     */
    protected volatile Page root;

    private final int id;
    private final String name;
    private final DataType keyType;
    private final DataType valueType;
    private final long createVersion;
    private ArrayList<Page> oldRoots = new ArrayList<Page>();

    private boolean closed;
    private boolean readOnly;

    protected MVMap(MVStore store, int id, String name,
            DataType keyType, DataType valueType, long createVersion) {
        this.store = store;
        this.id = id;
        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
        this.createVersion = createVersion;
        this.root = Page.createEmpty(this,  createVersion - 1);
    }

    /**
     * Add or replace a key-value pair.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    @SuppressWarnings("unchecked")
    public V put(K key, V value) {
        checkWrite();
        long writeVersion = store.getCurrentVersion();
        Page p = root.copyOnWrite(writeVersion);
        if (p.getKeyCount() > store.getMaxPageSize()) {
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
        }
        Object result = put(p, writeVersion, key, value);
        setRoot(p);
        return (V) result;
    }

    /**
     * Add or update a key-value pair.
     *
     * @param p the page
     * @param writeVersion the write version
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value, or null
     */
    protected Object put(Page p, long writeVersion, Object key, Object value) {
        if (p.isLeaf()) {
            int index = p.binarySearch(key);
            if (index < 0) {
                index = -index - 1;
                p.insertLeaf(index, key, value);
                return null;
            }
            return p.setValue(index, value);
        }
        // p is a node
        int index = p.binarySearch(key);
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        Page c = p.getChildPage(index).copyOnWrite(writeVersion);
        if (c.getKeyCount() >= store.getMaxPageSize()) {
            // split on the way down
            int at = c.getKeyCount() / 2;
            Object k = c.getKey(at);
            Page split = c.split(at);
            p.setChild(index, split);
            p.insertNode(index, k, c);
            // now we are not sure where to add
            return put(p, writeVersion, key, value);
        }
        Object result = put(c, writeVersion, key, value);
        p.setChild(index, c);
        return result;
    }

    /**
     * Get the first key, or null if the map is empty.
     *
     * @return the first key, or null
     */
    public K firstKey() {
        return getFirstLast(true);
    }

    /**
     * Get the last key, or null if the map is empty.
     *
     * @return the last key, or null
     */
    public K lastKey() {
        return getFirstLast(false);
    }

    @SuppressWarnings("unchecked")
    private K getFirstLast(boolean first) {
        checkOpen();
        Page p = root;
        if (size() == 0) {
            return null;
        }
        while (true) {
            if (p.isLeaf()) {
                return (K) p.getKey(first ? 0 : p.getKeyCount() - 1);
            }
            p = p.getChildPage(first ? 0 : p.getChildPageCount() - 1);
        }
    }

    /**
     * Get the smallest key that is larger than the given key, or null if no
     * such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K higherKey(K key) {
        return getMinMax(key, false, true);
    }

    /**
     * Get the smallest key that is larger or equal to this key.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K ceilingKey(K key) {
        return getMinMax(key, false, false);
    }

    /**
     * Get the largest key that is smaller or equal to this key.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K floorKey(K key) {
        return getMinMax(key, true, false);
    }

    /**
     * Get the largest key that is smaller than the given key, or null if no
     * such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K lowerKey(K key) {
        return getMinMax(key, true, true);
    }

    private K getMinMax(K key, boolean min, boolean excluding) {
        checkOpen();
        if (size() == 0) {
            return null;
        }
        return getMinMax(root, key, min, excluding);
    }

    @SuppressWarnings("unchecked")
    private K getMinMax(Page p, K key, boolean min, boolean excluding) {
        if (p.isLeaf()) {
            if (key == null) {
                return (K) p.getKey(min ? 0 : p.getKeyCount() - 1);
            }
            int x = p.binarySearch(key);
            if (x < 0) {
                x = -x - (min ? 2 : 1);
            } else if (excluding) {
                x += min ? -1 : 1;
            }
            if (x < 0 || x >= p.getKeyCount()) {
                return null;
            }
            return (K) p.getKey(x);
        }
        int x;
        if (key == null) {
            x = min ? 0 : p.getKeyCount() - 1;
        } else {
            x = p.binarySearch(key);
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
        }
        while (true) {
            if (x < 0 || x >= p.getChildPageCount()) {
                return null;
            }
            K k = getMinMax(p.getChildPage(x), key, min, excluding);
            if (k != null) {
                return k;
            }
            x += min ? -1 : 1;
        }
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
        return (V) binarySearch(root, key);
    }

    /**
     * Get the value for the given key, or null if not found.
     *
     * @param p the page
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
        root.removeAllRecursive();
        setRoot(Page.createEmpty(this, store.getCurrentVersion()));
    }

    /**
     * Remove all entries, and close the map.
     */
    public void removeMap() {
        if (!store.isMetaMap(this)) {
            checkWrite();
            root.removeAllRecursive();
            store.removeMap(name);
            close();
        }
    }

    /**
     * Close the map, making it read only and release the memory.
     */
    public void close() {
        closed = true;
        readOnly = true;
        removeAllOldVersions();
        root = null;
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * Remove a key-value pair, if the key exists.
     *
     * @param key the key (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    public V remove(Object key) {
        checkWrite();
        long writeVersion = store.getCurrentVersion();
        Page p = root.copyOnWrite(writeVersion);
        @SuppressWarnings("unchecked")
        V result = (V) remove(p, writeVersion, key);
        setRoot(p);
        return result;
    }

    /**
     * Add a key-value pair if it does not yet exist.
     *
     * @param key the key (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    public synchronized V putIfAbsent(K key, V value) {
        V old = get(key);
        if (old == null) {
            put(key, value);
        }
        return old;
    }

    /**
     * Remove a key-value pair if the value matches the stored one.
     *
     * @param key the key (may not be null)
     * @param value the expected value
     * @return true if the item was removed
     */
    public synchronized boolean remove(Object key, Object value) {
        V old = get(key);
        if (old.equals(value)) {
            remove(key);
            return true;
        }
        return false;
    }

    /**
     * Replace a value for an existing key, if the value matches.
     *
     * @param key the key (may not be null)
     * @param oldValue the expected value
     * @param newValue the new value
     * @return true if the value was replaced
     */
    public synchronized boolean replace(K key, V oldValue, V newValue) {
        V old = get(key);
        if (old.equals(oldValue)) {
            put(key, newValue);
            return true;
        }
        return false;
    }

    /**
     * Replace a value for an existing key.
     *
     * @param key the key (may not be null)
     * @param value the new value
     * @return true if the value was replaced
     */
    public synchronized V replace(K key, V value) {
        V old = get(key);
        if (old != null) {
            put(key, value);
            return old;
        }
        return null;
    }

    /**
     * Remove a key-value pair.
     *
     * @param p the page (may not be null)
     * @param writeVersion the write version
     * @param key the key
     * @return the old value, or null if the key did not exist
     */
    protected Object remove(Page p, long writeVersion, Object key) {
        int index = p.binarySearch(key);
        Object result = null;
        if (p.isLeaf()) {
            if (index >= 0) {
                result = p.getValue(index);
                p.remove(index);
                if (p.getKeyCount() == 0) {
                    removePage(p);
                }
            }
            return result;
        }
        // node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        Page cOld = p.getChildPage(index);
        Page c = cOld.copyOnWrite(writeVersion);
        long oldCount = c.getTotalCount();
        result = remove(c, writeVersion, key);
        if (oldCount == c.getTotalCount()) {
            return null;
        }
        // TODO merge if the c key count is below the threshold
        if (c.getTotalCount() == 0) {
            // this child was deleted
            if (p.getKeyCount() == 0) {
                p.setChild(index, c);
                removePage(p);
            } else {
                p.remove(index);
            }
        } else {
            p.setChild(index, c);
        }
        return result;
    }

    protected void setRoot(Page newRoot) {
        if (root != newRoot) {
            removeUnusedOldVersions();
            if (root.getVersion() != newRoot.getVersion()) {
                ArrayList<Page> list = oldRoots;
                if (list.size() > 0) {
                    Page last = list.get(list.size() - 1);
                    if (last.getVersion() != root.getVersion()) {
                        list.add(root);
                    }
                } else {
                    list.add(root);
                }
                store.markChanged(this);
            }
            root = newRoot;
        }
    }

    /**
     * Check whether this map has any unsaved changes.
     *
     * @return true if there are unsaved changes.
     */
    public boolean hasUnsavedChanges() {
        return !oldRoots.isEmpty();
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
        root = rootPos == 0 ? Page.createEmpty(this, -1) : readPage(rootPos);
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

    /**
     * Iterate over all keys in changed pages.
     *
     * @param version the old version
     * @return the iterator
     */
    public Iterator<K> changeIterator(long version) {
        checkOpen();
        MVMap<K, V> old = openVersion(version);
        return new ChangeCursor<K, V>(this, root, old.root);
    }

    public Set<Map.Entry<K, V>> entrySet() {
        HashMap<K, V> map = new HashMap<K, V>();
        for (K k : keySet()) {
            map.put(k,  get(k));
        }
        return map.entrySet();
    }

    public Set<K> keySet() {
        checkOpen();
        final MVMap<K, V> map = this;
        final Page root = this.root;
        return new AbstractSet<K>() {

            @Override
            public Iterator<K> iterator() {
                return new Cursor<K, V>(map, root, null);
            }

            @Override
            public int size() {
                return MVMap.this.size();
            }

            @Override
            public boolean contains(Object o) {
                return MVMap.this.containsKey(o);
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

    MVStore getStore() {
        return store;
    }

    int getId() {
        return id;
    }

    /**
     * Rollback to the given version.
     *
     * @param version the version
     */
    void rollbackTo(long version) {
        checkWrite();
        removeUnusedOldVersions();
        if (version <= createVersion) {
            removeMap();
        } else if (root.getVersion() >= version) {
            // iterating in descending order -
            // this is not terribly efficient if there are many versions
            ArrayList<Page> list = oldRoots;
            while (list.size() > 0) {
                int i = list.size() - 1;
                Page p = list.get(i);
                root = p;
                list.remove(i);
                if (p.getVersion() < version) {
                    break;
                }
            }
        }
    }

    /**
     * Forget all old versions.
     */
    void removeAllOldVersions() {
        // create a new instance
        // because another thread might iterate over it
        oldRoots = new ArrayList<Page>();
    }

    /**
     * Forget those old versions that are no longer needed.
     */
    void removeUnusedOldVersions() {
        long oldest = store.getRetainVersion();
        if (oldest == -1) {
            return;
        }
        int i = searchRoot(oldest);
        if (i < 0) {
            return;
        }
        // create a new instance
        // because another thread might iterate over it
        ArrayList<Page> list = new ArrayList<Page>();
        list.addAll(oldRoots.subList(i, oldRoots.size()));
        oldRoots = list;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Check whether the map is open.
     *
     * @throws IllegalStateException if the map is closed
     */
    protected void checkOpen() {
        if (closed) {
            throw new IllegalStateException("This map is closed");
        }
    }

    /**
     * Check whether writing is allowed.
     *
     * @throws IllegalStateException if the map is read-only
     */
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

    public boolean equals(Object o) {
        return this == o;
    }

    public int size() {
        long size = getSize();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    public long getSize() {
        return root.getTotalCount();
    }

    long getCreateVersion() {
        return createVersion;
    }

    /**
     * Remove the given page (make the space available).
     *
     * @param p the page
     */
    protected void removePage(Page p) {
        store.removePage(p.getPos());
    }

    /**
     * Open an old version for the given map.
     *
     * @param version the version
     * @return the map
     */
    public MVMap<K, V> openVersion(long version) {
        if (readOnly) {
            throw new IllegalArgumentException("This map is read-only - need to call the method on the writable map");
        }
        if (version < createVersion) {
            throw new IllegalArgumentException("Unknown version");
        }
        Page newest = null;
        // need to copy because it can change
        Page r = root;
        if (r.getVersion() == version) {
            newest = r;
        } else {
            // find the newest page that has a getVersion() <= version
            int i = searchRoot(version);
            if (i < 0) {
                // not found
                if (i == -1) {
                    // smaller than all in-memory versions
                    return store.openMapVersion(version, name);
                }
                i = -i - 2;
            }
            newest = oldRoots.get(i);
        }
        MVMap<K, V> m = new MVMap<K, V>(store, id, name, keyType, valueType, createVersion);
        m.readOnly = true;
        m.root = newest;
        return m;
    }

    private int searchRoot(long version) {
        int low = 0, high = oldRoots.size() - 1;
        while (low <= high) {
            int x = (low + high) >>> 1;
            long v = oldRoots.get(x).getVersion();
            if (v < version) {
                low = x + 1;
            } else if (version < v) {
                high = x - 1;
            } else {
                return x;
            }
        }
        return -(low + 1);
    }

    public long getVersion() {
        return root.getVersion();
    }

    protected int getChildPageCount(Page p) {
        return p.getChildPageCount();
    }

}
