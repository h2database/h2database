/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.mvstore.type.StringDataType;

/**
 * A stored map.
 * <p>
 * Read operations can happen concurrently with all other
 * operations, without risk of corruption.
 * <p>
 * Write operations first read the relevant area from disk to memory
 * concurrently, and only then modify the data. The in-memory part of write
 * operations is synchronized. For scalable concurrent in-memory write
 * operations, the map should be split into multiple smaller sub-maps that are
 * then synchronized independently.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class MVMap<K, V> extends AbstractMap<K, V>
        implements ConcurrentMap<K, V>, Cloneable {

    /**
     * The store.
     */
    public final MVStore store;

    /**
     * Reference to the current root page.
     */
    private final     AtomicReference<RootReference> root;

    private int       id;
    private long      createVersion;
    private final     DataType keyType;
    private final     DataType valueType;

    /**
     * Whether the map is closed. Volatile so we don't accidentally write to a
     * closed map in multithreaded mode.
     */
    private volatile  boolean closed;
    private           boolean readOnly;
    private           boolean isVolatile;

    private   static final long KEEP_CURRENT = -2;
    public    static final long INITIAL_VERSION = -1;

    protected MVMap(Map<String, Object> config) {
        this((MVStore)config.get("store"),
             (DataType)config.get("key"),
             (DataType)config.get("val"),
             DataUtils.readHexInt(config, "id", 0),
             DataUtils.readHexLong(config, "createVersion", 0),
             new AtomicReference<RootReference>()
             );
        setInitialRoot(createEmptyLeaf(), store.getCurrentVersion());
    }

    // constructor for cloneIt()
    protected MVMap(MVMap<K,V> source) {
        this(source.store, source.keyType, source.valueType, source.id, source.createVersion,
                new AtomicReference<>(source.root.get()));
    }

    // meta map constructor
    MVMap(MVStore store) {
        this(store, StringDataType.INSTANCE,StringDataType.INSTANCE, 0, 0, new AtomicReference<RootReference>());
        setInitialRoot(createEmptyLeaf(), store.getCurrentVersion());
    }

    @SuppressWarnings("unchecked")
    private MVMap(MVStore store, DataType keyType, DataType valueType, int id, long createVersion,
                  AtomicReference<RootReference> root) {
        this.store = store;
        this.id = id;
        this.createVersion = createVersion;
        this.keyType = keyType;
        this.valueType = valueType;
        this.root = root;
    }

    protected MVMap<K,V> cloneIt() {
        return new MVMap<>(this);
    }

    /**
     * Get the metadata key for the root of the given map id.
     *
     * @param mapId the map id
     * @return the metadata key
     */
    static String getMapRootKey(int mapId) {
        return "root." + Integer.toHexString(mapId);
    }

    /**
     * Get the metadata key for the given map id.
     *
     * @param mapId the map id
     * @return the metadata key
     */
    static String getMapKey(int mapId) {
        return "map." + Integer.toHexString(mapId);
    }

    /**
     * Open this map.
     */
    protected void init() {}

    /**
     * Add or replace a key-value pair.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    @Override
    public V put(K key, V value) {
        return put(key, value, DecisionMaker.PUT);
    }

    /**
     * Add or replace a key-value pair.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    @SuppressWarnings("unchecked")
    public final V put(K key, V value, DecisionMaker<? super V> decisionMaker) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        return operate(key, value, decisionMaker);
    }

    /**
     * Get the first key, or null if the map is empty.
     *
     * @return the first key, or null
     */
    public final K firstKey() {
        return getFirstLast(true);
    }

    /**
     * Get the last key, or null if the map is empty.
     *
     * @return the last key, or null
     */
    public final K lastKey() {
        return getFirstLast(false);
    }

    /**
     * Get the key at the given index.
     * <p>
     * This is a O(log(size)) operation.
     *
     * @param index the index
     * @return the key
     */
    @SuppressWarnings("unchecked")
    public final K getKey(long index) {
        if (index < 0 || index >= sizeAsLong()) {
            return null;
        }
        Page p = getRootPage();
        long offset = 0;
        while (true) {
            if (p.isLeaf()) {
                if (index >= offset + p.getKeyCount()) {
                    return null;
                }
                return (K) p.getKey((int) (index - offset));
            }
            int i = 0, size = getChildPageCount(p);
            for (; i < size; i++) {
                long c = p.getCounts(i);
                if (index < c + offset) {
                    break;
                }
                offset += c;
            }
            if (i == size) {
                return null;
            }
            p = p.getChildPage(i);
        }
    }

    /**
     * Get the key list. The list is a read-only representation of all keys.
     * <p>
     * The get and indexOf methods are O(log(size)) operations. The result of
     * indexOf is cast to an int.
     *
     * @return the key list
     */
    public final List<K> keyList() {
        return new AbstractList<K>() {

            @Override
            public K get(int index) {
                return getKey(index);
            }

            @Override
            public int size() {
                return MVMap.this.size();
            }

            @Override
            @SuppressWarnings("unchecked")
            public int indexOf(Object key) {
                return (int) getKeyIndex((K) key);
            }

        };
    }

    /**
     * Get the index of the given key in the map.
     * <p>
     * This is a O(log(size)) operation.
     * <p>
     * If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys. See also Arrays.binarySearch.
     *
     * @param key the key
     * @return the index
     */
    public final long getKeyIndex(K key) {
        Page p = getRootPage();
        if (p.getTotalCount() == 0) {
            return -1;
        }
        long offset = 0;
        while (true) {
            int x = p.binarySearch(key);
            if (p.isLeaf()) {
                if (x < 0) {
                    offset = -offset;
                }
                return offset + x;
            }
            if (x++ < 0) {
                x = -x;
            }
            for (int i = 0; i < x; i++) {
                offset += p.getCounts(i);
            }
            p = p.getChildPage(x);
        }
    }

    /**
     * Get the first (lowest) or last (largest) key.
     *
     * @param first whether to retrieve the first key
     * @return the key, or null if the map is empty
     */
    @SuppressWarnings("unchecked")
    private K getFirstLast(boolean first) {
        Page p = getRootPage();
        if (p.getKeyCount() == 0) {
            return null;
        }
        while (true) {
            if (p.isLeaf()) {
                return (K) p.getKey(first ? 0 : p.getKeyCount() - 1);
            }
            p = p.getChildPage(first ? 0 : getChildPageCount(p) - 1);
        }
    }

    /**
     * Get the smallest key that is larger than the given key, or null if no
     * such key exists.
     *
     * @param key the key
     * @return the result
     */
    public final K higherKey(K key) {
        return getMinMax(key, false, true);
    }

    /**
     * Get the smallest key that is larger or equal to this key.
     *
     * @param key the key
     * @return the result
     */
    public final K ceilingKey(K key) {
        return getMinMax(key, false, false);
    }

    /**
     * Get the largest key that is smaller or equal to this key.
     *
     * @param key the key
     * @return the result
     */
    public final K floorKey(K key) {
        return getMinMax(key, true, false);
    }

    /**
     * Get the largest key that is smaller than the given key, or null if no
     * such key exists.
     *
     * @param key the key
     * @return the result
     */
    public final K lowerKey(K key) {
        return getMinMax(key, true, true);
    }

    /**
     * Get the smallest or largest key using the given bounds.
     *
     * @param key the key
     * @param min whether to retrieve the smallest key
     * @param excluding if the given upper/lower bound is exclusive
     * @return the key, or null if no such key exists
     */
    private K getMinMax(K key, boolean min, boolean excluding) {
        return getMinMax(getRootPage(), key, min, excluding);
    }

    @SuppressWarnings("unchecked")
    private K getMinMax(Page p, K key, boolean min, boolean excluding) {
        int x = p.binarySearch(key);
        if (p.isLeaf()) {
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
        if (x++ < 0) {
            x = -x;
        }
        while (true) {
            if (x < 0 || x >= getChildPageCount(p)) {
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
     * Get the value for the given key, or null if not found.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        Page p = getRootPage();
        return (V) Page.get(p, key);
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

    @Override
    public final boolean containsKey(Object key) {
        return get(key) != null;
    }

    /**
     * Remove all entries.
     */
    @Override
    public void clear() {
        RootReference rootReference;
        Page emptyRootPage = createEmptyLeaf();
        int attempt = 0;
        do {
            rootReference = getRoot();
        } while (!updateRoot(rootReference, emptyRootPage, ++attempt));
        rootReference.root.removeAllRecursive();
    }

    /**
     * Close the map. Accessing the data is still possible (to allow concurrent
     * reads), but it is marked as closed.
     */
    final void close() {
        closed = true;
    }

    public final boolean isClosed() {
        return closed;
    }

    /**
     * Remove a key-value pair, if the key exists.
     *
     * @param key the key (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        return operate((K)key, null, DecisionMaker.REMOVE);
    }

    /**
     * Add a key-value pair if it does not yet exist.
     *
     * @param key the key (may not be null)
     * @param value the new value
     * @return the old value if the key existed, or null otherwise
     */
    @Override
    public final V putIfAbsent(K key, V value) {
        return put(key, value, DecisionMaker.IF_ABSENT);
    }

    /**
     * Remove a key-value pair if the value matches the stored one.
     *
     * @param key the key (may not be null)
     * @param value the expected value
     * @return true if the item was removed
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(Object key, Object value) {
        EqualsDecisionMaker<V> decisionMaker = new EqualsDecisionMaker<>(valueType, (V)value);
        operate((K)key, null, decisionMaker);
        return decisionMaker.getDecision() != Decision.ABORT;
    }

    /**
     * Check whether the two values are equal.
     *
     * @param a the first value
     * @param b the second value
     * @return true if they are equal
     */
    public final boolean areValuesEqual(Object a, Object b) {
        return areValuesEqual(valueType, a, b);
    }

    /**
     * Check whether the two values are equal.
     *
     * @param a the first value
     * @param b the second value
     * @param datatype to use for comparison
     * @return true if they are equal
     */
    public static boolean areValuesEqual(DataType datatype, Object a, Object b) {
        return a == b
            || a != null && b != null && datatype.compare(a, b) == 0;
    }

    /**
     * Replace a value for an existing key, if the value matches.
     *
     * @param key the key (may not be null)
     * @param oldValue the expected value
     * @param newValue the new value
     * @return true if the value was replaced
     */
    @Override
    public final boolean replace(K key, V oldValue, V newValue) {
        EqualsDecisionMaker<V> decisionMaker = new EqualsDecisionMaker<>(valueType, oldValue);
        V result = put(key, newValue, decisionMaker);
        boolean res = decisionMaker.getDecision() != Decision.ABORT;
        assert !res || areValuesEqual(oldValue, result) : oldValue + " != " + result;
        return res;
    }

    /**
     * Replace a value for an existing key.
     *
     * @param key the key (may not be null)
     * @param value the new value
     * @return the old value, if the value was replaced, or null
     */
    @Override
    public final V replace(K key, V value) {
        return put(key, value, DecisionMaker.IF_PRESENT);
    }

    /**
     * Compare two keys.
     *
     * @param a the first key
     * @param b the second key
     * @return -1 if the first key is smaller, 1 if bigger, 0 if equal
     */
    final int compare(Object a, Object b) {
        return keyType.compare(a, b);
    }

    /**
     * Get the key type.
     *
     * @return the key type
     */
    public final DataType getKeyType() {
        return keyType;
    }

    /**
     * Get the value type.
     *
     * @return the value type
     */
    public final DataType getValueType() {
        return valueType;
    }

    /**
     * Read a page.
     *
     * @param pos the position of the page
     * @return the page
     */
    final Page readPage(long pos) {
        return store.readPage(this, pos);
    }

    /**
     * Set the position of the root page.
     * @param rootPos the position, 0 for empty
     * @param version to set for this map
     *
     */
    final void setRootPos(long rootPos, long version) {
        Page root = readOrCreateRootPage(rootPos);
        setInitialRoot(root, version);
        setWriteVersion(store.getCurrentVersion());
    }

    private Page readOrCreateRootPage(long rootPos) {
        Page root = rootPos == 0 ? createEmptyLeaf() : readPage(rootPos);
        return root;
    }

    /**
     * Iterate over a number of keys.
     *
     * @param from the first key to return
     * @return the iterator
     */
    public final Iterator<K> keyIterator(K from) {
        return new Cursor<K, V>(getRootPage(), from);
    }

    /**
     * Re-write any pages that belong to one of the chunks in the given set.
     *
     * @param set the set of chunk ids
     * @return whether rewriting was successful
     */
    final boolean rewrite(Set<Integer> set) {
        rewrite(getRootPage(), set);
        return true;
    }

    private int rewrite(Page p, Set<Integer> set) {
        if (p.isLeaf()) {
            long pos = p.getPos();
            int chunkId = DataUtils.getPageChunkId(pos);
            if (!set.contains(chunkId)) {
                return 0;
            }
            if (p.getKeyCount() > 0) {
                @SuppressWarnings("unchecked")
                K key = (K) p.getKey(0);
                V value = get(key);
                if (value != null) {
                    if (isClosed()) {
                        return 0;
                    }
                    replace(key, value, value);
                }
            }
            return 1;
        }
        int writtenPageCount = 0;
        for (int i = 0; i < getChildPageCount(p); i++) {
            long childPos = p.getChildPagePos(i);
            if (childPos != 0 && DataUtils.getPageType(childPos) == DataUtils.PAGE_TYPE_LEAF) {
                // we would need to load the page, and it's a leaf:
                // only do that if it's within the set of chunks we are
                // interested in
                int chunkId = DataUtils.getPageChunkId(childPos);
                if (!set.contains(chunkId)) {
                    continue;
                }
            }
            writtenPageCount += rewrite(p.getChildPage(i), set);
        }
        if (writtenPageCount == 0) {
            long pos = p.getPos();
            int chunkId = DataUtils.getPageChunkId(pos);
            if (set.contains(chunkId)) {
                // an inner node page that is in one of the chunks,
                // but only points to chunks that are not in the set:
                // if no child was changed, we need to do that now
                // (this is not needed if anyway one of the children
                // was changed, as this would have updated this
                // page as well)
                Page p2 = p;
                while (!p2.isLeaf()) {
                    p2 = p2.getChildPage(0);
                }
                @SuppressWarnings("unchecked")
                K key = (K) p2.getKey(0);
                V value = get(key);
                if (value != null) {
                    if (isClosed()) {
                        return 0;
                    }
                    replace(key, value, value);
                }
                writtenPageCount++;
            }
        }
        return writtenPageCount;
    }

    /**
     * Get a cursor to iterate over a number of keys and values.
     *
     * @param from the first key to return
     * @return the cursor
     */
    public final Cursor<K, V> cursor(K from) {
        return new Cursor<>(getRootPage(), from);
    }

    @Override
    public final Set<Map.Entry<K, V>> entrySet() {
        final Page root = this.getRootPage();
        return new AbstractSet<Entry<K, V>>() {

            @Override
            public Iterator<Entry<K, V>> iterator() {
                final Cursor<K, V> cursor = new Cursor<>(root, null);
                return new Iterator<Entry<K, V>>() {

                    @Override
                    public boolean hasNext() {
                        return cursor.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        K k = cursor.next();
                        return new DataUtils.MapEntry<>(k, cursor.getValue());
                    }

                    @Override
                    public void remove() {
                        throw DataUtils.newUnsupportedOperationException(
                                "Removing is not supported");
                    }
                };

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

    @Override
    public Set<K> keySet() {
        final Page root = this.getRootPage();
        return new AbstractSet<K>() {

            @Override
            public Iterator<K> iterator() {
                return new Cursor<K, V>(root, null);
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
     * Get the map name.
     *
     * @return the name
     */
    public final String getName() {
        return store.getMapName(id);
    }

    public final MVStore getStore() {
        return store;
    }

    /**
     * Get the map id. Please note the map id may be different after compacting
     * a store.
     *
     * @return the map id
     */
    public final int getId() {
        return id;
    }

    /**
     * The current root page (may not be null).
     *
     * @return the root page
     */
    public final Page getRootPage() {
        return getRoot().root;
    }

    public final RootReference getRoot() {
        return root.get();
    }

    final void setRoot(Page rootPage) {
        int attempt = 0;
        while (setNewRoot(null, rootPage, KEEP_CURRENT, ++attempt, false) == null) {/**/}
    }

    final void setInitialRoot(Page rootPage, long version) {
        root.set(new RootReference(rootPage, version));
    }

    /**
     * Try to set the new root reference from now on.
     *
     * @param oldRoot previous root reference
     * @param newRootPage the new root page
     * @param newVersion corresponding new version
     * @param attemptUpdateCounter how many attempt (including current)
     *                            were made to update root
     * @param obeyLock false means override root iven if it marked as locked (used to unlock)
     *                 true will fail to update, if rott is currently locked
     * @return new RootReference or null if update failed
     */
    private RootReference setNewRoot(RootReference oldRoot, Page newRootPage, long newVersion,
                                    int attemptUpdateCounter, boolean obeyLock) {
        RootReference currentRoot = getRoot();
        assert newRootPage != null || currentRoot != null;
        if (currentRoot != oldRoot && oldRoot != null) {
            return null;
        }

        RootReference previous = currentRoot;
        long updateCounter = 1;
        if(currentRoot != null) {
            if (obeyLock && currentRoot.semaphore) {
                return null;
            }

            if (newRootPage == null) {
                newRootPage = currentRoot.root;
            }

            if (newVersion == KEEP_CURRENT) {
                newVersion = currentRoot.version;
                previous = currentRoot.previous;
            } else {

                RootReference tmp = previous;
                while ((tmp = tmp.previous) != null && tmp.root == newRootPage) {
                    previous = tmp;
                }
            }

            updateCounter += currentRoot.updateCounter;
            attemptUpdateCounter += currentRoot.updateAttemptCounter;
        }

        RootReference updatedRootReference = new RootReference(newRootPage, newVersion, previous, updateCounter,
                attemptUpdateCounter, false);
        boolean success = root.compareAndSet(currentRoot, updatedRootReference);
        return success ? updatedRootReference : null;
    }

    /**
     * Rollback to the given version.
     *
     * @param version the version
     */
    final void rollbackTo(long version) {
        // check if the map was removed and re-created later ?
        if (version > createVersion) {
            rollbackRoot(version);
        }
    }

    void rollbackRoot(long version)
    {
        RootReference rootReference = getRoot();
        RootReference previous;
        while (rootReference.version >= version && (previous = rootReference.previous) != null) {
            if (root.compareAndSet(rootReference, previous)) {
                rootReference = previous;
                closed = false;
            }
        }
        setWriteVersion(version);
    }

    /**
     * Use the new root page from now on.
     * @param oldRoot the old root reference, will use the current root reference, if null is specified
     * @param newRoot the new root page
     */
    protected final boolean updateRoot(RootReference oldRoot, Page newRoot, int attemptUpdateCounter) {
        return setNewRoot(oldRoot, newRoot, KEEP_CURRENT, attemptUpdateCounter, true) != null;
    }

    /**
     * Forget those old versions that are no longer needed.
     * @param rootReference to inspect
     */
    void removeUnusedOldVersions(RootReference rootReference) {
        long oldest = store.getOldestVersionToKeep();
        // We are trying to keep at least one previous version (if any) here.
        // This is not really necessary, just need to mimic existing
        // behaviour embeded in tests.
        boolean head = true;
        RootReference previous;
        while ((previous = rootReference.previous) != null) {
            if (previous.version < oldest && !head) {
                rootReference.previous = null;
                break;
            }
            rootReference = previous;
            head = false;
        }
    }

    public final boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Set the volatile flag of the map.
     *
     * @param isVolatile the volatile flag
     */
    public final void setVolatile(boolean isVolatile) {
        this.isVolatile = isVolatile;
    }

    /**
     * Whether this is volatile map, meaning that changes
     * are not persisted. By default (even if the store is not persisted),
     * maps are not volatile.
     *
     * @return whether this map is volatile
     */
    public final boolean isVolatile() {
        return isVolatile;
    }

    /**
     * This method is called before writing to the map. The default
     * implementation checks whether writing is allowed, and tries
     * to detect concurrent modification.
     *
     * @throws UnsupportedOperationException if the map is read-only,
     *      or if another thread is concurrently writing
     */
    protected final void beforeWrite() {
        if (closed) {
            int id = getId();
            String mapName = store.getMapName(id);
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_CLOSED, "Map {0}({1}) is closed", mapName, id, store.getPanicException());
        }
        if (readOnly) {
            throw DataUtils.newUnsupportedOperationException(
                    "This map is read-only");
        }
        store.beforeWrite(this);
    }

    @Override
    public final int hashCode() {
        return id;
    }

    @Override
    public final boolean equals(Object o) {
        return this == o;
    }

    /**
     * Get the number of entries, as a integer. Integer.MAX_VALUE is returned if
     * there are more than this entries.
     *
     * @return the number of entries, as an integer
     */
    @Override
    public final int size() {
        long size = sizeAsLong();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    /**
     * Get the number of entries, as a long.
     *
     * @return the number of entries
     */
    public final long sizeAsLong() {
        return getRootPage().getTotalCount();
    }

    @Override
    public boolean isEmpty() {
        Page rootPage = getRootPage();
        return rootPage.isLeaf() && rootPage.getKeyCount() == 0;
    }

    public final long getCreateVersion() {
        return createVersion;
    }

    /**
     * Remove the given page (make the space available).
     *
     * @param pos the position of the page to remove
     * @param memory the number of bytes used for this page
     */
    protected final void removePage(long pos, int memory) {
        store.removePage(this, pos, memory);
    }

    /**
     * Open an old version for the given map.
     *
     * @param version the version
     * @return the map
     */
    public final MVMap<K, V> openVersion(long version) {
        if (readOnly) {
            throw DataUtils.newUnsupportedOperationException(
                    "This map is read-only; need to call " +
                    "the method on the writable map");
        }
        DataUtils.checkArgument(version >= createVersion,
                "Unknown version {0}; this map was created in version is {1}",
                version, createVersion);
        RootReference rootReference = getRoot();
        removeUnusedOldVersions(rootReference);
        while (rootReference != null && rootReference.version > version) {
            rootReference = rootReference.previous;
        }

        if (rootReference == null) {
            // smaller than all in-memory versions
            MVMap<K, V> map = openReadOnly(store.getRootPos(getId(), version), version);
            return map;
        }
        MVMap<K, V> m = openReadOnly(rootReference.root, version);
        assert m.getVersion() <= version : m.getVersion() + " <= " + version;
        return m;
    }

    /**
     * Open a copy of the map in read-only mode.
     *
     * @param rootPos position of the root page
     * @param version to open
     * @return the opened map
     */
    final MVMap<K, V> openReadOnly(long rootPos, long version) {
        Page root = readOrCreateRootPage(rootPos);
        return openReadOnly(root, version);
    }

    private MVMap<K, V> openReadOnly(Page root, long version) {
        MVMap<K, V> m = cloneIt();
        m.readOnly = true;
        m.setInitialRoot(root, version);
        return m;
    }

    public final long getVersion() {
        RootReference rootReference = getRoot();
        RootReference previous = rootReference.previous;
        return previous == null || previous.root != rootReference.root ?
                    rootReference.version :
                    previous.version == INITIAL_VERSION ? store.getLastStoredVersion() : previous.version;
    }

    final boolean hasChangesSince(long version) {
        return getVersion() > version;
    }

    /**
     * Get the child page count for this page. This is to allow another map
     * implementation to override the default, in case the last child is not to
     * be used.
     *
     * @param p the page
     * @return the number of direct children
     */
    protected int getChildPageCount(Page p) {
        return p.getRawChildPageCount();
    }

    /**
     * Get the map type. When opening an existing map, the map type must match.
     *
     * @return the map type
     */
    public String getType() {
        return null;
    }

    /**
     * Get the map metadata as a string.
     *
     * @param name the map name (or null)
     * @return the string
     */
     protected String asString(String name) {
        StringBuilder buff = new StringBuilder();
        if (name != null) {
            DataUtils.appendMap(buff, "name", name);
        }
        if (createVersion != 0) {
            DataUtils.appendMap(buff, "createVersion", createVersion);
        }
        String type = getType();
        if (type != null) {
            DataUtils.appendMap(buff, "type", type);
        }
        return buff.toString();
    }

    final RootReference setWriteVersion(long writeVersion) {
        int attempt = 0;
        while(true) {
            RootReference rootReference = getRoot();
            if(rootReference.version >= writeVersion) {
                return rootReference;
            } else if (isClosed()) {
                if (rootReference.version < store.getOldestVersionToKeep()) {
                    return null;
                }
                return rootReference;
            }
            RootReference updatedRootReference = new RootReference(rootReference, writeVersion, ++attempt);
            if(root.compareAndSet(rootReference, updatedRootReference)) {
                removeUnusedOldVersions(updatedRootReference);
                return updatedRootReference;
            }
        }
    }

    public Page createEmptyLeaf() {
        return Page.createEmptyLeaf(this);
    }

    public Page createEmptyNode() {
        return Page.createEmptyNode(this);
    }

    /**
     * Copy a map. All pages are copied.
     *
     * @param sourceMap the source map
     */
    final void copyFrom(MVMap<K, V> sourceMap) {
        beforeWrite();
        setRoot(copy(sourceMap.getRootPage(), null));
    }

    private Page copy(Page source, CursorPos parent) {
        Page target = source.copy(this);
        store.registerUnsavedPage(target.getMemory());
        if (!source.isLeaf()) {
            CursorPos pos = new CursorPos(target, 0, parent);
            for (int i = 0; i < getChildPageCount(target); i++) {
                if (source.getChildPagePos(i) != 0) {
                    // position 0 means no child
                    // (for example the last entry of an r-tree node)
                    // (the MVMap is also used for r-trees for compacting)
                    pos.index = i;
                    Page child = copy(source.getChildPage(i), pos);
                    target.setChild(i, child);
                    pos.page = target;
                }
            }

            if(store.isSaveNeeded()) {
                Page child = target;
                for(CursorPos p = parent; p != null; p = p.parent) {
                    p.page.setChild(p.index, child);
                    child = p.page;
                }
                setRoot(child);
                beforeWrite();
            }
        }
        return target;
    }

    @Override
    public final String toString() {
        return asString(null);
    }

    public static final class RootReference
    {
        /**
         * The root page.
         */
        public  final    Page          root;
        /**
         * The version used for writing.
         */
        public  final    long          version;
        /**
         * Indicator that map is locked for update.
         */
        public  final    boolean       semaphore;
        /**
         * Reference to the previous root in the chain.
         */
        public  volatile RootReference previous;
        /**
         * Counter for successful root updates.
         */
        public  final    long          updateCounter;
        /**
         * Counter for attempted root updates.
         */
        public  final    long          updateAttemptCounter;

        private RootReference(Page root, long version, RootReference previous,
                              long updateCounter, long updateAttemptCounter,
                              boolean semaphore) {
            this.root = root;
            this.version = version;
            this.previous = previous;
            this.updateCounter = updateCounter;
            this.updateAttemptCounter = updateAttemptCounter;
            this.semaphore = semaphore;
        }

        // This one is used for locking
        private RootReference(RootReference r) {
            this.root = r.root;
            this.version = r.version;
            this.previous = r.previous;
            this.updateCounter = r.updateCounter;
            this.updateAttemptCounter = r.updateAttemptCounter;
            this.semaphore = true;
        }

        // This one is used for unlocking
        private RootReference(RootReference r, Page root, int attempt) {
            this.root = root;
            this.version = r.version;
            this.previous = r.previous;
            this.updateCounter = r.updateCounter + 1;
            this.updateAttemptCounter = r.updateAttemptCounter + attempt;
            this.semaphore = false;
        }

        // This one is used for version change
        private RootReference(RootReference r, long version, int attempt) {
            RootReference previous = r;
            RootReference tmp;
            while ((tmp = previous.previous) != null && tmp.root == r.root) {
                previous = tmp;
            }
            this.root = r.root;
            this.version = version;
            this.previous = previous;
            this.updateCounter = r.updateCounter + 1;
            this.updateAttemptCounter = r.updateAttemptCounter + attempt;
            this.semaphore = r.semaphore;
        }

        // This one is used for r/o snapshots
        private RootReference(Page root, long version) {
            this.root = root;
            this.version = version;
            this.previous = null;
            this.updateCounter = 1;
            this.updateAttemptCounter = 1;
            this.semaphore = false;
        }

        @Override
        public String toString() {
            return "RootReference("+ System.identityHashCode(root)+","+version+","+ semaphore +")";
        }
    }

    /**
     * A builder for maps.
     *
     * @param <M> the map type
     * @param <K> the key type
     * @param <V> the value type
     */
    public interface MapBuilder<M extends MVMap<K, V>, K, V> {

        /**
         * Create a new map of the given type.
         * @param store which will own this map
         * @param config configuration
         *
         * @return the map
         */
        M create(MVStore store, Map<String, Object> config);

        DataType getKeyType();

        DataType getValueType();

        void setKeyType(DataType dataType);

        void setValueType(DataType dataType);

    }

    /**
     * A builder for this class.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public abstract static class BasicBuilder<M extends MVMap<K, V>, K, V> implements MapBuilder<M, K, V> {

        private DataType keyType;
        private DataType valueType;

        /**
         * Create a new builder with the default key and value data types.
         */
        protected BasicBuilder() {
            // ignore
        }

        @Override
        public DataType getKeyType() {
            return keyType;
        }

        @Override
        public DataType getValueType() {
            return valueType;
        }

        @Override
        public void setKeyType(DataType keyType) {
            this.keyType = keyType;
        }

        @Override
        public void setValueType(DataType valueType) {
            this.valueType = valueType;
        }

        /**
         * Set the key data type.
         *
         * @param keyType the key type
         * @return this
         */
        public BasicBuilder<M, K, V> keyType(DataType keyType) {
            this.keyType = keyType;
            return this;
        }

        /**
         * Set the value data type.
         *
         * @param valueType the value type
         * @return this
         */
        public BasicBuilder<M, K, V> valueType(DataType valueType) {
            this.valueType = valueType;
            return this;
        }

        @Override
        public M create(MVStore store, Map<String, Object> config) {
            if (getKeyType() == null) {
                setKeyType(new ObjectDataType());
            }
            if (getValueType() == null) {
                setValueType(new ObjectDataType());
            }
            DataType keyType = getKeyType();
            DataType valueType = getValueType();
            config.put("store", store);
            config.put("key", keyType);
            config.put("val", valueType);
            return create(config);
        }

        protected abstract M create(Map<String, Object> config);

    }

    /**
     * A builder for this class.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static class Builder<K, V> extends BasicBuilder<MVMap<K, V>, K, V> {

        public Builder() {}

        public Builder<K,V> keyType(DataType dataType) {
            setKeyType(dataType);
            return this;
        }

        public Builder<K,V> valueType(DataType dataType) {
            setValueType(dataType);
            return this;
        }

        protected MVMap<K, V> create(Map<String, Object> config) {
            Object type = config.get("type");
            if(type == null || type.equals("rtree")) {
                return new MVMap<>(config);
            }
            throw new IllegalArgumentException("Incompatible map type");
        }
    }

    public interface EntryProcessor<K,V> {
        boolean process(K key, V value);
    }

    @SuppressWarnings("unchecked")
    public static <K,V> void process(Page root, K from, EntryProcessor<K,V> entryProcessor) {
        CursorPos cursorPos = Cursor.traverseDown(root, from);
        CursorPos keeper = null;
        while (true) {
            Page page = cursorPos.page;
            int index = cursorPos.index;
            if (index >= (page.isLeaf() ? page.getKeyCount() : root.map.getChildPageCount(page))) {
                CursorPos tmp = cursorPos;
                cursorPos = cursorPos.parent;
                tmp.parent = keeper;
                keeper = tmp;
                if(cursorPos == null) {
                    return;
                }
            } else {
                while (!page.isLeaf()) {
                    page = page.getChildPage(index);
                    if (keeper == null) {
                        cursorPos = new CursorPos(page, 0, cursorPos);
                    } else {
                        CursorPos tmp = keeper;
                        keeper = keeper.parent;
                        tmp.parent = cursorPos;
                        tmp.page = page;
                        tmp.index = 0;
                        cursorPos = tmp;
                    }
                    index = 0;
                }
                K key = (K) page.getKey(index);
                V value = (V) page.getValue(index);
                if(entryProcessor.process(key, value)) {
                    return;
                }
            }
            ++cursorPos.index;
        }

    }

    public interface LeafProcessor
    {
        CursorPos locate(Page rootPage);
        Page[] process(CursorPos pos);
        CursorPos locateNext(Page rootPage);
        void stepBack();
        void confirmSuccess();
    }

    public final void operateBatch(LeafProcessor processor) {
        beforeWrite();
        int attempt = 0;
        RootReference rootReference = getRoot();
        RootReference oldRootReference = null;
        CursorPos pos;
        while(true) {
            if (rootReference.semaphore) {
                Thread.yield();
                rootReference = getRoot();
                continue;
            }
            pos = processor.locate(rootReference.root);

            if (pos == null) {
                return;
            }
            Page replacement[] = processor.process(pos);
            if (replacement == null) {
                if(rootReference != getRoot()) {
                    processor.stepBack();
                    rootReference = getRoot();
                    continue;
                }
                return;
            }
            int contention = 0;
            if (oldRootReference != null) {
                long updateAttemptCounter = rootReference.updateAttemptCounter - oldRootReference.updateAttemptCounter;
                assert updateAttemptCounter >= 0 : updateAttemptCounter;
                long updateCounter = rootReference.updateCounter - oldRootReference.updateCounter;
                assert updateCounter >= 0 : updateCounter;
                assert updateAttemptCounter >= updateCounter : updateAttemptCounter + " >= " + updateCounter;
                contention = (int)((updateAttemptCounter+1) / (updateCounter+1));
            }
            oldRootReference = rootReference;
            ++attempt;
            CursorPos tip = pos;
            Page p = pos.page;
            pos = pos.parent;

            int unsavedMemory = 0;
            boolean needUnlock = false;
            try {
                if (attempt > 4 && !(needUnlock = lockRoot(processor, rootReference, attempt, contention))) {
                    processor.stepBack();
                    rootReference = getRoot();
                    continue;
                }
                int index;
                switch (replacement.length) {
                case 0:
                    if (pos != null) {
                        p = pos.page;
                        index = pos.index;
                        pos = pos.parent;
                        assert p.getKeyCount() > 0;
                        if (p.getKeyCount() == 1) {
                            assert index <= 1;
                            p = p.getChildPage(1 - index);
                            break;
                        }
                        p = p.copy();
                        p.remove(index);
                    } else {
                        p = createEmptyLeaf();
                    }
                    break;
                case 1:
                    p = replacement[0];
                    int keyCount;
                    while ((keyCount = p.getKeyCount()) > store.getKeysPerPage() || p.getMemory() > store.getMaxPageSize()
                            && keyCount > (p.isLeaf() ? 1 : 2)) {
                        long totalCount = p.getTotalCount();
                        int at = keyCount >> 1;
                        Object k = p.getKey(at);
                        Page split = p.split(at);
                        unsavedMemory += p.getMemory();
                        unsavedMemory += split.getMemory();
                        if (pos == null) {
                            Object keys[] = { k };
                            Page.PageReference children[] = {
                                    new Page.PageReference(p),
                                    new Page.PageReference(split)
                            };
                            p = Page.create(this, keys, null, children, totalCount, 0);
                            break;
                        }
                        Page c = p;
                        p = pos.page;
                        index = pos.index;
                        pos = pos.parent;
                        p = p.copy();
                        p.setChild(index, split);
                        p.insertNode(index, k, c);
                    }
                default:
                    break;
                }
                unsavedMemory += p.getMemory();
                while (pos != null) {
                    Page c = p;
                    p = pos.page;
                    p = p.copy();
                    p.setChild(pos.index, c);
                    unsavedMemory += p.getMemory();
                    pos = pos.parent;
                }
                if ((pos = processor.locateNext(p)) != null) {
                    continue; // Multi-node batch operation
                }
                if(needUnlock) {
                    unlockRoot(p, attempt);
                    needUnlock = false;
                } else if(!updateRoot(rootReference, p, attempt)) {
                    processor.stepBack();
                    rootReference = getRoot();
                    continue;
                }
                while (tip != null) {
                    tip.page.removePage();
                    tip = tip.parent;
                }
                if (store.getFileStore() != null) {
                    store.registerUnsavedPage(unsavedMemory);
                }
                processor.confirmSuccess();
                break;
            } finally {
                if(needUnlock) {
                    unlockRoot(rootReference.root, attempt);
                }
            }
        }
    }

    private boolean lockRoot(LeafProcessor processor, RootReference rootReference,
                             int attempt, int contention) {
        boolean success = root.compareAndSet(rootReference, new RootReference(rootReference));
        if (!success) {
            processor.stepBack();
            if(attempt > 8) {
                if (attempt <= 24) {
                    Thread.yield();
                } else {
                    try {
                        Thread.sleep(0, 100 / contention + 50);
                    } catch (InterruptedException ignore) {/**/}
                }
            }
        }
        return success;
    }

    private void unlockRoot(Page newRoot, int attempt) {
        boolean success;
        do {
            RootReference rootReference = getRoot();
            RootReference updatedRootReference = new RootReference(rootReference, newRoot, attempt);
            success = root.compareAndSet(rootReference, updatedRootReference);
        } while(!success);
    }

    public static CursorPos traverseDown(Page p, Object key) {
        CursorPos pos = null;
        while (!p.isLeaf()) {
            assert p.getKeyCount() > 0;
            int index = p.binarySearch(key) + 1;
            if (index < 0) {
                index = -index;
            }
            pos = new CursorPos(p, index, pos);
            p = p.getChildPage(index);
        }
        return new CursorPos(p, p.binarySearch(key), pos);
    }

    public static final class SingleDecisionMaker<K,V> implements LeafProcessor
    {
        private final DecisionMaker<? super V> decisionMaker;
        private final K key;
        private final V value;
        private       V result;

        public SingleDecisionMaker(K key, V value, DecisionMaker<? super V> decisionMaker) {
            this.decisionMaker = decisionMaker;
            this.key = key;
            this.value = value;
        }

        @Override
        public CursorPos locate(Page rootPage) {
            return traverseDown(rootPage, key);
        }

        @Override
        public CursorPos locateNext(Page rootPage) {
            return null;
        }

        @Override
        public void stepBack() {
            decisionMaker.reset();
        }

        @Override
        public void confirmSuccess() {}

        @SuppressWarnings("unchecked")
        @Override
        public Page[] process(CursorPos pos) {
            Page leaf = pos.page;
            int index = pos.index;
            result = index < 0 ? null : (V)leaf.getValue(index);
            Decision decision = decisionMaker.decide(result, value);
            switch (decision) {
                case ABORT:
                    return null;
                case REMOVE: {
                    if (index < 0) {
                        return null;
                    }
                    if (leaf.getTotalCount() == 1) {
                        return new Page[0];
                    }
                    leaf = leaf.copy();
                    leaf.remove(index);
                    return new Page[] { leaf };
                }
                case PUT: {
                    V v = decisionMaker.selectValue(result, value);
                    leaf = leaf.copy();
                    if (index < 0) {
                        leaf.insertLeaf(-index - 1, key, v);
                    } else {
                        leaf.setValue(index, v);
                    }
                    return new Page[] { leaf };
                }
                default:
                return null;
            }
        }

        public V getResult() {
            return result;
        }
    }

    public enum Decision { ABORT, REMOVE, PUT }

    public abstract static class DecisionMaker<V> {
        public static final DecisionMaker<Object> DEFAULT = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return providedValue == null ? Decision.REMOVE : Decision.PUT;
            }

            @Override
            public String toString() {
                return "default";
            }
        };

        public static final DecisionMaker<Object> PUT = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return Decision.PUT;
            }

            @Override
            public String toString() {
                return "put";
            }
        };

        public static final DecisionMaker<Object> REMOVE = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return Decision.REMOVE;
            }

            @Override
            public String toString() {
                return "remove";
            }
        };

        private static final DecisionMaker<Object> IF_ABSENT = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return existingValue == null ? Decision.PUT : Decision.ABORT;
            }

            @Override
            public String toString() {
                return "if_absent";
            }
        };

        private static final DecisionMaker<Object> IF_PRESENT = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return existingValue != null ? Decision.PUT : Decision.ABORT;
            }

            @Override
            public String toString() {
                return "if_present";
            }
        };

        public abstract Decision decide(V existingValue, V providedValue);
        public <T extends V> T selectValue(T existingValue, T providedValue) {
            return providedValue;
        }
        public void reset() {}
    }

    public V operate(K key, V value, DecisionMaker<? super V> decisionMaker) {
        SingleDecisionMaker<K, V> processor = new SingleDecisionMaker<>(key, value, decisionMaker);
        operateBatch(processor);
        return processor.getResult();
    }

    private static final class EqualsDecisionMaker<V> extends DecisionMaker<V> {
        private final DataType dataType;
        private final V        expectedValue;
        private       Decision decision;

        private EqualsDecisionMaker(DataType dataType, V expectedValue) {
            this.dataType = dataType;
            this.expectedValue = expectedValue;
        }

        @Override
        public Decision decide(V existingValue, V providedValue) {
            assert decision == null;
            decision = !areValuesEqual(dataType, expectedValue, existingValue) ? Decision.ABORT :
                                            providedValue == null ? Decision.REMOVE : Decision.PUT;
            return decision;
        }

        @Override
        public void reset() {
            decision = null;
        }

        public Decision getDecision() {
            return decision;
        }

        @Override
        public String toString() {
            return "equals_to "+expectedValue;
        }
    }

}
