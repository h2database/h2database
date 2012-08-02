/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A scan resistent cache. It is meant to cache objects that are relatively
 * costly to acquire, for example file content.
 * <p>
 * This implementation is not multi-threading save. Null keys or null values are
 * not allowed. There is no guard against bad hash functions, so it is important
 * to the hash function of the key is good. The map fill factor is at most 75%.
 * <p>
 * Each entry is assigned a distinct memory size, and the cache will try to use
 * at most the specified amount of memory. The memory unit is not relevant,
 * however it is suggested to use bytes as the unit.
 * <p>
 * This class implements the LIRS replacement algorithm invented by Xiaodong
 * Zhang and Song Jiang as described in
 * http://www.cse.ohio-state.edu/~zhang/lirs-sigmetrics-02.html with a few
 * smaller changes: An additional queue for non-resident entries is used, to
 * prevent unbound memory usage. The maximum size of this queue is at most the
 * size of the rest of the stack. About 6.25% of the mapped entries are cold.
 *
 * @author Thomas Mueller
 * @param <K> the key type
 * @param <V> the value type
 */
public class LIRSCache<K, V> implements Map<K, V> {

    /**
     * The maximum memory this cache should use.
     */
    private long maxMemory;

    /**
     * The average memory used by one entry.
     */
    private int averageMemory;

    /**
     * The currently used memory.
     */
    private long usedMemory;

    /**
     * The number of (hot, cold, and non-resident) entries in the map.
     */
    private int mapSize;

    /**
     * The LIRS stack size.
     */
    private int stackSize;

    /**
     * The size of the LIRS queue for resident cold entries.
     */
    private int queueSize;

    /**
     * The size of the LIRS queue for non-resident cold entries.
     */
    private int queue2Size;

    /**
     * The map array. The size is always a power of 2.
     */
    private Entry<K, V>[] entries;

    /**
     * The bit mask that is applied to the key hash code to get the index in the
     * map array. The mask is the length of the array minus one.
     */
    private int mask;

    /**
     * The stack of recently referenced elements. This includes all hot entries,
     * the recently referenced cold entries, and all non-resident cold entries.
     */
    private Entry<K, V> stack;

    /**
     * The queue of resident cold entries.
     */
    private Entry<K, V> queue;

    /**
     * The queue of non-resident cold entries.
     */
    private Entry<K, V> queue2;

    /**
     * Create a new cache.
     *
     * @param maxMemory the maximum memory to use
     * @param averageMemory the average memory usage of an object
     */
    private LIRSCache(long maxMemory, int averageMemory) {
        setMaxMemory(maxMemory);
        setAverageMemory(averageMemory);
        clear();
    }

    /**
     * Create a new cache with the given memory size. To just limit the number
     * of entries, use the required number as the maximum memory, and an average
     * size of 1.
     *
     * @param maxMemory the maximum memory to use (1 or larger)
     * @param averageMemory the average memory (1 or larger)
     * @return the cache
     */
    public static <K, V> LIRSCache<K, V> newInstance(int maxMemory, int averageMemory) {
        return new LIRSCache<K, V>(maxMemory, averageMemory);
    }

    /**
     * Clear the cache. This method will clear all entries (including
     * non-resident keys) and resize the internal array.
     **/
    public void clear() {

        // calculate the size of the map array
        // assume a fill factor of at most 80%
        long maxLen = (long) (maxMemory / averageMemory / 0.75);
        // the size needs to be a power of 2
        long l = 8;
        while (l < maxLen) {
            l += l;
        }
        // the array size is at most 2^31 elements
        int len = (int) Math.min(1L << 31, l);
        // the bit mask has all bits set
        mask = len - 1;

        // initialize the stack and queue heads
        stack = new Entry<K, V>();
        stack.stackPrev = stack.stackNext = stack;
        queue = new Entry<K, V>();
        queue.queuePrev = queue.queueNext = queue;
        queue2 = new Entry<K, V>();
        queue2.queuePrev = queue2.queueNext = queue2;

        // first set to null - avoiding out of memory
        entries = null;
        @SuppressWarnings("unchecked")
        Entry<K, V>[] e = new Entry[len];
        entries = e;

        mapSize = 0;
        usedMemory = 0;
        stackSize = queueSize = queue2Size = 0;
    }

    /**
     * Get the value for the given key if the entry is cached. This method does
     * not modify the internal state.
     *
     * @param key the key (may not be null)
     * @return the value, or null if there is no resident entry
     */
    public V peek(K key) {
        Entry<K, V> e = find(key);
        return e == null ? null : e.value;
    }

    /**
     * Get the memory used for the given key.
     *
     * @param key the key (may not be null)
     * @return the memory, or 0 if there is no resident entry
     */
    public int getMemory(K key) {
        Entry<K, V> e = find(key);
        return e == null ? 0 : e.memory;
    }

    /**
     * Get the value for the given key if the entry is cached. This method
     * adjusts the internal state of the cache, to ensure commonly used entries
     * stay in the cache.
     *
     * @param key the key (may not be null)
     * @return the value, or null if there is no resident entry
     */
    public V get(Object key) {
        Entry<K, V> e = find(key);
        if (e == null || e.value == null) {
            // either the entry was not found, or it was a non-resident entry
            return null;
        } else if (e.isHot()) {
            if (e != stack.stackNext) {
                // move a hot entries to the top of the stack
                // unless it is already there
                boolean wasEnd = e == stack.stackPrev;
                removeFromStack(e);
                if (wasEnd) {
                    // if moving the last entry, the last entry
                    // could not be cold, which is not allowed
                    pruneStack();
                }
                addToStack(e);
            }
        } else {
            removeFromQueue(e);
            if (e.stackNext != null) {
                // resident cold entries become hot
                // if they are on the stack
                removeFromStack(e);
                // which means a hot entry needs to become cold
                convertOldestHotToCold();
            } else {
                // cold entries that are not on the stack
                // move to the front of the queue
                addToQueue(queue, e);
            }
            // in any case, the cold entry is moved to the top of the stack
            addToStack(e);
        }
        return e.value;
    }

    /**
     * Add an entry to the cache using the average memory size.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     */
    public V put(K key, V value) {
        return put(key, value, averageMemory);
    }

    /**
     * Add an entry to the cache. The entry may or may not exist in the cache
     * yet. This method will usually mark unknown entries as cold and known
     * entries as hot.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @param memory the memory used for the given entry
     */
    public V put(K key, V value, int memory) {
        if (value == null) {
            throw new NullPointerException();
        }
        V old;
        Entry<K, V> e = find(key);
        if (e == null) {
            old = null;
        } else {
            old = e.value;
            remove(key);
        }
        e = new Entry<K, V>();
        e.key = key;
        e.value = value;
        e.memory = memory;
        int index = key.hashCode() & mask;
        e.mapNext = entries[index];
        entries[index] = e;
        usedMemory += memory;
        if (usedMemory > maxMemory && mapSize > 0) {
            // an old entry needs to be removed
            evict(e);
        }
        mapSize++;
        // added entries are always added to the stack
        addToStack(e);
        return old;
    }

    /**
     * Remove an entry. Both resident and non-resident entries can be removed.
     *
     * @param key the key (may not be null)
     * @return the old value, or null if there is no resident entry
     */
    public V remove(Object key) {
        int hash = key.hashCode();
        int index = hash & mask;
        Entry<K, V> e = entries[index];
        if (e == null) {
            return null;
        }
        V old;
        if (e.key.equals(key)) {
            old = e.value;
            entries[index] = e.mapNext;
        } else {
            Entry<K, V> last;
            do {
                last = e;
                e = e.mapNext;
                if (e == null) {
                    return null;
                }
            } while (!e.key.equals(key));
            old = e.value;
            last.mapNext = e.mapNext;
        }
        mapSize--;
        usedMemory -= e.memory;
        if (e.stackNext != null) {
            removeFromStack(e);
        }
        if (e.isHot()) {
            // when removing a hot entry, the newest cold entry gets hot,
            // so the number of hot entries does not change
            e = queue.queueNext;
            if (e != queue) {
                removeFromQueue(e);
                if (e.stackNext == null) {
                    addToStackBottom(e);
                }
            }
        } else {
            removeFromQueue(e);
        }
        pruneStack();
        return old;
    }

    /**
     * Evict cold entries (resident and non-resident) until the memory limit is
     * reached.
     *
     * @param newCold a new cold entry
     */
    private void evict(Entry<K, V> newCold) {
        // ensure there are not too many hot entries:
        // left shift of 5 is multiplication by 32, that means if there are less
        // than 1/32 (3.125%) cold entries, a new hot entry needs to become cold
        while ((queueSize << 5) < mapSize) {
            convertOldestHotToCold();
        }
        // the new cold entry is at the top of the queue
        addToQueue(queue, newCold);
        // the oldest resident cold entries become non-resident
        while (usedMemory > maxMemory) {
            Entry<K, V> e = queue.queuePrev;
            usedMemory -= e.memory;
            removeFromQueue(e);
            e.value = null;
            e.memory = 0;
            addToQueue(queue2, e);
            // the size of the non-resident-cold entries needs to be limited
            while (queue2Size + queue2Size > stackSize) {
                e = queue2.queuePrev;
                remove(e.key);
            }
        }
    }

    private void convertOldestHotToCold() {
        // the last entry of the stack is known to be hot
        Entry<K, V> last = stack.stackPrev;
        // remove from stack - which is done anyway in the stack pruning, but we
        // can do it here as well
        removeFromStack(last);
        // adding an entry to the queue will make it cold
        addToQueue(queue, last);
        pruneStack();
    }

    /**
     * Ensure the last entry of the stack is cold.
     */
    private void pruneStack() {
        while (true) {
            Entry<K, V> last = stack.stackPrev;
            if (last == stack || last.isHot()) {
                break;
            }
            // the cold entry is still in the queue
            removeFromStack(last);
        }
    }

    /**
     * Try to find an entry in the map.
     *
     * @param key the key
     * @return the entry (might be a non-resident)
     */
    private Entry<K, V> find(Object key) {
        int hash = key.hashCode();
        Entry<K, V> e = entries[hash & mask];
        while (e != null && !e.key.equals(key)) {
            e = e.mapNext;
        }
        return e;
    }

    private void addToStack(Entry<K, V> e) {
        e.stackPrev = stack;
        e.stackNext = stack.stackNext;
        e.stackNext.stackPrev = e;
        stack.stackNext = e;
        stackSize++;
    }

    private void addToStackBottom(Entry<K, V> e) {
        e.stackNext = stack;
        e.stackPrev = stack.stackPrev;
        e.stackPrev.stackNext = e;
        stack.stackPrev = e;
        stackSize++;
    }

    private void removeFromStack(Entry<K, V> e) {
        e.stackPrev.stackNext = e.stackNext;
        e.stackNext.stackPrev = e.stackPrev;
        e.stackPrev = e.stackNext = null;
        stackSize--;
    }

    private void addToQueue(Entry<K, V> q, Entry<K, V> e) {
        e.queuePrev = q;
        e.queueNext = q.queueNext;
        e.queueNext.queuePrev = e;
        q.queueNext = e;
        if (e.value != null) {
            queueSize++;
        } else {
            queue2Size++;
        }
    }

    private void removeFromQueue(Entry<K, V> e) {
        e.queuePrev.queueNext = e.queueNext;
        e.queueNext.queuePrev = e.queuePrev;
        e.queuePrev = e.queueNext = null;
        if (e.value != null) {
            queueSize--;
        } else {
            queue2Size--;
        }
    }

    /**
     * Get the list of keys. This method allows to view the internal state of
     * the cache.
     *
     * @param cold if true, only keys for the cold entries are returned
     * @param nonResident true for non-resident entries
     * @return the key list
     */
    public List<K> keys(boolean cold, boolean nonResident) {
        ArrayList<K> s = new ArrayList<K>();
        if (cold) {
            Entry<K, V> start = nonResident ? queue2 : queue;
            for (Entry<K, V> e = start.queueNext; e != start; e = e.queueNext) {
                s.add(e.key);
            }
        } else {
            for (Entry<K, V> e = stack.stackNext; e != stack; e = e.stackNext) {
                s.add(e.key);
            }
        }
        return s;
    }

    /**
     * Get the number of resident entries.
     *
     * @return the number of entries
     */
    public int size() {
        return mapSize - queue2Size;
    }

    /**
     * Check whether there are any resident entries in the map.
     *
     * @return true if there are no keys
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Check whether there is a resident entry for the given key.
     *
     * @return true if there is a resident entry
     */
    public boolean containsKey(Object key) {
        Entry<K, V> e = find(key);
        return e != null && e.value != null;
    }

    /**
     * Check whether there are any keys for the given value.
     *
     * @return true if there is a key for this value
     */
    public boolean containsValue(Object value) {
        return values().contains(value);
    }

    /**
     * Add all entries of the given map to this map. This method will use the
     * average memory size.
     *
     * @param m the source map
     */
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    /**
     * Get the set of keys for resident entries.
     *
     * @return the set of keys
     */
    public Set<K> keySet() {
        HashSet<K> set = new HashSet<K>();
        for (Entry<K, V> e = stack.stackNext; e != stack; e = e.stackNext) {
            set.add(e.key);
        }
        for (Entry<K, V> e = queue.queueNext; e != queue; e = e.queueNext) {
            set.add(e.key);
        }
        return set;
    }

    /**
     * Get the collection of values.
     *
     * @return the collection of values
     */
    public Collection<V> values() {
        ArrayList<V> list = new ArrayList<V>();
        for (K k : keySet()) {
            list.add(get(k));
        }
        return list;
    }

    /**
     * Get the entry set for all resident entries.
     *
     * @return the entry set
     */
    public Set<Map.Entry<K, V>> entrySet() {
        HashMap<K, V> map = new HashMap<K, V>();
        for (K k : keySet()) {
            map.put(k,  find(k).value);
        }
        return map.entrySet();
    }

    /**
     * Get the number of hot entries in the cache.
     *
     * @return the number of hot entries
     */
    public int sizeHot() {
        return mapSize - queueSize - queue2Size;
    }

    /**
     * Get the number of non-resident entries in the cache.
     *
     * @return the number of non-resident entries
     */
    public int sizeNonResident() {
        return queue2Size;
    }

    /**
     * Get the length of the internal map array.
     *
     * @return the size of the array
     */
    public int sizeMapArray() {
        return entries.length;
    }

    /**
     * Get the currently used memory.
     *
     * @return the used memory
     */
    public long getUsedMemory() {
        return usedMemory;
    }

    /**
     * Set the maximum memory this cache should use. This will not immediately
     * cause entries to get removed however; it will only change the limit. To
     * resize the internal array, call the clear method.
     *
     * @param maxMemory the maximum size (1 or larger)
     */
    public void setMaxMemory(long maxMemory) {
        if (maxMemory <= 0) {
            throw new IllegalArgumentException("Max memory must be larger than 0");
        }
        this.maxMemory = maxMemory;
    }

    /**
     * Get the maximum memory to use.
     *
     * @return the maximum memory
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * Set the average memory used per entry. It is used to calculate the length
     * of the internal array.
     *
     * @param averageMemory the average memory used (1 or larger)
     */
    public void setAverageMemory(int averageMemory) {
        if (averageMemory <= 0) {
            throw new IllegalArgumentException("Average memory must be larger than 0");
        }
        this.averageMemory = averageMemory;
    }

    /**
     * Get the average memory used per entry.
     *
     * @return the average memory
     */
    public int getAverageMemory() {
        return averageMemory;
    }

    /**
     * A cache entry. Each entry is either hot (low inter-reference recency;
     * LIR), cold (high inter-reference recency; HIR), or non-resident-cold. Hot
     * entries are in the stack only. Cold entries are in the queue, and may be
     * in the stack. Non-resident-cold entries have their value set to null and
     * are in the stack and in the non-resident queue.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    static class Entry<K, V> {

        /**
         * The key.
         */
        K key;

        /**
         * The value. Set to null for non-resident-cold entries.
         */
        V value;

        /**
         * The estimated memory used.
         */
        int memory;

        /**
         * The next entry in the stack.
         */
        Entry<K, V> stackNext;

        /**
         * The previous entry in the stack.
         */
        Entry<K, V> stackPrev;

        /**
         * The next entry in the queue (either the resident queue or the
         * non-resident queue).
         */
        Entry<K, V> queueNext;

        /**
         * The previous entry in the queue.
         */
        Entry<K, V> queuePrev;

        /**
         * The next entry in the map
         */
        Entry<K, V> mapNext;

        /**
         * Whether this entry is hot. Cold entries are in one of the two queues.
         *
         * @return whether the entry is hot
         */
        boolean isHot() {
            return queueNext == null;
        }

    }

}
