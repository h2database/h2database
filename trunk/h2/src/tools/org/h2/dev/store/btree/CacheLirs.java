/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.util.ArrayList;
import java.util.List;

/**
 * A cache.
 * <p>
 * This implementation is not multi-threading save.
 *
 * It is important to use a good hash function for the key (there is no guard against bad hash functions).
 * <p>
 * An implementation of the LIRS replacement algorithm from Xiaodong Zhang and
 * Song Jiang as described in
 * http://www.cse.ohio-state.edu/~zhang/lirs-sigmetrics-02.html with a few
 * smaller changes: An additional queue for non-resident entries is used, to
 * prevent unbound memory usage. The maximum size of this queue is at most the
 * size of the rest of the stack. This implementation allows each entry to have
 * a distinct memory size. At most 6.25% of the mapped entries are cold.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class CacheLirs<K, V> {

    private long maxMemory;
    private long currentMemory;
    private int averageMemory;
    private int mapSize, stackSize, queueSize, queue2Size;
    private Entry<K, V>[] entries;
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
    private CacheLirs(long maxMemory, int averageMemory) {
        this.maxMemory = maxMemory;
        this.averageMemory = averageMemory;
        clear();
    }

    /**
     * Create a new cache.
     *
     * @param size the maximum number of elements
     */
    public static <K, V> CacheLirs<K, V> newInstance(int size) {
        return new CacheLirs<K, V>(size, 1);
    }

    /**
     * Clear the cache.
     */
    public void clear() {
        long maxLen = (long) (maxMemory / averageMemory / 0.75);
        long l = 8;
        while (l < maxLen) {
            l += l;
        }
        int len = (int) Math.min(1L << 31, l);
        mask = len - 1;
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
        currentMemory = 0;
        stackSize = queueSize = queue2Size = 0;
    }

    /**
     * Get an entry if the entry is cached. This method does not modify the
     * internal state.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    public V peek(K key) {
        Entry<K, V> e = find(key);
        return e == null ? null : e.value;
    }

    /**
     * Get an entry if the entry is cached. This method adjusts the internal
     * state of the cache, to ensure commonly used entries stay in the cache.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    public V get(K key) {
        Entry<K, V> e = find(key);
        if (e == null || e.value == null) {
            return null;
        } else if (e.isHot()) {
            if (e != stack.stackNext) {
                boolean wasEnd = e == stack.stackPrev;
                removeFromStack(e);
                if (wasEnd) {
                    pruneStack();
                }
                addToStack(e);
            }
        } else {
            removeFromQueue(e);
            if (e.stackNext != null) {
                removeFromStack(e);
                convertOldestHotToCold();
            } else {
                addToQueue(queue, e);
            }
            addToStack(e);
        }
        return e.value;
    }

    /**
     * Add an entry to the cache. This method is the same as adding an entry
     * with the average memory size.
     *
     * @param key the key
     * @param value the value
     */
    public void put(K key, V value) {
        put(key, value, averageMemory);
    }

    /**
     * Add an entry to the cache. The entry may or may not exist in the cache
     * yet. This method will usually mark unknown entries as cold and known
     * entries as hot.
     *
     * @param key the key
     * @param value the value
     * @param memory the memory used for the given entry
     */
    public void put(K key, V value, int memory) {
        if (find(key) != null) {
            remove(key);
        }
        Entry<K, V> e = new Entry<K, V>();
        e.key = key;
        e.value = value;
        e.memory = memory;
        int index = key.hashCode() & mask;
        e.chained = entries[index];
        entries[index] = e;
        currentMemory += memory;
        if (currentMemory > maxMemory && mapSize > 0) {
            evict(e);
        }
        mapSize++;
        addToStack(e);
    }

    /**
     * Remove an entry.
     *
     * @param key the key
     * @return true if the entry was found (resident or non-resident)
     */
    public boolean remove(K key) {
        int hash = key.hashCode();
        int index = hash & mask;
        Entry<K, V> e = entries[index];
        if (e == null) {
            return false;
        }
        if (e.key.equals(key)) {
            entries[index] = e.chained;
        } else {
            Entry<K, V> last;
            do {
                last = e;
                e = e.chained;
                if (e == null) {
                    return false;
                }
            } while (!e.key.equals(key));
            last.chained = e.chained;
        }
        mapSize--;
        currentMemory -= e.memory;
        if (e.stackNext != null) {
            removeFromStack(e);
        }
        if (e.isHot()) {
            // when removing a hot entry, convert the newest cold entry to hot,
            // so that we keep the number of hot entries
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
        return true;
    }

    private void evict(Entry<K, V> newCold) {
        while ((queueSize << 5) < mapSize) {
            convertOldestHotToCold();
        }
        addToQueue(queue, newCold);
        while (currentMemory > maxMemory) {
            Entry<K, V> e = queue.queuePrev;
            currentMemory -= e.memory;
            removeFromQueue(e);
            e.value = null;
            e.memory = 0;
            addToQueue(queue2, e);
            while (queue2Size + queue2Size > stackSize) {
                e = queue2.queuePrev;
                remove(e.key);
            }
        }
    }

    private void convertOldestHotToCold() {
        Entry<K, V> last = stack.stackPrev;
        removeFromStack(last);
        addToQueue(queue, last);
        pruneStack();
    }

    private void pruneStack() {
        while (true) {
            Entry<K, V> last = stack.stackPrev;
            if (last == stack || last.isHot()) {
                break;
            }
            removeFromStack(last);
        }
    }

    private Entry<K, V> find(K key) {
        int hash = key.hashCode();
        Entry<K, V> e = entries[hash & mask];
        while (e != null && !e.key.equals(key)) {
            e = e.chained;
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
     * Get the number of mapped entries (resident and non-resident).
     *
     * @return the number of entries
     */
    public int getSize() {
        return mapSize;
    }

    /**
     * Get the number of hot entries.
     *
     * @return the number of entries
     */
    public int getHotSize() {
        return mapSize - queueSize - queue2Size;
    }

    /**
     * Get the number of non-resident entries.
     *
     * @return the number of entries
     */
    public int getNonResidentSize() {
        return queue2Size;
    }

    /**
     * Get the list of keys for this map. This method allows to view the internal
     * state of the cache.
     *
     * @param cold if true only the keys for the cold entries are returned
     * @param nonResident true for non-resident entries
     * @return the key set
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
     * Get the currently used memory.
     *
     * @return the used memory
     */
    public long getUsedMemory() {
        return currentMemory;
    }

    /**
     * A cache entry. Each entry is either hot (low inter-reference recency;
     * lir), cold (high inter-reference recency; hir), or non-resident-cold. Hot
     * entries are in the stack only. Cold entries are in the queue, and may be
     * in the stack. Non-resident-cold entries have their value set to null and
     * are in the stack and in the non-resident queue.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    static class Entry<K, V> {
        K key;
        V value;
        int memory;
        Entry<K, V> stackPrev, stackNext;
        Entry<K, V> queuePrev, queueNext;
        Entry<K, V> chained;

        boolean isHot() {
            return queueNext == null;
        }

    }

}
