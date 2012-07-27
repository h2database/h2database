/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import org.h2.util.MathUtils;

/**
 * An implementation of the LIRS replacement algorithm from
 * Xiaodong Zhang and Song Jiang as described in
 * http://www.cse.ohio-state.edu/~zhang/lirs-sigmetrics-02.html
 * This algorithm is scan resistant.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class CacheLirs<K, V> {

    private long maxMemory = 100;
    private long maxMemoryHot;
    private long currentMemory;
    private long currentMemoryHot;
    private int averageMemory = 1;
    private Entry<K, V>[] entries;
    private int mask;
    private Entry<K, V> head;

    /**
     * Create a new cache.
     *
     * @param maxMemory the maximum memory to use
     * @param averageMemory the average memory usage of an object
     */
    public CacheLirs(long maxMemory, int averageMemory) {
        this.maxMemory = maxMemory;
        this.averageMemory = averageMemory;
        clear();
    }

    /**
     * Clear the cache.
     */
    public void clear() {
        int len = MathUtils.convertLongToInt(maxMemory / averageMemory);
        len = MathUtils.nextPowerOf2(len);
        maxMemoryHot = maxMemory * 98 / 100;
        maxMemoryHot = Math.min(maxMemoryHot, maxMemory - averageMemory);
        mask = len - 1;
        head = new Entry<K, V>();
        head.stackPrev = head.stackNext = head;
        head.queuePrev = head.queueNext = head;
        // first set to null - avoiding out of memory
        entries = null;
        @SuppressWarnings("unchecked")
        Entry<K, V>[] e = new Entry[len];
        entries = e;
        currentMemory = currentMemoryHot = 0;
    }

    /**
     * Get an entry if the entry is cached.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    public V get(K key) {
        Entry<K, V> e = find(key);
        if (e == null) {
            return null;
        } else if (e.hot) {
            if (e == head.stackNext) {
                // already the first element
            } else {
                boolean wasLast = e == head.stackPrev;
                e.removeFromStack();
                if (wasLast) {
                    pruneStack();
                }
                addToStack(e);
            }
        } else {
            if (e.stackPrev != null) {
                e.removeFromStack();
                e.hot = true;
                currentMemoryHot += e.memory;
                e.removeFromQueue();
                Entry<K, V> last = head.stackPrev;
                last.removeFromStack();
                last.hot = false;
                currentMemoryHot -= last.memory;
                addToQueue(last);
                pruneStack();
            } else {
                e.removeFromQueue();
                addToQueue(e);
            }
            addToStack(e);
        }
        return e.value;
    }

    /**
     * Add an entry to the cache. This will assume a memory usage of 1.
     *
     * @param key the key
     * @param value the value
     */
    void put(K key, V value) {
        put(key, value, 1);
    }

    /**
     * Add an entry to the cache.
     *
     * @param key the key
     * @param value the value
     * @param memory the memory used for the given entry
     */
    void put(K key, V value, int memory) {
        Entry<K, V> e = find(key);
        if (e != null) {
            if (currentMemory + memory > maxMemoryHot) {
                if (head != head.queueNext) {
                    remove(head.queueNext.key);
                }
            }
            remove(key);
        }
        e = new Entry<K, V>();
        e.key = key;
        int hash = key.hashCode();
        // all pages are hot until the memory limit is reached
        e.hot = currentMemory + memory <= maxMemoryHot;
        e.hashCode = hash;
        e.value = value;
        e.memory = memory;
        int index = hash & mask;
        e.chained = entries[index];
        entries[index] = e;
        currentMemory += memory;
        if (e.hot) {
            currentMemoryHot += memory;
        } else {
            if (currentMemory > maxMemory) {
                removeOld();
            }
            addToQueue(e);
        }
        addToStack(e);
    }

    /**
     * Remove an entry.
     *
     * @param key the key
     * @return true if the entry was found
     */
    public boolean remove(K key) {
        int hash = key.hashCode();
        int index = hash & mask;
        Entry<K, V> e = entries[index];
        if (e == null) {
            return false;
        }
        if (e.hashCode == hash && e.key.equals(key)) {
            entries[index] = e.chained;
        } else {
            Entry<K, V> last;
            do {
                last = e;
                e = e.chained;
                if (e == null) {
                    return false;
                }
            } while (e.hashCode != hash || !e.key.equals(key));
            last.chained = e.chained;
        }
        currentMemory -= e.memory;
        if (e.stackNext != null) {
            e.removeFromStack();
        }
        if (e.queueNext != null) {
            e.removeFromQueue();
        }
        if (e.hot) {
            e = head.queueNext;
            if (e != head) {
                e.removeFromQueue();
                e.hot = true;
                if (e.stackNext == null) {
                    // add to bottom of the stack
                    e.stackNext = head;
                    e.stackPrev = head.stackPrev;
                    e.stackPrev.stackNext = e;
                    head.stackPrev = e;
                }
            }
        }
        return true;
    }

    private void pruneStack() {
        while (true) {
            Entry<K, V> last = head.stackPrev;
            if (last == head || last.hot) {
                break;
            }
            last.removeFromStack();
        }
    }

    private void removeOld() {
        while (currentMemory > maxMemory) {
            remove(head.queuePrev.key);
        }
    }

    private Entry<K, V> find(K key) {
        int hash = key.hashCode();
        Entry<K, V> e = entries[hash & mask];
        while (e != null && (e.hashCode != hash || !e.key.equals(key))) {
            e = e.chained;
        }
        return e;
    }

    private void addToQueue(Entry<K, V> e) {
        e.queuePrev = head;
        e.queueNext = head.queueNext;
        e.queueNext.queuePrev = e;
        head.queueNext = e;
    }

    private void addToStack(Entry<K, V> e) {
        e.stackPrev = head;
        e.stackNext = head.stackNext;
        e.stackNext.stackPrev = e;
        head.stackNext = e;
    }

    /**
     * A cache entry.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    static class Entry<K, V> {
        K key;
        V value;
        int hashCode;
        int memory;
        boolean hot;
        Entry<K, V> stackPrev, stackNext, queuePrev, queueNext, chained;

        void removeFromStack() {
            stackPrev.stackNext = stackNext;
            stackNext.stackPrev = stackPrev;
            stackPrev = stackNext = null;
        }

        void removeFromQueue() {
            queuePrev.queueNext = queueNext;
            queueNext.queuePrev = queuePrev;
            queuePrev = queueNext = null;
        }

    }

}
