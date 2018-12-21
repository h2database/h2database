/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This hash map supports keys of type Value.
 * <p>
 * ValueHashMap is a very simple implementation without allocation of additional
 * objects for entries. It's very fast with good distribution of hashes, but if
 * hashes have a lot of collisions this implementation tends to be very slow.
 * <p>
 * HashMap in archaic versions of Java have some overhead for allocation of
 * entries, but slightly better behaviour with limited number of collisions,
 * because collisions have no impact on non-colliding entries. HashMap in modern
 * versions of Java also have the same overhead, but it builds a trees of keys
 * with colliding hashes, that's why even if the all keys have exactly the same
 * hash code it still offers a good performance similar to TreeMap. So
 * ValueHashMap is faster in typical cases, but may behave really bad in some
 * cases. HashMap is slower in typical cases, but its performance does not
 * degrade too much even in the worst possible case (if keys are comparable).
 *
 * @param <V> the value type
 */
public class ValueHashMap<V> extends HashBase {

    /**
     * Keys array.
     */
    Value[] keys;

    /**
     * Values array.
     */
    V[] values;

    @Override
    @SuppressWarnings("unchecked")
    protected void reset(int newLevel) {
        super.reset(newLevel);
        keys = new Value[len];
        values = (V[]) new Object[len];
    }

    @Override
    protected void rehash(int newLevel) {
        Value[] oldKeys = keys;
        V[] oldValues = values;
        reset(newLevel);
        int len = oldKeys.length;
        for (int i = 0; i < len; i++) {
            Value k = oldKeys[i];
            if (k != null && k != ValueNull.DELETED) {
                // skip the checkSizePut so we don't end up
                // accidentally recursing
                internalPut(k, oldValues[i], false);
            }
        }
    }

    private int getIndex(Value key) {
        int h = key.hashCode();
        /*
         * Add some protection against hashes with the same less significant bits
         * (ValueDouble with integer values, for example).
         */
        return (h ^ h >>> 16) & mask;
    }

    /**
     * Add or update a key value pair.
     *
     * @param key the key
     * @param value the new value
     */
    public void put(Value key, V value) {
        checkSizePut();
        internalPut(key, value, false);
    }

    /**
     * Add a key value pair, values for existing keys are not replaced.
     *
     * @param key the key
     * @param value the new value
     */
    public void putIfAbsent(Value key, V value) {
        checkSizePut();
        internalPut(key, value, true);
    }

    private void internalPut(Value key, V value, boolean ifAbsent) {
        int index = getIndex(key);
        int plus = 1;
        int deleted = -1;
        do {
            Value k = keys[index];
            if (k == null) {
                // found an empty record
                if (deleted >= 0) {
                    index = deleted;
                    deletedCount--;
                }
                size++;
                keys[index] = key;
                values[index] = value;
                return;
            } else if (k == ValueNull.DELETED) {
                // found a deleted record
                if (deleted < 0) {
                    deleted = index;
                }
            } else if (k.equals(key)) {
                if (ifAbsent) {
                    return;
                }
                // update existing
                values[index] = value;
                return;
            }
            index = (index + plus++) & mask;
        } while (plus <= len);
        // no space
        DbException.throwInternalError("hashmap is full");
    }

    /**
     * Remove a key value pair.
     *
     * @param key the key
     */
    public void remove(Value key) {
        checkSizeRemove();
        int index = getIndex(key);
        int plus = 1;
        do {
            Value k = keys[index];
            if (k == null) {
                // found an empty record
                return;
            } else if (k == ValueNull.DELETED) {
                // found a deleted record
            } else if (k.equals(key)) {
                // found the record
                keys[index] = ValueNull.DELETED;
                values[index] = null;
                deletedCount++;
                size--;
                return;
            }
            index = (index + plus++) & mask;
        } while (plus <= len);
        // not found
    }

    /**
     * Get the value for this key. This method returns null if the key was not
     * found.
     *
     * @param key the key
     * @return the value for the given key
     */
    public V get(Value key) {
        int index = getIndex(key);
        int plus = 1;
        do {
            Value k = keys[index];
            if (k == null) {
                // found an empty record
                return null;
            } else if (k == ValueNull.DELETED) {
                // found a deleted record
            } else if (k.equals(key)) {
                // found it
                return values[index];
            }
            index = (index + plus++) & mask;
        } while (plus <= len);
        return null;
    }

    /**
     * Get the keys.
     *
     * @return all keys
     */
    public Iterable<Value> keys() {
        return new KeyIterable();
    }

    private final class KeyIterable implements Iterable<Value> {
        KeyIterable() {
        }

        @Override
        public Iterator<Value> iterator() {
            return new UnifiedIterator<>(false);
        }
    }

    /**
     * Gets all map's entries.
     *
     * @return all map's entries.
     */
    public Iterable<Map.Entry<Value, V>> entries() {
        return new EntryIterable();
    }

    private final class EntryIterable implements Iterable<Map.Entry<Value, V>> {
        EntryIterable() {
        }

        @Override
        public Iterator<Map.Entry<Value, V>> iterator() {
            return new UnifiedIterator<>(true);
        }
    }

    final class UnifiedIterator<T> implements Iterator<T> {
        int keysIndex = -1;
        int left = size;

        private final boolean forEntries;

        UnifiedIterator(boolean forEntries) {
            this.forEntries = forEntries;
        }

        @Override
        public boolean hasNext() {
            return left > 0;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T next() {
            if (left <= 0)
                throw new NoSuchElementException();
            left--;
            for (;;) {
                keysIndex++;
                Value key = keys[keysIndex];
                if (key != null && key != ValueNull.DELETED) {
                    return (T) (forEntries ? new AbstractMap.SimpleImmutableEntry<>(key, values[keysIndex]) : key);
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Get the list of values.
     *
     * @return all values
     */
    public ArrayList<V> values() {
        ArrayList<V> list = new ArrayList<>(size);
        int len = keys.length;
        for (int i = 0; i < len; i++) {
            Value k = keys[i];
            if (k != null && k != ValueNull.DELETED) {
                list.add(values[i]);
            }
        }
        return list;
    }

}
