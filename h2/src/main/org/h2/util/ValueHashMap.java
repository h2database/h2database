/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.store.DataHandler;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This hash map supports keys of type Value.
 *
 * @param <V> the value type
 */
public class ValueHashMap<V> extends HashBase {

    private Value[] keys;
    private V[] values;
    private DataHandler handler;

    /**
     * Create a new value hash map using the given data handler.
     * The data handler is used to compare values.
     *
     * @param handler the data handler
     */
    private ValueHashMap(DataHandler handler) {
        this.handler = handler;
    }

    /**
     * Create a new value hash map using the given data handler.
     * The data handler is used to compare values.
     *
     * @param handler the data handler
     * @return the object
     */
    public static <T> ValueHashMap<T> newInstance(DataHandler handler) {
        return new ValueHashMap<T>(handler);
    }

    @SuppressWarnings("unchecked")
    protected void reset(int newLevel) {
        super.reset(newLevel);
        keys = new Value[len];
        values = (V[]) new Object[len];
    }

    protected void rehash(int newLevel) throws SQLException {
        Value[] oldKeys = keys;
        V[] oldValues = values;
        reset(newLevel);
        for (int i = 0; i < oldKeys.length; i++) {
            Value k = oldKeys[i];
            if (k != null && k != ValueNull.DELETED) {
                put(k, oldValues[i]);
            }
        }
    }

    private int getIndex(Value key) {
        return key.hashCode() & mask;
    }

    /**
     * Add or update a key value pair.
     *
     * @param key the key
     * @param value the new value
     */
    public void put(Value key, V value) throws SQLException {
        checkSizePut();
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
            } else if (handler.compareTypeSave(k, key) == 0) {
                // update existing
                values[index] = value;
                return;
            }
            index = (index + plus++) & mask;
        } while (plus <= len);
        // no space
        Message.throwInternalError("hashmap is full");
    }

    /**
     * Remove a key value pair.
     *
     * @param key the key
     */
    public void remove(Value key) throws SQLException {
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
            } else if (handler.compareTypeSave(k, key) == 0) {
                // found the record
                keys[index] = ValueNull.DELETED;
                values[index] = null;
                deletedCount++;
                size--;
                return;
            }
            index = (index + plus++) & mask;
        } while(plus <= len);
        // not found
    }

    /**
     * Get the value for this key. This method returns null if the key was not
     * found.
     *
     * @param key the key
     * @return the value for the given key
     */
    public V get(Value key) throws SQLException {
        int index = getIndex(key);
        int plus = 1;
        do {
            Value k = keys[index];
            if (k == null) {
                // found an empty record
                return null;
            } else if (k == ValueNull.DELETED) {
                // found a deleted record
            } else if (handler.compareTypeSave(k, key) == 0) {
                // found it
                return values[index];
            }
            index = (index + plus++) & mask;
        } while (plus <= len);
        return null;
    }

    /**
     * Get the list of keys.
     *
     * @return all keys
     */
    public ObjectArray<Value> keys() {
        ObjectArray<Value> list = ObjectArray.newInstance(size);
        for (Value k : keys) {
            if (k != null && k != ValueNull.DELETED) {
                list.add(k);
            }
        }
        return list;
    }

    /**
     * Get the list of values.
     *
     * @return all values
     */
    public ObjectArray<V> values() {
        ObjectArray<V> list = ObjectArray.newInstance(size);
        for (int i = 0; i < keys.length; i++) {
            Value k = keys[i];
            if (k != null && k != ValueNull.DELETED) {
                list.add(values[i]);
            }
        }
        return list;
    }

}
