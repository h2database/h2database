/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;

import org.h2.message.Message;

/**
 * A hash map with int key and int values. There is a restriction: the
 * value -1 (NOT_FOUND) cannot be stored in the map. 0 can be stored.
 * An empty record has key=0 and value=0.
 * A deleted record has key=0 and value=DELETED
 */
public class IntIntHashMap extends HashBase {

    /**
     * The value indicating that the entry has not been found.
     */
    public static final int NOT_FOUND = -1;

    private static final int DELETED = 1;
    private int[] keys;
    private int[] values;
    private int zeroValue;

    protected void reset(int newLevel) {
        super.reset(newLevel);
        keys = new int[len];
        values = new int[len];
    }

    /**
     * Store the given key-value pair. The value is overwritten or added.
     *
     * @param key the key
     * @param value the value (-1 is not supported)
     */
    public void put(int key, int value) {
        if (key == 0) {
            zeroKey = true;
            zeroValue = value;
        }
        try {
            checkSizePut();
        } catch (SQLException e) {
            // in fact, it is never thrown
            // TODO hash: maybe optimize it
        }
        int index = getIndex(key);
        int plus = 1;
        int deleted = -1;
        do {
            int k = keys[index];
            if (k == 0) {
                if (values[index] != DELETED) {
                    // found an empty record
                    if (deleted >= 0) {
                        index = deleted;
                        deletedCount--;
                    }
                    size++;
                    keys[index] = key;
                    values[index] = value;
                    return;
                }
                // found a deleted record
                if (deleted < 0) {
                    deleted = index;
                }
            } else if (k == key) {
                // update existing
                values[index] = value;
                return;
            }
            index = (index + plus++) & mask;
        } while(plus <= len);
        // no space
        Message.throwInternalError("hashmap is full");
    }

    /**
     * Remove the key-value pair with the given key.
     *
     * @param key the key
     */
    public void remove(int key) {
        if (key == 0) {
            zeroKey = false;
            return;
        }
        try {
            checkSizeRemove();
        } catch (SQLException e) {
            // in fact, it is never thrown
            // TODO hash: maybe optimize it
        }
        int index = getIndex(key);
        int plus = 1;
        do {
            int k = keys[index];
            if (k == key) {
                // found the record
                keys[index] = 0;
                values[index] = DELETED;
                deletedCount++;
                size--;
                return;
            } else if (k == 0 && values[index] == 0) {
                // found an empty record
                return;
            }
            index = (index + plus++) & mask;
        } while(plus <= len);
        // not found
    }

    protected void rehash(int newLevel) {
        int[] oldKeys = keys;
        int[] oldValues = values;
        reset(newLevel);
        for (int i = 0; i < oldKeys.length; i++) {
            int k = oldKeys[i];
            if (k != 0) {
                put(k, oldValues[i]);
            }
        }
    }

    /**
     * Get the value for the given key. This method returns NOT_FOUND if the
     * entry has not been found.
     *
     * @param key the key
     * @return the value or NOT_FOUND
     */
    public int get(int key) {
        if (key == 0) {
            return zeroKey ? zeroValue : NOT_FOUND;
        }
        int index = getIndex(key);
        int plus = 1;
        do {
            int k = keys[index];
            if (k == 0 && values[index] == 0) {
                // found an empty record
                return NOT_FOUND;
            } else if (k == key) {
                // found it
                return values[index];
            }
            index = (index + plus++) & mask;
        } while(plus <= len);
        return NOT_FOUND;
    }

}
