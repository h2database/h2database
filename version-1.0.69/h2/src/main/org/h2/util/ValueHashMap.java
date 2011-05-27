/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
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
 * This hash map supports keys of type Value and values of type Object.
 */
public class ValueHashMap extends HashBase {

    private Value[] keys;
    private Object[] values;
    private DataHandler database;

    public ValueHashMap(DataHandler database) {
        this.database = database;
    }

    protected void reset(int newLevel) {
        super.reset(newLevel);
        keys = new Value[len];
        values = new Object[len];
    }

    protected void rehash(int newLevel) throws SQLException {
        Value[] oldKeys = keys;
        Object[] oldValues = values;
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

    public void put(Value key, Object value) throws SQLException {
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
            } else if (database.compareTypeSave(k, key) == 0) {
                // update existing
                values[index] = value;
                return;
            }
            index = (index + plus++) & mask;
        } while (plus <= len);
        // no space
        throw Message.getInternalError("hashmap is full");
    }

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
            } else if (database.compareTypeSave(k, key) == 0) {
                // found the record
                keys[index] = ValueNull.DELETED;
                values[index] = null;
                deletedCount++;
                size--;
                return;
            }
            index = (index + plus++) & mask;
            k = keys[index];
        } while(plus <= len);
        // not found
    }

    public Object get(Value key) throws SQLException {
        int index = getIndex(key);
        int plus = 1;
        do {
            Value k = keys[index];
            if (k == null) {
                // found an empty record
                return null;
            } else if (k == ValueNull.DELETED) {
                // found a deleted record
            } else if (database.compareTypeSave(k, key) == 0) {
                // found it
                return values[index];
            }
            index = (index + plus++) & mask;
        } while (plus <= len);
        return null;
    }

    public ObjectArray keys() {
        ObjectArray list = new ObjectArray(size);
        for (int i = 0; i < keys.length; i++) {
            Value k = keys[i];
            if (k != null && k != ValueNull.DELETED) {
                list.add(k);
            }
        }
        return list;
    }

    public ObjectArray values() {
        ObjectArray list = new ObjectArray(size);
        for (int i = 0; i < keys.length; i++) {
            Value k = keys[i];
            if (k != null && k != ValueNull.DELETED) {
                list.add(values[i]);
            }
        }
        return list;
    }

}
