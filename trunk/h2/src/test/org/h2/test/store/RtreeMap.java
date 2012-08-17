/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import org.h2.dev.store.btree.BtreeMap;
import org.h2.dev.store.btree.BtreeMapStore;
import org.h2.dev.store.btree.DataType;

/**
 * A stored r-tree.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class RtreeMap<K, V> extends BtreeMap<K, V> {

    RtreeMap(BtreeMapStore store, int id, String name, DataType keyType,
            DataType valueType, long createVersion) {
        super(store, id, name, keyType, valueType, createVersion);
    }

}
