/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import org.h2.dev.store.btree.MVMap;
import org.h2.dev.store.btree.MVStore;
import org.h2.dev.store.btree.DataType;
import org.h2.dev.store.btree.MapFactory;
import org.h2.dev.store.btree.StringType;

/**
 * A data type factory.
 */
public class TestMapFactory implements MapFactory {

    @Override
    public <K, V> MVMap<K, V> buildMap(String mapType, MVStore store,
            int id, String name, DataType keyType, DataType valueType,
            long createVersion) {
        if (mapType.equals("s")) {
            return new SequenceMap<K, V>(store, id, name, keyType, valueType, createVersion);
        } else if (mapType.equals("r")) {
            return new MVRTreeMap<K, V>(store, id, name, keyType, valueType, createVersion);
        } else {
            throw new RuntimeException("Unsupported map type " + mapType);
        }
    }

    @Override
    public DataType buildDataType(String s) {
        if (s.length() == 0) {
            return new StringType();
        }
        switch (s.charAt(0)) {
        case 'i':
            return new IntegerType();
        case 'r':
            return RowType.fromString(s, this);
        case 's':
            return SpatialType.fromString(s);
        }
        throw new RuntimeException("Unknown data type " + s);
    }

    @Override
    public String getDataType(Class<?> objectClass) {
        if (objectClass == Integer.class) {
            return "i";
        }
        throw new RuntimeException("Unsupported object class " + objectClass.toString());
    }

}
