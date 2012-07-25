/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import org.h2.dev.store.btree.DataType;
import org.h2.dev.store.btree.DataTypeFactory;
import org.h2.dev.store.btree.StringType;

/**
 * A data type factory.
 */
public class TestTypeFactory implements DataTypeFactory {

    public DataType fromString(String s) {
        if (s.length() == 0) {
            return new StringType();
        }
        switch (s.charAt(0)) {
        case 'i':
            return new IntegerType();
        case 'r':
            return RowType.fromString(s, this);
        }
        throw new RuntimeException("Unknown data type " + s);
    }

    public DataType getDataType(Class<?> objectClass) {
        if (objectClass == Integer.class) {
            return new IntegerType();
        }
        throw new RuntimeException("Unsupported object class " + objectClass.toString());
    }

}
