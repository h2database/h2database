/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

/**
 * A factory for data types.
 */
public class DataTypeFactory {

    /**
     * Get the class with the given name.
     *
     * @param name the name
     * @return the data type
     */
    static DataType getDataType(String name) {
        if (name.equals("i")) {
            return new IntegerType();
        } else if (name.equals("s")) {
            return new StringType();
        }
        throw new RuntimeException("Unknown data type name " + name);
    }

    /**
     * Get the data type object for the given class.
     *
     * @param objectClass the class
     * @return the data type object
     */
    static DataType getDataType(Class<?> objectClass) {
        if (objectClass == Integer.class) {
            return new IntegerType();
        } else if (objectClass == String.class) {
            return new StringType();
        }
        throw new RuntimeException("Unsupported object class " + objectClass.toString());
    }

}
