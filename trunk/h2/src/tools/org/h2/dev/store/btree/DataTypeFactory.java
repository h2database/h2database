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
public interface DataTypeFactory {

    /**
     * Read the data type.
     *
     * @param s the string
     * @return the type
     */
    DataType fromString(String s);

    /**
     * Get the data type object for the given class.
     *
     * @param objectClass the class
     * @return the data type object
     */
    DataType getDataType(Class<?> objectClass);

}
