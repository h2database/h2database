/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;


/**
 * A factory for maps and data types.
 */
public interface DataTypeFactory {

    /**
     * Parse the data type.
     *
     * @param dataType the string and type specific meta data
     * @return the type
     */
    DataType buildDataType(String dataType);

    /**
     * Get the data type object for the given class.
     *
     * @param objectClass the class
     * @return the data type object
     */
    String getDataType(Class<?> objectClass);

}
