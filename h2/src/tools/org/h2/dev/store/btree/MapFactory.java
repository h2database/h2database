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
public interface MapFactory {

    /**
     * Build a map.
     *
     * @param mapType the map type and type specific meta data
     * @param store the store
     * @param id the unique map id
     * @param name the map name
     * @param keyType the key type
     * @param valueType the value type
     * @param createVersion when the map was created
     * @return the map
     */
     <K, V> BtreeMap<K, V> buildMap(
             String mapType, BtreeMapStore store, int id, String name,
             DataType keyType, DataType valueType, long createVersion);

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
