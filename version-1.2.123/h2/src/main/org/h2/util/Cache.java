/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;

/**
 * The cache keeps frequently used objects in the main memory.
 */
public interface Cache {

    /**
     * Get all objects in the cache that have been changed.
     *
     * @return the list of objects
     */
    ObjectArray<CacheObject> getAllChanged();

    /**
     * Clear the cache.
     */
    void clear();

    /**
     * Get an element in the cache if it is available.
     * This will move the item to the front of the list.
     *
     * @param pos the unique key of the element
     * @return the element or null
     */
    CacheObject get(int pos);

    /**
     * Add an element to the cache. Other items may fall out of the cache
     * because of this. It is not allowed to add the same record twice.
     *
     * @param r the object
     */
    void put(CacheObject r) throws SQLException;

    /**
     * Update an element in the cache.
     * This will move the item to the front of the list.
     *
     * @param pos the unique key of the element
     * @param record the element
     * @return the element
     */
    CacheObject update(int pos, CacheObject record) throws SQLException;

    /**
     * Remove an object from the cache.
     *
     * @param pos the unique key of the element
     */
    void remove(int pos);

    /**
     * Get an element from the cache if it is available.
     * This will not move the item to the front of the list.
     *
     * @param pos the unique key of the element
     * @return the element or null
     */
    CacheObject find(int pos);

    /**
     * Set the maximum memory to be used by this cache.
     *
     * @param size in number of double words (4 bytes)
     */
    void setMaxSize(int size) throws SQLException;

    /**
     * Get the name of the cache type in a human readable form.
     *
     * @return the cache type name
     */
    String getTypeName();

    /**
     * Get the maximum size in words (4 bytes).
     *
     * @return the maximum size in number of double words (4 bytes)
     */
    int getMaxSize();

    /**
     * Get the used size in words (4 bytes).
     *
     * @return the current size in number of double words (4 bytes)
     */
    int getSize();

}
