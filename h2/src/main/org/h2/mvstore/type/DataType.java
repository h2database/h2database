/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.h2.mvstore.WriteBuffer;

/**
 * A data type.
 */
public interface DataType<T> extends Comparator<T> {

    /**
     * Compare two keys.
     *
     * @param a the first key
     * @param b the second key
     * @return -1 if the first key is smaller, 1 if larger, and 0 if equal
     * @throws UnsupportedOperationException if the type is not orderable
     */
    @Override
    int compare(T a, T b);

    /**
     * Perform binary search for the key within the storage
     * @param key to search for
     * @param storage to search within (an array of type T)
     * @param size number of data items in the storage
     * @param initialGuess for key position
     * @return index of the key , if found, - index of the insertion point, if not
     */
    int binarySearch(T key, Object storage, int size, int initialGuess);

    /**
     * Calculates the amount of used memory in bytes.
     *
     * @param obj the object
     * @return the used memory
     */
    int getMemory(T obj);

    /**
     * Whether memory estimation based on previously seen values is allowed/desirable
     * @return true if memory estimation is allowed
     */
    boolean isMemoryEstimationAllowed();

    /**
     * Write an object.
     *
     * @param buff the target buffer
     * @param obj the value
     */
    void write(WriteBuffer buff, T obj);

    /**
     * Write a list of objects.
     *
     * @param buff the target buffer
     * @param storage the objects
     * @param len the number of objects to write
     */
    void write(WriteBuffer buff, Object storage, int len);

    /**
     * Read an object.
     *
     * @param buff the source buffer
     * @return the object
     */
    T read(ByteBuffer buff);

    /**
     * Read a list of objects.
     *
     * @param buff the target buffer
     * @param storage the objects
     * @param len the number of objects to read
     */
    void read(ByteBuffer buff, Object storage, int len);

    /**
     * Create storage object of array type to hold values
     *
     * @param size number of values to hold
     * @return storage object
     */
    T[] createStorage(int size);
}

