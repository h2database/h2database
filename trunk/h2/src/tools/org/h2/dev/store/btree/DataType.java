/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.nio.ByteBuffer;

/**
 * A value type.
 */
interface DataType {

    /**
     * Compare two keys.
     *
     * @param a the first key
     * @param b the second key
     * @return -1 if the first key is smaller, 1 if larger, and 0 if equal
     */
    int compare(Object a, Object b);

    /**
     * Get the length in bytes.
     *
     * @param obj the object
     * @return the length
     */
    int length(Object obj);

    /**
     * Write the object.
     *
     * @param buff the target buffer
     * @param x the value
     */
    void write(ByteBuffer buff, Object x);

    /**
     * Read an object.
     *
     * @param buff the source buffer
     * @return the object
     */
    Object read(ByteBuffer buff);

    /**
     * Get the tag name of the class.
     *
     * @return the tag name
     */
    String getName();

}

