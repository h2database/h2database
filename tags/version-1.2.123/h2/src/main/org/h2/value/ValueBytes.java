/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.constant.SysProperties;
import org.h2.util.ByteUtils;
import org.h2.util.MemoryUtils;

/**
 * Implementation of the BINARY data type.
 */
public class ValueBytes extends ValueBytesBase {

    private static final ValueBytes EMPTY = new ValueBytes(MemoryUtils.EMPTY_BYTES);

    protected ValueBytes(byte[] v) {
        super(v);
    }

    /**
     * Get or create a bytes value for the given byte array.
     * Clone the data.
     *
     * @param b the byte array
     * @return the value
     */
    public static ValueBytes get(byte[] b) {
        if (b.length == 0) {
            return EMPTY;
        }
        b = ByteUtils.cloneByteArray(b);
        return getNoCopy(b);
    }

    /**
     * Get or create a bytes value for the given byte array.
     * Do not clone the date.
     *
     * @param b the byte array
     * @return the value
     */
    public static ValueBytes getNoCopy(byte[] b) {
        if (b.length == 0) {
            return EMPTY;
        }
        ValueBytes obj = new ValueBytes(b);
        if (b.length > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueBytes) Value.cache(obj);
    }

    public int getType() {
        return Value.BYTES;
    }

}
