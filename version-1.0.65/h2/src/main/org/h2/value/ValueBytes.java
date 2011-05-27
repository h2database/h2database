/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.constant.SysProperties;
import org.h2.util.ByteUtils;

/**
 * Implementation of the BINARY data type.
 */
public class ValueBytes extends ValueBytesBase {

    private static final ValueBytes EMPTY = new ValueBytes(new byte[0]);

    protected ValueBytes(byte[] v) {
        super(v);
    }

    public static ValueBytes get(byte[] b) {
        b = ByteUtils.cloneByteArray(b);
        return getNoCopy(b);
    }

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
