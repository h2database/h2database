/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.h2.constant.SysProperties;
import org.h2.util.MemoryUtils;
import org.h2.util.ObjectUtils;

/**
 * Implementation of the OBJECT data type.
 */
public class ValueJavaObject extends ValueBytesBase {

    private static final ValueJavaObject EMPTY = new ValueJavaObject(MemoryUtils.EMPTY_BYTES);

    protected ValueJavaObject(byte[] v) {
        super(v);
    }

    /**
     * Get or create a java object value for the given byte array.
     * Do not clone the data.
     *
     * @param b the byte array
     * @return the value
     */
    public static ValueJavaObject getNoCopy(byte[] b) {
        if (b.length == 0) {
            return EMPTY;
        }
        ValueJavaObject obj = new ValueJavaObject(b);
        if (b.length > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueJavaObject) Value.cache(obj);
    }

    public int getType() {
        return Value.JAVA_OBJECT;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        Object obj = ObjectUtils.deserialize(getBytesNoCopy());
        prep.setObject(parameterIndex, obj, Types.JAVA_OBJECT);
    }

}
