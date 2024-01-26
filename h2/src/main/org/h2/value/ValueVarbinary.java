/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.nio.charset.StandardCharsets;

import org.h2.engine.SysProperties;
import org.h2.util.Utils;

/**
 * Implementation of the BINARY VARYING data type.
 */
public final class ValueVarbinary extends ValueBytesBase {

    /**
     * Empty value.
     */
    public static final ValueVarbinary EMPTY = new ValueVarbinary(Utils.EMPTY_BYTES);

    /**
     * Associated TypeInfo.
     */
    private TypeInfo type;

    private ValueVarbinary(byte[] value) {
        super(value);
    }

    /**
     * Get or create a VARBINARY value for the given byte array.
     * Clone the data.
     *
     * @param b the byte array
     * @return the value
     */
    public static ValueVarbinary get(byte[] b) {
        if (b.length == 0) {
            return EMPTY;
        }
        b = Utils.cloneByteArray(b);
        return getNoCopy(b);
    }

    /**
     * Get or create a VARBINARY value for the given byte array.
     * Do not clone the date.
     *
     * @param b the byte array
     * @return the value
     */
    public static ValueVarbinary getNoCopy(byte[] b) {
        if (b.length == 0) {
            return EMPTY;
        }
        ValueVarbinary obj = new ValueVarbinary(b);
        if (b.length > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueVarbinary) Value.cache(obj);
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            long precision = value.length;
            this.type = type = new TypeInfo(VARBINARY, precision, 0, null);
        }
        return type;
    }

    @Override
    public int getValueType() {
        return VARBINARY;
    }

    @Override
    public String getString() {
        return new String(value, StandardCharsets.UTF_8);
    }

}
