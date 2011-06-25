/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.constant.SysProperties;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * Implementation of the BINARY data type.
 * It is also the base class for ValueJavaObject.
 */
public class ValueBytes extends Value {

    private static final ValueBytes EMPTY = new ValueBytes(Utils.EMPTY_BYTES);

    private final byte[] value;
    private int hash;

    protected ValueBytes(byte[] v) {
        this.value = v;
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
        b = Utils.cloneByteArray(b);
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

    public String getSQL() {
        return "X'" + getString() + "'";
    }

    public byte[] getBytesNoCopy() {
        return value;
    }

    public byte[] getBytes() {
        return Utils.cloneByteArray(value);
    }

    protected int compareSecure(Value v, CompareMode mode) {
        byte[] v2 = ((ValueBytes) v).value;
        return Utils.compareNotNull(value, v2);
    }

    public String getString() {
        return StringUtils.convertBytesToHex(value);
    }

    public long getPrecision() {
        return value.length;
    }

    public int hashCode() {
        if (hash == 0) {
            hash = Utils.getByteArrayHash(value);
        }
        return hash;
    }

    public Object getObject() {
        return getBytes();
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setBytes(parameterIndex, value);
    }

    public int getDisplaySize() {
        return MathUtils.convertLongToInt(value.length * 2L);
    }

    public int getMemory() {
        return value.length + 24;
    }

    public boolean equals(Object other) {
        return other instanceof ValueBytes && Utils.compareNotNull(value, ((ValueBytes) other).value) == 0;
    }

    public Value convertPrecision(long precision, boolean force) {
        if (value.length <= precision) {
            return this;
        }
        int len = MathUtils.convertLongToInt(precision);
        byte[] buff = new byte[len];
        System.arraycopy(value, 0, buff, 0, len);
        return get(buff);
    }

}
