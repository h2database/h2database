/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import org.h2.util.ByteUtils;
import org.h2.util.RandomUtils;
import org.h2.util.StringUtils;

/**
 * Implementation of the UUID data type.
 */
public class ValueUuid extends Value {

    /**
     * The precision of this value in number of bytes.
     */
    private static final int PRECISION = 16;

    /**
     * The display size of the textual representation of a UUID.
     * Example: cd38d882-7ada-4589-b5fb-7da0ca559d9a
     */
    private static final int DISPLAY_SIZE = 36;

    private final long high, low;

    private ValueUuid(long high, long low) {
        this.high = high;
        this.low = low;
    }

    public int hashCode() {
        return (int) ((high >>> 32) ^ high ^ (low >>> 32) ^ low);
    }

    /**
     * Create a new UUID using the pseudo random number generator.
     *
     * @return the new UUID
     */
    public static ValueUuid getNewRandom() {
        long high = RandomUtils.getSecureLong();
        long low = RandomUtils.getSecureLong();
        // version 4 (random)
        high = (high & (~0xf000L)) | 0x4000L;
        // variant (Leach-Salz)
        low = (low & 0x3fffffffffffffffL) | 0x8000000000000000L;
        return new ValueUuid(high, low);
    }

    /**
     * Get or create a UUID for the given 32 bytes.
     *
     * @param binary the byte array (must be at least 32 bytes long)
     * @return the UUID
     */
    public static ValueUuid get(byte[] binary) {
        if (binary.length < 32) {
            return get(ByteUtils.convertBytesToString(binary));
        }
        long high = ByteUtils.readLong(binary, 0);
        long low = ByteUtils.readLong(binary, 16);
        return (ValueUuid) Value.cache(new ValueUuid(high, low));
    }

    /**
     * Get or create a UUID for the given high and low order values.
     *
     * @param high the most significant bits
     * @param low the least significant bits
     * @return the UUID
     */
    public static ValueUuid get(long high, long low) {
        return (ValueUuid) Value.cache(new ValueUuid(high, low));
    }

    /**
     * Get or create a UUID for the given text representation.
     *
     * @param s the text representation of the UUID
     * @return the UUID
     */
    public static ValueUuid get(String s) {
        long high = 0, low = 0;
        int i = 0;
        for (int j = 0; i < s.length() && j < 16; i++) {
            char ch = s.charAt(i);
            if (ch != '-') {
                high = (high << 4) | Character.digit(ch, 16);
                j++;
            }
        }
        for (int j = 0; i < s.length() && j < 16; i++) {
            char ch = s.charAt(i);
            if (ch != '-') {
                low = (low << 4) | Character.digit(ch, 16);
                j++;
            }
        }
        return (ValueUuid) Value.cache(new ValueUuid(high, low));
    }

    public String getSQL() {
        return StringUtils.quoteStringSQL(getString());
    }

    public int getType() {
        return Value.UUID;
    }

    public long getPrecision() {
        return PRECISION;
    }

    private void appendHex(StringBuilder buff, long x, int bytes) {
        for (int i = bytes * 8 - 4; i >= 0; i -= 8) {
            buff.append(Integer.toHexString((int) (x >> i) & 0xf)).
                append(Integer.toHexString((int) (x >> (i - 4)) & 0xf));
        }
    }

    public String getString() {
        StringBuilder buff = new StringBuilder(36);
        appendHex(buff, high >> 32, 4);
        buff.append('-');
        appendHex(buff, high >> 16, 2);
        buff.append('-');
        appendHex(buff, high, 2);
        buff.append('-');
        appendHex(buff, low >> 48, 2);
        buff.append('-');
        appendHex(buff, low, 6);
        return buff.toString();
    }

    protected int compareSecure(Value o, CompareMode mode) {
        if (o == this) {
            return 0;
        }
        ValueUuid v = (ValueUuid) o;
        if (high == v.high) {
            return (low == v.low) ? 0 : (low > v.low ? 1 : -1);
        }
        return high > v.high ? 1 : -1;
    }

    public boolean equals(Object other) {
        return other instanceof ValueUuid && compareSecure((Value) other, null) == 0;
    }

    public Object getObject() {
        return new UUID(high, low);
    }

    public byte[] getBytes() {
        byte[] buff = new byte[16];
        for (int i = 0; i < 8; i++) {
            buff[i] = (byte) ((high >> (8 * (8 - i))) & 255);
            buff[8 + i] = (byte) ((low >> (8 * (8 - i))) & 255);
        }
        return buff;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setBytes(parameterIndex, getBytes());
    }

    /**
     * Get the most significant 64 bits of this UUID.
     *
     * @return the high order bits
     */
    public long getHigh() {
        return high;
    }

    /**
     * Get the least significant 64 bits of this UUID.
     *
     * @return the low order bits
     */
    public long getLow() {
        return low;
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

}
