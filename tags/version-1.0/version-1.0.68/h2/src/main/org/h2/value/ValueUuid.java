/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.util.ByteUtils;
import org.h2.util.RandomUtils;
import org.h2.util.StringUtils;

/**
 * Implementation of the UUID data type.
 */
public class ValueUuid extends Value {
    // number of bytes
    public static final int PRECISION = 16; 
    
    // cd38d882-7ada-4589-b5fb-7da0ca559d9a
    public static final int DISPLAY_SIZE = 36; 
    
    private final long high, low;

    private ValueUuid(long high, long low) {
        this.high = high;
        this.low = low;
    }

    public int hashCode() {
        return (int) ((high >>> 32) ^ high ^ (low >>> 32) ^ low);
    }

    public static ValueUuid getNewRandom() {
        long high = RandomUtils.getSecureLong();
        long low = RandomUtils.getSecureLong();
        high = (high & (~0xf000L)) | 0x4000L; // version 4 (random)
        low = (low & 0x3fffffffffffffffL) | 0x8000000000000000L; // variant (Leach-Salz)
        return new ValueUuid(high, low);
    }

    public static ValueUuid get(byte[] binary) {
        if (binary.length < 32) {
            return get(ByteUtils.convertBytesToString(binary));
        }
        long high = ByteUtils.readLong(binary, 0);
        long low = ByteUtils.readLong(binary, 16);
        return (ValueUuid) Value.cache(new ValueUuid(high, low));
    }

    public static ValueUuid get(long high, long low) {
        return (ValueUuid) Value.cache(new ValueUuid(high, low));
    }

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

    private void appendHex(StringBuffer buff, long x, int bytes) {
        for (int i = bytes * 8 - 4; i >= 0; i -= 8) {
            buff.append(Integer.toHexString((int) (x >> i) & 0xf));
            buff.append(Integer.toHexString((int) (x >> (i - 4)) & 0xf));
        }
    }

    public String getString() {
        StringBuffer buff = new StringBuffer(36);
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
        } else {
            return high > v.high ? 1 : -1;
        }
    }

    public boolean equals(Object other) {
        return other instanceof ValueUuid && compareSecure((Value) other, null) == 0;
    }

    public Object getObject() {
        // TODO needs to be documented
        return new long[]{high, low};
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

    public long getHigh() {
        return high;
    }

    public long getLow() {
        return low;
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

}
