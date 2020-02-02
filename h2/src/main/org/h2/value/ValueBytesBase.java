/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.Arrays;

import org.h2.engine.CastDataProvider;
import org.h2.util.Bits;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * Base implementation of byte array based data types.
 */
abstract class ValueBytesBase extends Value {

    /**
     * The value.
     */
    byte[] value;

    /**
     * The hash code.
     */
    int hash;

    ValueBytesBase(byte[] value) {
        this.value = value;
    }

    @Override
    public byte[] getBytesNoCopy() {
        return value;
    }

    @Override
    public final byte[] getBytes() {
        return Utils.cloneByteArray(getBytesNoCopy());
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider) {
        byte[] v2 = ((ValueBytesBase) v).value;
        int valueType = getValueType();
        if (valueType == GEOMETRY || valueType == JSON || mode.isBinaryUnsigned()) {
            return Bits.compareNotNullUnsigned(value, v2);
        }
        return Bits.compareNotNullSigned(value, v2);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return StringUtils.convertBytesToHex(builder.append("X'"), getBytesNoCopy()).append('\'');
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            h = getClass().hashCode() ^ Utils.getByteArrayHash(value);
            if (h == 0) {
                h = 1_234_570_417;
            }
            hash = h;
        }
        return h;
    }

    @Override
    public Object getObject() {
        return getBytes();
    }

    @Override
    public int getMemory() {
        return value.length + 24;
    }

    @Override
    public boolean equals(Object other) {
        return other != null && getClass() == other.getClass() && Arrays.equals(value, ((ValueBytesBase) other).value);
    }

}
