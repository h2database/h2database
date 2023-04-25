/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.Arrays;

import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.message.DbException;
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
        int length = value.length;
        if (length > Constants.MAX_STRING_LENGTH) {
            throw DbException.getValueTooLongException(getTypeName(getValueType()),
                    StringUtils.convertBytesToHex(value, 41), length);
        }
        this.value = value;
    }

    @Override
    public final byte[] getBytes() {
        return Utils.cloneByteArray(value);
    }

    @Override
    public final byte[] getBytesNoCopy() {
        return value;
    }

    @Override
    public final int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider) {
        return Bits.compareNotNullUnsigned(value, ((ValueBytesBase) v).value);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return StringUtils.convertBytesToHex(builder.append("X'"), value).append('\'');
    }

    @Override
    public final int hashCode() {
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
    public int getMemory() {
        return value.length + 24;
    }

    @Override
    public final boolean equals(Object other) {
        return other != null && getClass() == other.getClass() && Arrays.equals(value, ((ValueBytesBase) other).value);
    }

}
