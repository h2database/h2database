/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.message.DbException;

/**
 * Base implementation of String based data types.
 */
abstract class ValueStringBase extends Value {

    /**
     * The value.
     */
    String value;

    private TypeInfo type;

    ValueStringBase(String v) {
        int length = v.length();
        if (length > Constants.MAX_STRING_LENGTH) {
            throw DbException.getValueTooLongException(getTypeName(getValueType()), v, length);
        }
        this.value = v;
    }

    @Override
    public final TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            int length = value.length();
            this.type = type = new TypeInfo(getValueType(), length, 0, null);
        }
        return type;
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider) {
        return mode.compareString(value, ((ValueStringBase) v).value, false);
    }

    @Override
    public int hashCode() {
        // TODO hash performance: could build a quicker hash
        // by hashing the size and a few characters
        return getClass().hashCode() ^ value.hashCode();

        // proposed code:
//        private int hash = 0;
//
//        public int hashCode() {
//            int h = hash;
//            if (h == 0) {
//                String s = value;
//                int l = s.length();
//                if (l > 0) {
//                    if (l < 16)
//                        h = s.hashCode();
//                    else {
//                        h = l;
//                        for (int i = 1; i <= l; i <<= 1)
//                            h = 31 *
//                                (31 * h + s.charAt(i - 1)) +
//                                s.charAt(l - i);
//                    }
//                    hash = h;
//                }
//            }
//            return h;
//        }
    }

    @Override
    public final String getString() {
        return value;
    }

    @Override
    public final byte[] getBytes() {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public final boolean getBoolean() {
        String s = value.trim();
        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("t") || s.equalsIgnoreCase("yes")
                || s.equalsIgnoreCase("y")) {
            return true;
        } else if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("f") || s.equalsIgnoreCase("no")
                || s.equalsIgnoreCase("n")) {
            return false;
        }
        try {
            // convert to a number, and if it is not 0 then it is true
            return new BigDecimal(s).signum() != 0;
        } catch (NumberFormatException e) {
            throw getDataConversionError(BOOLEAN);
        }
    }

    @Override
    public final byte getByte() {
        try {
            return Byte.parseByte(value.trim());
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, value);
        }
    }

    @Override
    public final short getShort() {
        try {
            return Short.parseShort(value.trim());
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, value);
        }
    }

    @Override
    public final int getInt() {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, value);
        }
    }

    @Override
    public final long getLong() {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, value);
        }
    }

    @Override
    public final BigDecimal getBigDecimal() {
        try {
            return new BigDecimal(value.trim());
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, value);
        }
    }

    @Override
    public final float getFloat() {
        try {
            return Float.parseFloat(value.trim());
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, value);
        }
    }

    @Override
    public final double getDouble() {
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, value);
        }
    }

    @Override
    public final int getMemory() {
        /*
         * Java 11 with -XX:-UseCompressedOops
         * Empty string: 88 bytes
         * 1 to 4 UTF-16 chars: 96 bytes
         */
        return value.length() * 2 + 94;
    }

    @Override
    public boolean equals(Object other) {
        return other != null && getClass() == other.getClass() && value.equals(((ValueStringBase) other).value);
    }

}
