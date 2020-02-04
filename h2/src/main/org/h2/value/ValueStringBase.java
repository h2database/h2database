/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.engine.CastDataProvider;

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
        this.value = v;
    }

    @Override
    public final TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            int length = value.length();
            this.type = type = new TypeInfo(getValueType(), length, 0, length, null);
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
    public String getString() {
        return value;
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public int getMemory() {
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
