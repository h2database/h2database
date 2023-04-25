/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.nio.charset.Charset;
import java.text.CollationKey;
import java.text.Collator;
import java.util.Comparator;
import java.util.Locale;

import org.h2.util.Bits;

/**
 * The charset collator sorts strings according to the order in the given charset.
 */
public class CharsetCollator extends Collator {

    /**
     * The comparator used to compare byte arrays.
     */
    static final Comparator<byte[]> COMPARATOR = Bits::compareNotNullSigned;

    private final Charset charset;

    public CharsetCollator(Charset charset) {
        this.charset = charset;
    }

    public Charset getCharset() {
        return charset;
    }

    @Override
    public int compare(String source, String target) {
        return COMPARATOR.compare(toBytes(source), toBytes(target));
    }

    /**
     * Convert the source to bytes, using the character set.
     *
     * @param source the source
     * @return the bytes
     */
    byte[] toBytes(String source) {
        if (getStrength() <= Collator.SECONDARY) {
            // TODO perform case-insensitive comparison properly
            source = source.toUpperCase(Locale.ROOT);
        }
        return source.getBytes(charset);
    }

    @Override
    public CollationKey getCollationKey(String source) {
        return new CharsetCollationKey(source);
    }

    @Override
    public int hashCode() {
        return 255;
    }

    private class CharsetCollationKey extends CollationKey {

        private final byte[] bytes;

        CharsetCollationKey(String source) {
            super(source);
            bytes = toBytes(source);
        }

        @Override
        public int compareTo(CollationKey target) {
            return COMPARATOR.compare(bytes, target.toByteArray());
        }

        @Override
        public byte[] toByteArray() {
            return bytes;
        }

    }
}
