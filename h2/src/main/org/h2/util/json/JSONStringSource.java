/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.math.BigDecimal;

import org.h2.util.StringUtils;

/**
 * JSON string source.
 */
public final class JSONStringSource extends JSONTextSource {

    /**
     * Parses source string to a specified target.
     *
     * @param string
     *            source
     * @param target
     *            target
     * @param <R>
     *            the type of the result
     * @return the result of the target
     */
    public static <R> R parse(String string, JSONTarget<R> target) {
        new JSONStringSource(string, target).parse();
        return target.getResult();
    }

    /**
     * Normalizes textual JSON representation.
     *
     * @param string
     *            source representation
     * @return normalized representation
     */
    public static byte[] normalize(String string) {
        return parse(string, new JSONByteArrayTarget());
    }

    private final String string;

    private final int length;

    private int index;

    JSONStringSource(String string, JSONTarget<?> target) {
        super(target);
        this.string = string;
        this.length = string.length();
        if (length == 0) {
            throw new IllegalArgumentException();
        }
        // Ignore BOM
        if (string.charAt(index) == '\uFEFF') {
            index++;
        }
    }

    @Override
    int nextCharAfterWhitespace() {
        int index = this.index;
        while (index < length) {
            char ch = string.charAt(index++);
            switch (ch) {
            case '\t':
            case '\n':
            case '\r':
            case ' ':
                break;
            default:
                this.index = index;
                return ch;
            }
        }
        return -1;
    }

    @Override
    void readKeyword1(String keyword) {
        int l = keyword.length() - 1;
        if (!string.regionMatches(index, keyword, 1, l)) {
            throw new IllegalArgumentException();
        }
        index += l;
    }

    @Override
    void parseNumber(boolean positive) {
        int index = this.index;
        int start = index - 1;
        index = skipInt(index, positive);
        l: if (index < length) {
            char ch = string.charAt(index);
            if (ch == '.') {
                index = skipInt(index + 1, false);
                if (index >= length) {
                    break l;
                }
                ch = string.charAt(index);
            }
            if (ch == 'E' || ch == 'e') {
                if (++index >= length) {
                    throw new IllegalArgumentException();
                }
                ch = string.charAt(index);
                if (ch == '+' || ch == '-') {
                    index++;
                }
                index = skipInt(index, false);
            }
        }
        target.valueNumber(new BigDecimal(string.substring(start, index)));
        this.index = index;
    }

    private int skipInt(int index, boolean hasInt) {
        while (index < length) {
            char ch = string.charAt(index);
            if (ch >= '0' && ch <= '9') {
                hasInt = true;
                index++;
            } else {
                break;
            }
        }
        if (!hasInt) {
            throw new IllegalArgumentException();
        }
        return index;
    }

    @Override
    int nextChar() {
        if (index >= length) {
            throw new IllegalArgumentException();
        }
        return string.charAt(index++);
    }

    @Override
    char readHex() {
        if (index + 3 >= length) {
            throw new IllegalArgumentException();
        }
        try {
            return (char) Integer.parseInt(string.substring(index, index += 4), 16);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public String toString() {
        return StringUtils.addAsterisk(string, index);
    }

}
