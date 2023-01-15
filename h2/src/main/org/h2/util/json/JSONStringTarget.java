/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.math.BigDecimal;

import org.h2.util.ByteStack;

/**
 * JSON String target.
 */
public final class JSONStringTarget extends JSONTarget<String> {

    /**
     * The hex characters.
     */
    static final char[] HEX = "0123456789abcdef".toCharArray();

    /**
     * A JSON object.
     */
    static final byte OBJECT = 1;

    /**
     * A JSON array.
     */
    static final byte ARRAY = 2;

    /**
     * Encodes a JSON string and appends it to the specified string builder.
     *
     * @param builder
     *            the string builder to append to
     * @param s
     *            the string to encode
     * @param asciiPrintableOnly
     *            whether all non-printable, non-ASCII characters, and {@code '}
     *            (single quote) characters should be escaped
     * @return the specified string builder
     */
    public static StringBuilder encodeString(StringBuilder builder, String s, boolean asciiPrintableOnly) {
        builder.append('"');
        for (int i = 0, length = s.length(); i < length; i++) {
            char c = s.charAt(i);
            switch (c) {
            case '\b':
                builder.append("\\b");
                break;
            case '\t':
                builder.append("\\t");
                break;
            case '\f':
                builder.append("\\f");
                break;
            case '\n':
                builder.append("\\n");
                break;
            case '\r':
                builder.append("\\r");
                break;
            case '"':
                builder.append("\\\"");
                break;
            case '\'':
                if (asciiPrintableOnly) {
                    builder.append("\\u0027");
                } else {
                    builder.append('\'');
                }
                break;
            case '\\':
                builder.append("\\\\");
                break;
            default:
                if (c < ' ') {
                    builder.append("\\u00") //
                            .append(HEX[c >>> 4 & 0xf]) //
                            .append(HEX[c & 0xf]);
                } else if (!asciiPrintableOnly || c <= 0x7f) {
                    builder.append(c);
                } else {
                    builder.append("\\u") //
                            .append(HEX[c >>> 12 & 0xf]) //
                            .append(HEX[c >>> 8 & 0xf]) //
                            .append(HEX[c >>> 4 & 0xf]) //
                            .append(HEX[c & 0xf]);
                }
            }
        }
        return builder.append('"');
    }

    private final StringBuilder builder;

    private final ByteStack stack;

    private final boolean asciiPrintableOnly;

    private boolean needSeparator;

    private boolean afterName;

    /**
     * Creates new instance of JSON String target.
     */
    public JSONStringTarget() {
        this(false);
    }

    /**
     * Creates new instance of JSON String target.
     *
     * @param asciiPrintableOnly
     *            whether all non-printable, non-ASCII characters, and {@code '}
     *            (single quote) characters should be escaped
     */
    public JSONStringTarget(boolean asciiPrintableOnly) {
        builder = new StringBuilder();
        stack = new ByteStack();
        this.asciiPrintableOnly = asciiPrintableOnly;
    }

    @Override
    public void startObject() {
        beforeValue();
        afterName = false;
        stack.push(OBJECT);
        builder.append('{');
    }

    @Override
    public void endObject() {
        if (afterName || stack.poll(-1) != OBJECT) {
            throw new IllegalStateException();
        }
        builder.append('}');
        afterValue();
    }

    @Override
    public void startArray() {
        beforeValue();
        afterName = false;
        stack.push(ARRAY);
        builder.append('[');
    }

    @Override
    public void endArray() {
        if (stack.poll(-1) != ARRAY) {
            throw new IllegalStateException();
        }
        builder.append(']');
        afterValue();
    }

    @Override
    public void member(String name) {
        if (afterName || stack.peek(-1) != OBJECT) {
            throw new IllegalStateException();
        }
        afterName = true;
        beforeValue();
        encodeString(builder, name, asciiPrintableOnly).append(':');
    }

    @Override
    public void valueNull() {
        beforeValue();
        builder.append("null");
        afterValue();
    }

    @Override
    public void valueFalse() {
        beforeValue();
        builder.append("false");
        afterValue();
    }

    @Override
    public void valueTrue() {
        beforeValue();
        builder.append("true");
        afterValue();
    }

    @Override
    public void valueNumber(BigDecimal number) {
        beforeValue();
        String s = number.toString();
        int index = s.indexOf('E');
        if (index >= 0 && s.charAt(++index) == '+') {
            builder.append(s, 0, index).append(s, index + 1, s.length());
        } else {
            builder.append(s);
        }
        afterValue();
    }

    @Override
    public void valueString(String string) {
        beforeValue();
        encodeString(builder, string, asciiPrintableOnly);
        afterValue();
    }

    private void beforeValue() {
        if (!afterName && stack.peek(-1) == OBJECT) {
            throw new IllegalStateException();
        }
        if (needSeparator) {
            if (stack.isEmpty()) {
                throw new IllegalStateException();
            }
            needSeparator = false;
            builder.append(',');
        }
    }

    private void afterValue() {
        needSeparator = true;
        afterName = false;
    }

    @Override
    public boolean isPropertyExpected() {
        return !afterName && stack.peek(-1) == OBJECT;
    }

    @Override
    public boolean isValueSeparatorExpected() {
        return needSeparator;
    }

    @Override
    public String getResult() {
        if (!stack.isEmpty() || builder.length() == 0) {
            throw new IllegalStateException();
        }
        return builder.toString();
    }

}
