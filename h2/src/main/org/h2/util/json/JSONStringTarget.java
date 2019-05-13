/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.math.BigDecimal;

import org.h2.util.ByteStack;

/**
 * JSON String target.
 */
public final class JSONStringTarget extends JSONTarget {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private static final byte OBJECT = 1;

    private static final byte ARRAY = 2;

    /**
     * Encodes a JSON string and appends it to the specified string builder.
     *
     * @param builder
     *            the string builder to append to
     * @param s
     *            the string to encode
     * @return the specified string builder
     */
    public static StringBuilder encodeString(StringBuilder builder, String s) {
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
            case '\\':
                builder.append("\\\\");
                break;
            default:
                if (c >= ' ') {
                    builder.append(c);
                } else {
                    builder.append("\\u00") //
                            .append(HEX[c >>> 4 & 0xf]) //
                            .append(HEX[c & 0xf]);
                }
            }
        }
        return builder.append('"');
    }

    private final StringBuilder builder;

    private final ByteStack stack;

    private boolean needSeparator;

    private boolean afterName;

    /**
     * Creates new instance of JSON String target.
     */
    public JSONStringTarget() {
        builder = new StringBuilder();
        stack = new ByteStack();
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
        encodeString(builder, name).append(':');
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
        encodeString(builder, string);
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
    public boolean isProperyExpected() {
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
