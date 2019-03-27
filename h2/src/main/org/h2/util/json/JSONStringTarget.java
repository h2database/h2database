/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.math.BigDecimal;

/**
 * JSON String target.
 */
public final class JSONStringTarget extends JSONTarget {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final StringBuilder builder;

    /**
     * Creates new instance of JSON String target.
     */
    public JSONStringTarget() {
        builder = new StringBuilder();
    }

    @Override
    public void startObject() {
        builder.append('{');
    }

    @Override
    public void endObject() {
        builder.append('}');
    }

    @Override
    public void startArray() {
        builder.append('[');
    }

    @Override
    public void endArray() {
        builder.append(']');
    }

    @Override
    public void member(String name) {
        writeString(name);
        builder.append(':');
    }

    @Override
    public void valueNull() {
        builder.append("null");
    }

    @Override
    public void valueFalse() {
        builder.append("false");
    }

    @Override
    public void valueTrue() {
        builder.append("true");
    }

    @Override
    public void valueNumber(BigDecimal number) {
        builder.append(number.toString());
    }

    @Override
    public void valueString(String string) {
        writeString(string);
    }

    private void writeString(String s) {
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
        builder.append('"');
    }

    @Override
    public void valueSeparator() {
        builder.append(',');
    }

    /**
     * @return the result string
     */
    public String getString() {
        return builder.toString();
    }

}
