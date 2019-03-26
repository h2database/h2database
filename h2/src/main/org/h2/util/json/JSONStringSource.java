/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.math.BigDecimal;

/**
 * JSON string source.
 */
public final class JSONStringSource {

    private final String string;
    private final JSONTarget target;
    private int index;
    private final StringBuilder builder;

    /**
     * Parses source string to a specified target.
     *
     * @param string
     *            source
     * @param target
     *            target
     */
    public static void parse(String string, JSONTarget target) {
        new JSONStringSource(string, target).parse();
    }

    /**
     * Normalizes textual JSON representation.
     *
     * @param string
     *            source representation
     * @return normalized representation
     */
    public static String normalize(String string) {
        JSONStringTarget target = new JSONStringTarget();
        JSONStringSource.parse(string, target);
        return target.getString();
    }

    private JSONStringSource(String string, JSONTarget target) {
        this.string = string;
        this.target = target;
        builder = new StringBuilder();
    }

    private void parse() {
        int length = string.length();
        if (length == 0) {
            throw new IllegalArgumentException();
        }
        // Ignore BOM
        if (string.charAt(index) == '\uFEFF') {
            index++;
        }
        parseValue(length);
        for (int index = this.index; index < length; index++) {
            if (!isWhitespace(string.charAt(index))) {
                throw new IllegalArgumentException();
            }
        }
    }

    private void parseValue(int length) {
        switch (skipWhitespace(length)) {
        case 'f':
            if (index + 4 > length || !string.regionMatches(index, "false", 1, 4)) {
                throw new IllegalArgumentException();
            }
            target.valueFalse();
            index += 4;
            break;
        case 'n':
            if (index + 3 > length || !string.regionMatches(index, "null", 1, 3)) {
                throw new IllegalArgumentException();
            }
            index += 3;
            target.valueNull();
            break;
        case 't':
            if (index + 3 > length || !string.regionMatches(index, "true", 1, 3)) {
                throw new IllegalArgumentException();
            }
            index += 3;
            target.valueTrue();
            break;
        case '{':
            parseObject(length);
            break;
        case '[':
            parseArray(length);
            break;
        case '"':
            target.valueString(readString(length));
            break;
        case '-':
            parseNumber(length, false);
            break;
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            parseNumber(length, true);
            break;
        default:
            throw new IllegalArgumentException();
        }
    }

    private void parseObject(int length) {
        target.startObject();
        switch (skipWhitespace(length)) {
        case '"':
            parseMember(length);
            break;
        case '}':
            target.endObject();
            return;
        default:
            throw new IllegalArgumentException();
        }
        for (;;) {
            switch (skipWhitespace(length)) {
            case ',':
                target.valueSeparator();
                if (skipWhitespace(length) != '"') {
                    throw new IllegalArgumentException();
                }
                parseMember(length);
                break;
            case '}':
                target.endObject();
                return;
            default:
                throw new IllegalArgumentException();
            }
        }
    }

    private void parseArray(int length) {
        target.startArray();
        if (skipWhitespace(length) == ']') {
            target.endArray();
            return;
        }
        index--;
        parseValue(length);
        for (;;) {
            switch (skipWhitespace(length)) {
            case ',':
                target.valueSeparator();
                parseValue(length);
                break;
            case ']':
                target.endArray();
                return;
            default:
                throw new IllegalArgumentException();
            }
        }
    }

    private void parseMember(int length) {
        target.member(readString(length));
        if (skipWhitespace(length) != ':') {
            throw new IllegalArgumentException();
        }
        parseValue(length);
    }

    private char skipWhitespace(int length) {
        int index = this.index;
        while (index < length) {
            char ch = string.charAt(index++);
            if (!isWhitespace(ch)) {
                this.index = index;
                return ch;
            }
        }
        throw new IllegalArgumentException();
    }

    private static boolean isWhitespace(char ch) {
        switch (ch) {
        case '\t':
        case '\n':
        case '\r':
        case ' ':
            return true;
        default:
            return false;
        }
    }

    private void parseNumber(int length, boolean positive) {
        int index = this.index;
        int start = index - 1;
        index = skipInt(index, length, positive);
        l: if (index < length) {
            char ch = string.charAt(index++);
            if (ch == '.') {
                index = skipInt(index, length, false);
                if (index >= length) {
                    break l;
                }
                ch = string.charAt(index++);
            }
            if (ch == 'E' || ch == 'e') {
                if (index >= length) {
                    throw new IllegalArgumentException();
                }
                ch = string.charAt(index);
                if (ch == '+' || ch == '-') {
                    index++;
                }
                index = skipInt(index, length, false);
            } else {
                index--;
            }
        }
        target.valueNumber(new BigDecimal(string.substring(start, index)));
        this.index = index;
    }

    private int skipInt(int index, int length, boolean hasInt) {
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

    private String readString(int length) {
        builder.setLength(0);
        boolean inSurrogate = false;
        for (;;) {
            if (index >= length) {
                throw new IllegalArgumentException();
            }
            char ch = string.charAt(index++);
            switch (ch) {
            case '"':
                if (inSurrogate) {
                    throw new IllegalArgumentException();
                }
                return builder.toString();
            case '\\':
                ch = string.charAt(index++);
                switch (ch) {
                case '"':
                case '/':
                case '\\':
                    appendNonSurrogate(ch, inSurrogate);
                    break;
                case 'b':
                    appendNonSurrogate('\b', inSurrogate);
                    break;
                case 'f':
                    appendNonSurrogate('\f', inSurrogate);
                    break;
                case 'n':
                    appendNonSurrogate('\n', inSurrogate);
                    break;
                case 'r':
                    appendNonSurrogate('\r', inSurrogate);
                    break;
                case 't':
                    appendNonSurrogate('\t', inSurrogate);
                    break;
                case 'u':
                    if (index + 3 >= length) {
                        throw new IllegalArgumentException();
                    }
                    try {
                        ch = (char) Integer.parseInt(string.substring(index, index += 4), 16);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException();
                    }
                    inSurrogate = appendChar(ch, inSurrogate);
                    break;
                default:
                    throw new IllegalArgumentException();
                }
                break;
            default:
                inSurrogate = appendChar(ch, inSurrogate);
            }
        }
    }

    private void appendNonSurrogate(char ch, boolean inSurrogate) {
        if (inSurrogate) {
            throw new IllegalArgumentException();
        }
        builder.append(ch);
    }

    private boolean appendChar(char ch, boolean inSurrogate) {
        if (inSurrogate != Character.isLowSurrogate(ch)) {
            throw new IllegalArgumentException();
        }
        if (inSurrogate) {
            inSurrogate = false;
        } else if (Character.isHighSurrogate(ch)) {
            inSurrogate = true;
        }
        builder.append(ch);
        return inSurrogate;
    }

}
