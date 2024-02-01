/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

/**
 * JSON text source.
 */
public abstract class JSONTextSource {

    /**
     * The output.
     */
    final JSONTarget<?> target;

    private final StringBuilder builder;

    JSONTextSource(JSONTarget<?> target) {
        this.target = target;
        builder = new StringBuilder();
    }

    /**
     * Parse the text and write it to the output.
     */
    final void parse() {
        boolean comma = false;
        for (int ch; (ch = nextCharAfterWhitespace()) >= 0;) {
            if (ch == '}' || ch == ']') {
                if (comma) {
                    throw new IllegalArgumentException();
                }
                if (ch == '}') {
                    target.endObject();
                } else {
                    target.endArray();
                }
                continue;
            }
            if (ch == ',') {
                if (comma || !target.isValueSeparatorExpected()) {
                    throw new IllegalArgumentException();
                }
                comma = true;
                continue;
            }
            if (comma != target.isValueSeparatorExpected()) {
                throw new IllegalArgumentException();
            }
            comma = false;
            switch (ch) {
            case 'f':
                readKeyword1("false");
                target.valueFalse();
                break;
            case 'n':
                readKeyword1("null");
                target.valueNull();
                break;
            case 't':
                readKeyword1("true");
                target.valueTrue();
                break;
            case '{':
                target.startObject();
                break;
            case '[':
                target.startArray();
                break;
            case '"': {
                String s = readString();
                if (target.isPropertyExpected()) {
                    if (nextCharAfterWhitespace() != ':') {
                        throw new IllegalArgumentException();
                    }
                    target.member(s);
                } else {
                    target.valueString(s);
                }
                break;
            }
            case '-':
                parseNumber(false);
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
                parseNumber(true);
                break;
            default:
                throw new IllegalArgumentException();
            }
        }
    }

    /**
     * Skip all whitespace characters, and get the next character.
     *
     * @return the character code
     */
    abstract int nextCharAfterWhitespace();

    /**
     * Read the specified keyword, or (it there is no match), throw an
     * IllegalArgumentException.
     *
     * @param keyword the expected keyword
     */
    abstract void readKeyword1(String keyword);

    /**
     * Parse a number.
     *
     * @param positive whether it needs to be positive
     */
    abstract void parseNumber(boolean positive);

    /**
     * Read the next character.
     *
     * @return the character code
     */
    abstract int nextChar();

    /**
     * Read 4 hex characters (0-9, a-f, A-F), and return the Unicode character.
     *
     * @return the character
     */
    abstract char readHex();

    private String readString() {
        builder.setLength(0);
        boolean inSurrogate = false;
        for (;;) {
            int ch = nextChar();
            switch (ch) {
            case '"':
                if (inSurrogate) {
                    throw new IllegalArgumentException();
                }
                return builder.toString();
            case '\\':
                ch = nextChar();
                switch (ch) {
                case '"':
                case '/':
                case '\\':
                    appendNonSurrogate((char) ch, inSurrogate);
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
                    inSurrogate = appendChar(readHex(), inSurrogate);
                    break;
                default:
                    throw new IllegalArgumentException();
                }
                break;
            default:
                if (Character.isBmpCodePoint(ch)) {
                    inSurrogate = appendChar((char) ch, inSurrogate);
                } else {
                    if (inSurrogate) {
                        throw new IllegalArgumentException();
                    }
                    builder.appendCodePoint(ch);
                    inSurrogate = false;
                }
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
