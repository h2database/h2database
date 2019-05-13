/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.h2.util.StringUtils;

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
     * Parses source bytes to a specified target.
     *
     * @param bytes
     *            source
     * @param target
     *            target
     */
    public static void parse(byte[] bytes, JSONTarget target) {
        int length = bytes.length;
        Charset charset = StandardCharsets.UTF_8;
        if (length >= 4) {
            byte b0 = bytes[0];
            byte b1 = bytes[1];
            byte b2 = bytes[2];
            byte b3 = bytes[3];
            switch (b0) {
            case -2:
                if (b1 == -1) {
                    charset = StandardCharsets.UTF_16BE;
                }
                break;
            case -1:
                if (b1 == -2) {
                    if (b2 == 0 && b3 == 0) {
                        charset = Charset.forName("UTF-32LE");
                    } else {
                        charset = StandardCharsets.UTF_16LE;
                    }
                }
                break;
            case 0:
                if (b1 != 0) {
                    charset = StandardCharsets.UTF_16BE;
                } else if (b2 == 0 && b3 != 0 || b2 == -2 && b3 == -1) {
                    charset = Charset.forName("UTF-32BE");
                }
                break;
            default:
                if (b1 == 0) {
                    if (b2 == 0 && b3 == 0) {
                        charset = Charset.forName("UTF-32LE");
                    } else {
                        charset = StandardCharsets.UTF_16LE;
                    }
                }
                break;
            }
        } else if (length >= 2) {
            byte b0 = bytes[0];
            byte b1 = bytes[1];
            if (b0 != 0) {
                if (b1 == 0) {
                    charset = StandardCharsets.UTF_16LE;
                }
            } else if (b1 != 0) {
                charset = StandardCharsets.UTF_16BE;
            }
        }
        new JSONStringSource(new String(bytes, charset), target).parse();
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
        return target.getResult();
    }

    /**
     * Converts bytes into normalized String JSON representation.
     *
     * @param bytes
     *            source representation
     * @return normalized representation
     */
    public static String normalize(byte[] bytes) {
        JSONStringTarget target = new JSONStringTarget();
        JSONStringSource.parse(bytes, target);
        return target.getResult();
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
        boolean comma = false;
        for (int ch; (ch = nextChar(length)) >= 0;) {
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
                readKeyword1("false", length);
                target.valueFalse();
                break;
            case 'n':
                readKeyword1("null", length);
                target.valueNull();
                break;
            case 't':
                readKeyword1("true", length);
                target.valueTrue();
                break;
            case '{':
                target.startObject();
                break;
            case '[':
                target.startArray();
                break;
            case '"': {
                String s = readString(length);
                ch = nextChar(length);
                if (ch == ':') {
                    target.member(s);
                } else {
                    if (ch >= 0) {
                        index--;
                    }
                    target.valueString(s);
                }
                break;
            }
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
    }

    private int nextChar(int length) {
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

    private void readKeyword1(String keyword, int length) {
        int l = keyword.length() - 1;
        if (!string.regionMatches(index, keyword, 1, l)) {
            throw new IllegalArgumentException();
        }
        index += l;
    }

    private void parseNumber(int length, boolean positive) {
        int index = this.index;
        int start = index - 1;
        index = skipInt(index, length, positive);
        l: if (index < length) {
            char ch = string.charAt(index);
            if (ch == '.') {
                index = skipInt(index + 1, length, false);
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
                index = skipInt(index, length, false);
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
                if (index >= length) {
                    throw new IllegalArgumentException();
                }
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

    @Override
    public String toString() {
        return StringUtils.addAsterisk(string, index);
    }

}
