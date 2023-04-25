/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * JSON byte array source.
 */
public final class JSONBytesSource extends JSONTextSource {

    /**
     * Parses source bytes to a specified target.
     *
     * @param bytes
     *            source
     * @param target
     *            target
     * @param <R>
     *            the type of the result
     * @return the result of the target
     */
    public static <R> R parse(byte[] bytes, JSONTarget<R> target) {
        int length = bytes.length;
        Charset charset = null;
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
        (charset == null ? new JSONBytesSource(bytes, target)
                : new JSONStringSource(new String(bytes, charset), target)).parse();
        return target.getResult();
    }

    /**
     * Converts bytes into normalized JSON representation.
     *
     * @param bytes
     *            source representation
     * @return normalized representation
     */
    public static byte[] normalize(byte[] bytes) {
        return parse(bytes, new JSONByteArrayTarget());
    }

    private final byte[] bytes;

    private final int length;

    private int index;

    JSONBytesSource(byte[] bytes, JSONTarget<?> target) {
        super(target);
        this.bytes = bytes;
        this.length = bytes.length;
        // Ignore BOM
        if (nextChar() != '\uFEFF') {
            index = 0;
        }
    }

    @Override
    int nextCharAfterWhitespace() {
        int index = this.index;
        while (index < length) {
            byte ch = bytes[index++];
            switch (ch) {
            case '\t':
            case '\n':
            case '\r':
            case ' ':
                break;
            default:
                if (ch < 0) {
                    throw new IllegalArgumentException();
                }
                this.index = index;
                return ch;
            }
        }
        return -1;
    }

    @Override
    void readKeyword1(String keyword) {
        int l = keyword.length() - 1;
        if (index + l > length) {
            throw new IllegalArgumentException();
        }
        for (int i = index, j = 1; j <= l; i++, j++) {
            if (bytes[i] != keyword.charAt(j)) {
                throw new IllegalArgumentException();
            }
        }
        index += l;
    }

    @Override
    void parseNumber(boolean positive) {
        int index = this.index;
        int start = index - 1;
        index = skipInt(index, positive);
        l: if (index < length) {
            byte ch = bytes[index];
            if (ch == '.') {
                index = skipInt(index + 1, false);
                if (index >= length) {
                    break l;
                }
                ch = bytes[index];
            }
            if (ch == 'E' || ch == 'e') {
                if (++index >= length) {
                    throw new IllegalArgumentException();
                }
                ch = bytes[index];
                if (ch == '+' || ch == '-') {
                    index++;
                }
                index = skipInt(index, false);
            }
        }
        target.valueNumber(new BigDecimal(new String(bytes, start, index - start, StandardCharsets.ISO_8859_1)));
        this.index = index;
    }

    private int skipInt(int index, boolean hasInt) {
        while (index < length) {
            byte ch = bytes[index];
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
        int b1 = bytes[index++] & 0xff;
        if (b1 >= 0x80) {
            if (b1 >= 0xe0) {
                if (b1 >= 0xf0) {
                    if (index + 2 >= length) {
                        throw new IllegalArgumentException();
                    }
                    int b2 = bytes[index++] & 0xff;
                    int b3 = bytes[index++] & 0xff;
                    int b4 = bytes[index++] & 0xff;
                    b1 = ((b1 & 0xf) << 18) + ((b2 & 0x3f) << 12) + ((b3 & 0x3f) << 6) + (b4 & 0x3f);
                    if (b1 < 0x10000 || b1 > Character.MAX_CODE_POINT || (b2 & 0xc0) != 0x80 || (b3 & 0xc0) != 0x80
                            || (b4 & 0xc0) != 0x80) {
                        throw new IllegalArgumentException();
                    }
                } else {
                    if (index + 1 >= length) {
                        throw new IllegalArgumentException();
                    }
                    int b2 = bytes[index++] & 0xff;
                    int b3 = bytes[index++] & 0xff;
                    b1 = ((b1 & 0xf) << 12) + ((b2 & 0x3f) << 6) + (b3 & 0x3f);
                    if (b1 < 0x800 || (b2 & 0xc0) != 0x80 || (b3 & 0xc0) != 0x80) {
                        throw new IllegalArgumentException();
                    }
                }
            } else {
                if (index >= length) {
                    throw new IllegalArgumentException();
                }
                int b2 = bytes[index++] & 0xff;
                b1 = ((b1 & 0x1f) << 6) + (b2 & 0x3f);
                if (b1 < 0x80 || (b2 & 0xc0) != 0x80) {
                    throw new IllegalArgumentException();
                }
            }
        }
        return b1;
    }

    @Override
    char readHex() {
        if (index + 3 >= length) {
            throw new IllegalArgumentException();
        }
        int ch;
        try {
            ch = Integer.parseInt(new String(bytes, index, 4, StandardCharsets.ISO_8859_1), 16);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException();
        }
        index += 4;
        return (char) ch;
    }

    @Override
    public String toString() {
        return new String(bytes, 0, index, StandardCharsets.UTF_8) + "[*]"
                + new String(bytes, index, length, StandardCharsets.UTF_8);
    }

}
