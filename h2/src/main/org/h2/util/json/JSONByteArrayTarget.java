/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import static org.h2.util.json.JSONStringTarget.ARRAY;
import static org.h2.util.json.JSONStringTarget.HEX;
import static org.h2.util.json.JSONStringTarget.OBJECT;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import org.h2.util.ByteStack;

/**
 * JSON byte array target.
 */
public final class JSONByteArrayTarget extends JSONTarget<byte[]> {

    private static final byte[] NULL_BYTES = "null".getBytes(StandardCharsets.ISO_8859_1);

    private static final byte[] FALSE_BYTES = "false".getBytes(StandardCharsets.ISO_8859_1);

    private static final byte[] TRUE_BYTES = "true".getBytes(StandardCharsets.ISO_8859_1);

    private static final byte[] U00_BYTES = "\\u00".getBytes(StandardCharsets.ISO_8859_1);

    /**
     * Encodes a JSON string and appends it to the specified output stream.
     *
     * @param baos
     *            the output stream to append to
     * @param s
     *            the string to encode
     * @return the specified output stream
     */
    public static ByteArrayOutputStream encodeString(ByteArrayOutputStream baos, String s) {
        baos.write('"');
        for (int i = 0, length = s.length(); i < length; i++) {
            char c = s.charAt(i);
            switch (c) {
            case '\b':
                baos.write('\\');
                baos.write('b');
                break;
            case '\t':
                baos.write('\\');
                baos.write('t');
                break;
            case '\f':
                baos.write('\\');
                baos.write('f');
                break;
            case '\n':
                baos.write('\\');
                baos.write('n');
                break;
            case '\r':
                baos.write('\\');
                baos.write('r');
                break;
            case '"':
                baos.write('\\');
                baos.write('"');
                break;
            case '\\':
                baos.write('\\');
                baos.write('\\');
                break;
            default:
                if (c >= ' ') {
                    if (c < 0x80) {
                        baos.write(c);
                    } else if (c < 0x800) {
                        baos.write(0xc0 | c >> 6);
                        baos.write(0x80 | c & 0x3f);
                    } else if (!Character.isSurrogate(c)) {
                        baos.write(0xe0 | c >> 12);
                        baos.write(0x80 | c >> 6 & 0x3f);
                        baos.write(0x80 | c & 0x3f);
                    } else {
                        char c2;
                        if (!Character.isHighSurrogate(c) || ++i >= length
                                || !Character.isLowSurrogate(c2 = s.charAt(i))) {
                            throw new IllegalArgumentException();
                        }
                        int uc = Character.toCodePoint(c, c2);
                        baos.write(0xf0 | uc >> 18);
                        baos.write(0x80 | uc >> 12 & 0x3f);
                        baos.write(0x80 | uc >> 6 & 0x3f);
                        baos.write(0x80 | uc & 0x3f);
                    }
                } else {
                    baos.write(U00_BYTES, 0, 4);
                    baos.write(HEX[c >>> 4 & 0xf]);
                    baos.write(HEX[c & 0xf]);
                }
            }
        }
        baos.write('"');
        return baos;
    }

    private final ByteArrayOutputStream baos;

    private final ByteStack stack;

    private boolean needSeparator;

    private boolean afterName;

    /**
     * Creates new instance of JSON byte array target.
     */
    public JSONByteArrayTarget() {
        baos = new ByteArrayOutputStream();
        stack = new ByteStack();
    }

    @Override
    public void startObject() {
        beforeValue();
        afterName = false;
        stack.push(OBJECT);
        baos.write('{');
    }

    @Override
    public void endObject() {
        if (afterName || stack.poll(-1) != OBJECT) {
            throw new IllegalStateException();
        }
        baos.write('}');
        afterValue();
    }

    @Override
    public void startArray() {
        beforeValue();
        afterName = false;
        stack.push(ARRAY);
        baos.write('[');
    }

    @Override
    public void endArray() {
        if (stack.poll(-1) != ARRAY) {
            throw new IllegalStateException();
        }
        baos.write(']');
        afterValue();
    }

    @Override
    public void member(String name) {
        if (afterName || stack.peek(-1) != OBJECT) {
            throw new IllegalStateException();
        }
        afterName = true;
        beforeValue();
        encodeString(baos, name).write(':');
    }

    @Override
    public void valueNull() {
        beforeValue();
        baos.write(NULL_BYTES, 0, 4);
        afterValue();
    }

    @Override
    public void valueFalse() {
        beforeValue();
        baos.write(FALSE_BYTES, 0, 5);
        afterValue();
    }

    @Override
    public void valueTrue() {
        beforeValue();
        baos.write(TRUE_BYTES, 0, 4);
        afterValue();
    }

    @Override
    public void valueNumber(BigDecimal number) {
        beforeValue();
        String s = number.toString();
        int index = s.indexOf('E');
        byte[] b = s.getBytes(StandardCharsets.ISO_8859_1);
        if (index >= 0 && s.charAt(++index) == '+') {
            baos.write(b, 0, index);
            baos.write(b, index + 1, b.length - index - 1);
        } else {
            baos.write(b, 0, b.length);
        }
        afterValue();
    }

    @Override
    public void valueString(String string) {
        beforeValue();
        encodeString(baos, string);
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
            baos.write(',');
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
    public byte[] getResult() {
        if (!stack.isEmpty() || baos.size() == 0) {
            throw new IllegalStateException();
        }
        return baos.toByteArray();
    }

}
