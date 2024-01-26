/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Lazarev Nikita <lazarevn@ispras.ru>
 */
package org.h2.value;

import java.io.ByteArrayOutputStream;
import java.lang.ref.SoftReference;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import org.h2.util.json.JSONBoolean;
import org.h2.util.json.JSONByteArrayTarget;
import org.h2.util.json.JSONBytesSource;
import org.h2.util.json.JSONItemType;
import org.h2.util.json.JSONNull;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONStringSource;
import org.h2.util.json.JSONStringTarget;
import org.h2.util.json.JSONValue;
import org.h2.util.json.JSONValueTarget;

/**
 * Implementation of the JSON data type.
 */
public final class ValueJson extends ValueBytesBase {

    private static final byte[] NULL_BYTES = "null".getBytes(StandardCharsets.ISO_8859_1),
            TRUE_BYTES = "true".getBytes(StandardCharsets.ISO_8859_1),
            FALSE_BYTES = "false".getBytes(StandardCharsets.ISO_8859_1);

    /**
     * {@code null} JSON value.
     */
    public static final ValueJson NULL = new ValueJson(NULL_BYTES);

    /**
     * {@code true} JSON value.
     */
    public static final ValueJson TRUE = new ValueJson(TRUE_BYTES);

    /**
     * {@code false} JSON value.
     */
    public static final ValueJson FALSE = new ValueJson(FALSE_BYTES);

    /**
     * {@code 0} JSON value.
     */
    public static final ValueJson ZERO = new ValueJson(new byte[] { '0' });

    private volatile SoftReference<JSONValue> decompositionRef;

    private ValueJson(byte[] value) {
        super(value);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        String s = JSONBytesSource.parse(value, new JSONStringTarget(true));
        return builder.append("JSON '").append(s).append('\'');
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_JSON;
    }

    @Override
    public int getValueType() {
        return Value.JSON;
    }

    @Override
    public String getString() {
        return new String(value, StandardCharsets.UTF_8);
    }

    /**
     * Returns JSON item type.
     *
     * @return JSON item type
     */
    public JSONItemType getItemType() {
        switch (value[0]) {
        case '[':
            return JSONItemType.ARRAY;
        case '{':
            return JSONItemType.OBJECT;
        default:
            return JSONItemType.SCALAR;
        }
    }

    /**
     * Returns decomposed value.
     *
     * @return decomposed value.
     */
    public JSONValue getDecomposition() {
        SoftReference<JSONValue> decompositionRef = this.decompositionRef;
        JSONValue decomposition;
        if (decompositionRef == null || (decomposition = decompositionRef.get()) == null) {
            decomposition = JSONBytesSource.parse(value, new JSONValueTarget());
            this.decompositionRef = new SoftReference<>(decomposition);
        }
        return decomposition;
    }

    /**
     * Returns JSON value with the specified content.
     *
     * @param s
     *            JSON representation, will be normalized
     * @return JSON value
     * @throws DbException
     *             on invalid JSON
     */
    public static ValueJson fromJson(String s) {
        byte[] bytes;
        try {
            bytes = JSONStringSource.normalize(s);
        } catch (RuntimeException ex) {
            if (s.length() > 80) {
                s = new StringBuilder(83).append(s, 0, 80).append("...").toString();
            }
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
        }
        return getInternal(bytes);
    }

    /**
     * Returns JSON value with the specified content.
     *
     * @param bytes
     *            JSON representation, will be normalized
     * @return JSON value
     * @throws DbException
     *             on invalid JSON
     */
    public static ValueJson fromJson(byte[] bytes) {
        try {
            bytes = JSONBytesSource.normalize(bytes);
        } catch (RuntimeException ex) {
            StringBuilder builder = new StringBuilder().append("X'");
            if (bytes.length > 40) {
                StringUtils.convertBytesToHex(builder, bytes, 40).append("...");
            } else {
                StringUtils.convertBytesToHex(builder, bytes);
            }
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, builder.append('\'').toString());
        }
        return getInternal(bytes);
    }

    /**
     * Returns JSON value with the specified content.
     *
     * @param value
     *            JSON
     * @return JSON value
     * @throws DbException
     *             on invalid JSON
     */
    public static ValueJson fromJson(JSONValue value) {
        if (value instanceof JSONNull) {
            return NULL;
        }
        if (value instanceof JSONBoolean) {
            return ((JSONBoolean) value).getBoolean() ? TRUE : FALSE;
        }
        if (value instanceof JSONNumber) {
            // Use equals() to check both value and scale
            if (((JSONNumber) value).getBigDecimal().equals(BigDecimal.ZERO)) {
                return ZERO;
            }
        }
        JSONByteArrayTarget target = new JSONByteArrayTarget();
        value.addTo(target);
        ValueJson v = new ValueJson(target.getResult());
        v.decompositionRef = new SoftReference<>(value);
        return v;
    }

    /**
     * Returns JSON value with the specified boolean content.
     *
     * @param bool
     *            boolean value
     * @return JSON value
     */
    public static ValueJson get(boolean bool) {
        return bool ? TRUE : FALSE;
    }

    /**
     * Returns JSON value with the specified numeric content.
     *
     * @param number
     *            integer value
     * @return JSON value
     */
    public static ValueJson get(int number) {
        return number != 0 ? getNumber(Integer.toString(number)) : ZERO;
    }

    /**
     * Returns JSON value with the specified numeric content.
     *
     * @param number
     *            long value
     * @return JSON value
     */
    public static ValueJson get(long number) {
        return number != 0L ? getNumber(Long.toString(number)) : ZERO;
    }

    /**
     * Returns JSON value with the specified numeric content.
     *
     * @param number
     *            big decimal value
     * @return JSON value
     */
    public static ValueJson get(BigDecimal number) {
        if (number.signum() == 0 && number.scale() == 0) {
            return ZERO;
        }
        String s = number.toString();
        int index = s.indexOf('E');
        if (index >= 0 && s.charAt(++index) == '+') {
            int length = s.length();
            s = new StringBuilder(length - 1).append(s, 0, index).append(s, index + 1, length).toString();
        }
        return getNumber(s);
    }

    /**
     * Returns JSON value with the specified string content.
     *
     * @param string
     *            string value
     * @return JSON value
     */
    public static ValueJson get(String string) {
        return new ValueJson(JSONByteArrayTarget.encodeString( //
                new ByteArrayOutputStream(string.length() + 2), string).toByteArray());
    }

    /**
     * Returns JSON value with the specified content.
     *
     * @param bytes
     *            normalized JSON representation
     * @return JSON value
     */
    public static ValueJson getInternal(byte[] bytes) {
        int l = bytes.length;
        switch (l) {
        case 1:
            if (bytes[0] == '0') {
                return ZERO;
            }
            break;
        case 4:
            if (Arrays.equals(TRUE_BYTES, bytes)) {
                return TRUE;
            } else if (Arrays.equals(NULL_BYTES, bytes)) {
                return NULL;
            }
            break;
        case 5:
            if (Arrays.equals(FALSE_BYTES, bytes)) {
                return FALSE;
            }
        }
        return new ValueJson(bytes);
    }

    private static ValueJson getNumber(String s) {
        return new ValueJson(s.getBytes(StandardCharsets.ISO_8859_1));
    }

    @Override
    public int getMemory() {
        return value.length + 96;
    }

}
