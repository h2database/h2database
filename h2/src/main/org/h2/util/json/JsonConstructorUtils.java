/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.io.ByteArrayOutputStream;

import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;

/**
 * Utilities for JSON constructors.
 */
public final class JsonConstructorUtils {

    /**
     * The ABSENT ON NULL flag.
     */
    public static final int JSON_ABSENT_ON_NULL = 1;

    /**
     * The WITH UNIQUE KEYS flag.
     */
    public static final int JSON_WITH_UNIQUE_KEYS = 2;

    private JsonConstructorUtils() {
    }

    /**
     * Appends a value to a JSON object in the specified string builder.
     *
     * @param baos
     *            the output stream to append to
     * @param key
     *            the name of the property
     * @param value
     *            the value of the property
     */
    public static void jsonObjectAppend(ByteArrayOutputStream baos, String key, Value value) {
        if (baos.size() > 1) {
            baos.write(',');
        }
        JSONByteArrayTarget.encodeString(baos, key).write(':');
        byte[] b = value.convertToJson(TypeInfo.TYPE_JSON, Value.CONVERT_TO, null).getBytesNoCopy();
        baos.write(b, 0, b.length);
    }

    /**
     * Appends trailing closing brace to the specified string builder with a
     * JSON object, validates it, and converts to a JSON value.
     *
     * @param baos
     *            the output stream with the object
     * @param flags
     *            the flags ({@link #JSON_WITH_UNIQUE_KEYS})
     * @return the JSON value
     * @throws DbException
     *             if {@link #JSON_WITH_UNIQUE_KEYS} is specified and keys are
     *             not unique
     */
    public static Value jsonObjectFinish(ByteArrayOutputStream baos, int flags) {
        baos.write('}');
        byte[] result = baos.toByteArray();
        if ((flags & JSON_WITH_UNIQUE_KEYS) != 0) {
            try {
                JSONBytesSource.parse(result, new JSONValidationTargetWithUniqueKeys());
            } catch (RuntimeException ex) {
                String s = JSONBytesSource.parse(result, new JSONStringTarget());
                throw DbException.getInvalidValueException("JSON WITH UNIQUE KEYS",
                        s.length() < 128 ? result : s.substring(0, 128) + "...");
            }
        }
        return ValueJson.getInternal(result);
    }

    /**
     * Appends a value to a JSON array in the specified output stream.
     *
     * @param baos
     *            the output stream to append to
     * @param value
     *            the value
     * @param flags
     *            the flags ({@link #JSON_ABSENT_ON_NULL})
     */
    public static void jsonArrayAppend(ByteArrayOutputStream baos, Value value, int flags) {
        if (value == ValueNull.INSTANCE || value == ValueJson.NULL) {
            if ((flags & JSON_ABSENT_ON_NULL) != 0) {
                return;
            }
            value = ValueJson.NULL;
        }
        if (baos.size() > 1) {
            baos.write(',');
        }
        byte[] b = value.convertTo(TypeInfo.TYPE_JSON).getBytesNoCopy();
        baos.write(b, 0, b.length);
    }

}
