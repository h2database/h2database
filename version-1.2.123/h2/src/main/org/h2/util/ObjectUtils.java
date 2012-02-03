/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;

/**
 * Utility class for object creation and serialization.
 * Starting with Java 1.5, some objects are re-used.
 */
public class ObjectUtils {

    /**
     * The maximum number of elements to copy using a Java loop. This value was
     * found by running tests using the Sun JDK 1.4 and JDK 1.6 on Windows XP.
     * The biggest difference is for size smaller than 40 (more than 50% saving).
     */
    private static final int MAX_JAVA_LOOP_COPY = 50;

    private ObjectUtils() {
        // utility class
    }

    /**
     * Serialize the object to a byte array.
     *
     * @param obj the object to serialize
     * @return the byte array
     */
    public static byte[] serialize(Object obj) throws SQLException {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            return out.toByteArray();
        } catch (Throwable e) {
            throw Message.getSQLException(ErrorCode.SERIALIZATION_FAILED_1, e, e.toString());
        }
    }

    /**
     * De-serialize the byte array to an object.
     *
     * @param data the byte array
     * @return the object
     * @throws SQLException
     */
    public static Object deserialize(byte[] data) throws SQLException {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ObjectInputStream is = new ObjectInputStream(in);
            Object obj = is.readObject();
            return obj;
        } catch (Throwable e) {
            throw Message.getSQLException(ErrorCode.DESERIALIZATION_FAILED_1, e, e.toString());
        }
    }

    /**
     * Copy the elements of the source array to the target array.
     * System.arraycopy is used for larger arrays, but for very small arrays it
     * is faster to use a regular loop.
     *
     * @param source the source array
     * @param target the target array
     * @param size the number of elements to copy
     */
    public static void arrayCopy(Object[] source, Object[] target, int size) {
        if (size > MAX_JAVA_LOOP_COPY) {
            System.arraycopy(source, 0, target, 0, size);
        } else {
            for (int i = 0; i < size; i++) {
                target[i] = source[i];
            }
        }
    }

    /**
     * Calculate the hash code of the given object. The object may be null.
     *
     * @param o the object
     * @return the hash code, or 0 if the object is null
     */
    public static int hashCode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

}
