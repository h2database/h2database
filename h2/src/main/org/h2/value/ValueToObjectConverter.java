/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLXML;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.UUID;

import org.h2.api.Interval;
import org.h2.jdbc.JdbcArray;
import org.h2.jdbc.JdbcBlob;
import org.h2.jdbc.JdbcClob;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcLob;
import org.h2.jdbc.JdbcResultSet;
import org.h2.jdbc.JdbcSQLXML;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.util.JSR310Utils;
import org.h2.util.JdbcUtils;
import org.h2.util.LegacyDateTimeUtils;

/**
 * Data type conversion methods between values and Java objects.
 */
public final class ValueToObjectConverter extends TraceObject {

    /**
     * Converts the specified value to an object of the specified type.
     *
     * @param <T>
     *            the type
     * @param type
     *            the class
     * @param value
     *            the value
     * @param conn
     *            the connection
     * @return the object of the specified class representing the specified
     *         value, or {@code null}
     */
    @SuppressWarnings("unchecked")
    public static <T> T valueToObject(Class<T> type, Value value, JdbcConnection conn) {
        if (value == ValueNull.INSTANCE) {
            return null;
        } else if (type == BigDecimal.class) {
            return (T) value.getBigDecimal();
        } else if (type == BigInteger.class) {
            return (T) value.getBigDecimal().toBigInteger();
        } else if (type == String.class) {
            return (T) value.getString();
        } else if (type == Boolean.class) {
            return (T) (Boolean) value.getBoolean();
        } else if (type == Byte.class) {
            return (T) (Byte) value.getByte();
        } else if (type == Short.class) {
            return (T) (Short) value.getShort();
        } else if (type == Integer.class) {
            return (T) (Integer) value.getInt();
        } else if (type == Long.class) {
            return (T) (Long) value.getLong();
        } else if (type == Float.class) {
            return (T) (Float) value.getFloat();
        } else if (type == Double.class) {
            return (T) (Double) value.getDouble();
        } else if (type == UUID.class) {
            return (T) value.convertToUuid().getObject();
        } else if (type == byte[].class) {
            return (T) value.getBytes();
        } else if (type == Interval.class) {
            if (!(value instanceof ValueInterval)) {
                value = value.convertTo(TypeInfo.TYPE_INTERVAL_DAY_TO_SECOND);
            }
            ValueInterval v = (ValueInterval) value;
            return (T) new Interval(v.getQualifier(), false, v.getLeading(), v.getRemaining());
        } else if (DataType.isGeometryClass(type)) {
            return (T) value.convertToGeometry(null).getObject();
        } else if (type == LocalDate.class) {
            return (T) JSR310Utils.valueToLocalDate(value, conn);
        } else if (type == LocalTime.class) {
            return (T) JSR310Utils.valueToLocalTime(value, conn);
        } else if (type == LocalDateTime.class) {
            return (T) JSR310Utils.valueToLocalDateTime(value, conn);
        } else if (type == Instant.class) {
            return (T) JSR310Utils.valueToInstant(value, conn);
        } else if (type == OffsetTime.class) {
            return (T) JSR310Utils.valueToOffsetTime(value, conn);
        } else if (type == OffsetDateTime.class) {
            return (T) JSR310Utils.valueToOffsetDateTime(value, conn);
        } else if (type == ZonedDateTime.class) {
            return (T) JSR310Utils.valueToZonedDateTime(value, conn);
        } else if (type == Period.class) {
            return (T) JSR310Utils.valueToPeriod(value);
        } else if (type == Duration.class) {
            return (T) JSR310Utils.valueToDuration(value);
        } else if (type.isArray()) {
            return (T) valueToArray(type, value, conn);
        } else {
            return (T) valueToOther(type, value, conn);
        }
    }

    private static Object valueToArray(Class<?> type, Value value, JdbcConnection conn) {
        Value[] array = ((ValueArray) value).getList();
        Class<?> componentType = type.getComponentType();
        int length = array.length;
        Object[] objArray = (Object[]) java.lang.reflect.Array.newInstance(componentType, length);
        for (int i = 0; i < length; i++) {
            objArray[i] = valueToObject(componentType, array[i], conn);
        }
        return objArray;
    }

    private static Object valueToOther(Class<?> type, Value value, JdbcConnection conn) {
        if (type == Object.class) {
            return JdbcUtils.deserialize(
                    value.convertToJavaObject(TypeInfo.TYPE_JAVA_OBJECT, Value.CONVERT_TO, null).getBytesNoCopy(),
                    conn.getJavaObjectSerializer());
        } else if (type == InputStream.class) {
            return value.getInputStream();
        } else if (type == Reader.class) {
            return value.getReader();
        } else if (type == java.sql.Array.class) {
            return new JdbcArray(conn, value, getNextId(TraceObject.ARRAY));
        } else if (type == Blob.class) {
            return new JdbcBlob(conn, value, JdbcLob.State.WITH_VALUE, getNextId(TraceObject.BLOB));
        } else if (type == Clob.class) {
            return new JdbcClob(conn, value, JdbcLob.State.WITH_VALUE, getNextId(TraceObject.CLOB));
        } else if (type == SQLXML.class) {
            return new JdbcSQLXML(conn, value, JdbcLob.State.WITH_VALUE, getNextId(TraceObject.SQLXML));
        } else if (type == ResultSet.class) {
            return new JdbcResultSet(conn, null, null, value.convertToResultSet().getResult(),
                    getNextId(TraceObject.RESULT_SET), false, true, false);
        } else {
            Object obj = LegacyDateTimeUtils.valueToLegacyType(type, value, conn);
            if (obj != null) {
                return obj;
            }
            if (value.getValueType() == Value.JAVA_OBJECT) {
                obj = JdbcUtils.deserialize(value.getBytesNoCopy(), conn.getJavaObjectSerializer());
                if (type.isAssignableFrom(obj.getClass())) {
                    return obj;
                }
            }
            throw DbException.getUnsupportedException("converting to class " + type.getName());
        }
    }

    private ValueToObjectConverter() {
    }

}
