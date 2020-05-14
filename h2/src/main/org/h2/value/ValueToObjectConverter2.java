/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import static org.h2.value.ValueToObjectConverter.GEOMETRY_CLASS;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
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

import org.h2.message.TraceObject;
import org.h2.util.LegacyDateTimeUtils;
import org.h2.util.Utils;

/**
 * Data type conversion methods between values and Java objects to use on the
 * server side on H2 only.
 */
public final class ValueToObjectConverter2 extends TraceObject {

    /**
     * Get the type information for the given Java class.
     *
     * @param clazz
     *            the Java class
     * @return the value type
     */
    public static TypeInfo classToType(Class<?> clazz) {
        if (clazz == null) {
            return TypeInfo.TYPE_NULL;
        }
        if (clazz.isPrimitive()) {
            clazz = Utils.getNonPrimitiveClass(clazz);
        }
        if (clazz == Void.class) {
            return TypeInfo.TYPE_NULL;
        } else if (clazz == String.class || clazz == Character.class) {
            return TypeInfo.TYPE_VARCHAR;
        } else if (clazz == byte[].class) {
            return TypeInfo.TYPE_VARBINARY;
        } else if (clazz == Boolean.class) {
            return TypeInfo.TYPE_BOOLEAN;
        } else if (clazz == Byte.class) {
            return TypeInfo.TYPE_TINYINT;
        } else if (clazz == Short.class) {
            return TypeInfo.TYPE_SMALLINT;
        } else if (clazz == Integer.class) {
            return TypeInfo.TYPE_INTEGER;
        } else if (clazz == Long.class) {
            return TypeInfo.TYPE_BIGINT;
        } else if (clazz == Float.class) {
            return TypeInfo.TYPE_REAL;
        } else if (clazz == Double.class) {
            return TypeInfo.TYPE_DOUBLE;
        } else if (clazz == LocalDate.class) {
            return TypeInfo.TYPE_DATE;
        } else if (clazz == LocalTime.class) {
            return TypeInfo.TYPE_TIME;
        } else if (clazz == OffsetTime.class) {
            return TypeInfo.TYPE_TIME_TZ;
        } else if (clazz == LocalDateTime.class) {
            return TypeInfo.TYPE_TIMESTAMP;
        } else if (clazz == OffsetDateTime.class || clazz == ZonedDateTime.class || clazz == Instant.class) {
            return TypeInfo.TYPE_TIMESTAMP_TZ;
        } else if (clazz == Period.class) {
            return TypeInfo.TYPE_INTERVAL_YEAR_TO_MONTH;
        } else if (clazz == Duration.class) {
            return TypeInfo.TYPE_INTERVAL_DAY_TO_SECOND;
        } else if (UUID.class == clazz) {
            return TypeInfo.TYPE_UUID;
        } else if (clazz.isArray()) {
            return TypeInfo.getTypeInfo(Value.ARRAY, Integer.MAX_VALUE, 0, classToType(clazz.getComponentType()));
        } else if (Clob.class.isAssignableFrom(clazz) || Reader.class.isAssignableFrom(clazz)) {
            return TypeInfo.TYPE_CLOB;
        } else if (Blob.class.isAssignableFrom(clazz) || InputStream.class.isAssignableFrom(clazz)) {
            return TypeInfo.TYPE_BLOB;
        } else if (BigDecimal.class.isAssignableFrom(clazz)) {
            return TypeInfo.TYPE_NUMERIC;
        } else if (GEOMETRY_CLASS != null && GEOMETRY_CLASS.isAssignableFrom(clazz)) {
            return TypeInfo.TYPE_GEOMETRY;
        } else if (Array.class.isAssignableFrom(clazz)) {
            return TypeInfo.TYPE_ARRAY;
        } else if (ResultSet.class.isAssignableFrom(clazz)) {
            return TypeInfo.TYPE_RESULT_SET;
        } else {
            TypeInfo t = LegacyDateTimeUtils.legacyClassToType(clazz);
            if (t != null) {
                return t;
            }
            return TypeInfo.TYPE_JAVA_OBJECT;
        }
    }

    private ValueToObjectConverter2() {
    }

}
