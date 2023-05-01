/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import static org.h2.value.ValueToObjectConverter.GEOMETRY_CLASS;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
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

import org.h2.api.IntervalQualifier;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcResultSet;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.util.IntervalUtils;
import org.h2.util.JSR310Utils;
import org.h2.util.JdbcUtils;
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
            return TypeInfo.TYPE_NUMERIC_FLOATING_POINT;
        } else if (GEOMETRY_CLASS != null && GEOMETRY_CLASS.isAssignableFrom(clazz)) {
            return TypeInfo.TYPE_GEOMETRY;
        } else if (Array.class.isAssignableFrom(clazz)) {
            return TypeInfo.TYPE_ARRAY_UNKNOWN;
        } else if (ResultSet.class.isAssignableFrom(clazz)) {
            return TypeInfo.TYPE_ROW_EMPTY;
        } else {
            TypeInfo t = LegacyDateTimeUtils.legacyClassToType(clazz);
            if (t != null) {
                return t;
            }
            return TypeInfo.TYPE_JAVA_OBJECT;
        }
    }

    /**
     * Read a value from the given result set.
     *
     * @param session
     *            the session
     * @param rs
     *            the result set
     * @param columnIndex
     *            the column index (1-based)
     * @param type
     *            the data type
     * @return the value
     */
    public static Value readValue(Session session, ResultSet rs, int columnIndex, int type) {
        Value v;
        if (rs instanceof JdbcResultSet) {
            v = ValueToObjectConverter.readValue(session, (JdbcResultSet) rs, columnIndex);
        } else {
            try {
                v = readValueOther(session, rs, columnIndex, type);
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        }
        return v;
    }

    private static Value readValueOther(Session session, ResultSet rs, int columnIndex, int type)
            throws SQLException {
        Value v;
        switch (type) {
        case Value.NULL:
            v = ValueNull.INSTANCE;
            break;
        case Value.CHAR: {
            String s = rs.getString(columnIndex);
            v = (s == null) ? ValueNull.INSTANCE : ValueChar.get(s);
            break;
        }
        case Value.VARCHAR: {
            String s = rs.getString(columnIndex);
            v = (s == null) ? ValueNull.INSTANCE : ValueVarchar.get(s, session);
            break;
        }
        case Value.CLOB: {
            if (session == null) {
                String s = rs.getString(columnIndex);
                v = s == null ? ValueNull.INSTANCE : ValueClob.createSmall(s);
            } else {
                Reader in = rs.getCharacterStream(columnIndex);
                v = in == null ? ValueNull.INSTANCE
                        : session.addTemporaryLob(
                                session.getDataHandler().getLobStorage().createClob(new BufferedReader(in), -1));
            }
            break;
        }
        case Value.VARCHAR_IGNORECASE: {
            String s = rs.getString(columnIndex);
            v = s == null ? ValueNull.INSTANCE : ValueVarcharIgnoreCase.get(s);
            break;
        }
        case Value.BINARY: {
            byte[] bytes = rs.getBytes(columnIndex);
            v = bytes == null ? ValueNull.INSTANCE : ValueBinary.getNoCopy(bytes);
            break;
        }
        case Value.VARBINARY: {
            byte[] bytes = rs.getBytes(columnIndex);
            v = bytes == null ? ValueNull.INSTANCE : ValueVarbinary.getNoCopy(bytes);
            break;
        }
        case Value.BLOB: {
            if (session == null) {
                byte[] buff = rs.getBytes(columnIndex);
                v = buff == null ? ValueNull.INSTANCE : ValueBlob.createSmall(buff);
            } else {
                InputStream in = rs.getBinaryStream(columnIndex);
                v = in == null ? ValueNull.INSTANCE
                        : session.addTemporaryLob(session.getDataHandler().getLobStorage().createBlob(in, -1));
            }
            break;
        }
        case Value.BOOLEAN: {
            boolean value = rs.getBoolean(columnIndex);
            v = rs.wasNull() ? ValueNull.INSTANCE : ValueBoolean.get(value);
            break;
        }
        case Value.TINYINT: {
            byte value = rs.getByte(columnIndex);
            v = rs.wasNull() ? ValueNull.INSTANCE : ValueTinyint.get(value);
            break;
        }
        case Value.SMALLINT: {
            short value = rs.getShort(columnIndex);
            v = rs.wasNull() ? ValueNull.INSTANCE : ValueSmallint.get(value);
            break;
        }
        case Value.INTEGER: {
            int value = rs.getInt(columnIndex);
            v = rs.wasNull() ? ValueNull.INSTANCE : ValueInteger.get(value);
            break;
        }
        case Value.BIGINT: {
            long value = rs.getLong(columnIndex);
            v = rs.wasNull() ? ValueNull.INSTANCE : ValueBigint.get(value);
            break;
        }
        case Value.NUMERIC: {
            BigDecimal value = rs.getBigDecimal(columnIndex);
            v = value == null ? ValueNull.INSTANCE : ValueNumeric.getAnyScale(value);
            break;
        }
        case Value.REAL: {
            float value = rs.getFloat(columnIndex);
            v = rs.wasNull() ? ValueNull.INSTANCE : ValueReal.get(value);
            break;
        }
        case Value.DOUBLE: {
            double value = rs.getDouble(columnIndex);
            v = rs.wasNull() ? ValueNull.INSTANCE : ValueDouble.get(value);
            break;
        }
        case Value.DECFLOAT: {
            BigDecimal value = rs.getBigDecimal(columnIndex);
            v = value == null ? ValueNull.INSTANCE : ValueDecfloat.get(value);
            break;
        }
        case Value.DATE: {
            try {
                LocalDate value = rs.getObject(columnIndex, LocalDate.class);
                v = value == null ? ValueNull.INSTANCE : JSR310Utils.localDateToValue(value);
                break;
            } catch (SQLException ignore) {
                Date value = rs.getDate(columnIndex);
                v = value == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromDate(session, null, value);
            }
            break;
        }
        case Value.TIME: {
            try {
                LocalTime value = rs.getObject(columnIndex, LocalTime.class);
                v = value == null ? ValueNull.INSTANCE : JSR310Utils.localTimeToValue(value);
                break;
            } catch (SQLException ignore) {
                Time value = rs.getTime(columnIndex);
                v = value == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromTime(session, null, value);
            }
            break;
        }
        case Value.TIME_TZ: {
            try {
                OffsetTime value = rs.getObject(columnIndex, OffsetTime.class);
                v = value == null ? ValueNull.INSTANCE : JSR310Utils.offsetTimeToValue(value);
                break;
            } catch (SQLException ignore) {
                Object obj = rs.getObject(columnIndex);
                if (obj == null) {
                    v = ValueNull.INSTANCE;
                } else {
                    v = ValueTimeTimeZone.parse(obj.toString(), session);
                }
            }
            break;
        }
        case Value.TIMESTAMP: {
            try {
                LocalDateTime value = rs.getObject(columnIndex, LocalDateTime.class);
                v = value == null ? ValueNull.INSTANCE : JSR310Utils.localDateTimeToValue(value);
                break;
            } catch (SQLException ignore) {
                Timestamp value = rs.getTimestamp(columnIndex);
                v = value == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromTimestamp(session, null, value);
            }
            break;
        }
        case Value.TIMESTAMP_TZ: {
            try {
                OffsetDateTime value = rs.getObject(columnIndex, OffsetDateTime.class);
                v = value == null ? ValueNull.INSTANCE : JSR310Utils.offsetDateTimeToValue(value);
                break;
            } catch (SQLException ignore) {
                Object obj = rs.getObject(columnIndex);
                if (obj == null) {
                    v = ValueNull.INSTANCE;
                } else if (obj instanceof ZonedDateTime) {
                    v = JSR310Utils.zonedDateTimeToValue((ZonedDateTime) obj);
                } else {
                    v = ValueTimestampTimeZone.parse(obj.toString(), session);
                }
            }
            break;
        }
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND: {
            String s = rs.getString(columnIndex);
            v = s == null ? ValueNull.INSTANCE
                    : IntervalUtils.parseFormattedInterval(IntervalQualifier.valueOf(type - Value.INTERVAL_YEAR), s);
            break;
        }
        case Value.JAVA_OBJECT: {
            byte[] buff;
            try {
                buff = rs.getBytes(columnIndex);
            } catch (SQLException ignore) {
                try {
                    Object o = rs.getObject(columnIndex);
                    buff = o != null ? JdbcUtils.serialize(o, session.getJavaObjectSerializer()) : null;
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            }
            v = buff == null ? ValueNull.INSTANCE : ValueJavaObject.getNoCopy(buff);
            break;
        }
        case Value.ENUM: {
            int value = rs.getInt(columnIndex);
            v = rs.wasNull() ? ValueNull.INSTANCE : ValueInteger.get(value);
            break;
        }
        case Value.GEOMETRY: {
            Object x = rs.getObject(columnIndex);
            v = x == null ? ValueNull.INSTANCE : ValueGeometry.getFromGeometry(x);
            break;
        }
        case Value.JSON: {
            Object x = rs.getObject(columnIndex);
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                Class<?> clazz = x.getClass();
                if (clazz == byte[].class) {
                    v = ValueJson.fromJson((byte[]) x);
                } else if (clazz == String.class) {
                    v = ValueJson.fromJson((String) x);
                } else {
                    v = ValueJson.fromJson(x.toString());
                }
            }
            break;
        }
        case Value.UUID: {
            Object o = rs.getObject(columnIndex);
            if (o == null) {
                v = ValueNull.INSTANCE;
            } else if (o instanceof UUID) {
                v = ValueUuid.get((UUID) o);
            } else if (o instanceof byte[]) {
                v = ValueUuid.get((byte[]) o);
            } else {
                v = ValueUuid.get((String) o);
            }
            break;
        }
        case Value.ARRAY: {
            Array array = rs.getArray(columnIndex);
            if (array == null) {
                v = ValueNull.INSTANCE;
            } else {
                Object[] list = (Object[]) array.getArray();
                if (list == null) {
                    v = ValueNull.INSTANCE;
                } else {
                    int len = list.length;
                    Value[] values = new Value[len];
                    for (int i = 0; i < len; i++) {
                        values[i] = ValueToObjectConverter.objectToValue(session, list[i], Value.NULL);
                    }
                    v = ValueArray.get(values, session);
                }
            }
            break;
        }
        case Value.ROW: {
            Object o = rs.getObject(columnIndex);
            if (o == null) {
                v = ValueNull.INSTANCE;
            } else if (o instanceof ResultSet) {
                v = ValueToObjectConverter.resultSetToValue(session, (ResultSet) o);
            } else {
                Object[] list = (Object[]) o;
                int len = list.length;
                Value[] values = new Value[len];
                for (int i = 0; i < len; i++) {
                    values[i] = ValueToObjectConverter.objectToValue(session, list[i], Value.NULL);
                }
                v = ValueRow.get(values);
            }
            break;
        }
        default:
            throw DbException.getInternalError("data type " + type);
        }
        return v;
    }

    private ValueToObjectConverter2() {
    }

}
