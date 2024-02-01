/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.UUID;

import org.h2.api.ErrorCode;
import org.h2.api.Interval;
import org.h2.engine.Session;
import org.h2.expression.Format;
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
     * The Geometry class. This object is null if the JTS jar file is not in the
     * classpath.
     */
    public static final Class<?> GEOMETRY_CLASS;

    private static final String GEOMETRY_CLASS_NAME = "org.locationtech.jts.geom.Geometry";

    static {
        Class<?> g;
        try {
            g = JdbcUtils.loadUserClass(GEOMETRY_CLASS_NAME);
        } catch (Exception e) {
            g = null;
        }
        GEOMETRY_CLASS = g;
    }

    /**
     * Convert a Java object to a value.
     *
     * @param session
     *            the session
     * @param x
     *            the value
     * @param type
     *            the suggested value type, or {@code Value#UNKNOWN}
     * @return the value
     */
    public static Value objectToValue(Session session, Object x, int type) {
        if (x == null) {
            return ValueNull.INSTANCE;
        } else if (type == Value.JAVA_OBJECT) {
            return ValueJavaObject.getNoCopy(JdbcUtils.serialize(x, session.getJavaObjectSerializer()));
        }
        Value v;
        Class<?> clazz;
        if (x instanceof Value) {
            v = (Value) x;
            if (v instanceof ValueLob) {
                session.addTemporaryLob((ValueLob) v);
            }
        } else if ((clazz = x.getClass()) == String.class) {
            v = ValueVarchar.get((String) x, session);
        } else if (clazz == Long.class) {
            v = ValueBigint.get((Long) x);
        } else if (clazz == Integer.class) {
            v = ValueInteger.get((Integer) x);
        } else if (clazz == Boolean.class) {
            v = ValueBoolean.get((Boolean) x);
        } else if (clazz == Byte.class) {
            v = ValueTinyint.get((Byte) x);
        } else if (clazz == Short.class) {
            v = ValueSmallint.get((Short) x);
        } else if (clazz == Float.class) {
            v = ValueReal.get((Float) x);
        } else if (clazz == Double.class) {
            v = ValueDouble.get((Double) x);
        } else if (clazz == byte[].class) {
            v = ValueVarbinary.get((byte[]) x);
        } else if (clazz == UUID.class) {
            v = ValueUuid.get((UUID) x);
        } else if (clazz == Character.class) {
            v = ValueChar.get(((Character) x).toString());
        } else if (clazz == LocalDate.class) {
            v = JSR310Utils.localDateToValue((LocalDate) x);
        } else if (clazz == LocalTime.class) {
            v = JSR310Utils.localTimeToValue((LocalTime) x);
        } else if (clazz == LocalDateTime.class) {
            v = JSR310Utils.localDateTimeToValue((LocalDateTime) x);
        } else if (clazz == Instant.class) {
            v = JSR310Utils.instantToValue((Instant) x);
        } else if (clazz == OffsetTime.class) {
            v = JSR310Utils.offsetTimeToValue((OffsetTime) x);
        } else if (clazz == OffsetDateTime.class) {
            v = JSR310Utils.offsetDateTimeToValue((OffsetDateTime) x);
        } else if (clazz == ZonedDateTime.class) {
            v = JSR310Utils.zonedDateTimeToValue((ZonedDateTime) x);
        } else if (clazz == Interval.class) {
            Interval i = (Interval) x;
            v = ValueInterval.from(i.getQualifier(), i.isNegative(), i.getLeading(), i.getRemaining());
        } else if (clazz == Period.class) {
            v = JSR310Utils.periodToValue((Period) x);
        } else if (clazz == Duration.class) {
            v = JSR310Utils.durationToValue((Duration) x);
        } else if (x instanceof Object[]) {
            v = arrayToValue(session, x);
        } else if (GEOMETRY_CLASS != null && GEOMETRY_CLASS.isAssignableFrom(clazz)) {
            v = ValueGeometry.getFromGeometry(x);
        } else if (x instanceof BigInteger) {
            v = ValueNumeric.get((BigInteger) x);
        } else if (x instanceof BigDecimal) {
            v = ValueNumeric.getAnyScale((BigDecimal) x);
        } else {
            v = otherToValue(session, x);
        }
        if (type == Value.JSON) {
            v = Format.applyJSON(v);
        }
        return v;
    }

    private static Value otherToValue(Session session, Object x) {
        if (x instanceof Array) {
            Array array = (Array) x;
            try {
                return arrayToValue(session, array.getArray());
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else if (x instanceof ResultSet) {
            return resultSetToValue(session, (ResultSet) x);
        }
        ValueLob lob;
        if (x instanceof Reader) {
            Reader r = (Reader) x;
            if (!(r instanceof BufferedReader)) {
                r = new BufferedReader(r);
            }
            lob = session.getDataHandler().getLobStorage().createClob(r, -1);
        } else if (x instanceof Clob) {
            try {
                Clob clob = (Clob) x;
                Reader r = new BufferedReader(clob.getCharacterStream());
                lob = session.getDataHandler().getLobStorage().createClob(r, clob.length());
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else if (x instanceof InputStream) {
            lob = session.getDataHandler().getLobStorage().createBlob((InputStream) x, -1);
        } else if (x instanceof Blob) {
            try {
                Blob blob = (Blob) x;
                lob = session.getDataHandler().getLobStorage().createBlob(blob.getBinaryStream(), blob.length());
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else if (x instanceof SQLXML) {
            try {
                lob = session.getDataHandler().getLobStorage()
                        .createClob(new BufferedReader(((SQLXML) x).getCharacterStream()), -1);
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else {
            Value v = LegacyDateTimeUtils.legacyObjectToValue(session, x);
            if (v != null) {
                return v;
            }
            return ValueJavaObject.getNoCopy(JdbcUtils.serialize(x, session.getJavaObjectSerializer()));
        }
        return session.addTemporaryLob(lob);
    }

    private static Value arrayToValue(Session session, Object x) {
        // (a.getClass().isArray());
        // (a.getClass().getComponentType().isPrimitive());
        Object[] o = (Object[]) x;
        int len = o.length;
        Value[] v = new Value[len];
        for (int i = 0; i < len; i++) {
            v[i] = objectToValue(session, o[i], Value.UNKNOWN);
        }
        return ValueArray.get(v, session);
    }

    static Value resultSetToValue(Session session, ResultSet rs) {
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            LinkedHashMap<String, TypeInfo> columns = readResultSetMeta(session, meta, columnCount);
            if (!rs.next()) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, "Empty ResultSet to ROW value");
            }
            Value[] list = new Value[columnCount];
            Iterator<Entry<String, TypeInfo>> iterator = columns.entrySet().iterator();
            for (int j = 0; j < columnCount; j++) {
                list[j] = ValueToObjectConverter.objectToValue(session, rs.getObject(j + 1),
                        iterator.next().getValue().getValueType());
            }
            if (rs.next()) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, "Multi-row ResultSet to ROW value");
            }
            return ValueRow.get(new ExtTypeInfoRow(columns), list);
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private static LinkedHashMap<String, TypeInfo> readResultSetMeta(Session session, ResultSetMetaData meta,
            int columnCount) throws SQLException {
        LinkedHashMap<String, TypeInfo> columns = new LinkedHashMap<>();
        for (int i = 0; i < columnCount; i++) {
            String alias = meta.getColumnLabel(i + 1);
            String columnTypeName = meta.getColumnTypeName(i + 1);
            int columnType = DataType.convertSQLTypeToValueType(meta.getColumnType(i + 1), columnTypeName);
            int precision = meta.getPrecision(i + 1);
            int scale = meta.getScale(i + 1);
            TypeInfo typeInfo;
            if (columnType == Value.ARRAY && columnTypeName.endsWith(" ARRAY")) {
                typeInfo = TypeInfo
                        .getTypeInfo(Value.ARRAY, -1L, 0,
                                TypeInfo.getTypeInfo(DataType.getTypeByName(
                                        columnTypeName.substring(0, columnTypeName.length() - 6),
                                        session.getMode()).type));
            } else {
                typeInfo = TypeInfo.getTypeInfo(columnType, precision, scale, null);
            }
            columns.put(alias, typeInfo);
        }
        return columns;
    }

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
            return (T) value.convertToUuid().getUuid();
        } else if (type == byte[].class) {
            return (T) value.getBytes();
        } else if (type == Character.class) {
            String s = value.getString();
            return (T) (Character) (s.isEmpty() ? ' ' : s.charAt(0));
        } else if (type == Interval.class) {
            if (!(value instanceof ValueInterval)) {
                value = value.convertTo(TypeInfo.TYPE_INTERVAL_DAY_TO_SECOND);
            }
            ValueInterval v = (ValueInterval) value;
            return (T) new Interval(v.getQualifier(), false, v.getLeading(), v.getRemaining());
        } else if (type == LocalDate.class) {
            return (T) JSR310Utils.valueToLocalDate(value, conn);
        } else if (type == LocalTime.class) {
            return (T) JSR310Utils.valueToLocalTime(value, conn);
        } else if (type == LocalDateTime.class) {
            return (T) JSR310Utils.valueToLocalDateTime(value, conn);
        } else if (type == OffsetTime.class) {
            return (T) JSR310Utils.valueToOffsetTime(value, conn);
        } else if (type == OffsetDateTime.class) {
            return (T) JSR310Utils.valueToOffsetDateTime(value, conn);
        } else if (type == ZonedDateTime.class) {
            return (T) JSR310Utils.valueToZonedDateTime(value, conn);
        } else if (type == Instant.class) {
            return (T) JSR310Utils.valueToInstant(value, conn);
        } else if (type == Period.class) {
            return (T) JSR310Utils.valueToPeriod(value);
        } else if (type == Duration.class) {
            return (T) JSR310Utils.valueToDuration(value);
        } else if (type.isArray()) {
            return (T) valueToArray(type, value, conn);
        } else if (GEOMETRY_CLASS != null && GEOMETRY_CLASS.isAssignableFrom(type)) {
            return (T) value.convertToGeometry(null).getGeometry();
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
            return new JdbcResultSet(conn, null, null, value.convertToAnyRow().getResult(),
                    getNextId(TraceObject.RESULT_SET), true, false, false);
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

    /**
     * Get the name of the Java class for the given value type.
     *
     * @param type
     *            the value type
     * @param forJdbc
     *            if {@code true} get class for JDBC layer, if {@code false} get
     *            class for Java functions API
     * @return the class
     */
    public static Class<?> getDefaultClass(int type, boolean forJdbc) {
        switch (type) {
        case Value.NULL:
            return Void.class;
        case Value.CHAR:
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.ENUM:
            return String.class;
        case Value.CLOB:
            return Clob.class;
        case Value.BINARY:
        case Value.VARBINARY:
        case Value.JSON:
            return byte[].class;
        case Value.BLOB:
            return Blob.class;
        case Value.BOOLEAN:
            return Boolean.class;
        case Value.TINYINT:
            if (forJdbc) {
                return Integer.class;
            }
            return Byte.class;
        case Value.SMALLINT:
            if (forJdbc) {
                return Integer.class;
            }
            return Short.class;
        case Value.INTEGER:
            return Integer.class;
        case Value.BIGINT:
            return Long.class;
        case Value.NUMERIC:
        case Value.DECFLOAT:
            return BigDecimal.class;
        case Value.REAL:
            return Float.class;
        case Value.DOUBLE:
            return Double.class;
        case Value.DATE:
            return forJdbc ? java.sql.Date.class : LocalDate.class;
        case Value.TIME:
            return forJdbc ? java.sql.Time.class : LocalTime.class;
        case Value.TIME_TZ:
            return OffsetTime.class;
        case Value.TIMESTAMP:
            return forJdbc ? java.sql.Timestamp.class : LocalDateTime.class;
        case Value.TIMESTAMP_TZ:
            return OffsetDateTime.class;
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
        case Value.INTERVAL_MINUTE_TO_SECOND:
            return Interval.class;
        case Value.JAVA_OBJECT:
            return forJdbc ? Object.class : byte[].class;
        case Value.GEOMETRY: {
            Class<?> clazz = GEOMETRY_CLASS;
            return clazz != null ? clazz : String.class;
        }
        case Value.UUID:
            return UUID.class;
        case Value.ARRAY:
            if (forJdbc) {
                return Array.class;
            }
            return Object[].class;
        case Value.ROW:
            if (forJdbc) {
                return ResultSet.class;
            }
            return Object[].class;
        default:
            throw DbException.getUnsupportedException("data type " + type);
        }
    }

    /**
     * Converts the specified value to the default Java object for its type.
     *
     * @param value
     *            the value
     * @param conn
     *            the connection
     * @param forJdbc
     *            if {@code true} perform conversion for JDBC layer, if
     *            {@code false} perform conversion for Java functions API
     * @return the object
     */
    public static Object valueToDefaultObject(Value value, JdbcConnection conn, boolean forJdbc) {
        switch (value.getValueType()) {
        case Value.NULL:
            return null;
        case Value.CHAR:
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.ENUM:
            return value.getString();
        case Value.CLOB:
            return new JdbcClob(conn, value, JdbcLob.State.WITH_VALUE, getNextId(TraceObject.CLOB));
        case Value.BINARY:
        case Value.VARBINARY:
        case Value.JSON:
            return value.getBytes();
        case Value.BLOB:
            return new JdbcBlob(conn, value, JdbcLob.State.WITH_VALUE, getNextId(TraceObject.BLOB));
        case Value.BOOLEAN:
            return value.getBoolean();
        case Value.TINYINT:
            if (forJdbc) {
                return value.getInt();
            }
            return value.getByte();
        case Value.SMALLINT:
            if (forJdbc) {
                return value.getInt();
            }
            return value.getShort();
        case Value.INTEGER:
            return value.getInt();
        case Value.BIGINT:
            return value.getLong();
        case Value.NUMERIC:
        case Value.DECFLOAT:
            return value.getBigDecimal();
        case Value.REAL:
            return value.getFloat();
        case Value.DOUBLE:
            return value.getDouble();
        case Value.DATE:
            return forJdbc ? LegacyDateTimeUtils.toDate(conn, null, value) : JSR310Utils.valueToLocalDate(value, null);
        case Value.TIME:
            return forJdbc ? LegacyDateTimeUtils.toTime(conn, null, value) : JSR310Utils.valueToLocalTime(value, null);
        case Value.TIME_TZ:
            return JSR310Utils.valueToOffsetTime(value, null);
        case Value.TIMESTAMP:
            return forJdbc ? LegacyDateTimeUtils.toTimestamp(conn, null, value)
                    : JSR310Utils.valueToLocalDateTime(value, null);
        case Value.TIMESTAMP_TZ:
            return JSR310Utils.valueToOffsetDateTime(value, null);
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
        case Value.INTERVAL_MINUTE_TO_SECOND:
            return ((ValueInterval) value).getInterval();
        case Value.JAVA_OBJECT:
            return forJdbc ? JdbcUtils.deserialize(value.getBytesNoCopy(), conn.getJavaObjectSerializer())
                    : value.getBytes();
        case Value.GEOMETRY:
            return GEOMETRY_CLASS != null ? ((ValueGeometry) value).getGeometry() : value.getString();
        case Value.UUID:
            return ((ValueUuid) value).getUuid();
        case Value.ARRAY:
            if (forJdbc) {
                return new JdbcArray(conn, value, getNextId(TraceObject.ARRAY));
            }
            return valueToDefaultArray(value, conn, forJdbc);
        case Value.ROW:
            if (forJdbc) {
                return new JdbcResultSet(conn, null, null, ((ValueRow) value).getResult(),
                        getNextId(TraceObject.RESULT_SET), true, false, false);
            }
            return valueToDefaultArray(value, conn, forJdbc);
        default:
            throw DbException.getUnsupportedException("data type " + value.getValueType());
        }
    }

    /**
     * Converts the specified array value to array of default Java objects for
     * its type.
     *
     * @param value
     *            the array value
     * @param conn
     *            the connection
     * @param forJdbc
     *            if {@code true} perform conversion for JDBC layer, if
     *            {@code false} perform conversion for Java functions API
     * @return the object
     */
    public static Object valueToDefaultArray(Value value, JdbcConnection conn, boolean forJdbc) {
        Value[] values = ((ValueCollectionBase) value).getList();
        int len = values.length;
        Object[] list = new Object[len];
        for (int i = 0; i < len; i++) {
            list[i] = valueToDefaultObject(values[i], conn, forJdbc);
        }
        return list;
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
     * @return the value
     */
    public static Value readValue(Session session, JdbcResultSet rs, int columnIndex) {
        Value value = rs.getInternal(columnIndex);
        switch (value.getValueType()) {
        case Value.CLOB:
            value = session.addTemporaryLob(
                    session.getDataHandler().getLobStorage().createClob(new BufferedReader(value.getReader()), -1));
            break;
        case Value.BLOB:
            value = session
                    .addTemporaryLob(session.getDataHandler().getLobStorage().createBlob(value.getInputStream(), -1));
        }
        return value;
    }

    private ValueToObjectConverter() {
    }

}
