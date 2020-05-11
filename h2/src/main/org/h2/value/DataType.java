/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import org.h2.api.ErrorCode;
import org.h2.api.H2Type;
import org.h2.api.Interval;
import org.h2.api.IntervalQualifier;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Mode;
import org.h2.engine.SessionInterface;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.util.JSR310Utils;
import org.h2.util.JdbcUtils;
import org.h2.util.LegacyDateTimeUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * This class contains meta data information about data types,
 * and can convert between Java objects and Values.
 */
public class DataType {

    /**
     * This constant is used to represent the type of a ResultSet. There is no
     * equivalent java.sql.Types value, but Oracle uses it to represent a
     * ResultSet (OracleTypes.CURSOR = -10).
     */
    public static final int TYPE_RESULT_SET = -10;

    /**
     * The Geometry class. This object is null if the jts jar file is not in the
     * classpath.
     */
    public static final Class<?> GEOMETRY_CLASS;

    private static final String GEOMETRY_CLASS_NAME =
            "org.locationtech.jts.geom.Geometry";

    /**
     * The list of types.
     */
    private static final ArrayList<DataType> TYPES = new ArrayList<>(96);
    private static final HashMap<String, DataType> TYPES_BY_NAME = new HashMap<>(128);
    /**
     * Mapping from Value type numbers to DataType.
     */
    static final DataType[] TYPES_BY_VALUE_TYPE = new DataType[Value.TYPE_COUNT];

    /**
     * The value type of this data type.
     */
    public int type;

    /**
     * The data type name.
     */
    public String name;

    /**
     * The SQL type.
     */
    public int sqlType;

    /**
     * The minimum supported precision.
     */
    public long minPrecision;

    /**
     * The maximum supported precision.
     */
    public long maxPrecision;

    /**
     * The lowest possible scale.
     */
    public int minScale;

    /**
     * The highest possible scale.
     */
    public int maxScale;

    /**
     * If this is a numeric type.
     */
    public boolean decimal;

    /**
     * The prefix required for the SQL literal representation.
     */
    public String prefix;

    /**
     * The suffix required for the SQL literal representation.
     */
    public String suffix;

    /**
     * The list of parameters used in the column definition.
     */
    public String params;

    /**
     * If this is an autoincrement type.
     */
    public boolean autoIncrement;

    /**
     * If this data type is an autoincrement type.
     */
    public boolean caseSensitive;

    /**
     * If the precision parameter is supported.
     */
    public boolean supportsPrecision;

    /**
     * If the scale parameter is supported.
     */
    public boolean supportsScale;

    /**
     * The default precision.
     */
    public long defaultPrecision;

    /**
     * The default scale.
     */
    public int defaultScale;

    /**
     * If this data type should not be listed in the database meta data.
     */
    public boolean hidden;

    static {
        Class<?> g;
        try {
            g = JdbcUtils.loadUserClass(GEOMETRY_CLASS_NAME);
        } catch (Exception e) {
            // class is not in the classpath - ignore
            g = null;
        }
        GEOMETRY_CLASS = g;

        DataType dataType = new DataType();
        dataType.defaultPrecision = dataType.maxPrecision = dataType.minPrecision = ValueNull.PRECISION;
        add(Value.NULL, Types.NULL,
                dataType,
                new String[]{"NULL"}
        );
        add(Value.VARCHAR, Types.VARCHAR,
                createString(true, false),
                new String[]{"VARCHAR", "CHARACTER VARYING", "CHAR VARYING",
                        "NCHAR VARYING", "NATIONAL CHARACTER VARYING", "NATIONAL CHAR VARYING",
                        "VARCHAR2", "NVARCHAR", "NVARCHAR2",
                        "VARCHAR_CASESENSITIVE", "TID",
                        "LONGVARCHAR", "LONGNVARCHAR"}
        );
        add(Value.CHAR, Types.CHAR,
                createString(true, true),
                new String[]{"CHAR", "CHARACTER", "NCHAR", "NATIONAL CHARACTER", "NATIONAL CHAR"}
        );
        add(Value.VARCHAR_IGNORECASE, Types.VARCHAR,
                createString(false, false),
                new String[]{"VARCHAR_IGNORECASE"}
        );
        add(Value.BOOLEAN, Types.BOOLEAN,
                createNumeric(ValueBoolean.PRECISION, 0, false),
                new String[]{"BOOLEAN", "BIT", "BOOL"}
        );
        add(Value.TINYINT, Types.TINYINT,
                createNumeric(ValueTinyint.PRECISION, 0, false),
                new String[]{"TINYINT"}
        );
        add(Value.SMALLINT, Types.SMALLINT,
                createNumeric(ValueSmallint.PRECISION, 0, false),
                new String[]{"SMALLINT", "YEAR", "INT2"}
        );
        add(Value.INTEGER, Types.INTEGER,
                createNumeric(ValueInteger.PRECISION, 0, false),
                new String[]{"INTEGER", "INT", "MEDIUMINT", "INT4", "SIGNED"}
        );
        add(Value.INTEGER, Types.INTEGER,
                createNumeric(ValueInteger.PRECISION, 0, true),
                new String[]{"SERIAL"}
        );
        add(Value.BIGINT, Types.BIGINT,
                createNumeric(ValueBigint.PRECISION, 0, false),
                new String[]{"BIGINT", "INT8", "LONG"}
        );
        add(Value.BIGINT, Types.BIGINT,
                createNumeric(ValueBigint.PRECISION, 0, true),
                new String[]{"IDENTITY", "BIGSERIAL"}
        );
        dataType = new DataType();
        dataType.minPrecision = 1;
        dataType.maxPrecision = Integer.MAX_VALUE;
        dataType.defaultPrecision = ValueNumeric.DEFAULT_PRECISION;
        dataType.defaultScale = ValueNumeric.DEFAULT_SCALE;
        dataType.maxScale = ValueNumeric.MAXIMUM_SCALE;
        dataType.minScale = ValueNumeric.MINIMUM_SCALE;
        dataType.params = "PRECISION,SCALE";
        dataType.supportsPrecision = true;
        dataType.supportsScale = true;
        dataType.decimal = true;
        if (SysProperties.BIG_DECIMAL_IS_DECIMAL) {
            add(Value.NUMERIC, Types.DECIMAL,
                    dataType,
                    new String[]{"DECIMAL", "NUMERIC", "DEC", "NUMBER"}
            );
        } else {
            add(Value.NUMERIC, Types.NUMERIC,
                    dataType,
                    new String[]{"NUMERIC", "DECIMAL", "DEC", "NUMBER"}
            );
        }
        add(Value.REAL, Types.REAL,
                createNumeric(ValueReal.PRECISION, 0, false),
                new String[] {"REAL", "FLOAT4"}
        );
        add(Value.DOUBLE, Types.DOUBLE,
                createNumeric(ValueDouble.PRECISION, 0, false),
                new String[] { "DOUBLE PRECISION", "DOUBLE" }
        );
        add(Value.DOUBLE, Types.FLOAT,
                createNumeric(ValueDouble.PRECISION, 0, false),
                new String[] {"FLOAT", "FLOAT8" }
        );
        add(Value.TIME, Types.TIME,
                createDate(ValueTime.MAXIMUM_PRECISION, ValueTime.DEFAULT_PRECISION,
                        "TIME", true, ValueTime.DEFAULT_SCALE, ValueTime.MAXIMUM_SCALE),
                new String[]{"TIME", "TIME WITHOUT TIME ZONE"}
        );
        add(Value.TIME_TZ, Types.TIME_WITH_TIMEZONE,
                createDate(ValueTimeTimeZone.MAXIMUM_PRECISION, ValueTimeTimeZone.DEFAULT_PRECISION,
                        "TIME WITH TIME ZONE", true, ValueTime.DEFAULT_SCALE,
                        ValueTime.MAXIMUM_SCALE),
                new String[]{"TIME WITH TIME ZONE"}
        );
        add(Value.DATE, Types.DATE,
                createDate(ValueDate.PRECISION, ValueDate.PRECISION,
                        "DATE", false, 0, 0),
                new String[]{"DATE"}
        );
        add(Value.TIMESTAMP, Types.TIMESTAMP,
                createDate(ValueTimestamp.MAXIMUM_PRECISION, ValueTimestamp.DEFAULT_PRECISION,
                        "TIMESTAMP", true, ValueTimestamp.DEFAULT_SCALE, ValueTimestamp.MAXIMUM_SCALE),
                new String[]{"TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE",
                        "DATETIME", "DATETIME2", "SMALLDATETIME"}
        );
        add(Value.TIMESTAMP_TZ, Types.TIMESTAMP_WITH_TIMEZONE,
                createDate(ValueTimestampTimeZone.MAXIMUM_PRECISION, ValueTimestampTimeZone.DEFAULT_PRECISION,
                        "TIMESTAMP WITH TIME ZONE", true, ValueTimestamp.DEFAULT_SCALE,
                        ValueTimestamp.MAXIMUM_SCALE),
                new String[]{"TIMESTAMP WITH TIME ZONE"}
        );
        add(Value.VARBINARY, Types.VARBINARY,
                createBinary(false),
                new String[]{"VARBINARY", "BINARY VARYING", "RAW", "BYTEA", "LONG RAW", "LONGVARBINARY"}
        );
        add(Value.BINARY, Types.BINARY,
                createBinary(true),
                new String[]{"BINARY"}
        );
        dataType = new DataType();
        dataType.prefix = dataType.suffix = "'";
        dataType.defaultPrecision = dataType.maxPrecision = dataType.minPrecision = ValueUuid.PRECISION;
        add(Value.UUID, Types.BINARY,
                dataType,
                // UNIQUEIDENTIFIER is the MSSQL mode equivalent
                new String[]{"UUID", "UNIQUEIDENTIFIER"}
        );
        add(Value.JAVA_OBJECT, Types.JAVA_OBJECT,
                createString(false, false),
                new String[]{"JAVA_OBJECT", "OBJECT", "OTHER"}
        );
        add(Value.BLOB, Types.BLOB,
                createLob(false),
                new String[]{"BLOB", "BINARY LARGE OBJECT", "TINYBLOB", "MEDIUMBLOB",
                    "LONGBLOB", "IMAGE"}
        );
        add(Value.CLOB, Types.CLOB,
                createLob(true),
                new String[]{"CLOB", "CHARACTER LARGE OBJECT", "CHAR LARGE OBJECT", "TINYTEXT", "TEXT", "MEDIUMTEXT",
                    "LONGTEXT", "NTEXT", "NCLOB", "NCHAR LARGE OBJECT", "NATIONAL CHARACTER LARGE OBJECT"}
        );
        add(Value.GEOMETRY, Types.OTHER,
                createGeometry(),
                new String[]{"GEOMETRY"}
        );
        dataType = new DataType();
        dataType.prefix = "ARRAY[";
        dataType.suffix = "]";
        dataType.params = "CARDINALITY";
        dataType.caseSensitive = false;
        dataType.supportsPrecision = true;
        dataType.defaultPrecision = dataType.maxPrecision = Integer.MAX_VALUE;
        add(Value.ARRAY, Types.ARRAY,
                dataType,
                new String[]{"ARRAY"}
        );
        dataType = new DataType();
        dataType.maxPrecision = dataType.defaultPrecision = Integer.MAX_VALUE;
        add(Value.RESULT_SET, DataType.TYPE_RESULT_SET,
                dataType,
                new String[]{"RESULT_SET"}
        );
        dataType = createString(false, false);
        dataType.supportsPrecision = false;
        add(Value.ENUM, Types.OTHER,
                dataType,
                new String[]{"ENUM"}
        );
        for (int i = Value.INTERVAL_YEAR; i <= Value.INTERVAL_MINUTE_TO_SECOND; i++) {
            addInterval(i);
        }
        add(Value.JSON, Types.OTHER,
                createString(true, false, "JSON '", "'"),
                new String[]{"JSON"}
        );
        // Row value doesn't have a type name
        dataType = new DataType();
        dataType.type = Value.ROW;
        dataType.name = "ROW";
        dataType.sqlType = Types.OTHER;
        dataType.prefix = "ROW(";
        dataType.suffix = ")";
        TYPES_BY_VALUE_TYPE[Value.ROW] = dataType;
    }

    private static void addInterval(int type) {
        IntervalQualifier qualifier = IntervalQualifier.valueOf(type - Value.INTERVAL_YEAR);
        String name = qualifier.toString();
        DataType dataType = new DataType();
        dataType.prefix = "INTERVAL '";
        dataType.suffix = "' " + name;
        dataType.supportsPrecision = true;
        dataType.defaultPrecision = ValueInterval.DEFAULT_PRECISION;
        dataType.minPrecision = 1;
        dataType.maxPrecision = ValueInterval.MAXIMUM_PRECISION;
        if (qualifier.hasSeconds()) {
            dataType.supportsScale = true;
            dataType.defaultScale = ValueInterval.DEFAULT_SCALE;
            dataType.maxScale = ValueInterval.MAXIMUM_SCALE;
            dataType.params = "PRECISION,SCALE";
        } else {
            dataType.params = "PRECISION";
        }
        add(type, Types.OTHER, dataType,
                new String[]{("INTERVAL " + name).intern()}
        );
    }

    private static void add(int type, int sqlType,
            DataType dataType, String[] names) {
        for (int i = 0; i < names.length; i++) {
            DataType dt = new DataType();
            dt.type = type;
            dt.sqlType = sqlType;
            dt.name = names[i];
            dt.autoIncrement = dataType.autoIncrement;
            dt.decimal = dataType.decimal;
            dt.minPrecision = dataType.minPrecision;
            dt.maxPrecision = dataType.maxPrecision;
            dt.maxScale = dataType.maxScale;
            dt.minScale = dataType.minScale;
            dt.params = dataType.params;
            dt.prefix = dataType.prefix;
            dt.suffix = dataType.suffix;
            dt.supportsPrecision = dataType.supportsPrecision;
            dt.supportsScale = dataType.supportsScale;
            dt.defaultPrecision = dataType.defaultPrecision;
            dt.defaultScale = dataType.defaultScale;
            dt.caseSensitive = dataType.caseSensitive;
            dt.hidden = i > 0;
            TYPES_BY_NAME.put(dt.name, dt);
            if (TYPES_BY_VALUE_TYPE[type] == null) {
                TYPES_BY_VALUE_TYPE[type] = dt;
            }
            TYPES.add(dt);
        }
    }

    /**
     * Create a numeric data type without parameters.
     *
     * @param precision precision
     * @param scale scale
     * @param autoInc whether the data type is an auto-increment type
     * @return data type
     */
    public static DataType createNumeric(int precision, int scale, boolean autoInc) {
        DataType dataType = new DataType();
        dataType.defaultPrecision = dataType.maxPrecision = dataType.minPrecision = precision;
        dataType.defaultScale = dataType.maxScale = dataType.minScale = scale;
        dataType.decimal = true;
        dataType.autoIncrement = autoInc;
        return dataType;
    }

    /**
     * Create a date-time data type.
     *
     * @param maxPrecision maximum supported precision
     * @param precision default precision
     * @param prefix the prefix for SQL literal representation
     * @param supportsScale whether the scale parameter is supported
     * @param scale default scale
     * @param maxScale highest possible scale
     * @return data type
     */
    public static DataType createDate(int maxPrecision, int precision, String prefix,
            boolean supportsScale, int scale, int maxScale) {
        DataType dataType = new DataType();
        dataType.prefix = prefix + " '";
        dataType.suffix = "'";
        dataType.maxPrecision = maxPrecision;
        dataType.defaultPrecision = dataType.minPrecision = precision;
        if (supportsScale) {
            dataType.params = "SCALE";
            dataType.supportsScale = true;
            dataType.maxScale = maxScale;
            dataType.defaultScale = scale;
        }
        return dataType;
    }

    private static DataType createString(boolean caseSensitive, boolean fixedLength) {
        return createString(caseSensitive, fixedLength, "'", "'");
    }

    private static DataType createBinary(boolean fixedLength) {
        return createString(false, fixedLength, "X'", "'");
    }

    private static DataType createString(boolean caseSensitive, boolean fixedLength, String prefix, String suffix) {
        DataType dataType = new DataType();
        dataType.prefix = prefix;
        dataType.suffix = suffix;
        dataType.params = "LENGTH";
        dataType.caseSensitive = caseSensitive;
        dataType.supportsPrecision = true;
        dataType.minPrecision = 1;
        dataType.maxPrecision = Integer.MAX_VALUE;
        dataType.defaultPrecision = fixedLength ? 1 : Integer.MAX_VALUE;
        return dataType;
    }

    private static DataType createLob(boolean clob) {
        DataType t = clob ? createString(true, false) : createBinary(false);
        t.maxPrecision = Long.MAX_VALUE;
        t.defaultPrecision = Long.MAX_VALUE;
        return t;
    }

    private static DataType createGeometry() {
        DataType dataType = new DataType();
        dataType.prefix = "'";
        dataType.suffix = "'";
        dataType.params = "TYPE,SRID";
        dataType.maxPrecision = Integer.MAX_VALUE;
        dataType.defaultPrecision = Integer.MAX_VALUE;
        return dataType;
    }

    /**
     * Get the list of data types.
     *
     * @return the list
     */
    public static ArrayList<DataType> getTypes() {
        return TYPES;
    }

    /**
     * Read a value from the given result set.
     *
     * @param session the session
     * @param rs the result set
     * @param columnIndex the column index (1 based)
     * @param type the data type
     * @return the value
     */
    public static Value readValue(SessionInterface session, ResultSet rs, int columnIndex, int type) {
        try {
            Value v;
            switch (type) {
            case Value.NULL:
                v = ValueNull.INSTANCE;
                break;
            case Value.VARBINARY: {
                /*
                 * Both BINARY and UUID may be mapped to Value.BYTES. getObject() returns byte[]
                 * for SQL BINARY, UUID for SQL UUID and null for SQL NULL.
                 */
                Object o = rs.getObject(columnIndex);
                if (o instanceof byte[]) {
                    v = ValueVarbinary.getNoCopy((byte[]) o);
                } else if (o != null) {
                    v = ValueUuid.get((UUID) o);
                } else {
                    v = ValueNull.INSTANCE;
                }
                break;
            }
            case Value.BINARY: {
                byte[] bytes = rs.getBytes(columnIndex);
                if (bytes != null) {
                    v = ValueBinary.getNoCopy(bytes);
                } else {
                    v = ValueNull.INSTANCE;
                }
                break;
            }
            case Value.UUID: {
                Object o = rs.getObject(columnIndex);
                if (o instanceof UUID) {
                    v = ValueUuid.get((UUID) o);
                } else if (o != null) {
                    v = ValueUuid.get((byte[]) o);
                } else {
                    v = ValueNull.INSTANCE;
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
            case Value.DATE: {
                try {
                    Object value = rs.getObject(columnIndex, LocalDate.class);
                    v = value == null ? ValueNull.INSTANCE : JSR310Utils.localDateToValue(value);
                    break;
                } catch (SQLException ignore) {
                    // Nothing to do
                }
                Date value = rs.getDate(columnIndex);
                v = value == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromDate(session, null, value);
                break;
            }
            case Value.TIME: {
                try {
                    Object value = rs.getObject(columnIndex, LocalTime.class);
                    v = value == null ? ValueNull.INSTANCE : JSR310Utils.localTimeToValue(value);
                    break;
                } catch (SQLException ignore) {
                    // Nothing to do
                }
                Time value = rs.getTime(columnIndex);
                v = value == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromTime(session, null, value);
                break;
            }
            case Value.TIME_TZ: {
                try {
                    Object value = rs.getObject(columnIndex, OffsetTime.class);
                    v = value == null ? ValueNull.INSTANCE : JSR310Utils.offsetTimeToValue(value);
                    break;
                } catch (SQLException ignore) {
                    // Nothing to do
                }
                Object obj = rs.getObject(columnIndex);
                if (obj == null) {
                    v = ValueNull.INSTANCE;
                } else {
                    v = ValueTimeTimeZone.parse(obj.toString());
                }
                break;
            }
            case Value.TIMESTAMP: {
                try {
                    Object value = rs.getObject(columnIndex, LocalDateTime.class);
                    v = value == null ? ValueNull.INSTANCE : JSR310Utils.localDateTimeToValue(value);
                    break;
                } catch (SQLException ignore) {
                    // Nothing to do
                }
                Timestamp value = rs.getTimestamp(columnIndex);
                v = value == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromTimestamp(session, null, value);
                break;
            }
            case Value.TIMESTAMP_TZ: {
                try {
                    Object value = rs.getObject(columnIndex, OffsetDateTime.class);
                    v = value == null ? ValueNull.INSTANCE : JSR310Utils.offsetDateTimeToValue(value);
                    break;
                } catch (SQLException ignore) {
                    // Nothing to do
                }
                Object obj = rs.getObject(columnIndex);
                if (obj == null) {
                    v = ValueNull.INSTANCE;
                } else if (obj instanceof ZonedDateTime) {
                    v = JSR310Utils.zonedDateTimeToValue(obj);
                } else {
                    v = ValueTimestampTimeZone.parse(obj.toString(), session);
                }
                break;
            }
            case Value.NUMERIC: {
                BigDecimal value = rs.getBigDecimal(columnIndex);
                v = value == null ? ValueNull.INSTANCE : ValueNumeric.get(value);
                break;
            }
            case Value.DOUBLE: {
                double value = rs.getDouble(columnIndex);
                v = rs.wasNull() ? ValueNull.INSTANCE : ValueDouble.get(value);
                break;
            }
            case Value.REAL: {
                float value = rs.getFloat(columnIndex);
                v = rs.wasNull() ? ValueNull.INSTANCE : ValueReal.get(value);
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
            case Value.SMALLINT: {
                short value = rs.getShort(columnIndex);
                v = rs.wasNull() ? ValueNull.INSTANCE : ValueSmallint.get(value);
                break;
            }
            case Value.VARCHAR_IGNORECASE: {
                String s = rs.getString(columnIndex);
                v = (s == null) ? ValueNull.INSTANCE : ValueVarcharIgnoreCase.get(s);
                break;
            }
            case Value.CHAR: {
                String s = rs.getString(columnIndex);
                v = (s == null) ? ValueNull.INSTANCE : ValueChar.get(s);
                break;
            }
            case Value.VARCHAR: {
                String s = rs.getString(columnIndex);
                v = (s == null) ? ValueNull.INSTANCE : ValueVarchar.get(s);
                break;
            }
            case Value.CLOB: {
                if (session == null) {
                    String s = rs.getString(columnIndex);
                    v = s == null ? ValueNull.INSTANCE :
                        ValueLobInMemory.createSmallLob(Value.CLOB, s.getBytes(StandardCharsets.UTF_8));
                } else {
                    Reader in = rs.getCharacterStream(columnIndex);
                    if (in == null) {
                        v = ValueNull.INSTANCE;
                    } else {
                        v = session.addTemporaryLob(
                                session.getDataHandler().getLobStorage().createClob(new BufferedReader(in), -1));
                    }
                }
                break;
            }
            case Value.BLOB: {
                if (session == null) {
                    byte[] buff = rs.getBytes(columnIndex);
                    return buff == null ? ValueNull.INSTANCE : ValueLobInMemory.createSmallLob(Value.BLOB, buff);
                }
                InputStream in = rs.getBinaryStream(columnIndex);
                if (in == null) {
                    v = ValueNull.INSTANCE;
                } else {
                    v = session.addTemporaryLob(session.getDataHandler().getLobStorage().createBlob(in, -1));
                }
                break;
            }
            case Value.JAVA_OBJECT: {
                byte[] buff = rs.getBytes(columnIndex);
                v = buff == null ? ValueNull.INSTANCE : ValueJavaObject.getNoCopy(buff);
                break;
            }
            case Value.ARRAY: {
                Array array = rs.getArray(columnIndex);
                if (array == null) {
                    return ValueNull.INSTANCE;
                }
                Object[] list = (Object[]) array.getArray();
                if (list == null) {
                    return ValueNull.INSTANCE;
                }
                int len = list.length;
                Value[] values = new Value[len];
                for (int i = 0; i < len; i++) {
                    values[i] = DataType.convertToValue(session, list[i], Value.NULL);
                }
                v = ValueArray.get(values, session);
                break;
            }
            case Value.ENUM: {
                int value = rs.getInt(columnIndex);
                v = rs.wasNull() ? ValueNull.INSTANCE : ValueInteger.get(value);
                break;
            }
            case Value.ROW: {
                Object[] list = (Object[]) rs.getObject(columnIndex);
                if (list == null) {
                    return ValueNull.INSTANCE;
                }
                int len = list.length;
                Value[] values = new Value[len];
                for (int i = 0; i < len; i++) {
                    values[i] = DataType.convertToValue(session, list[i], Value.NULL);
                }
                v = ValueRow.get(values);
                break;
            }
            case Value.RESULT_SET: {
                ResultSet x = (ResultSet) rs.getObject(columnIndex);
                if (x == null) {
                    return ValueNull.INSTANCE;
                }
                return ValueResultSet.get(session, x, Integer.MAX_VALUE);
            }
            case Value.GEOMETRY: {
                Object x = rs.getObject(columnIndex);
                if (x == null) {
                    return ValueNull.INSTANCE;
                }
                return ValueGeometry.getFromGeometry(x);
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
                Object x = rs.getObject(columnIndex);
                if (x == null) {
                    return ValueNull.INSTANCE;
                }
                Interval interval = (Interval) x;
                return ValueInterval.from(interval.getQualifier(), interval.isNegative(),
                        interval.getLeading(), interval.getRemaining());
            }
            case Value.JSON: {
                Object x = rs.getObject(columnIndex);
                if (x == null) {
                    return ValueNull.INSTANCE;
                }
                Class<?> clazz = x.getClass();
                if (clazz == byte[].class) {
                    return ValueJson.fromJson((byte[]) x);
                } else if (clazz == String.class) {
                    return ValueJson.fromJson((String) x);
                } else {
                    return ValueJson.fromJson(x.toString());
                }
            }
            default:
                throw DbException.throwInternalError("type="+type);
            }
            return v;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Get the name of the Java class for the given value type.
     *
     * @param type the value type
     * @param forResultSet return mapping for result set
     * @return the class name
     */
    public static String getTypeClassName(int type, boolean forResultSet) {
        switch (type) {
        case Value.BOOLEAN:
            // "java.lang.Boolean";
            return Boolean.class.getName();
        case Value.TINYINT:
            if (forResultSet && !SysProperties.OLD_RESULT_SET_GET_OBJECT) {
                // "java.lang.Integer";
                return Integer.class.getName();
            }
            // "java.lang.Byte";
            return Byte.class.getName();
        case Value.SMALLINT:
            if (forResultSet && !SysProperties.OLD_RESULT_SET_GET_OBJECT) {
                // "java.lang.Integer";
                return Integer.class.getName();
            }
            // "java.lang.Short";
            return Short.class.getName();
        case Value.INTEGER:
            // "java.lang.Integer";
            return Integer.class.getName();
        case Value.BIGINT:
            // "java.lang.Long";
            return Long.class.getName();
        case Value.NUMERIC:
            // "java.math.BigDecimal";
            return BigDecimal.class.getName();
        case Value.TIME:
            // "java.sql.Time";
            return Time.class.getName();
        case Value.TIME_TZ:
            // "java.time.OffsetTime";
            return OffsetTime.class.getName();
        case Value.DATE:
            // "java.sql.Date";
            return Date.class.getName();
        case Value.TIMESTAMP:
            // "java.sql.Timestamp";
            return Timestamp.class.getName();
        case Value.TIMESTAMP_TZ:
            // "java.time.OffsetDateTime";
            return OffsetDateTime.class.getName();
        case Value.VARBINARY:
        case Value.BINARY:
        case Value.UUID:
        case Value.JSON:
            // "[B", not "byte[]";
            return byte[].class.getName();
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.CHAR:
        case Value.ENUM:
            // "java.lang.String";
            return String.class.getName();
        case Value.BLOB:
            // "java.sql.Blob";
            return java.sql.Blob.class.getName();
        case Value.CLOB:
            // "java.sql.Clob";
            return java.sql.Clob.class.getName();
        case Value.DOUBLE:
            // "java.lang.Double";
            return Double.class.getName();
        case Value.REAL:
            // "java.lang.Float";
            return Float.class.getName();
        case Value.NULL:
            return null;
        case Value.JAVA_OBJECT:
            // "java.lang.Object";
            return Object.class.getName();
        case Value.UNKNOWN:
            // anything
            return Object.class.getName();
        case Value.ARRAY:
            return Array.class.getName();
        case Value.RESULT_SET:
            return ResultSet.class.getName();
        case Value.GEOMETRY:
            return GEOMETRY_CLASS != null ? GEOMETRY_CLASS_NAME : String.class.getName();
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
            // "org.h2.api.Interval"
            return Interval.class.getName();
        default:
            throw DbException.throwInternalError("type="+type);
        }
    }

    /**
     * Get the data type object for the given value type.
     *
     * @param type the value type
     * @return the data type object
     */
    public static DataType getDataType(int type) {
        if (type == Value.UNKNOWN) {
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?");
        }
        if (type >= Value.NULL && type < Value.TYPE_COUNT) {
            DataType dt = TYPES_BY_VALUE_TYPE[type];
            if (dt != null) {
                return dt;
            }
        }
        return TYPES_BY_VALUE_TYPE[Value.NULL];
    }

    /**
     * Convert a value type to a SQL type.
     *
     * @param type the value type
     * @return the SQL type
     */
    public static int convertTypeToSQLType(int type) {
        return getDataType(type).sqlType;
    }

    /**
     * Convert a SQL type to a value type using SQL type name, in order to
     * manage SQL type extension mechanism.
     *
     * @param sqlType the SQL type
     * @param sqlTypeName the SQL type name
     * @return the value type
     */
    public static int convertSQLTypeToValueType(int sqlType, String sqlTypeName) {
        switch (sqlType) {
            case Types.BINARY:
                if (sqlTypeName.equalsIgnoreCase("UUID")) {
                    return Value.UUID;
                }
                break;
            case Types.OTHER: {
                DataType type = TYPES_BY_NAME.get(StringUtils.toUpperEnglish(sqlTypeName));
                if (type != null) {
                    return type.type;
                }
            }
        }
        return convertSQLTypeToValueType(sqlType);
    }

    /**
     * Get the SQL type from the result set meta data for the given column. This
     * method uses the SQL type and type name.
     *
     * @param meta the meta data
     * @param columnIndex the column index (1, 2,...)
     * @return the value type
     */
    public static int getValueTypeFromResultSet(ResultSetMetaData meta,
            int columnIndex) throws SQLException {
        return convertSQLTypeToValueType(
                meta.getColumnType(columnIndex),
                meta.getColumnTypeName(columnIndex));
    }

    /**
     * Check whether the specified column needs the binary representation.
     *
     * @param meta
     *            metadata
     * @param column
     *            column index
     * @return {@code true} if column needs the binary representation,
     *         {@code false} otherwise
     * @throws SQLException
     *             on SQL exception
     */
    public static boolean isBinaryColumn(ResultSetMetaData meta, int column) throws SQLException {
        switch (meta.getColumnType(column)) {
        case Types.BINARY:
            if (meta.getColumnTypeName(column).equals("UUID")) {
                break;
            }
            //$FALL-THROUGH$
        case Types.LONGVARBINARY:
        case Types.VARBINARY:
        case Types.JAVA_OBJECT:
        case Types.BLOB:
            return true;
        }
        return false;
    }

    /**
     * Convert a SQL type to a value type.
     *
     * @param sqlType the SQL type
     * @return the value type
     */
    public static int convertSQLTypeToValueType(SQLType sqlType) {
        if (sqlType instanceof H2Type) {
            return sqlType.getVendorTypeNumber();
        } else if (sqlType instanceof JDBCType) {
            return convertSQLTypeToValueType(sqlType.getVendorTypeNumber());
        } else {
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, sqlType == null ? "<null>"
                    : unknownSqlTypeToString(new StringBuilder(), sqlType).toString());
        }
    }

    /**
     * Convert a SQL type to a value type.
     *
     * @param sqlType the SQL type
     * @return the value type
     */
    public static int convertSQLTypeToValueType(int sqlType) {
        switch (sqlType) {
        case Types.CHAR:
        case Types.NCHAR:
            return Value.CHAR;
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            return Value.VARCHAR;
        case Types.NUMERIC:
        case Types.DECIMAL:
            return Value.NUMERIC;
        case Types.BIT:
        case Types.BOOLEAN:
            return Value.BOOLEAN;
        case Types.INTEGER:
            return Value.INTEGER;
        case Types.SMALLINT:
            return Value.SMALLINT;
        case Types.TINYINT:
            return Value.TINYINT;
        case Types.BIGINT:
            return Value.BIGINT;
        case Types.REAL:
            return Value.REAL;
        case Types.DOUBLE:
        case Types.FLOAT:
            return Value.DOUBLE;
        case Types.BINARY:
            return Value.BINARY;
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            return Value.VARBINARY;
        case Types.OTHER:
            return Value.UNKNOWN;
        case Types.JAVA_OBJECT:
            return Value.JAVA_OBJECT;
        case Types.DATE:
            return Value.DATE;
        case Types.TIME:
            return Value.TIME;
        case Types.TIMESTAMP:
            return Value.TIMESTAMP;
        case Types.TIME_WITH_TIMEZONE:
            return Value.TIME_TZ;
        case Types.TIMESTAMP_WITH_TIMEZONE:
            return Value.TIMESTAMP_TZ;
        case Types.BLOB:
            return Value.BLOB;
        case Types.CLOB:
        case Types.NCLOB:
            return Value.CLOB;
        case Types.NULL:
            return Value.NULL;
        case Types.ARRAY:
            return Value.ARRAY;
        case DataType.TYPE_RESULT_SET:
            return Value.RESULT_SET;
        default:
            throw DbException.get(
                    ErrorCode.UNKNOWN_DATA_TYPE_1, Integer.toString(sqlType));
        }
    }

    /**
     * Convert a SQL type to a debug string.
     *
     * @param sqlType the SQL type
     * @return the textual representation
     */
    public static String sqlTypeToString(SQLType sqlType) {
        if (sqlType == null) {
            return "null";
        }
        if (sqlType instanceof JDBCType) {
            return "JDBCType." + sqlType.getName();
        }
        if (sqlType instanceof H2Type) {
            return sqlType.toString();
        }
        return unknownSqlTypeToString(new StringBuilder("/* "), sqlType).append(" */ null").toString();
    }

    private static StringBuilder unknownSqlTypeToString(StringBuilder builder, SQLType sqlType) {
        return builder.append(StringUtils.quoteJavaString(sqlType.getVendor())).append('/')
                .append(StringUtils.quoteJavaString(sqlType.getName())).append(" [")
                .append(sqlType.getVendorTypeNumber()).append(']');
    }

    /**
     * Get the type information for the given Java class.
     *
     * @param x the Java class
     * @return the value type
     */
    public static TypeInfo getTypeFromClass(Class <?> x) {
        // TODO refactor: too many if/else in functions, can reduce!
        if (x == null || Void.TYPE == x) {
            return TypeInfo.TYPE_NULL;
        }
        if (x.isPrimitive()) {
            x = Utils.getNonPrimitiveClass(x);
        }
        if (String.class == x) {
            return TypeInfo.TYPE_VARCHAR;
        } else if (Integer.class == x) {
            return TypeInfo.TYPE_INTEGER;
        } else if (Long.class == x) {
            return TypeInfo.TYPE_BIGINT;
        } else if (Boolean.class == x) {
            return TypeInfo.TYPE_BOOLEAN;
        } else if (Double.class == x) {
            return TypeInfo.TYPE_DOUBLE;
        } else if (Byte.class == x) {
            return TypeInfo.TYPE_TINYINT;
        } else if (Short.class == x) {
            return TypeInfo.TYPE_SMALLINT;
        } else if (Character.class == x) {
            throw DbException.get(
                    ErrorCode.DATA_CONVERSION_ERROR_1, "char (not supported)");
        } else if (Float.class == x) {
            return TypeInfo.TYPE_REAL;
        } else if (byte[].class == x) {
            return TypeInfo.TYPE_VARBINARY;
        } else if (UUID.class == x) {
            return TypeInfo.TYPE_UUID;
        } else if (Void.class == x) {
            return TypeInfo.TYPE_NULL;
        } else if (BigDecimal.class.isAssignableFrom(x)) {
            return TypeInfo.TYPE_NUMERIC;
        } else if (ResultSet.class.isAssignableFrom(x)) {
            return TypeInfo.TYPE_RESULT_SET;
        } else if (ValueLob.class.isAssignableFrom(x)) {
            return TypeInfo.TYPE_BLOB;
// FIXME no way to distinguish between these 2 types
//        } else if (ValueLob.class.isAssignableFrom(x)) {
//            return TypeInfo.TYPE_CLOB;
        } else if (Date.class.isAssignableFrom(x)) {
            return TypeInfo.TYPE_DATE;
        } else if (Time.class.isAssignableFrom(x)) {
            return TypeInfo.TYPE_TIME;
        } else if (Timestamp.class.isAssignableFrom(x)) {
            return TypeInfo.TYPE_TIMESTAMP;
        } else if (java.util.Date.class.isAssignableFrom(x)) {
            return TypeInfo.TYPE_TIMESTAMP;
        } else if (java.io.Reader.class.isAssignableFrom(x)) {
            return TypeInfo.TYPE_CLOB;
        } else if (java.sql.Clob.class.isAssignableFrom(x)) {
            return TypeInfo.TYPE_CLOB;
        } else if (java.io.InputStream.class.isAssignableFrom(x)) {
            return TypeInfo.TYPE_BLOB;
        } else if (java.sql.Blob.class.isAssignableFrom(x)) {
            return TypeInfo.TYPE_BLOB;
        } else if (Object[].class.isAssignableFrom(x)) {
            // this includes String[] and so on
            return TypeInfo.getTypeInfo(Value.ARRAY, Integer.MAX_VALUE, 0, getTypeFromClass(x.getComponentType()));
        } else if (isGeometryClass(x)) {
            return TypeInfo.TYPE_GEOMETRY;
        } else if (LocalDate.class == x) {
            return TypeInfo.TYPE_DATE;
        } else if (LocalTime.class == x) {
            return TypeInfo.TYPE_TIME;
        } else if (OffsetTime.class == x) {
            return TypeInfo.TYPE_TIME_TZ;
        } else if (LocalDateTime.class == x) {
            return TypeInfo.TYPE_TIMESTAMP;
        } else if (OffsetDateTime.class == x || ZonedDateTime.class == x || Instant.class == x) {
            return TypeInfo.TYPE_TIMESTAMP_TZ;
        } else {
            return TypeInfo.TYPE_JAVA_OBJECT;
        }
    }

    /**
     * Convert a Java object to a value.
     *
     * @param session the session
     * @param x the value
     * @param type the value type
     * @return the value
     */
    public static Value convertToValue(SessionInterface session, Object x, int type) {
        if (x == null) {
            return ValueNull.INSTANCE;
        } else if (type == Value.JAVA_OBJECT) {
            return ValueJavaObject.getNoCopy(JdbcUtils.serialize(x, session.getJavaObjectSerializer()));
        } else if (x instanceof String) {
            return ValueVarchar.get((String) x, session);
        } else if (x instanceof Value) {
            Value v = (Value) x;
            if (v instanceof ValueLob) {
                session.addTemporaryLob((ValueLob) v);
            }
            return v;
        } else if (x instanceof Long) {
            return ValueBigint.get((Long) x);
        } else if (x instanceof Integer) {
            return ValueInteger.get((Integer) x);
        } else if (x instanceof BigInteger) {
            return ValueNumeric.get((BigInteger) x);
        } else if (x instanceof BigDecimal) {
            return ValueNumeric.get((BigDecimal) x);
        } else if (x instanceof Boolean) {
            return ValueBoolean.get((Boolean) x);
        } else if (x instanceof Byte) {
            return ValueTinyint.get((Byte) x);
        } else if (x instanceof Short) {
            return ValueSmallint.get((Short) x);
        } else if (x instanceof Float) {
            return ValueReal.get((Float) x);
        } else if (x instanceof Double) {
            return ValueDouble.get((Double) x);
        } else if (x instanceof byte[]) {
            return ValueVarbinary.get((byte[]) x);
        } else if (x instanceof Date) {
            return LegacyDateTimeUtils.fromDate(session, null, (Date) x);
        } else if (x instanceof Time) {
            return LegacyDateTimeUtils.fromTime(session, null, (Time) x);
        } else if (x instanceof Timestamp) {
            return LegacyDateTimeUtils.fromTimestamp(session, null, (Timestamp) x);
        } else if (x instanceof java.util.Date) {
            return LegacyDateTimeUtils.fromTimestamp(session, ((java.util.Date) x).getTime(), 0);
        } else if (x instanceof Array) {
            Array array = (Array) x;
            try {
                return convertToValue(session, array.getArray(), Value.ARRAY);
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else if (x instanceof ResultSet) {
            return ValueResultSet.get(session, (ResultSet) x, Integer.MAX_VALUE);
        } else if (x instanceof UUID) {
            return ValueUuid.get((UUID) x);
        }
        Class<?> clazz = x.getClass();
        if (x instanceof Object[]) {
            // (a.getClass().isArray());
            // (a.getClass().getComponentType().isPrimitive());
            Object[] o = (Object[]) x;
            int len = o.length;
            Value[] v = new Value[len];
            for (int i = 0; i < len; i++) {
                v[i] = convertToValue(session, o[i], Value.UNKNOWN);
            }
            return ValueArray.get(v, session);
        } else if (x instanceof Character) {
            return ValueChar.get(((Character) x).toString());
        } else if (isGeometry(x)) {
            return ValueGeometry.getFromGeometry(x);
        } else if (clazz == LocalDate.class) {
            return JSR310Utils.localDateToValue(x);
        } else if (clazz == LocalTime.class) {
            return JSR310Utils.localTimeToValue(x);
        } else if (clazz == LocalDateTime.class) {
            return JSR310Utils.localDateTimeToValue(x);
        } else if (clazz == Instant.class) {
            return JSR310Utils.instantToValue(x);
        } else if (clazz == OffsetTime.class) {
            return JSR310Utils.offsetTimeToValue(x);
        } else if (clazz == OffsetDateTime.class) {
            return JSR310Utils.offsetDateTimeToValue(x);
        } else if (clazz == ZonedDateTime.class) {
            return JSR310Utils.zonedDateTimeToValue(x);
        } else if (x instanceof Interval) {
            Interval i = (Interval) x;
            return ValueInterval.from(i.getQualifier(), i.isNegative(), i.getLeading(), i.getRemaining());
        } else if (clazz == Period.class) {
            return JSR310Utils.periodToValue(x);
        } else if (clazz == Duration.class) {
            return JSR310Utils.durationToValue(x);
        } else {
            return convertToValueLobOrObject(session, x);
        }
    }

    private static Value convertToValueLobOrObject(SessionInterface session, Object x) {
        DataHandler dataHandler = session.getDataHandler();
        ValueLob lob;
        if (x instanceof Reader) {
            Reader r = (Reader) x;
            if (!(r instanceof BufferedReader)) {
                r = new BufferedReader(r);
            }
            lob = dataHandler.getLobStorage().createClob(r, -1);
        } else if (x instanceof Clob) {
            try {
                Clob clob = (Clob) x;
                Reader r = new BufferedReader(clob.getCharacterStream());
                lob = dataHandler.getLobStorage().createClob(r, clob.length());
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else if (x instanceof InputStream) {
            lob = dataHandler.getLobStorage().createBlob((InputStream) x, -1);
        } else if (x instanceof Blob) {
            try {
                Blob blob = (Blob) x;
                lob = dataHandler.getLobStorage().createBlob(blob.getBinaryStream(), blob.length());
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else if (x instanceof SQLXML) {
            try {
                lob = dataHandler.getLobStorage().createClob(
                        new BufferedReader(((SQLXML) x).getCharacterStream()), -1);
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else {
            return ValueJavaObject.getNoCopy(JdbcUtils.serialize(x, session.getJavaObjectSerializer()));
        }
        return session.addTemporaryLob(lob);
    }

    /**
     * Extract object of the specified type.
     *
     * @param <T> the type
     * @param type the class
     * @param value the value, shouldn't be {@link ValueNull}
     * @param provider the cast information provider
     * @return an instance of the specified class, or {@code null} if not supported
     */
    @SuppressWarnings("unchecked")
    public static <T> T extractObjectOfType(Class<T> type, Value value, CastDataProvider provider) {
        if (type == BigDecimal.class) {
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
            return (T) JSR310Utils.valueToLocalDate(value, provider);
        } else if (type == LocalTime.class) {
            return (T) JSR310Utils.valueToLocalTime(value, provider);
        } else if (type == LocalDateTime.class) {
            return (T) JSR310Utils.valueToLocalDateTime(value, provider);
        } else if (type == Instant.class) {
            return (T) JSR310Utils.valueToInstant(value, provider);
        } else if (type == OffsetTime.class) {
            return (T) JSR310Utils.valueToOffsetTime(value, provider);
        } else if (type == OffsetDateTime.class) {
            return (T) JSR310Utils.valueToOffsetDateTime(value, provider);
        } else if (type == ZonedDateTime.class) {
            return (T) JSR310Utils.valueToZonedDateTime(value, provider);
        } else if (type == Period.class) {
            return (T) JSR310Utils.valueToPeriod(value);
        } else if (type == Duration.class) {
            return (T) JSR310Utils.valueToDuration(value);
        } else if (type == Object.class) {
            return (T) value.convertToJavaObject(TypeInfo.TYPE_JAVA_OBJECT, Value.CONVERT_TO, null).getObject();
        } else if (type == InputStream.class) {
            return (T) value.getInputStream();
        } else if (type == Reader.class) {
            return (T) value.getReader();
        } else if (type.isArray()) {
            Value[] array = ((ValueArray) value).getList();
            Class<?> componentType = type.getComponentType();
            Object[] objArray = (Object[]) java.lang.reflect.Array.newInstance(componentType, array.length);
            for (int i = 0; i < objArray.length; i++) {
                value = array[i];
                Object element;
                if (value == ValueNull.INSTANCE) {
                    element = null;
                } else {
                    element = extractObjectOfType(componentType, value, provider);
                    if (element == null) {
                        // Unsupported
                        return null;
                    }
                }
                objArray[i] = element;
            }
            return (T) objArray;
        }
        return LegacyDateTimeUtils.extractObjectOfLegacyType(type, value, provider);
    }

    /**
     * Check whether a given class matches the Geometry class.
     *
     * @param x the class
     * @return true if it is a Geometry class
     */
    public static boolean isGeometryClass(Class<?> x) {
        if (x == null || GEOMETRY_CLASS == null) {
            return false;
        }
        return GEOMETRY_CLASS.isAssignableFrom(x);
    }

    /**
     * Check whether a given object is a Geometry object.
     *
     * @param x the object
     * @return true if it is a Geometry object
     */
    public static boolean isGeometry(Object x) {
        if (x == null) {
            return false;
        }
        return isGeometryClass(x.getClass());
    }

    /**
     * Get a data type object from a type name.
     *
     * @param s the type name
     * @param mode database mode
     * @return the data type object
     */
    public static DataType getTypeByName(String s, Mode mode) {
        DataType result = mode.typeByNameMap.get(s);
        if (result == null) {
            result = TYPES_BY_NAME.get(s);
        }
        return result;
    }

    /**
     * Returns whether columns with the specified data type may have an index.
     *
     * @param type the data type
     * @return whether an index is allowed
     */
    public static boolean isIndexable(TypeInfo type) {
        switch(type.getValueType()) {
        case Value.ARRAY: {
            ExtTypeInfo extTypeInfo = type.getExtTypeInfo();
            if (extTypeInfo != null) {
                return isIndexable((TypeInfo) extTypeInfo);
            }
        }
        //$FALL-THROUGH$
        case Value.UNKNOWN:
        case Value.NULL:
        case Value.BLOB:
        case Value.CLOB:
        case Value.RESULT_SET:
        case Value.ROW:
            return false;
        default:
            return true;
        }
    }

    /**
     * Returns whether values of the specified data types have
     * session-independent compare results.
     *
     * @param type1
     *            the first data type
     * @param type2
     *            the second data type
     * @return are values have session-independent compare results
     */
    public static boolean areStableComparable(TypeInfo type1, TypeInfo type2) {
        int t1 = type1.getValueType();
        int t2 = type2.getValueType();
        switch (t1) {
        case Value.UNKNOWN:
        case Value.NULL:
        case Value.BLOB:
        case Value.CLOB:
        case Value.RESULT_SET:
        case Value.ROW:
            return false;
        case Value.DATE:
        case Value.TIMESTAMP:
            // DATE is equal to TIMESTAMP at midnight
            return t2 == Value.DATE || t2 == Value.TIMESTAMP;
        case Value.TIME:
        case Value.TIME_TZ:
        case Value.TIMESTAMP_TZ:
            // Conversions depend on current timestamp and time zone
            return t1 == t2;
        case Value.ARRAY:
            if (t2 == Value.ARRAY) {
                return areStableComparable((TypeInfo) type1.getExtTypeInfo(), (TypeInfo) type2.getExtTypeInfo());
            }
            return false;
        default:
            switch (t2) {
            case Value.UNKNOWN:
            case Value.NULL:
            case Value.BLOB:
            case Value.CLOB:
            case Value.RESULT_SET:
            case Value.ROW:
                return false;
            default:
                return true;
            }
        }
    }

    /**
     * Check if the given value type is a date-time type (TIME, DATE, TIMESTAMP,
     * TIMESTAMP_TZ).
     *
     * @param type the value type
     * @return true if the value type is a date-time type
     */
    public static boolean isDateTimeType(int type) {
        switch (type) {
        case Value.TIME:
        case Value.TIME_TZ:
        case Value.DATE:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            return true;
        default:
            return false;
        }
    }

    /**
     * Check if the given value type is an interval type.
     *
     * @param type the value type
     * @return true if the value type is an interval type
     */
    public static boolean isIntervalType(int type) {
        return type >= Value.INTERVAL_YEAR && type <= Value.INTERVAL_MINUTE_TO_SECOND;
    }

    /**
     * Check if the given value type is a year-month interval type.
     *
     * @param type the value type
     * @return true if the value type is a year-month interval type
     */
    public static boolean isYearMonthIntervalType(int type) {
        return type == Value.INTERVAL_YEAR || type == Value.INTERVAL_MONTH || type == Value.INTERVAL_YEAR_TO_MONTH;
    }

    /**
     * Check if the given value type is a large object (BLOB or CLOB).
     *
     * @param type the value type
     * @return true if the value type is a lob type
     */
    public static boolean isLargeObject(int type) {
        return type == Value.BLOB || type == Value.CLOB;
    }

    /**
     * Check if the given value type is a numeric type.
     *
     * @param type the value type
     * @return true if the value type is a numeric type
     */
    public static boolean isNumericType(int type) {
        return type >= Value.TINYINT && type <= Value.NUMERIC;
    }

    /**
     * Check if the given value type is a binary string type.
     *
     * @param type the value type
     * @return true if the value type is a binary string type
     */
    public static boolean isBinaryStringType(int type) {
        return type >= Value.BINARY && type <= Value.BLOB;
    }

    /**
     * Check if the given value type is a character string type.
     *
     * @param type the value type
     * @return true if the value type is a character string type
     */
    public static boolean isCharacterStringType(int type) {
        return type >= Value.CHAR && type <= Value.VARCHAR_IGNORECASE;
    }

    /**
     * Check if the given value type is a String (VARCHAR,...).
     *
     * @param type the value type
     * @return true if the value type is a String type
     */
    public static boolean isStringType(int type) {
        return type == Value.VARCHAR || type == Value.CHAR || type == Value.VARCHAR_IGNORECASE;
    }

    /**
     * Check if the given value type is a binary string type or a compatible
     * special data type such as Java object, UUID, geometry object, or JSON.
     *
     * @param type
     *            the value type
     * @return true if the value type is a binary string type or a compatible
     *         special data type
     */
    public static boolean isBinaryStringOrSpecialBinaryType(int type) {
        switch (type) {
        case Value.VARBINARY:
        case Value.BINARY:
        case Value.BLOB:
        case Value.JAVA_OBJECT:
        case Value.UUID:
        case Value.GEOMETRY:
        case Value.JSON:
            return true;
        default:
            return false;
        }
    }

    /**
     * Check if the given type has total ordering.
     *
     * @param type the value type
     * @return true if the value type has total ordering
     */
    public static boolean hasTotalOrdering(int type) {
        switch (type) {
        case Value.BOOLEAN:
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT:
        // Negative zeroes and NaNs are normalized
        case Value.DOUBLE:
        case Value.REAL:
        case Value.TIME:
        case Value.DATE:
        case Value.TIMESTAMP:
        case Value.VARBINARY:
        // Serialized data is compared
        case Value.JAVA_OBJECT:
        case Value.UUID:
        // EWKB is used
        case Value.GEOMETRY:
        case Value.ENUM:
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
        case Value.BINARY:
            return true;
        default:
            return false;
        }
    }

    /**
     * Check if the given value type supports the add operation.
     *
     * @param type the value type
     * @return true if add is supported
     */
    public static boolean supportsAdd(int type) {
        switch (type) {
        case Value.TINYINT:
        case Value.NUMERIC:
        case Value.DOUBLE:
        case Value.REAL:
        case Value.INTEGER:
        case Value.BIGINT:
        case Value.SMALLINT:
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
            return true;
        default:
            return false;
        }
    }

    /**
     * Performs saturated addition of precision values.
     *
     * @param p1
     *            the first summand
     * @param p2
     *            the second summand
     * @return the sum of summands, or {@link Long#MAX_VALUE} if either argument
     *         is negative or sum is out of range
     */
    public static long addPrecision(long p1, long p2) {
        long sum = p1 + p2;
        if ((p1 | p2 | sum) < 0) {
            return Long.MAX_VALUE;
        }
        return sum;
    }

    /**
     * Get the data type that will not overflow when calling 'add' 2 billion
     * times.
     *
     * @param type the value type
     * @return the data type that supports adding
     */
    public static int getAddProofType(int type) {
        switch (type) {
        case Value.TINYINT:
            return Value.BIGINT;
        case Value.REAL:
            return Value.DOUBLE;
        case Value.INTEGER:
            return Value.BIGINT;
        case Value.BIGINT:
            return Value.NUMERIC;
        case Value.SMALLINT:
            return Value.BIGINT;
        default:
            return type;
        }
    }

    /**
     * Get the default value in the form of a Java object for the given Java
     * class.
     *
     * @param clazz the Java class
     * @return the default object
     */
    public static Object getDefaultForPrimitiveType(Class<?> clazz) {
        if (clazz == Boolean.TYPE) {
            return Boolean.FALSE;
        } else if (clazz == Byte.TYPE) {
            return (byte) 0;
        } else if (clazz == Character.TYPE) {
            return (char) 0;
        } else if (clazz == Short.TYPE) {
            return (short) 0;
        } else if (clazz == Integer.TYPE) {
            return 0;
        } else if (clazz == Long.TYPE) {
            return 0L;
        } else if (clazz == Float.TYPE) {
            return (float) 0;
        } else if (clazz == Double.TYPE) {
            return (double) 0;
        }
        throw DbException.throwInternalError(
                "primitive=" + clazz.toString());
    }

}
