/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.h2.api.ErrorCode;
import org.h2.api.H2Type;
import org.h2.api.IntervalQualifier;
import org.h2.engine.Mode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.util.StringUtils;

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
     * If this data type is case sensitive.
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
        dataType.params = "ELEMENT [,...]";
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
        dataType = new DataType();
        dataType.prefix = "ROW(";
        dataType.suffix = ")";
        dataType.params = "NAME DATA_TYPE [,...]";
        add(Value.ROW, Types.OTHER, dataType, new String[] {"ROW"});
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
        case Value.UNKNOWN:
        case Value.NULL:
        case Value.BLOB:
        case Value.CLOB:
        case Value.RESULT_SET:
            return false;
        case Value.ARRAY:
            return isIndexable((TypeInfo) type.getExtTypeInfo());
        case Value.ROW: {
            ExtTypeInfoRow ext = (ExtTypeInfoRow) type.getExtTypeInfo();
            for (Map.Entry<String, TypeInfo> entry : ext.getFields()) {
                if (!isIndexable(entry.getValue())) {
                    return false;
                }
            }
        }
        //$FALL-THROUGH$
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
