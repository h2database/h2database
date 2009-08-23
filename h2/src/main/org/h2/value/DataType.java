/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.SessionInterface;
import org.h2.jdbc.JdbcBlob;
import org.h2.jdbc.JdbcClob;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.Message;
import org.h2.util.New;
import org.h2.util.ObjectUtils;
import org.h2.util.StringUtils;

/**
 * This class contains meta data information about data types,
 * and can convert between Java objects and Values.
 */
public class DataType {

    /**
     * This constant is used for JDK 1.3 compatibility
     * and equal to java.sql.Types.BOOLEAN
     */
    public static final int TYPE_BOOLEAN = 16;

    /**
     * This constant is used for JDK 1.3 compatibility
     * and equal to java.sql.Types.DATALINK
     */
    public static final int TYPE_DATALINK = 70;

    /**
     * This constant is used for JDK 1.5 compatibility
     * and equal to java.sql.Types.LONGNVARCHAR
     */
    public static final int TYPE_LONGNVARCHAR = -16;

    /**
     * This constant is used for JDK 1.5 compatibility
     * and equal to java.sql.Types.NCHAR
     */
    public static final int TYPE_NCHAR = -15;

    /**
     * This constant is used for JDK 1.5 compatibility
     * and equal to java.sql.Types.NVARCHAR
     */
    public static final int TYPE_NVARCHAR = -9;

    /**
     * This constant is used for JDK 1.5 compatibility
     * and equal to java.sql.Types.NCLOB
     */
    public static final int TYPE_NCLOB = 2011;

    /**
     * The list of types. An ArrayList so that Tomcat doesn't set it to null
     * when clearing references.
     */
    private static final ArrayList<DataType> TYPES = New.arrayList();
    private static final HashMap<String, DataType> TYPES_BY_NAME = New.hashMap();
    private static final ArrayList<DataType> TYPES_BY_VALUE_TYPE = New.arrayList();

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
     * The Java class name.
     */
    public String jdbc;

    /**
     * How closely the data type maps to the corresponding JDBC SQL type (low is
     * best).
     */
    public int sqlTypePos;

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
     * The default display size.
     */
    public int defaultDisplaySize;

    /**
     * If this data type should not be listed in the database meta data.
     */
    public boolean hidden;

    /**
     * The number of bytes required for an object.
     */
    public int memory;

    static {
        for (int i = 0; i < Value.TYPE_COUNT; i++) {
            TYPES_BY_VALUE_TYPE.add(null);
        }
        add(Value.NULL, Types.NULL, "Null",
                new DataType(),
                new String[]{"NULL"},
                1
        );
        add(Value.STRING, Types.VARCHAR, "String",
                createString(true),
                new String[]{"VARCHAR", "VARCHAR2", "NVARCHAR", "NVARCHAR2", "VARCHAR_CASESENSITIVE", "CHARACTER VARYING", "TID"},
                4
        );
        add(Value.STRING, Types.LONGVARCHAR, "String",
                createString(true),
                new String[]{"LONGVARCHAR", "LONGNVARCHAR"},
                4
        );
        add(Value.STRING_FIXED, Types.CHAR, "String",
                createString(true),
                new String[]{"CHAR", "CHARACTER", "NCHAR"},
                4
        );
        add(Value.STRING_IGNORECASE, Types.VARCHAR, "String",
                createString(false),
                new String[]{"VARCHAR_IGNORECASE"},
                4
        );
        add(Value.BOOLEAN, DataType.TYPE_BOOLEAN, "Boolean",
                createDecimal(ValueBoolean.PRECISION, ValueBoolean.PRECISION, 0, ValueBoolean.DISPLAY_SIZE, false, false),
                new String[]{"BOOLEAN", "BIT", "BOOL"},
                1
        );
        add(Value.BYTE, Types.TINYINT, "Byte",
                createDecimal(ValueByte.PRECISION, ValueByte.PRECISION, 0, ValueByte.DISPLAY_SIZE, false, false),
                new String[]{"TINYINT"},
                1
        );
        add(Value.SHORT, Types.SMALLINT, "Short",
                createDecimal(ValueShort.PRECISION, ValueShort.PRECISION, 0, ValueShort.DISPLAY_SIZE, false, false),
                new String[]{"SMALLINT", "YEAR", "INT2"},
                5
        );
        add(Value.INT, Types.INTEGER, "Int",
                createDecimal(ValueInt.PRECISION, ValueInt.PRECISION, 0, ValueInt.DISPLAY_SIZE, false, false),
                new String[]{"INTEGER", "INT", "MEDIUMINT", "INT4", "SIGNED"},
                5
        );
        add(Value.LONG, Types.BIGINT, "Long",
                createDecimal(ValueLong.PRECISION, ValueLong.PRECISION, 0, ValueLong.DISPLAY_SIZE, false, false),
                new String[]{"BIGINT", "INT8", "LONG"},
                5
        );
        add(Value.LONG, Types.BIGINT, "Long",
                createDecimal(ValueLong.PRECISION, ValueLong.PRECISION, 0, ValueLong.DISPLAY_SIZE, false, true),
                new String[]{"IDENTITY", "SERIAL"},
                5
        );
        add(Value.DECIMAL, Types.DECIMAL, "BigDecimal",
                createDecimal(Integer.MAX_VALUE, ValueDecimal.DEFAULT_PRECISION, ValueDecimal.DEFAULT_SCALE, ValueDecimal.DEFAULT_DISPLAY_SIZE, true, false),
                new String[]{"DECIMAL", "DEC"},
                17
        );
        add(Value.DECIMAL, Types.NUMERIC, "BigDecimal",
                createDecimal(Integer.MAX_VALUE, ValueDecimal.DEFAULT_PRECISION, ValueDecimal.DEFAULT_SCALE, ValueDecimal.DEFAULT_DISPLAY_SIZE, true, false),
                new String[]{"NUMERIC", "NUMBER"},
                17
        );
        add(Value.FLOAT, Types.REAL, "Float",
                createDecimal(ValueFloat.PRECISION, ValueFloat.PRECISION, 0, ValueFloat.DISPLAY_SIZE, false, false),
                new String[] {"REAL", "FLOAT4"},
                5
        );
        add(Value.DOUBLE, Types.DOUBLE, "Double",
                createDecimal(ValueDouble.PRECISION, ValueDouble.PRECISION, 0, ValueDouble.DISPLAY_SIZE, false, false),
                new String[] { "DOUBLE", "DOUBLE PRECISION" },
                4
        );
        add(Value.DOUBLE, Types.FLOAT, "Double",
                createDecimal(ValueDouble.PRECISION, ValueDouble.PRECISION, 0, ValueDouble.DISPLAY_SIZE, false, false),
                new String[] {"FLOAT", "FLOAT8" },
                4
        );
        add(Value.TIME, Types.TIME, "Time",
                createDate(ValueTime.PRECISION, "TIME", 0, ValueTime.DISPLAY_SIZE),
                new String[]{"TIME"},
                10
        );
        add(Value.DATE, Types.DATE, "Date",
                createDate(ValueDate.PRECISION, "DATE", 0, ValueDate.DISPLAY_SIZE),
                new String[]{"DATE"},
                10
        );
        add(Value.TIMESTAMP, Types.TIMESTAMP, "Timestamp",
                createDate(ValueTimestamp.PRECISION, "TIMESTAMP", ValueTimestamp.DEFAULT_SCALE, ValueTimestamp.DISPLAY_SIZE),
                new String[]{"TIMESTAMP", "DATETIME", "SMALLDATETIME"},
                12
        );
        add(Value.BYTES, Types.VARBINARY, "Bytes",
                createString(false),
                new String[]{"VARBINARY"},
                8
        );
        add(Value.BYTES, Types.BINARY, "Bytes",
                createString(false),
                new String[]{"BINARY", "RAW", "BYTEA", "LONG RAW"},
                8
        );
        add(Value.BYTES, Types.LONGVARBINARY, "Bytes",
                createString(false),
                new String[]{"LONGVARBINARY"},
                8
        );
        add(Value.UUID, Types.BINARY, "Bytes",
                createString(false),
                new String[]{"UUID"},
                8
        );
        add(Value.JAVA_OBJECT, Types.OTHER, "Object",
                createString(false),
                new String[]{"OTHER", "OBJECT", "JAVA_OBJECT"},
                8
        );
        add(Value.BLOB, Types.BLOB, "Blob",
                createLob(),
                new String[]{"BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "IMAGE", "OID"},
                10
        );
        add(Value.CLOB, Types.CLOB, "Clob",
                createLob(),
                new String[]{"CLOB", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "NTEXT", "NCLOB"},
                10
        );
        DataType dataType = new DataType();
        dataType.prefix = "(";
        dataType.suffix = "')";
        add(Value.ARRAY, Types.ARRAY, "Array",
                dataType,
                new String[]{"ARRAY"},
                10
        );
        dataType = new DataType();
        add(Value.RESULT_SET, 0, "ResultSet",
                dataType,
                new String[]{"RESULT_SET"},
                20
        );
        for (int i = 0; i < TYPES_BY_VALUE_TYPE.size(); i++) {
            DataType dt = TYPES_BY_VALUE_TYPE.get(i);
            if (dt == null) {
                Message.throwInternalError("unmapped type " + i);
            }
            Value.getOrder(i);
        }
    }

    private static void add(int type, int sqlType, String jdbc, DataType dataType, String[] names, int memory) {
        for (int i = 0; i < names.length; i++) {
            DataType dt = new DataType();
            dt.type = type;
            dt.sqlType = sqlType;
            dt.jdbc = jdbc;
            dt.name = names[i];
            dt.autoIncrement = dataType.autoIncrement;
            dt.decimal = dataType.decimal;
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
            dt.defaultDisplaySize = dataType.defaultDisplaySize;
            dt.caseSensitive = dataType.caseSensitive;
            dt.hidden = i > 0;
            dt.memory = memory;
            for (DataType t2 : TYPES) {
                if (t2.sqlType == dt.sqlType) {
                    dt.sqlTypePos++;
                }
            }
            TYPES_BY_NAME.put(dt.name, dt);
            if (TYPES_BY_VALUE_TYPE.get(type) == null) {
                TYPES_BY_VALUE_TYPE.set(type, dt);
            }
            TYPES.add(dt);
        }
    }

    private static DataType createDecimal(int maxPrecision, int defaultPrecision, int defaultScale, int defaultDisplaySize, boolean needsPrecisionAndScale, boolean autoInc) {
        DataType dataType = new DataType();
        dataType.maxPrecision = maxPrecision;
        dataType.defaultPrecision = defaultPrecision;
        dataType.defaultScale = defaultScale;
        dataType.defaultDisplaySize = defaultDisplaySize;
        if (needsPrecisionAndScale) {
            dataType.params = "PRECISION,SCALE";
            dataType.supportsPrecision = true;
            dataType.supportsScale = true;
        }
        dataType.decimal = true;
        dataType.autoIncrement = autoInc;
        return dataType;
    }

    private static DataType createDate(int precision, String prefix, int scale, int displaySize) {
        DataType dataType = new DataType();
        dataType.prefix = prefix + " '";
        dataType.suffix = "'";
        dataType.maxPrecision = precision;
        dataType.supportsScale = scale != 0;
        dataType.maxScale = scale;
        dataType.defaultPrecision = precision;
        dataType.defaultScale = scale;
        dataType.defaultDisplaySize = displaySize;
        return dataType;
    }

    private static DataType createString(boolean caseSensitive) {
        DataType dataType = new DataType();
        dataType.prefix = "'";
        dataType.suffix = "'";
        dataType.params = "LENGTH";
        dataType.caseSensitive = caseSensitive;
        dataType.supportsPrecision = true;
        dataType.maxPrecision = Integer.MAX_VALUE;
        dataType.defaultPrecision = Integer.MAX_VALUE;
        dataType.defaultDisplaySize = Integer.MAX_VALUE;
        return dataType;
    }

    private static DataType createLob() {
        DataType t = createString(true);
        t.maxPrecision = Long.MAX_VALUE;
        t.defaultPrecision = Long.MAX_VALUE;
        return t;
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
    public static Value readValue(SessionInterface session, ResultSet rs, int columnIndex, int type) throws SQLException {
        Value v;
        switch(type) {
        case Value.NULL: {
            return ValueNull.INSTANCE;
        }
        case Value.BYTES: {
            byte[] buff = rs.getBytes(columnIndex);
            v = buff == null ? (Value) ValueNull.INSTANCE : ValueBytes.getNoCopy(buff);
            break;
        }
        case Value.UUID: {
            byte[] buff = rs.getBytes(columnIndex);
            v = buff == null ? (Value) ValueNull.INSTANCE : ValueUuid.get(buff);
            break;
        }
        case Value.BOOLEAN: {
            boolean value = rs.getBoolean(columnIndex);
            v = rs.wasNull() ? (Value) ValueNull.INSTANCE : ValueBoolean.get(value);
            break;
        }
        case Value.BYTE: {
            byte value = rs.getByte(columnIndex);
            v = rs.wasNull() ? (Value) ValueNull.INSTANCE : ValueByte.get(value);
            break;
        }
        case Value.DATE: {
            Date value = rs.getDate(columnIndex);
            v = value == null ? (Value) ValueNull.INSTANCE : ValueDate.get(value);
            break;
        }
        case Value.TIME: {
            Time value = rs.getTime(columnIndex);
            v = value == null ? (Value) ValueNull.INSTANCE : ValueTime.get(value);
            break;
        }
        case Value.TIMESTAMP: {
            Timestamp value = rs.getTimestamp(columnIndex);
            v = value == null ? (Value) ValueNull.INSTANCE : ValueTimestamp.get(value);
            break;
        }
        case Value.DECIMAL: {
            BigDecimal value = rs.getBigDecimal(columnIndex);
            v = value == null ? (Value) ValueNull.INSTANCE : ValueDecimal.get(value);
            break;
        }
        case Value.DOUBLE: {
            double value = rs.getDouble(columnIndex);
            v = rs.wasNull() ? (Value) ValueNull.INSTANCE : ValueDouble.get(value);
            break;
        }
        case Value.FLOAT: {
            float value = rs.getFloat(columnIndex);
            v = rs.wasNull() ? (Value) ValueNull.INSTANCE : ValueFloat.get(value);
            break;
        }
        case Value.INT: {
            int value = rs.getInt(columnIndex);
            v = rs.wasNull() ? (Value) ValueNull.INSTANCE : ValueInt.get(value);
            break;
        }
        case Value.LONG: {
            long value = rs.getLong(columnIndex);
            v = rs.wasNull() ? (Value) ValueNull.INSTANCE : ValueLong.get(value);
            break;
        }
        case Value.SHORT: {
            short value = rs.getShort(columnIndex);
            v = rs.wasNull() ? (Value) ValueNull.INSTANCE : ValueShort.get(value);
            break;
        }
        case Value.STRING_IGNORECASE: {
            String s = rs.getString(columnIndex);
            v = (s == null) ? (Value) ValueNull.INSTANCE : ValueStringIgnoreCase.get(s);
            break;
        }
        case Value.STRING_FIXED: {
            String s = rs.getString(columnIndex);
            v = (s == null) ? (Value) ValueNull.INSTANCE : ValueStringFixed.get(s);
            break;
        }
        case Value.STRING: {
            String s = rs.getString(columnIndex);
            v = (s == null) ? (Value) ValueNull.INSTANCE : ValueString.get(s);
            break;
        }
        case Value.CLOB: {
            if (session == null) {
                v = ValueLob.createSmallLob(Value.CLOB, StringUtils.utf8Encode(rs.getString(columnIndex)));
            } else {
                Reader in = rs.getCharacterStream(columnIndex);
                if (in == null) {
                    v = ValueNull.INSTANCE;
                } else {
                    v = ValueLob.createClob(new BufferedReader(in), -1, session.getDataHandler());
                }
            }
            break;
        }
        case Value.BLOB: {
            if (session == null) {
                v = ValueLob.createSmallLob(Value.BLOB, rs.getBytes(columnIndex));
            } else {
                InputStream in = rs.getBinaryStream(columnIndex);
                v = (in == null) ? (Value) ValueNull.INSTANCE : ValueLob.createBlob(in, -1, session.getDataHandler());
            }
            break;
        }
        case Value.JAVA_OBJECT: {
            byte[] buff = rs.getBytes(columnIndex);
            v = buff == null ? (Value) ValueNull.INSTANCE : ValueJavaObject.getNoCopy(buff);
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
            Value[] values = new Value[list.length];
            for (int i = 0; i < list.length; i++) {
                values[i] = DataType.convertToValue(session, list[i], Value.NULL);
            }
            v = ValueArray.get(values);
            break;
        }
        default:
            throw Message.throwInternalError("type="+type);
        }
        return v;
    }

    /**
     * Get the name of the Java class for the given value type.
     *
     * @param type the value type
     * @return the class name
     */
    public static String getTypeClassName(int type) {
        switch(type) {
        case Value.BOOLEAN:
            // "java.lang.Boolean";
            return Boolean.class.getName();
        case Value.BYTE:
            // "java.lang.Byte";
            return Byte.class.getName();
        case Value.SHORT:
            // "java.lang.Short";
            return Short.class.getName();
        case Value.INT:
            // "java.lang.Integer";
            return Integer.class.getName();
        case Value.LONG:
            // "java.lang.Long";
            return Long.class.getName();
        case Value.DECIMAL:
            // "java.math.BigDecimal";
            return BigDecimal.class.getName();
        case Value.TIME:
            // "java.sql.Time";
            return Time.class.getName();
        case Value.DATE:
            // "java.sql.Date";
            return Date.class.getName();
        case Value.TIMESTAMP:
            // "java.sql.Timestamp";
            return Timestamp.class.getName();
        case Value.BYTES:
        case Value.UUID:
            // "[B", not "byte[]";
            return byte[].class.getName();
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            // "java.lang.String";
            return String.class.getName();
        case Value.BLOB:
            if (SysProperties.RETURN_LOB_OBJECTS) {
                // "java.sql.Blob";
                return java.sql.Blob.class.getName();
            }
            // "java.io.InputStream";
            return java.io.InputStream.class.getName();
        case Value.CLOB:
            if (SysProperties.RETURN_LOB_OBJECTS) {
                // "java.sql.Clob";
                return java.sql.Clob.class.getName();
            }
            // "java.io.Reader";
            return java.io.Reader.class.getName();
        case Value.DOUBLE:
            // "java.lang.Double";
            return Double.class.getName();
        case Value.FLOAT:
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
        default:
            throw Message.throwInternalError("type="+type);
        }
    }

    /**
     * Get the data type object for the given value type.
     *
     * @param type the value type
     * @return the data type object
     */
    public static DataType getDataType(int type) {
        DataType dt = TYPES_BY_VALUE_TYPE.get(type);
        if (dt == null) {
            dt = TYPES_BY_VALUE_TYPE.get(Value.NULL);
        }
        return dt;
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
     * Convert a SQL type to a value type.
     *
     * @param sqlType the SQL type
     * @return the value type
     */
    public static int convertSQLTypeToValueType(int sqlType) throws SQLException {
        switch(sqlType) {
        case Types.CHAR:
        case DataType.TYPE_NCHAR:
            return Value.STRING_FIXED;
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case DataType.TYPE_NVARCHAR:
        case DataType.TYPE_LONGNVARCHAR:
            return Value.STRING;
        case Types.NUMERIC:
        case Types.DECIMAL:
            return Value.DECIMAL;
        case Types.BIT:
        case DataType.TYPE_BOOLEAN:
            return Value.BOOLEAN;
        case Types.INTEGER:
            return Value.INT;
        case Types.SMALLINT:
            return Value.SHORT;
        case Types.TINYINT:
            return Value.BYTE;
        case Types.BIGINT:
            return Value.LONG;
        case Types.REAL:
            return Value.FLOAT;
        case Types.DOUBLE:
        case Types.FLOAT:
            return Value.DOUBLE;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            return Value.BYTES;
        case Types.OTHER:
        case Types.JAVA_OBJECT:
            return Value.JAVA_OBJECT;
        case Types.DATE:
            return Value.DATE;
        case Types.TIME:
            return Value.TIME;
        case Types.TIMESTAMP:
            return Value.TIMESTAMP;
        case Types.BLOB:
            return Value.BLOB;
        case Types.CLOB:
        case DataType.TYPE_NCLOB:
            return Value.CLOB;
        case Types.NULL:
            return Value.NULL;
        case Types.ARRAY:
            return Value.ARRAY;
        default:
            throw Message.getSQLException(ErrorCode.UNKNOWN_DATA_TYPE_1, ""+sqlType);
        }
    }

    /**
     * Get the value type for the given Java class.
     *
     * @param x the Java class
     * @return the value type
     */
    public static int getTypeFromClass(Class < ? > x) throws SQLException {
        // TODO refactor: too many if/else in functions, can reduce!
        if (x == null) {
            return Value.NULL;
        }
        if (ResultSet.class.isAssignableFrom(x)) {
            return Value.RESULT_SET;
        } else if (String.class.isAssignableFrom(x)) {
            return Value.STRING;
        } else if (BigDecimal.class.isAssignableFrom(x)) {
            return Value.DECIMAL;
        } else if (Boolean.class.isAssignableFrom(x) || boolean.class.isAssignableFrom(x)) {
            return Value.BOOLEAN;
        } else if (Byte.class.isAssignableFrom(x) || byte.class.isAssignableFrom(x)) {
            return Value.BYTE;
        } else if (Short.class.isAssignableFrom(x) || short.class.isAssignableFrom(x)) {
            return Value.SHORT;
        } else if (Integer.class.isAssignableFrom(x) || int.class.isAssignableFrom(x)) {
            return Value.INT;
        } else if (Character.class.isAssignableFrom(x) || char.class.isAssignableFrom(x)) {
            throw Message.getSQLException(ErrorCode.DATA_CONVERSION_ERROR_1, "char (not supported)");
        } else if (Long.class.isAssignableFrom(x) || long.class.isAssignableFrom(x)) {
            return Value.LONG;
        } else if (Float.class.isAssignableFrom(x) || float.class.isAssignableFrom(x)) {
            return Value.FLOAT;
        } else if (Double.class.isAssignableFrom(x) || double.class.isAssignableFrom(x)) {
            return Value.DOUBLE;
        } else if (byte[].class.isAssignableFrom(x)) {
            return Value.BYTES;
        } else if (Date.class.isAssignableFrom(x)) {
            return Value.DATE;
        } else if (Time.class.isAssignableFrom(x)) {
            return Value.TIME;
        } else if (Timestamp.class.isAssignableFrom(x)) {
            return Value.TIMESTAMP;
        } else if (java.util.Date.class.isAssignableFrom(x)) {
            return Value.TIMESTAMP;
        } else if (java.io.Reader.class.isAssignableFrom(x)) {
            return Value.CLOB;
        } else if (java.sql.Clob.class.isAssignableFrom(x)) {
            return Value.CLOB;
        } else if (java.io.InputStream.class.isAssignableFrom(x)) {
            return Value.BLOB;
        } else if (java.sql.Blob.class.isAssignableFrom(x)) {
            return Value.BLOB;
        } else if (UUID.class.isAssignableFrom(x)) {
            return Value.UUID;
        } else if (Object[].class.isAssignableFrom(x)) {
            return Value.ARRAY;
        } else if (Void.TYPE == x) {
            return Value.NULL;
        } else {
            return Value.JAVA_OBJECT;
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
    public static Value convertToValue(SessionInterface session, Object x, int type) throws SQLException {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (type == Value.JAVA_OBJECT) {
            return ValueJavaObject.getNoCopy(ObjectUtils.serialize(x));
        }
        if (x instanceof String) {
            return ValueString.get((String) x);
        } else if (x instanceof BigDecimal) {
            return ValueDecimal.get((BigDecimal) x);
        } else if (x instanceof Boolean) {
            return ValueBoolean.get(((Boolean) x).booleanValue());
        } else if (x instanceof Byte) {
            return ValueByte.get(((Byte) x).byteValue());
        } else if (x instanceof Short) {
            return ValueShort.get(((Short) x).shortValue());
        } else if (x instanceof Integer) {
            return ValueInt.get(((Integer) x).intValue());
        } else if (x instanceof Long) {
            return ValueLong.get(((Long) x).longValue());
        } else if (x instanceof Float) {
            return ValueFloat.get(((Float) x).floatValue());
        } else if (x instanceof Double) {
            return ValueDouble.get(((Double) x).doubleValue());
        } else if (x instanceof byte[]) {
            return ValueBytes.get((byte[]) x);
        } else if (x instanceof Date) {
            return ValueDate.get((Date) x);
        } else if (x instanceof Time) {
            return ValueTime.get((Time) x);
        } else if (x instanceof Timestamp) {
            return ValueTimestamp.get((Timestamp) x);
        } else if (x instanceof java.util.Date) {
            return ValueTimestamp.get(new Timestamp(((java.util.Date) x).getTime()));
        } else if (x instanceof java.io.Reader) {
            Reader r = new BufferedReader((java.io.Reader) x);
            return ValueLob.createClob(r, -1, session.getDataHandler());
        } else if (x instanceof java.sql.Clob) {
            Reader r = new BufferedReader(((java.sql.Clob) x).getCharacterStream());
            return ValueLob.createClob(r, -1, session.getDataHandler());
        } else if (x instanceof java.io.InputStream) {
            return ValueLob.createBlob((java.io.InputStream) x, -1, session.getDataHandler());
        } else if (x instanceof java.sql.Blob) {
            return ValueLob.createBlob(((java.sql.Blob) x).getBinaryStream(), -1, session.getDataHandler());
        } else if (x instanceof ResultSet) {
            return ValueResultSet.get((ResultSet) x);
        } else if (x instanceof UUID) {
            UUID u = (UUID) x;
            return ValueUuid.get(u.getMostSignificantBits(), u.getLeastSignificantBits());
        } else if (x instanceof Object[]) {
            // (a.getClass().isArray());
            // (a.getClass().getComponentType().isPrimitive());
            Object[] o = (Object[]) x;
            int len = o.length;
            Value[] v = new Value[len];
            for (int i = 0; i < len; i++) {
                v[i] = convertToValue(session, o[i], type);
            }
            return ValueArray.get(v);
        } else if (x instanceof Character) {
            return ValueStringFixed.get(((Character) x).toString());
        } else {
            return ValueJavaObject.getNoCopy(ObjectUtils.serialize(x));
        }
    }

    /**
     * Get a data type object from a type name.
     *
     * @param s the type name
     * @return the data type object
     */
    public static DataType getTypeByName(String s) {
        return TYPES_BY_NAME.get(s);
    }

    /**
     * Check if the given value type is a large object (BLOB or CLOB).
     *
     * @param type the value type
     * @return true if the value type is a lob type
     */
    public static boolean isLargeObject(int type) {
        if (type == Value.BLOB || type == Value.CLOB) {
            return true;
        }
        return false;
    }

    /**
     * Check if the given value type supports the add operation.
     *
     * @param type the value type
     * @return true if add is supported
     */
    public static boolean supportsAdd(int type) {
        switch (type) {
        case Value.BYTE:
        case Value.DECIMAL:
        case Value.DOUBLE:
        case Value.FLOAT:
        case Value.INT:
        case Value.LONG:
        case Value.SHORT:
            return true;
        default:
            return false;
        }
    }

    /**
     * Get the data type that will not overflow when calling 'add' 2 billion times.
     *
     * @param type the value type
     * @return the data type that supports adding
     */
    public static int getAddProofType(int type) {
        switch (type) {
        case Value.BYTE:
            return Value.LONG;
        case Value.FLOAT:
            return Value.DOUBLE;
        case Value.INT:
            return Value.LONG;
        case Value.LONG:
            return Value.DECIMAL;
        case Value.SHORT:
            return Value.LONG;
        default:
            return type;
        }
    }

    /**
     * Get the default value in the form of a Java object for the given Java class.
     *
     * @param clazz the Java class
     * @return the default object
     */
    public static Object getDefaultForPrimitiveType(Class< ? > clazz) {
        if (clazz == Boolean.TYPE) {
            return Boolean.FALSE;
        } else if (clazz == Byte.TYPE) {
            return Byte.valueOf((byte) 0);
        } else if (clazz == Character.TYPE) {
            return Character.valueOf((char) 0);
        } else if (clazz == Short.TYPE) {
            return Short.valueOf((short) 0);
        } else if (clazz == Integer.TYPE) {
            return Integer.valueOf(0);
        } else if (clazz == Long.TYPE) {
            return Long.valueOf(0);
        } else if (clazz == Float.TYPE) {
            return Float.valueOf(0);
        } else if (clazz == Double.TYPE) {
            return Double.valueOf(0);
        }
        throw Message.throwInternalError("primitive=" + clazz.toString());
    }

    /**
     * Convert a value to the specified class.
     *
     * @param session the session
     * @param conn the database connection
     * @param v the value
     * @param paramClass the target class
     * @return the converted object
     */
    public static Object convertTo(SessionInterface session, JdbcConnection conn, Value v, Class< ? > paramClass)
            throws SQLException {
        if (paramClass == Blob.class) {
            return new JdbcBlob(conn, v, 0);
        } else if (paramClass == Clob.class) {
            return new JdbcClob(conn, v, 0);
        }
        throw Message.getUnsupportedException(paramClass.getName());
    }

}
