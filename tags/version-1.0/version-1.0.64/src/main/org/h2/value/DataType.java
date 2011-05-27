/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.SessionInterface;
import org.h2.jdbc.JdbcBlob;
import org.h2.jdbc.JdbcClob;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.util.ObjectArray;
import org.h2.util.ObjectUtils;
import org.h2.util.StringUtils;

/**
 * This class contains meta data information about data types,
 * and can convert between Java objects and Values.
 */
public class DataType {
    private static ObjectArray types = new ObjectArray();
    private static HashMap typesByName = new HashMap();
    private static DataType[] typesByValueType = new DataType[Value.TYPE_COUNT];
    public int type;
    public String name;
    public int sqlType;
    public String jdbc;

    // how closely the data type maps to the corresponding JDBC SQL type (low is best)
    public int sqlTypePos;

    public int maxPrecision;
    public int minScale, maxScale;
    public boolean decimal;
    public String prefix, suffix;
    public String params;
    public boolean autoInc;
    public boolean caseSensitive;
    public boolean supportsPrecision, supportsScale;
    public long defaultPrecision;
    public int defaultScale;
    public int defaultDisplaySize;
    public boolean hidden;
    public int memory;

    // for operations that include different types, convert both to the higher order
    public int order;

    // JDK 1.3 compatibility: Types.BOOLEAN
    public static final int TYPE_BOOLEAN = 16;

    // JDK 1.3 compatibility: Types.DATALINK
    public static final int TYPE_DATALINK = 70;

    static {
//#ifdef JDK14
        if (TYPE_BOOLEAN != Types.BOOLEAN) {
            new Exception("Types.BOOLEAN: " + Types.BOOLEAN).printStackTrace();
        }
        if (TYPE_DATALINK != Types.DATALINK) {
            new Exception("Types.DATALINK: " + Types.DATALINK).printStackTrace();
        }

//#endif
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
                new String[]{"LONGVARCHAR"},
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
                1
        );
        add(Value.INT, Types.INTEGER, "Int",
                createDecimal(ValueInt.PRECISION, ValueInt.PRECISION, 0, ValueInt.DISPLAY_SIZE, false, false),
                new String[]{"INTEGER", "INT", "MEDIUMINT", "INT4", "SIGNED"},
                1
        );
        add(Value.LONG, Types.BIGINT, "Long",
                createDecimal(ValueLong.PRECISION, ValueLong.PRECISION, 0, ValueLong.DISPLAY_SIZE, false, false),
                new String[]{"BIGINT", "INT8"},
                1
        );
        add(Value.LONG, Types.BIGINT, "Long",
                createDecimal(ValueLong.PRECISION, ValueLong.PRECISION, 0, ValueLong.DISPLAY_SIZE, false, true),
                new String[]{"IDENTITY", "SERIAL"},
                1
        );
        add(Value.DECIMAL, Types.DECIMAL, "BigDecimal",
                createDecimal(Integer.MAX_VALUE, ValueDecimal.DEFAULT_PRECISION, ValueDecimal.DEFAULT_SCALE, ValueDecimal.DEFAULT_DISPLAY_SIZE, true, false),
                new String[]{"DECIMAL", "DEC"},
                7
                // TODO value: are NaN, Inf, -Inf,... supported as well?
        );
        add(Value.DECIMAL, Types.NUMERIC, "BigDecimal",
                createDecimal(Integer.MAX_VALUE, ValueDecimal.DEFAULT_PRECISION, ValueDecimal.DEFAULT_SCALE, ValueDecimal.DEFAULT_DISPLAY_SIZE, true, false),
                new String[]{"NUMERIC", "NUMBER"},
                7
                // TODO value: are NaN, Inf, -Inf,... supported as well?
        );
        add(Value.FLOAT, Types.REAL, "Float",
                createDecimal(ValueFloat.PRECISION, ValueFloat.PRECISION, 0, ValueFloat.DISPLAY_SIZE, false, false),
                new String[] {"REAL", "FLOAT4"},
                1
        );
        add(Value.DOUBLE, Types.DOUBLE, "Double",
                createDecimal(ValueDouble.PRECISION, ValueDouble.PRECISION, 0, ValueDouble.DISPLAY_SIZE, false, false),
                new String[] { "DOUBLE", "DOUBLE PRECISION" },
                1
        );
        add(Value.DOUBLE, Types.FLOAT, "Double",
                createDecimal(ValueDouble.PRECISION, ValueDouble.PRECISION, 0, ValueDouble.DISPLAY_SIZE, false, false),
                new String[] {"FLOAT", "FLOAT8" },
                1
                // TODO value: show min and max values, E format if supported
        );
        add(Value.TIME, Types.TIME, "Time",
                createDate(ValueTime.PRECISION, "TIME", 0, ValueTime.DISPLAY_SIZE),
                new String[]{"TIME"},
                4
                // TODO value: min / max for time
        );
        add(Value.DATE, Types.DATE, "Date",
                createDate(ValueDate.PRECISION, "DATE", 0, ValueDate.DISPLAY_SIZE),
                new String[]{"DATE"},
                4
                // TODO value: min / max for date
        );
        add(Value.TIMESTAMP, Types.TIMESTAMP, "Timestamp",
                createDate(ValueTimestamp.PRECISION, "TIMESTAMP", ValueTimestamp.DEFAULT_SCALE, ValueTimestamp.DISPLAY_SIZE),
                new String[]{"TIMESTAMP", "DATETIME", "SMALLDATETIME"},
                4
                // TODO value: min / max for timestamp
        );
        add(Value.BYTES, Types.VARBINARY, "Bytes",
                createString(false),
                new String[]{"VARBINARY"},
                4
        );
        add(Value.BYTES, Types.BINARY, "Bytes",
                createString(false),
                new String[]{"BINARY", "RAW", "BYTEA", "LONG RAW"},
                4
        );
        add(Value.BYTES, Types.LONGVARBINARY, "Bytes",
                createString(false),
                new String[]{"LONGVARBINARY"},
                4
        );
        add(Value.UUID, Types.BINARY, "Bytes",
                createString(false),
                new String[]{"UUID"},
                4
        );
        add(Value.JAVA_OBJECT, Types.OTHER, "Object",
                createString(false),
                new String[]{"OTHER", "OBJECT", "JAVA_OBJECT"},
                4
        );
        add(Value.BLOB, Types.BLOB, "Bytes",
                createString(false),
                new String[]{"BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "IMAGE", "OID"},
                4
        );
        add(Value.CLOB, Types.CLOB, "String",
                createString(true),
                new String[]{"CLOB", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "NTEXT", "NCLOB"},
                4
        );
        DataType dataType = new DataType();
        dataType.prefix = "(";
        dataType.suffix = "')";
        add(Value.ARRAY, Types.ARRAY, "Array",
                dataType,
                new String[]{"ARRAY"},
                2
        );
        dataType = new DataType();
        add(Value.RESULT_SET, 0, "ResultSet",
                dataType,
                new String[]{"RESULT_SET"},
                2
        );
        for (int i = 0; i < typesByValueType.length; i++) {
            DataType dt = typesByValueType[i];
            if (dt == null) {
                throw Message.getInternalError("unmapped type " + i);
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
            dt.autoInc = dataType.autoInc;
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
            for (int j = 0; j < types.size(); j++) {
                DataType t2 = (DataType) types.get(j);
                if (t2.sqlType == dt.sqlType) {
                    dt.sqlTypePos++;
                }
            }
            typesByName.put(dt.name, dt);
            if (typesByValueType[type] == null) {
                typesByValueType[type] = dt;
            }
            types.add(dt);
        }
    }

    public static String getJdbcString(int type) {
        return typesByValueType[type].jdbc;
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
        dataType.autoInc = autoInc;
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

    public static ObjectArray getTypes() {
        return types;
    }

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
            throw Message.getInternalError("type="+type);
        }
        return v;
    }

    public static String getTypeClassName(int type) {
        switch(type) {
        case Value.BOOLEAN:
            return Boolean.class.getName(); //  "java.lang.Boolean";
        case Value.BYTE:
            return Byte.class.getName(); // "java.lang.Byte";
        case Value.SHORT:
            return Short.class.getName(); // "java.lang.Short";
        case Value.INT:
            return Integer.class.getName(); // "java.lang.Integer";
        case Value.LONG:
            return Long.class.getName(); // "java.lang.Long";
        case Value.DECIMAL:
            return BigDecimal.class.getName(); // "java.math.BigDecimal";
        case Value.TIME:
            return Time.class.getName(); // "java.sql.Time";
        case Value.DATE:
            return Date.class.getName(); // "java.sql.Date";
        case Value.TIMESTAMP:
            return Timestamp.class.getName(); // "java.sql.Timestamp";
        case Value.BYTES:
        case Value.UUID:
            return byte[].class.getName(); // "[B", not "byte[]";
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            return String.class.getName(); // "java.lang.String";
        case Value.BLOB:
            return java.sql.Blob.class.getName(); // "java.sql.Blob";
        case Value.CLOB:
            return java.sql.Clob.class.getName(); // "java.sql.Clob";
        case Value.DOUBLE:
            return Double.class.getName(); // "java.lang.Double";
        case Value.FLOAT:
            return Float.class.getName(); // "java.lang.Float";
        case Value.NULL:
            return null;
        case Value.JAVA_OBJECT:
            return Object.class.getName(); // "java.lang.Object";
        default:
            throw Message.getInternalError("type="+type);
        }
    }

    public static DataType getDataType(int type) {
        DataType dt = typesByValueType[type];
        if (dt == null) {
            dt = typesByValueType[Value.NULL];
        }
        return dt;
    }

    public static int convertTypeToSQLType(int type) {
        return getDataType(type).sqlType;
    }

    public static int convertSQLTypeToValueType(int sqlType) throws SQLException {
        switch(sqlType) {
        case Types.CHAR:
            return Value.STRING_FIXED;
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
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
            return Value.CLOB;
        case Types.NULL:
            return Value.NULL;
        case Types.ARRAY:
            return Value.ARRAY;
        default:
            throw Message.getSQLException(ErrorCode.UNKNOWN_DATA_TYPE_1, ""+sqlType);
        }
    }

    public static int getTypeFromClass(Class x) throws SQLException {
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
        } else if (Object[].class.isAssignableFrom(x)) {
            return Value.ARRAY;
        } else if (Void.TYPE == x) {
            return Value.NULL;
        } else {
            return Value.JAVA_OBJECT;
        }
    }

    public static Value convertToValue(SessionInterface session, Object x, int type) throws SQLException {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (type == Value.JAVA_OBJECT) {
            // serialize JAVA_OBJECT, even if the type is known
            if (Constants.SERIALIZE_JAVA_OBJECTS) {
                return ValueJavaObject.getNoCopy(ObjectUtils.serialize(x));
            }
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
        } else {
            if (Constants.SERIALIZE_JAVA_OBJECTS) {
                return ValueJavaObject.getNoCopy(ObjectUtils.serialize(x));
            } else {
                throw Message.getSQLException(ErrorCode.UNKNOWN_DATA_TYPE_1, x.getClass().getName());
            }
        }
    }

    public static DataType getTypeByName(String s) {
        return (DataType) typesByName.get(s);
    }

    public static boolean isLargeObject(int type) {
        if (type == Value.BLOB || type == Value.CLOB) {
            return true;
        }
        return false;
    }

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

    public static Object getDefaultForPrimitiveType(Class clazz) {
        if (clazz == Boolean.TYPE) {
            return Boolean.FALSE;
        } else if (clazz == Byte.TYPE) {
            return ObjectUtils.getByte((byte) 0);
        } else if (clazz == Character.TYPE) {
            return ObjectUtils.getCharacter((char) 0);
        } else if (clazz == Short.TYPE) {
            return ObjectUtils.getShort((short) 0);
        } else if (clazz == Integer.TYPE) {
            return ObjectUtils.getInteger(0);
        } else if (clazz == Long.TYPE) {
            return ObjectUtils.getLong(0);
        } else if (clazz == Float.TYPE) {
            return ObjectUtils.getFloat(0);
        } else if (clazz == Double.TYPE) {
            return ObjectUtils.getDouble(0);
        } else {
            throw Message.getInternalError("primitive=" + clazz.toString());
        }
    }

    public static Object convertTo(SessionInterface session, JdbcConnection conn, Value v, Class paramClass)
            throws JdbcSQLException {
        if (paramClass == java.sql.Blob.class) {
            return new JdbcBlob(session, conn, v, 0);
        } else if (paramClass == Clob.class) {
            return new JdbcClob(session, conn, v, 0);
        } else {
            throw Message.getUnsupportedException();
        }
    }

}
