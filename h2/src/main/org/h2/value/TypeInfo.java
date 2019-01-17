/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;

/**
 * Data type with parameters.
 */
public class TypeInfo {

    /**
     * UNKNOWN type with parameters.
     */
    public static final TypeInfo TYPE_UNKNOWN;

    /**
     * NULL type with parameters.
     */
    public static final TypeInfo TYPE_NULL;

    /**
     * BOOLEAN type with parameters.
     */
    public static final TypeInfo TYPE_BOOLEAN;

    /**
     * BYTE type with parameters.
     */
    public static final TypeInfo TYPE_BYTE;

    /**
     * SHORT type with parameters.
     */
    public static final TypeInfo TYPE_SHORT;

    /**
     * INT type with parameters.
     */
    public static final TypeInfo TYPE_INT;

    /**
     * LONG type with parameters.
     */
    public static final TypeInfo TYPE_LONG;

    /**
     * DECIMAL type with default parameters.
     */
    public static final TypeInfo TYPE_DECIMAL_DEFAULT;

    /**
     * DOUBLE type with parameters.
     */
    public static final TypeInfo TYPE_DOUBLE;

    /**
     * FLOAT type with parameters.
     */
    public static final TypeInfo TYPE_FLOAT;

    /**
     * TIME type with parameters.
     */
    public static final TypeInfo TYPE_TIME;

    /**
     * DATE type with parameters.
     */
    public static final TypeInfo TYPE_DATE;

    /**
     * TIMESTAMP type with parameters.
     */
    public static final TypeInfo TYPE_TIMESTAMP;

    /**
     * STRING type with default parameters.
     */
    public static final TypeInfo TYPE_STRING_DEFAULT;

    /**
     * ARRAY type with parameters.
     */
    public static final TypeInfo TYPE_ARRAY;

    /**
     * RESULT_SET type with parameters.
     */
    public static final TypeInfo TYPE_RESULT_SET;

    /**
     * JAVA_OBJECT type with parameters.
     */
    public static final TypeInfo TYPE_JAVA_OBJECT;

    /**
     * UUID type with parameters.
     */
    public static final TypeInfo TYPE_UUID;

    /**
     * GEOMETRY type with default parameters.
     */
    public static final TypeInfo TYPE_GEOMETRY;

    /**
     * TIMESTAMP WITH TIME ZONE type with parameters.
     */
    public static final TypeInfo TYPE_TIMESTAMP_TZ;

    /**
     * ENUM type with undefined parameters.
     */
    public static final TypeInfo TYPE_ENUM_UNDEFINED;

    /**
     * INTERVAL DAY type with parameters.
     */
    public static final TypeInfo TYPE_INTERVAL_DAY;

    /**
     * INTERVAL DAY TO SECOND type with parameters.
     */
    public static final TypeInfo TYPE_INTERVAL_DAY_TO_SECOND;

    /**
     * INTERVAL HOUR TO SECOND type with parameters.
     */
    public static final TypeInfo TYPE_INTERVAL_HOUR_TO_SECOND;

    /**
     * ROW (row value) type with parameters.
     */
    public static final TypeInfo TYPE_ROW;

    private static final TypeInfo[] TYPE_INFOS_BY_VALUE_TYPE;

    private final int valueType;

    private final long precision;

    private final int scale;

    private final int displaySize;

    private final ExtTypeInfo extTypeInfo;

    static {
        DataType[] type = DataType.TYPES_BY_VALUE_TYPE;
        TypeInfo[] infos = new TypeInfo[Value.TYPE_COUNT];
        for (int i = 0; i < type.length; i++) {
            DataType dt = type[i];
            if (dt != null) {
                Value.getOrder(i);
                infos[i] = createTypeInfo(i, dt);
            }
        }
        TYPE_UNKNOWN = new TypeInfo(Value.UNKNOWN, -1L, -1, -1, null);
        TYPE_NULL = infos[Value.NULL];
        TYPE_BOOLEAN = infos[Value.BOOLEAN];
        TYPE_BYTE = infos[Value.BYTE];
        TYPE_SHORT = infos[Value.SHORT];
        TYPE_INT = infos[Value.INT];
        TYPE_LONG = infos[Value.LONG];
        TYPE_DECIMAL_DEFAULT = infos[Value.DECIMAL];
        TYPE_DOUBLE = infos[Value.DOUBLE];
        TYPE_FLOAT = infos[Value.FLOAT];
        TYPE_TIME = infos[Value.TIME];
        TYPE_DATE = infos[Value.DATE];
        TYPE_TIMESTAMP = infos[Value.TIMESTAMP];
        TYPE_STRING_DEFAULT = infos[Value.STRING];
        TYPE_ARRAY = infos[Value.ARRAY];
        TYPE_RESULT_SET = infos[Value.RESULT_SET];
        TYPE_JAVA_OBJECT = infos[Value.JAVA_OBJECT];
        TYPE_UUID = infos[Value.UUID];
        TYPE_GEOMETRY = infos[Value.GEOMETRY];
        TYPE_TIMESTAMP_TZ = infos[Value.TIMESTAMP_TZ];
        TYPE_ENUM_UNDEFINED = infos[Value.ENUM];
        TYPE_INTERVAL_DAY = infos[Value.INTERVAL_DAY];
        TYPE_INTERVAL_DAY_TO_SECOND = infos[Value.INTERVAL_DAY_TO_SECOND];
        TYPE_INTERVAL_HOUR_TO_SECOND = infos[Value.INTERVAL_HOUR_TO_SECOND];
        TYPE_ROW = infos[Value.ROW];
        TYPE_INFOS_BY_VALUE_TYPE = infos;
    }

    /**
     * Get the data type with parameters object for the given value type and
     * maximum parameters.
     *
     * @param type
     *            the value type
     * @return the data type with parameters object
     */
    public static TypeInfo getTypeInfo(int type) {
        if (type == Value.UNKNOWN) {
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?");
        }
        if (type >= Value.NULL && type < Value.TYPE_COUNT) {
            TypeInfo t = TYPE_INFOS_BY_VALUE_TYPE[type];
            if (t != null) {
                return t;
            }
        }
        if (JdbcUtils.customDataTypesHandler != null) {
            DataType dt = JdbcUtils.customDataTypesHandler.getDataTypeById(type);
            if (dt != null) {
                return createTypeInfo(type, dt);
            }
        }
        return TYPE_NULL;
    }

    /**
     * Get the data type with parameters object for the given value type and the
     * specified parameters.
     *
     * @param type
     *            the value type
     * @param precision
     *            the precision
     * @param scale
     *            the scale
     * @param displaySize
     *            the display size in characters
     * @param extTypeInfo
     *            the extended type information, or null
     * @return the data type with parameters object
     */
    public static TypeInfo getTypeInfo(int type, long precision, int scale, int displaySize, ExtTypeInfo extTypeInfo) {
        switch (type) {
        case Value.NULL:
        case Value.BOOLEAN:
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
        case Value.LONG:
        case Value.DOUBLE:
        case Value.FLOAT:
        case Value.DATE:
        case Value.ARRAY:
        case Value.RESULT_SET:
        case Value.JAVA_OBJECT:
        case Value.UUID:
        case Value.ROW:
            return TYPE_INFOS_BY_VALUE_TYPE[type];
        case Value.UNKNOWN:
            return TYPE_UNKNOWN;
        case Value.DECIMAL:
            if (precision < 0) {
                precision = ValueDecimal.DEFAULT_PRECISION;
            }
            if (scale < 0) {
                scale = ValueDecimal.DEFAULT_SCALE;
            }
            return new TypeInfo(Value.DECIMAL, precision, scale, MathUtils.convertLongToInt(precision + 2), null);
        case Value.TIME:
            if (scale < 0 || scale >= ValueTime.MAXIMUM_SCALE) {
                return TYPE_TIME;
            }
            return new TypeInfo(Value.TIME, ValueTime.MAXIMUM_PRECISION, scale, ValueTime.DEFAULT_PRECISION, null);
        case Value.TIMESTAMP:
            if (scale < 0 || scale >= ValueTimestamp.MAXIMUM_SCALE) {
                return TYPE_TIMESTAMP;
            }
            return new TypeInfo(Value.TIMESTAMP, ValueTimestamp.MAXIMUM_PRECISION, scale,
                    ValueTimestamp.MAXIMUM_PRECISION, null);
        case Value.TIMESTAMP_TZ:
            if (scale < 0 || scale >= ValueTimestampTimeZone.MAXIMUM_SCALE) {
                return TYPE_TIMESTAMP_TZ;
            }
            return new TypeInfo(Value.TIMESTAMP_TZ, ValueTimestampTimeZone.MAXIMUM_PRECISION, scale,
                    ValueTimestampTimeZone.MAXIMUM_PRECISION, null);
        case Value.BYTES:
            if (precision < 0) {
                precision = Integer.MAX_VALUE;
            }
            return new TypeInfo(Value.BYTES, precision, scale, MathUtils.convertLongToInt(precision) * 2, null);
        case Value.STRING:
            if (precision < 0) {
                return TYPE_STRING_DEFAULT;
            }
            //$FALL-THROUGH$
        case Value.STRING_FIXED:
        case Value.STRING_IGNORECASE:
        case Value.BLOB:
        case Value.CLOB:
            return new TypeInfo(type, precision, 0, MathUtils.convertLongToInt(precision), null);
        case Value.GEOMETRY:
            if (extTypeInfo == null) {
                return TYPE_GEOMETRY;
            }
            return new TypeInfo(Value.GEOMETRY, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, extTypeInfo);
        case Value.ENUM:
            if (extTypeInfo == null) {
                return TYPE_ENUM_UNDEFINED;
            }
            return new TypeInfo(Value.ENUM, ValueEnum.PRECISION, 0, ValueEnum.DISPLAY_SIZE, extTypeInfo);
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_MINUTE:
            if (precision < 0 || precision > ValueInterval.MAXIMUM_PRECISION) {
                precision = ValueInterval.MAXIMUM_PRECISION;
            }
            return new TypeInfo(type, precision, 0, ValueInterval.getDisplaySize(type, (int) precision, 0), null);
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            if (precision < 0 || precision > ValueInterval.MAXIMUM_PRECISION) {
                precision = ValueInterval.MAXIMUM_PRECISION;
            }
            if (scale < 0 || scale > ValueInterval.MAXIMUM_SCALE) {
                scale = ValueInterval.MAXIMUM_SCALE;
            }
            return new TypeInfo(type, precision, scale, ValueInterval.getDisplaySize(type, (int) precision, scale),
                    null);
        }
        if (JdbcUtils.customDataTypesHandler != null) {
            DataType dt = JdbcUtils.customDataTypesHandler.getDataTypeById(type);
            if (dt != null) {
                return createTypeInfo(type, dt);
            }
        }
        return TYPE_NULL;
    }

    private static TypeInfo createTypeInfo(int valueType, DataType dataType) {
        return new TypeInfo(valueType, dataType.maxPrecision, dataType.maxScale, dataType.defaultDisplaySize, null);
    }

    /**
     * Creates new instance of data type with parameters.
     *
     * @param valueType
     *            the value type
     * @param precision
     *            the precision
     * @param scale
     *            the scale
     * @param displaySize
     *            the display size in characters
     * @param extTypeInfo
     *            the extended type information, or null
     */
    public TypeInfo(int valueType, long precision, int scale, int displaySize, ExtTypeInfo extTypeInfo) {
        this.valueType = valueType;
        this.precision = precision;
        this.scale = scale;
        this.displaySize = displaySize;
        this.extTypeInfo = extTypeInfo;
    }

    /**
     * Returns the value type.
     *
     * @return the value type
     */
    public int getValueType() {
        return valueType;
    }

    /**
     * Returns the precision.
     *
     * @return the precision
     */
    public long getPrecision() {
        return precision;
    }

    /**
     * Returns the scale.
     *
     * @return the scale
     */
    public int getScale() {
        return scale;
    }

    /**
     * Returns the display size in characters.
     *
     * @return the display size
     */
    public int getDisplaySize() {
        return displaySize;
    }

    /**
     * Returns the extended type information, or null.
     *
     * @return the extended type information, or null
     */
    public ExtTypeInfo getExtTypeInfo() {
        return extTypeInfo;
    }

    /**
     * Appends SQL representation of this object to the specified string
     * builder.
     *
     * @param builder
     *            string builder
     * @return the specified string builder
     */
    public StringBuilder getSQL(StringBuilder builder) {
        DataType dataType = DataType.getDataType(valueType);
        if (valueType == Value.TIMESTAMP_TZ) {
            builder.append("TIMESTAMP");
        } else {
            builder.append(dataType.name);
        }
        switch (valueType) {
        case Value.DECIMAL:
            builder.append('(').append(precision).append(", ").append(scale).append(')');
            break;
        case Value.GEOMETRY:
            if (extTypeInfo == null) {
                break;
            }
            //$FALL-THROUGH$
        case Value.ENUM:
            builder.append(extTypeInfo.getCreateSQL());
            break;
        case Value.BYTES:
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            if (precision < Integer.MAX_VALUE) {
                builder.append('(').append(precision).append(')');
            }
            break;
        case Value.TIME:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            if (scale != dataType.defaultScale) {
                builder.append('(').append(scale).append(')');
            }
            if (valueType == Value.TIMESTAMP_TZ) {
                builder.append(" WITH TIME ZONE");
            }
        }
        return builder;
    }

}
