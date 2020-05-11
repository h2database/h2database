/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.Objects;
import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.message.DbException;
import org.h2.util.MathUtils;

/**
 * Data type with parameters.
 */
public class TypeInfo extends ExtTypeInfo {

    /**
     * UNKNOWN type with parameters.
     */
    public static final TypeInfo TYPE_UNKNOWN;

    /**
     * NULL type with parameters.
     */
    public static final TypeInfo TYPE_NULL;

    /**
     * CHARACTER VARYING type with maximum parameters.
     */
    public static final TypeInfo TYPE_VARCHAR;

    /**
     * VARCHAR_IGNORECASE type with maximum parameters.
     */
    public static final TypeInfo TYPE_VARCHAR_IGNORECASE;

    /**
     * CLOB type with maximum parameters.
     */
    public static final TypeInfo TYPE_CLOB;

    /**
     * BINARY VARYING type with maximum parameters.
     */
    public static final TypeInfo TYPE_VARBINARY;

    /**
     * BLOB type with maximum parameters.
     */
    public static final TypeInfo TYPE_BLOB;

    /**
     * BOOLEAN type with parameters.
     */
    public static final TypeInfo TYPE_BOOLEAN;

    /**
     * TINYINT type with parameters.
     */
    public static final TypeInfo TYPE_TINYINT;

    /**
     * SMALLINT type with parameters.
     */
    public static final TypeInfo TYPE_SMALLINT;

    /**
     * INTEGER type with parameters.
     */
    public static final TypeInfo TYPE_INTEGER;

    /**
     * BIGINT type with parameters.
     */
    public static final TypeInfo TYPE_BIGINT;

    /**
     * NUMERIC type with maximum parameters.
     */
    public static final TypeInfo TYPE_NUMERIC;

    /**
     * NUMERIC type with parameters enough to hold a BIGINT value.
     */
    public static final TypeInfo TYPE_NUMERIC_BIGINT;

    /**
     * NUMERIC type that can hold values with floating point.
     */
    public static final TypeInfo TYPE_NUMERIC_FLOATING_POINT;

    /**
     * REAL type with parameters.
     */
    public static final TypeInfo TYPE_REAL;

    /**
     * DOUBLE PRECISION type with parameters.
     */
    public static final TypeInfo TYPE_DOUBLE;

    /**
     * DATE type with parameters.
     */
    public static final TypeInfo TYPE_DATE;

    /**
     * TIME type with maximum parameters.
     */
    public static final TypeInfo TYPE_TIME;

    /**
     * TIME WITH TIME ZONE type with maximum parameters.
     */
    public static final TypeInfo TYPE_TIME_TZ;

    /**
     * TIMESTAMP type with maximum parameters.
     */
    public static final TypeInfo TYPE_TIMESTAMP;

    /**
     * TIMESTAMP WITH TIME ZONE type with maximum parameters.
     */
    public static final TypeInfo TYPE_TIMESTAMP_TZ;

    /**
     * INTERVAL DAY type with maximum parameters.
     */
    public static final TypeInfo TYPE_INTERVAL_DAY;

    /**
     * INTERVAL YEAR TO MONTH type with maximum parameters.
     */
    public static final TypeInfo TYPE_INTERVAL_YEAR_TO_MONTH;

    /**
     * INTERVAL DAY TO SECOND type with maximum parameters.
     */
    public static final TypeInfo TYPE_INTERVAL_DAY_TO_SECOND;

    /**
     * INTERVAL HOUR TO SECOND type with maximum parameters.
     */
    public static final TypeInfo TYPE_INTERVAL_HOUR_TO_SECOND;

    /**
     * JAVA_OBJECT type with maximum parameters.
     */
    public static final TypeInfo TYPE_JAVA_OBJECT;

    /**
     * ENUM type with undefined parameters.
     */
    public static final TypeInfo TYPE_ENUM_UNDEFINED;

    /**
     * GEOMETRY type with default parameters.
     */
    public static final TypeInfo TYPE_GEOMETRY;

    /**
     * JSON type.
     */
    public static final TypeInfo TYPE_JSON;

    /**
     * UUID type with parameters.
     */
    public static final TypeInfo TYPE_UUID;

    /**
     * ARRAY type with maximum parameters.
     */
    public static final TypeInfo TYPE_ARRAY;

    /**
     * ROW (row value) type with parameters.
     */
    public static final TypeInfo TYPE_ROW;

    /**
     * RESULT_SET type with parameters.
     */
    public static final TypeInfo TYPE_RESULT_SET;

    private static final TypeInfo[] TYPE_INFOS_BY_VALUE_TYPE;

    private final int valueType;

    private final long precision;

    private final int scale;

    private final int displaySize;

    private final ExtTypeInfo extTypeInfo;

    static {
        TypeInfo[] infos = new TypeInfo[Value.TYPE_COUNT];
        TYPE_UNKNOWN = new TypeInfo(Value.UNKNOWN, -1L, -1, -1, null);
        // NULL
        infos[Value.NULL] = TYPE_NULL = new TypeInfo(Value.NULL, ValueNull.PRECISION, 0, ValueNull.DISPLAY_SIZE, null);
        // CHARACTER
        infos[Value.CHAR] = new TypeInfo(Value.CHAR, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
        infos[Value.VARCHAR] = TYPE_VARCHAR = new TypeInfo(Value.VARCHAR, Integer.MAX_VALUE, 0, Integer.MAX_VALUE,
                null);
        infos[Value.CLOB] = TYPE_CLOB = new TypeInfo(Value.CLOB, Long.MAX_VALUE, 0, Integer.MAX_VALUE, null);
        infos[Value.VARCHAR_IGNORECASE] = TYPE_VARCHAR_IGNORECASE = new TypeInfo(Value.VARCHAR_IGNORECASE,
                Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
        // BINARY
        infos[Value.BINARY] = new TypeInfo(Value.BINARY, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
        infos[Value.VARBINARY] = TYPE_VARBINARY = new TypeInfo(Value.VARBINARY, Integer.MAX_VALUE, 0,
                Integer.MAX_VALUE, null);
        infos[Value.BLOB] = TYPE_BLOB = new TypeInfo(Value.BLOB, Long.MAX_VALUE, 0, Integer.MAX_VALUE, null);
        // BOOLEAN
        infos[Value.BOOLEAN] = TYPE_BOOLEAN = new TypeInfo(Value.BOOLEAN, ValueBoolean.PRECISION, 0,
                ValueBoolean.DISPLAY_SIZE, null);
        // NUMERIC
        infos[Value.TINYINT] = TYPE_TINYINT = new TypeInfo(Value.TINYINT, ValueTinyint.PRECISION, 0,
                ValueTinyint.DISPLAY_SIZE, null);
        infos[Value.SMALLINT] = TYPE_SMALLINT = new TypeInfo(Value.SMALLINT, ValueSmallint.PRECISION, 0,
                ValueSmallint.DISPLAY_SIZE, null);
        infos[Value.INTEGER] = TYPE_INTEGER = new TypeInfo(Value.INTEGER, ValueInteger.PRECISION, 0,
                ValueInteger.DISPLAY_SIZE, null);
        infos[Value.BIGINT] = TYPE_BIGINT = new TypeInfo(Value.BIGINT, ValueBigint.PRECISION, 0,
                ValueBigint.DISPLAY_SIZE, null);
        infos[Value.NUMERIC] = TYPE_NUMERIC = new TypeInfo(Value.NUMERIC, Integer.MAX_VALUE, //
                ValueNumeric.MAXIMUM_SCALE, Integer.MAX_VALUE, null);
        TYPE_NUMERIC_BIGINT = new TypeInfo(Value.NUMERIC, ValueBigint.PRECISION, 0, ValueBigint.DISPLAY_SIZE, null);
        TYPE_NUMERIC_FLOATING_POINT = new TypeInfo(Value.NUMERIC, ValueNumeric.DEFAULT_PRECISION,
                ValueNumeric.DEFAULT_PRECISION / 2, ValueNumeric.DEFAULT_PRECISION + 2, null);
        infos[Value.REAL] = TYPE_REAL = new TypeInfo(Value.REAL, ValueReal.PRECISION, 0, ValueReal.DISPLAY_SIZE,
                null);
        infos[Value.DOUBLE] = TYPE_DOUBLE = new TypeInfo(Value.DOUBLE, ValueDouble.PRECISION, 0,
                ValueDouble.DISPLAY_SIZE, null);
        // DATETIME
        infos[Value.DATE] = TYPE_DATE = new TypeInfo(Value.DATE, ValueDate.PRECISION, 0, ValueDate.PRECISION, null);
        infos[Value.TIME] = TYPE_TIME = new TypeInfo(Value.TIME, ValueTime.MAXIMUM_PRECISION, ValueTime.MAXIMUM_SCALE,
                ValueTime.MAXIMUM_PRECISION, null);
        infos[Value.TIME_TZ] = TYPE_TIME_TZ = new TypeInfo(Value.TIME_TZ, ValueTimeTimeZone.MAXIMUM_PRECISION,
                ValueTime.MAXIMUM_SCALE, ValueTimeTimeZone.MAXIMUM_PRECISION, null);
        infos[Value.TIMESTAMP] = TYPE_TIMESTAMP = new TypeInfo(Value.TIMESTAMP, ValueTimestamp.MAXIMUM_PRECISION,
                ValueTimestamp.MAXIMUM_SCALE, ValueTimestamp.MAXIMUM_PRECISION, null);
        infos[Value.TIMESTAMP_TZ] = TYPE_TIMESTAMP_TZ = new TypeInfo(Value.TIMESTAMP_TZ,
                ValueTimestampTimeZone.MAXIMUM_PRECISION, ValueTimestamp.MAXIMUM_SCALE,
                ValueTimestampTimeZone.MAXIMUM_PRECISION, null);
        // INTERVAL
        for (int i = Value.INTERVAL_YEAR; i <= Value.INTERVAL_MINUTE_TO_SECOND; i++) {
            infos[i] = new TypeInfo(i, ValueInterval.MAXIMUM_PRECISION,
                    IntervalQualifier.valueOf(i - Value.INTERVAL_YEAR).hasSeconds() ? ValueInterval.MAXIMUM_SCALE : 0,
                    ValueInterval.getDisplaySize(i, ValueInterval.MAXIMUM_PRECISION,
                            // Scale will be ignored if it is not supported
                            ValueInterval.MAXIMUM_SCALE),
                    null);
        }
        TYPE_INTERVAL_DAY = infos[Value.INTERVAL_DAY];
        TYPE_INTERVAL_YEAR_TO_MONTH = infos[Value.INTERVAL_YEAR_TO_MONTH];
        TYPE_INTERVAL_DAY_TO_SECOND = infos[Value.INTERVAL_DAY_TO_SECOND];
        TYPE_INTERVAL_HOUR_TO_SECOND = infos[Value.INTERVAL_HOUR_TO_SECOND];
        // OTHER
        infos[Value.JAVA_OBJECT] = TYPE_JAVA_OBJECT = new TypeInfo(Value.JAVA_OBJECT, Integer.MAX_VALUE, 0,
                Integer.MAX_VALUE, null);
        infos[Value.ENUM] = TYPE_ENUM_UNDEFINED = new TypeInfo(Value.ENUM, Integer.MAX_VALUE, 0, Integer.MAX_VALUE,
                null);
        infos[Value.GEOMETRY] = TYPE_GEOMETRY = new TypeInfo(Value.GEOMETRY, Integer.MAX_VALUE, 0, Integer.MAX_VALUE,
                null);
        infos[Value.JSON] = TYPE_JSON = new TypeInfo(Value.JSON, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
        infos[Value.UUID] = TYPE_UUID = new TypeInfo(Value.UUID, ValueUuid.PRECISION, 0, ValueUuid.DISPLAY_SIZE, null);
        // COLLECTION
        infos[Value.ARRAY] = TYPE_ARRAY = new TypeInfo(Value.ARRAY, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
        infos[Value.ROW] = TYPE_ROW = new TypeInfo(Value.ROW, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
        infos[Value.RESULT_SET] = TYPE_RESULT_SET = new TypeInfo(Value.RESULT_SET, Integer.MAX_VALUE,
                Integer.MAX_VALUE, Integer.MAX_VALUE, null);
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
     * @param extTypeInfo
     *            the extended type information, or null
     * @return the data type with parameters object
     */
    public static TypeInfo getTypeInfo(int type, long precision, int scale, ExtTypeInfo extTypeInfo) {
        switch (type) {
        case Value.NULL:
        case Value.BOOLEAN:
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT:
        case Value.REAL:
        case Value.DOUBLE:
        case Value.DATE:
        case Value.UUID:
        case Value.JSON:
        case Value.ROW:
        case Value.RESULT_SET:
            return TYPE_INFOS_BY_VALUE_TYPE[type];
        case Value.UNKNOWN:
            return TYPE_UNKNOWN;
        case Value.CHAR:
            if (precision < 1) {
                precision = 1;
            } else if (precision > Integer.MAX_VALUE) {
                precision = Integer.MAX_VALUE;
            }
            return new TypeInfo(Value.CHAR, precision, 0, (int) precision, null);
        case Value.VARCHAR:
            if (precision < 1 || precision >= Integer.MAX_VALUE) {
                if (precision != 0) {
                    return TYPE_VARCHAR;
                }
                precision = 1;
            }
            return new TypeInfo(Value.VARCHAR, precision, 0, (int) precision, null);
        case Value.CLOB:
        case Value.BLOB:
            if (precision < 1) {
                precision = Long.MAX_VALUE;
            }
            return new TypeInfo(type, precision, 0, MathUtils.convertLongToInt(precision), null);
        case Value.VARCHAR_IGNORECASE:
            if (precision < 1 || precision >= Integer.MAX_VALUE) {
                if (precision != 0) {
                    return TYPE_VARCHAR_IGNORECASE;
                }
                precision = 1;
            }
            return new TypeInfo(Value.VARCHAR_IGNORECASE, precision, 0, (int) precision, null);
        case Value.BINARY:
            if (precision < 1) {
                precision = 1;
            } else if (precision > Integer.MAX_VALUE) {
                precision = Integer.MAX_VALUE;
            }
            return new TypeInfo(Value.BINARY, precision, 0, MathUtils.convertLongToInt(precision * 2), null);
        case Value.VARBINARY:
            if (precision < 1 || precision > Integer.MAX_VALUE) {
                if (precision != 0) {
                    return TYPE_VARBINARY;
                }
                precision = 1;
            }
            return new TypeInfo(Value.VARBINARY, precision, 0, MathUtils.convertLongToInt(precision * 2), null);
        case Value.NUMERIC:
            if (precision < 1) {
                precision = ValueNumeric.DEFAULT_PRECISION;
            } else if (precision > Integer.MAX_VALUE) {
                precision = Integer.MAX_VALUE;
            }
            return new TypeInfo(Value.NUMERIC, precision, scale, MathUtils.convertLongToInt(precision + 2), null);
        case Value.TIME: {
            if (scale < 0 || scale >= ValueTime.MAXIMUM_SCALE) {
                return TYPE_TIME;
            }
            int d = scale == 0 ? 8 : 9 + scale;
            return new TypeInfo(Value.TIME, d, scale, d, null);
        }
        case Value.TIME_TZ: {
            if (scale < 0 || scale >= ValueTime.MAXIMUM_SCALE) {
                return TYPE_TIME_TZ;
            }
            int d = scale == 0 ? 14 : 15 + scale;
            return new TypeInfo(Value.TIME_TZ, d, scale, d, null);
        }
        case Value.TIMESTAMP: {
            if (scale < 0 || scale >= ValueTimestamp.MAXIMUM_SCALE) {
                return TYPE_TIMESTAMP;
            }
            int d = scale == 0 ? 19 : 20 + scale;
            return new TypeInfo(Value.TIMESTAMP, d, scale, d, null);
        }
        case Value.TIMESTAMP_TZ: {
            if (scale < 0 || scale >= ValueTimestamp.MAXIMUM_SCALE) {
                return TYPE_TIMESTAMP_TZ;
            }
            int d = scale == 0 ? 25 : 26 + scale;
            return new TypeInfo(Value.TIMESTAMP_TZ, d, scale, d, null);
        }
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_MINUTE:
            if (precision < 1 || precision > ValueInterval.MAXIMUM_PRECISION) {
                precision = ValueInterval.MAXIMUM_PRECISION;
            }
            return new TypeInfo(type, precision, 0, ValueInterval.getDisplaySize(type, (int) precision, 0), null);
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            if (precision < 1 || precision > ValueInterval.MAXIMUM_PRECISION) {
                precision = ValueInterval.MAXIMUM_PRECISION;
            }
            if (scale < 0 || scale > ValueInterval.MAXIMUM_SCALE) {
                scale = ValueInterval.MAXIMUM_SCALE;
            }
            return new TypeInfo(type, precision, scale, ValueInterval.getDisplaySize(type, (int) precision, scale),
                    null);
        case Value.JAVA_OBJECT:
            if (precision < 1 || precision > Integer.MAX_VALUE) {
                return TYPE_JAVA_OBJECT;
            }
            return new TypeInfo(Value.JAVA_OBJECT, precision, 0, MathUtils.convertLongToInt(precision * 2), null);
        case Value.GEOMETRY:
            if (extTypeInfo instanceof ExtTypeInfoGeometry) {
                return new TypeInfo(Value.GEOMETRY, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, extTypeInfo);
            } else {
                return TYPE_GEOMETRY;
            }
        case Value.ENUM:
            if (extTypeInfo instanceof ExtTypeInfoEnum) {
                return ((ExtTypeInfoEnum) extTypeInfo).getType();
            } else {
                return TYPE_ENUM_UNDEFINED;
            }
        case Value.ARRAY:
            if (!(extTypeInfo instanceof TypeInfo)) {
                throw new IllegalArgumentException();
            }
            if (precision < 0 || precision >= Integer.MAX_VALUE) {
                precision = Integer.MAX_VALUE;
            }
            return new TypeInfo(Value.ARRAY, precision, 0, Integer.MAX_VALUE, extTypeInfo);
        }
        return TYPE_NULL;
    }

    /**
     * Get the higher data type of two data types. If values need to be
     * converted to match the other operands data type, the value with the lower
     * order is converted to the value with the higher order.
     *
     * @param type1
     *            the first data type
     * @param type2
     *            the second data type
     * @return the higher data type of the two
     */
    public static TypeInfo getHigherType(TypeInfo type1, TypeInfo type2) {
        int t1 = type1.getValueType(), t2 = type2.getValueType();
        int dataType = Value.getHigherOrder(t1, t2);
        switch (dataType) {
        case Value.NUMERIC: {
            type1 = type1.toNumericType();
            type2 = type2.toNumericType();
            long precision1 = type1.getPrecision(), precision2 = type2.getPrecision();
            int scale1 = type1.getScale(), scale2 = type2.getScale(), scale;
            if (scale1 < scale2) {
                precision1 += scale2 - scale1;
                scale = scale2;
            } else {
                precision2 += scale1 - scale2;
                scale = scale1;
            }
            return TypeInfo.getTypeInfo(dataType, Math.max(precision1, precision2), scale, null);
        }
        case Value.ARRAY:
            return getHigherArray(type1, type2, dimensions(type1), dimensions(type2));
        }
        ExtTypeInfo ext1 = type1.extTypeInfo;
        return TypeInfo.getTypeInfo(dataType, //
                Math.max(type1.getPrecision(), type2.getPrecision()), //
                Math.max(type1.getScale(), type2.getScale()), //
                dataType == t1 && ext1 != null ? ext1 : dataType == t2 ? type2.extTypeInfo : null);
    }

    private static int dimensions(TypeInfo type) {
        int result;
        for (result = 0; type.getValueType() == Value.ARRAY; result++) {
            type = (TypeInfo) type.extTypeInfo;
        }
        return result;
    }

    private static TypeInfo getHigherArray(TypeInfo type1, TypeInfo type2, int d1, int d2) {
        long precision;
        if (d1 > d2) {
            d1--;
            precision = Math.max(type1.getPrecision(), 1L);
            type1 = (TypeInfo) type1.extTypeInfo;
        } else if (d1 < d2) {
            d2--;
            precision = Math.max(1L, type2.getPrecision());
            type2 = (TypeInfo) type2.extTypeInfo;
        } else if (d1 > 0) {
            d1--;
            d2--;
            precision = Math.max(type1.getPrecision(), type2.getPrecision());
            type1 = (TypeInfo) type1.extTypeInfo;
            type2 = (TypeInfo) type2.extTypeInfo;
        } else {
            return getHigherType(type1, type2);
        }
        return TypeInfo.getTypeInfo(Value.ARRAY, precision, 0, getHigherArray(type1, type2, d1, d2));
    }

    /**
     * Determines whether two specified types are the same data types without
     * taking precision or scale into account.
     *
     * @param t1
     *            first data type
     * @param t2
     *            second data type
     * @return whether types are the same
     */
    public static boolean areSameTypes(TypeInfo t1, TypeInfo t2) {
        for (;;) {
            int valueType = t1.getValueType();
            if (valueType != t2.getValueType()) {
                return false;
            }
            ExtTypeInfo ext1 = t1.getExtTypeInfo(), ext2 = t2.getExtTypeInfo();
            if (valueType != Value.ARRAY) {
                return Objects.equals(ext1, ext2);
            }
            t1 = (TypeInfo) ext1;
            t2 = (TypeInfo) ext2;
        }
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

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        switch (valueType) {
        case Value.CHAR:
        case Value.BINARY:
            builder.append(DataType.getDataType(valueType).name);
            builder.append('(').append(precision).append(')');
            break;
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.VARBINARY:
        case Value.JAVA_OBJECT:
            builder.append(DataType.getDataType(valueType).name);
            if (precision < Integer.MAX_VALUE) {
                builder.append('(').append(precision).append(')');
            }
            break;
        case Value.NUMERIC:
            // Can be DECIMAL or NUMERIC
            builder.append(DataType.getDataType(valueType).name);
            builder.append('(').append(precision).append(", ").append(scale).append(')');
            break;
        case Value.TIME:
        case Value.TIME_TZ:
            builder.append("TIME");
            if (scale != ValueTime.DEFAULT_SCALE) {
                builder.append('(').append(scale).append(')');
            }
            if (valueType == Value.TIME_TZ) {
                builder.append(" WITH TIME ZONE");
            }
            break;
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            builder.append("TIMESTAMP");
            if (scale != ValueTimestamp.DEFAULT_SCALE) {
                builder.append('(').append(scale).append(')');
            }
            if (valueType == Value.TIMESTAMP_TZ) {
                builder.append(" WITH TIME ZONE");
            }
            break;
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
            IntervalQualifier.valueOf(valueType - Value.INTERVAL_YEAR).getTypeName(builder,
                    precision == ValueInterval.DEFAULT_PRECISION ? -1 : (int) precision,
                    scale == ValueInterval.DEFAULT_SCALE ? -1 : scale, false);
            break;
        case Value.GEOMETRY:
            builder.append("GEOMETRY");
            if (extTypeInfo != null) {
                extTypeInfo.getSQL(builder);
            }
            break;
        case Value.ENUM:
            extTypeInfo.getSQL(builder.append("ENUM"));
            break;
        case Value.ARRAY:
            if (extTypeInfo != null) {
                extTypeInfo.getSQL(builder).append(' ');
            }
            builder.append("ARRAY");
            if (precision < Integer.MAX_VALUE) {
                builder.append('[').append(precision).append(']');
            }
            break;
        default:
            builder.append(DataType.getDataType(valueType).name);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + valueType;
        result = 31 * result + (int) (precision ^ (precision >>> 32));
        result = 31 * result + scale;
        result = 31 * result + displaySize;
        result = 31 * result + ((extTypeInfo == null) ? 0 : extTypeInfo.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != TypeInfo.class) {
            return false;
        }
        TypeInfo other = (TypeInfo) obj;
        return valueType == other.valueType && precision == other.precision && scale == other.scale
                && displaySize == other.displaySize && Objects.equals(extTypeInfo, other.extTypeInfo);
    }

    /**
     * Convert this type information to compatible NUMERIC type information.
     *
     * @return NUMERIC type information
     */
    public TypeInfo toNumericType() {
        switch (valueType) {
        case Value.BOOLEAN:
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
            return getTypeInfo(Value.NUMERIC, precision, 0, null);
        case Value.BIGINT:
            return TYPE_NUMERIC_BIGINT;
        case Value.NUMERIC:
            return this;
        case Value.REAL:
            // Smallest REAL value is 1.4E-45 with precision 2 and scale 46
            // Largest REAL value is 3.4028235E+38 with precision 8 and scale -31
            return getTypeInfo(Value.NUMERIC, 85, 46, null);
        case Value.DOUBLE:
            // Smallest DOUBLE value is 4.9E-324 with precision 2 and scale 325
            // Largest DOUBLE value is 1.7976931348623157E+308 with precision 17
            // and scale -292
            return getTypeInfo(Value.NUMERIC, 634, 325, null);
        default:
            return TYPE_NUMERIC_FLOATING_POINT;
        }
    }

}
