/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.JdbcUtils;

/**
 * Data type with parameters.
 */
public class TypeInfo {

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
        TYPE_NULL = infos[Value.NULL];
        TYPE_BOOLEAN = infos[Value.BOOLEAN];
        TYPE_BYTE = infos[Value.BYTE];
        TYPE_SHORT = infos[Value.SHORT];
        TYPE_INT = infos[Value.INT];
        TYPE_LONG = infos[Value.LONG];
        TYPE_DOUBLE = infos[Value.DOUBLE];
        TYPE_FLOAT = infos[Value.FLOAT];
        TYPE_TIME = infos[Value.TIME];
        TYPE_DATE = infos[Value.DATE];
        TYPE_TIMESTAMP = infos[Value.TIMESTAMP];
        TYPE_ARRAY = infos[Value.ARRAY];
        TYPE_RESULT_SET = infos[Value.RESULT_SET];
        TYPE_JAVA_OBJECT = infos[Value.JAVA_OBJECT];
        TYPE_UUID = infos[Value.UUID];
        TYPE_GEOMETRY = infos[Value.GEOMETRY];
        TYPE_TIMESTAMP_TZ = infos[Value.TIMESTAMP_TZ];
        TYPE_ENUM_UNDEFINED = infos[Value.ENUM];
        TYPE_ROW = infos[Value.ROW];
        TYPE_INFOS_BY_VALUE_TYPE = infos;
    }

    /**
     * Get the data type with parameters object for the given value type and
     * maximum parameters.
     *
     * @param type the value type
     * @return the data type with parameters object
     */
    public static TypeInfo getTypeInfo(int type) {
        if (type == Value.UNKNOWN) {
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?");
        }
        if (type >= Value.NULL && type < Value.TYPE_COUNT) {
            TypeInfo t = TypeInfo.TYPE_INFOS_BY_VALUE_TYPE[type];
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
        return TypeInfo.TYPE_INFOS_BY_VALUE_TYPE[Value.NULL];
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
