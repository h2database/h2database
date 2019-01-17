/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

/**
 * Data type with parameters.
 */
public class TypeInfo {

    private final int valueType;

    private final long precision;

    private final int scale;

    private final int displaySize;

    private final ExtTypeInfo extTypeInfo;

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
