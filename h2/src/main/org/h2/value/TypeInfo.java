/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.Constants;
import org.h2.message.DbException;

/**
 * Data type with parameters.
 */
public class TypeInfo extends ExtTypeInfo implements Typed {

    /**
     * UNKNOWN type with parameters.
     */
    public static final TypeInfo TYPE_UNKNOWN;

    /**
     * NULL type with parameters.
     */
    public static final TypeInfo TYPE_NULL;

    /**
     * CHAR type with default parameters.
     */
    public static final TypeInfo TYPE_CHAR;

    /**
     * CHARACTER VARYING type with maximum parameters.
     */
    public static final TypeInfo TYPE_VARCHAR;

    /**
     * VARCHAR_IGNORECASE type with maximum parameters.
     */
    public static final TypeInfo TYPE_VARCHAR_IGNORECASE;

    /**
     * CHARACTER LARGE OBJECT type with maximum parameters.
     */
    public static final TypeInfo TYPE_CLOB;

    /**
     * BINARY type with default parameters.
     */
    public static final TypeInfo TYPE_BINARY;

    /**
     * BINARY VARYING type with maximum parameters.
     */
    public static final TypeInfo TYPE_VARBINARY;

    /**
     * BINARY LARGE OBJECT type with maximum parameters.
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
     * NUMERIC type with maximum precision and scale 0.
     */
    public static final TypeInfo TYPE_NUMERIC_SCALE_0;

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
     * DECFLOAT type with maximum parameters.
     */
    public static final TypeInfo TYPE_DECFLOAT;

    /**
     * DECFLOAT type with parameters enough to hold a BIGINT value.
     */
    public static final TypeInfo TYPE_DECFLOAT_BIGINT;

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
     * ARRAY type with unknown parameters.
     */
    public static final TypeInfo TYPE_ARRAY_UNKNOWN;

    /**
     * ROW (row value) type without fields.
     */
    public static final TypeInfo TYPE_ROW_EMPTY;

    private static final TypeInfo[] TYPE_INFOS_BY_VALUE_TYPE;

    private final int valueType;

    private final long precision;

    private final int scale;

    private final ExtTypeInfo extTypeInfo;

    static {
        TypeInfo[] infos = new TypeInfo[Value.TYPE_COUNT];
        TYPE_UNKNOWN = new TypeInfo(Value.UNKNOWN);
        // NULL
        infos[Value.NULL] = TYPE_NULL = new TypeInfo(Value.NULL);
        // CHARACTER
        infos[Value.CHAR] = TYPE_CHAR = new TypeInfo(Value.CHAR, -1L);
        infos[Value.VARCHAR] = TYPE_VARCHAR = new TypeInfo(Value.VARCHAR);
        infos[Value.CLOB] = TYPE_CLOB = new TypeInfo(Value.CLOB);
        infos[Value.VARCHAR_IGNORECASE] = TYPE_VARCHAR_IGNORECASE = new TypeInfo(Value.VARCHAR_IGNORECASE);
        // BINARY
        infos[Value.BINARY] = TYPE_BINARY = new TypeInfo(Value.BINARY, -1L);
        infos[Value.VARBINARY] = TYPE_VARBINARY = new TypeInfo(Value.VARBINARY);
        infos[Value.BLOB] = TYPE_BLOB = new TypeInfo(Value.BLOB);
        // BOOLEAN
        infos[Value.BOOLEAN] = TYPE_BOOLEAN = new TypeInfo(Value.BOOLEAN);
        // NUMERIC
        infos[Value.TINYINT] = TYPE_TINYINT = new TypeInfo(Value.TINYINT);
        infos[Value.SMALLINT] = TYPE_SMALLINT = new TypeInfo(Value.SMALLINT);
        infos[Value.INTEGER] = TYPE_INTEGER = new TypeInfo(Value.INTEGER);
        infos[Value.BIGINT] = TYPE_BIGINT = new TypeInfo(Value.BIGINT);
        TYPE_NUMERIC_SCALE_0 = new TypeInfo(Value.NUMERIC, Constants.MAX_NUMERIC_PRECISION, 0, null);
        TYPE_NUMERIC_BIGINT = new TypeInfo(Value.NUMERIC, ValueBigint.DECIMAL_PRECISION, 0, null);
        infos[Value.NUMERIC] = TYPE_NUMERIC_FLOATING_POINT = new TypeInfo(Value.NUMERIC,
                Constants.MAX_NUMERIC_PRECISION, Constants.MAX_NUMERIC_PRECISION / 2, null);
        infos[Value.REAL] = TYPE_REAL = new TypeInfo(Value.REAL);
        infos[Value.DOUBLE] = TYPE_DOUBLE = new TypeInfo(Value.DOUBLE);
        infos[Value.DECFLOAT] = TYPE_DECFLOAT = new TypeInfo(Value.DECFLOAT);
        TYPE_DECFLOAT_BIGINT = new TypeInfo(Value.DECFLOAT, (long) ValueBigint.DECIMAL_PRECISION);
        // DATETIME
        infos[Value.DATE] = TYPE_DATE = new TypeInfo(Value.DATE);
        infos[Value.TIME] = TYPE_TIME = new TypeInfo(Value.TIME, ValueTime.MAXIMUM_SCALE);
        infos[Value.TIME_TZ] = TYPE_TIME_TZ = new TypeInfo(Value.TIME_TZ, ValueTime.MAXIMUM_SCALE);
        infos[Value.TIMESTAMP] = TYPE_TIMESTAMP = new TypeInfo(Value.TIMESTAMP, ValueTimestamp.MAXIMUM_SCALE);
        infos[Value.TIMESTAMP_TZ] = TYPE_TIMESTAMP_TZ = new TypeInfo(Value.TIMESTAMP_TZ, ValueTimestamp.MAXIMUM_SCALE);
        // INTERVAL
        for (int i = Value.INTERVAL_YEAR; i <= Value.INTERVAL_MINUTE_TO_SECOND; i++) {
            infos[i] = new TypeInfo(i, ValueInterval.MAXIMUM_PRECISION,
                    IntervalQualifier.valueOf(i - Value.INTERVAL_YEAR).hasSeconds() ? ValueInterval.MAXIMUM_SCALE : -1,
                    null);
        }
        TYPE_INTERVAL_DAY = infos[Value.INTERVAL_DAY];
        TYPE_INTERVAL_YEAR_TO_MONTH = infos[Value.INTERVAL_YEAR_TO_MONTH];
        TYPE_INTERVAL_DAY_TO_SECOND = infos[Value.INTERVAL_DAY_TO_SECOND];
        TYPE_INTERVAL_HOUR_TO_SECOND = infos[Value.INTERVAL_HOUR_TO_SECOND];
        // OTHER
        infos[Value.JAVA_OBJECT] = TYPE_JAVA_OBJECT = new TypeInfo(Value.JAVA_OBJECT);
        infos[Value.ENUM] = TYPE_ENUM_UNDEFINED = new TypeInfo(Value.ENUM);
        infos[Value.GEOMETRY] = TYPE_GEOMETRY = new TypeInfo(Value.GEOMETRY);
        infos[Value.JSON] = TYPE_JSON = new TypeInfo(Value.JSON);
        infos[Value.UUID] = TYPE_UUID = new TypeInfo(Value.UUID);
        // COLLECTION
        infos[Value.ARRAY] = TYPE_ARRAY_UNKNOWN = new TypeInfo(Value.ARRAY);
        infos[Value.ROW] = TYPE_ROW_EMPTY = new TypeInfo(Value.ROW, -1L, -1, //
                new ExtTypeInfoRow(new LinkedHashMap<>()));
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
     *            the precision or {@code -1L} for default
     * @param scale
     *            the scale or {@code -1} for default
     * @param extTypeInfo
     *            the extended type information or null
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
        case Value.DATE:
        case Value.UUID:
            return TYPE_INFOS_BY_VALUE_TYPE[type];
        case Value.UNKNOWN:
            return TYPE_UNKNOWN;
        case Value.CHAR:
            if (precision < 1) {
                return TYPE_CHAR;
            }
            if (precision > Constants.MAX_STRING_LENGTH) {
                precision = Constants.MAX_STRING_LENGTH;
            }
            return new TypeInfo(Value.CHAR, precision);
        case Value.VARCHAR:
            if (precision < 1 || precision >= Constants.MAX_STRING_LENGTH) {
                if (precision != 0) {
                    return TYPE_VARCHAR;
                }
                precision = 1;
            }
            return new TypeInfo(Value.VARCHAR, precision);
        case Value.CLOB:
            if (precision < 1) {
                return TYPE_CLOB;
            }
            return new TypeInfo(Value.CLOB, precision);
        case Value.VARCHAR_IGNORECASE:
            if (precision < 1 || precision >= Constants.MAX_STRING_LENGTH) {
                if (precision != 0) {
                    return TYPE_VARCHAR_IGNORECASE;
                }
                precision = 1;
            }
            return new TypeInfo(Value.VARCHAR_IGNORECASE, precision);
        case Value.BINARY:
            if (precision < 1) {
                return TYPE_BINARY;
            }
            if (precision > Constants.MAX_STRING_LENGTH) {
                precision = Constants.MAX_STRING_LENGTH;
            }
            return new TypeInfo(Value.BINARY, precision);
        case Value.VARBINARY:
            if (precision < 1 || precision >= Constants.MAX_STRING_LENGTH) {
                if (precision != 0) {
                    return TYPE_VARBINARY;
                }
                precision = 1;
            }
            return new TypeInfo(Value.VARBINARY, precision);
        case Value.BLOB:
            if (precision < 1) {
                return TYPE_BLOB;
            }
            return new TypeInfo(Value.BLOB, precision);
        case Value.NUMERIC:
            if (precision < 1) {
                precision = -1L;
            } else if (precision > Constants.MAX_NUMERIC_PRECISION) {
                precision = Constants.MAX_NUMERIC_PRECISION;
            }
            if (scale < 0) {
                scale = -1;
            } else if (scale > ValueNumeric.MAXIMUM_SCALE) {
                scale = ValueNumeric.MAXIMUM_SCALE;
            }
            return new TypeInfo(Value.NUMERIC, precision, scale,
                    extTypeInfo instanceof ExtTypeInfoNumeric ? extTypeInfo : null);
        case Value.REAL:
            if (precision >= 1 && precision <= 24) {
                return new TypeInfo(Value.REAL, precision, -1, extTypeInfo);
            }
            return TYPE_REAL;
        case Value.DOUBLE:
            if (precision == 0 || precision >= 25 && precision <= 53) {
                return new TypeInfo(Value.DOUBLE, precision, -1, extTypeInfo);
            }
            return TYPE_DOUBLE;
        case Value.DECFLOAT:
            if (precision < 1) {
                precision = -1L;
            } else if (precision >= Constants.MAX_NUMERIC_PRECISION) {
                return TYPE_DECFLOAT;
            }
            return new TypeInfo(Value.DECFLOAT, precision, -1, null);
        case Value.TIME:
            if (scale < 0) {
                scale = -1;
            } else if (scale >= ValueTime.MAXIMUM_SCALE) {
                return TYPE_TIME;
            }
            return new TypeInfo(Value.TIME, scale);
        case Value.TIME_TZ:
            if (scale < 0) {
                scale = -1;
            } else if (scale >= ValueTime.MAXIMUM_SCALE) {
                return TYPE_TIME_TZ;
            }
            return new TypeInfo(Value.TIME_TZ, scale);
        case Value.TIMESTAMP:
            if (scale < 0) {
                scale = -1;
            } else if (scale >= ValueTimestamp.MAXIMUM_SCALE) {
                return TYPE_TIMESTAMP;
            }
            return new TypeInfo(Value.TIMESTAMP, scale);
        case Value.TIMESTAMP_TZ:
            if (scale < 0) {
                scale = -1;
            } else if (scale >= ValueTimestamp.MAXIMUM_SCALE) {
                return TYPE_TIMESTAMP_TZ;
            }
            return new TypeInfo(Value.TIMESTAMP_TZ, scale);
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_MINUTE:
            if (precision < 1) {
                precision = -1L;
            } else if (precision > ValueInterval.MAXIMUM_PRECISION) {
                precision = ValueInterval.MAXIMUM_PRECISION;
            }
            return new TypeInfo(type, precision);
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            if (precision < 1) {
                precision = -1L;
            } else if (precision > ValueInterval.MAXIMUM_PRECISION) {
                precision = ValueInterval.MAXIMUM_PRECISION;
            }
            if (scale < 0) {
                scale = -1;
            } else if (scale > ValueInterval.MAXIMUM_SCALE) {
                scale = ValueInterval.MAXIMUM_SCALE;
            }
            return new TypeInfo(type, precision, scale, null);
        case Value.JAVA_OBJECT:
            if (precision < 1) {
                return TYPE_JAVA_OBJECT;
            } else if (precision > Constants.MAX_STRING_LENGTH) {
                precision = Constants.MAX_STRING_LENGTH;
            }
            return new TypeInfo(Value.JAVA_OBJECT, precision);
        case Value.ENUM:
            if (extTypeInfo instanceof ExtTypeInfoEnum) {
                return ((ExtTypeInfoEnum) extTypeInfo).getType();
            } else {
                return TYPE_ENUM_UNDEFINED;
            }
        case Value.GEOMETRY:
            if (extTypeInfo instanceof ExtTypeInfoGeometry) {
                return new TypeInfo(Value.GEOMETRY, -1L, -1, extTypeInfo);
            } else {
                return TYPE_GEOMETRY;
            }
        case Value.JSON:
            if (precision < 1) {
                return TYPE_JSON;
            } else if (precision > Constants.MAX_STRING_LENGTH) {
                precision = Constants.MAX_STRING_LENGTH;
            }
            return new TypeInfo(Value.JSON, precision);
        case Value.ARRAY:
            if (!(extTypeInfo instanceof TypeInfo)) {
                throw new IllegalArgumentException();
            }
            if (precision < 0 || precision >= Constants.MAX_ARRAY_CARDINALITY) {
                precision = -1L;
            }
            return new TypeInfo(Value.ARRAY, precision, -1, extTypeInfo);
        case Value.ROW:
            if (!(extTypeInfo instanceof ExtTypeInfoRow)) {
                throw new IllegalArgumentException();
            }
            return new TypeInfo(Value.ROW, -1L, -1, extTypeInfo);
        }
        return TYPE_NULL;
    }

    /**
     * Get the higher data type of all values.
     *
     * @param values
     *            the values
     * @return the higher data type
     */
    public static TypeInfo getHigherType(Typed[] values) {
        int cardinality = values.length;
        TypeInfo type;
        if (cardinality == 0) {
            type = TypeInfo.TYPE_NULL;
        } else {
            type = values[0].getType();
            boolean hasUnknown = false, hasNull = false;
            switch (type.getValueType()) {
            case Value.UNKNOWN:
                hasUnknown = true;
                break;
            case Value.NULL:
                hasNull = true;
            }
            for (int i = 1; i < cardinality; i++) {
                TypeInfo t = values[i].getType();
                switch (t.getValueType()) {
                case Value.UNKNOWN:
                    hasUnknown = true;
                    break;
                case Value.NULL:
                    hasNull = true;
                    break;
                default:
                    type = getHigherType(type, t);
                }
            }
            if (type.getValueType() <= Value.NULL && hasUnknown) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, hasNull ? "NULL, ?" : "?");
            }
        }
        return type;
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
        int t1 = type1.getValueType(), t2 = type2.getValueType(), dataType;
        if (t1 == t2) {
            if (t1 == Value.UNKNOWN) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?, ?");
            }
            dataType = t1;
        } else {
            if (t1 < t2) {
                int t = t1;
                t1 = t2;
                t2 = t;
                TypeInfo type = type1;
                type1 = type2;
                type2 = type;
            }
            if (t1 == Value.UNKNOWN) {
                if (t2 == Value.NULL) {
                    throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?, NULL");
                }
                return type2;
            } else if (t2 == Value.UNKNOWN) {
                if (t1 == Value.NULL) {
                    throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "NULL, ?");
                }
                return type1;
            }
            if (t2 == Value.NULL) {
                return type1;
            }
            dataType = Value.getHigherOrderKnown(t1, t2);
        }
        long precision;
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
            return TypeInfo.getTypeInfo(Value.NUMERIC, Math.max(precision1, precision2), scale, null);
        }
        case Value.REAL:
        case Value.DOUBLE:
            precision = -1L;
            break;
        case Value.GEOMETRY:
            return getHigherGeometry(type1, type2);
        case Value.ARRAY:
            return getHigherArray(type1, type2, dimensions(type1), dimensions(type2));
        case Value.ROW:
            return getHigherRow(type1, type2);
        default:
            precision = Math.max(type1.getPrecision(), type2.getPrecision());
        }
        ExtTypeInfo ext1 = type1.extTypeInfo;
        return TypeInfo.getTypeInfo(dataType, //
                precision, //
                Math.max(type1.getScale(), type2.getScale()), //
                dataType == t1 && ext1 != null ? ext1 : dataType == t2 ? type2.extTypeInfo : null);
    }

    private static TypeInfo getHigherGeometry(TypeInfo type1, TypeInfo type2) {
        int t;
        Integer srid;
        ExtTypeInfo ext1 = type1.getExtTypeInfo(), ext2 = type2.getExtTypeInfo();
        if (ext1 instanceof ExtTypeInfoGeometry) {
            if (ext2 instanceof ExtTypeInfoGeometry) {
                ExtTypeInfoGeometry g1 = (ExtTypeInfoGeometry) ext1, g2 = (ExtTypeInfoGeometry) ext2;
                t = g1.getType();
                srid = g1.getSrid();
                int t2 = g2.getType();
                Integer srid2 = g2.getSrid();
                if (Objects.equals(srid, srid2)) {
                    if (t == t2) {
                        return type1;
                    } else if (srid == null) {
                        return TYPE_GEOMETRY;
                    } else {
                        t = 0;
                    }
                } else if (srid == null || srid2 == null) {
                    if (t == 0 || t != t2) {
                        return TYPE_GEOMETRY;
                    } else {
                        srid = null;
                    }
                } else {
                    throw DbException.get(ErrorCode.TYPES_ARE_NOT_COMPARABLE_2, type1.getTraceSQL(),
                            type2.getTraceSQL());
                }
            } else {
                return type2.getValueType() == Value.GEOMETRY ? TypeInfo.TYPE_GEOMETRY : type1;
            }
        } else if (ext2 instanceof ExtTypeInfoGeometry) {
            return type1.getValueType() == Value.GEOMETRY ? TypeInfo.TYPE_GEOMETRY : type2;
        } else {
            return TYPE_GEOMETRY;
        }
        return new TypeInfo(Value.GEOMETRY, -1L, -1, new ExtTypeInfoGeometry(t, srid));
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

    private static TypeInfo getHigherRow(TypeInfo type1, TypeInfo type2) {
        if (type1.getValueType() != Value.ROW) {
            type1 = typeToRow(type1);
        }
        if (type2.getValueType() != Value.ROW) {
            type2 = typeToRow(type2);
        }
        ExtTypeInfoRow ext1 = (ExtTypeInfoRow) type1.getExtTypeInfo(), ext2 = (ExtTypeInfoRow) type2.getExtTypeInfo();
        if (ext1.equals(ext2)) {
            return type1;
        }
        Set<Map.Entry<String, TypeInfo>> m1 = ext1.getFields(), m2 = ext2.getFields();
        int degree = m1.size();
        if (m2.size() != degree) {
            throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
        }
        LinkedHashMap<String, TypeInfo> m = new LinkedHashMap<>((int) Math.ceil(degree / .75));
        for (Iterator<Map.Entry<String, TypeInfo>> i1 = m1.iterator(), i2 = m2.iterator(); i1.hasNext();) {
            Map.Entry<String, TypeInfo> e1 = i1.next();
            m.put(e1.getKey(), getHigherType(e1.getValue(), i2.next().getValue()));
        }
        return TypeInfo.getTypeInfo(Value.ROW, 0, 0, new ExtTypeInfoRow(m));
    }

    private static TypeInfo typeToRow(TypeInfo type) {
        LinkedHashMap<String, TypeInfo> map = new LinkedHashMap<>(2);
        map.put("C1", type);
        return TypeInfo.getTypeInfo(Value.ROW, 0, 0, new ExtTypeInfoRow(map));
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
     * Checks whether two specified types are comparable and throws an exception
     * otherwise.
     *
     * @param t1
     *            first data type
     * @param t2
     *            second data type
     * @throws DbException
     *             if types aren't comparable
     */
    public static void checkComparable(TypeInfo t1, TypeInfo t2) {
        if (!areComparable(t1, t2)) {
            throw DbException.get(ErrorCode.TYPES_ARE_NOT_COMPARABLE_2, t1.getTraceSQL(), t2.getTraceSQL());
        }
    }

    /**
     * Determines whether two specified types are comparable.
     *
     * @param t1
     *            first data type
     * @param t2
     *            second data type
     * @return whether types are comparable
     */
    private static boolean areComparable(TypeInfo t1, TypeInfo t2) {
        int vt1 = (t1 = t1.unwrapRow()).getValueType(), vt2 = (t2 = t2.unwrapRow()).getValueType();
        if (vt1 > vt2) {
            int vt = vt1;
            vt1 = vt2;
            vt2 = vt;
            TypeInfo t = t1;
            t1 = t2;
            t2 = t;
        }
        if (vt1 <= Value.NULL) {
            return true;
        }
        if (vt1 == vt2) {
            switch (vt1) {
            case Value.ARRAY:
                return areComparable((TypeInfo) t1.getExtTypeInfo(), (TypeInfo) t2.getExtTypeInfo());
            case Value.ROW: {
                Set<Entry<String, TypeInfo>> f1 = ((ExtTypeInfoRow) t1.getExtTypeInfo()).getFields();
                Set<Entry<String, TypeInfo>> f2 = ((ExtTypeInfoRow) t2.getExtTypeInfo()).getFields();
                int degree = f1.size();
                if (f2.size() != degree) {
                    return false;
                }
                Iterator<Entry<String, TypeInfo>> i1 = f1.iterator(), i2 = f2.iterator();
                while (i1.hasNext()) {
                    if (!areComparable(i1.next().getValue(), i2.next().getValue())) {
                        return false;
                    }
                }
            }
            //$FALL-THROUGH$
            default:
                return true;
            }
        }
        byte g1 = Value.GROUPS[vt1], g2 = Value.GROUPS[vt2];
        if (g1 == g2) {
            switch (g1) {
            default:
                return true;
            case Value.GROUP_DATETIME:
                return vt1 != Value.DATE || vt2 != Value.TIME && vt2 != Value.TIME_TZ;
            case Value.GROUP_OTHER:
            case Value.GROUP_COLLECTION:
                return false;
            }
        }
        switch (g1) {
        case Value.GROUP_CHARACTER_STRING:
            switch (g2) {
            case Value.GROUP_NUMERIC:
            case Value.GROUP_DATETIME:
            case Value.GROUP_INTERVAL_YM:
            case Value.GROUP_INTERVAL_DT:
                return true;
            case Value.GROUP_OTHER:
                switch (vt2) {
                case Value.ENUM:
                case Value.GEOMETRY:
                case Value.JSON:
                case Value.UUID:
                    return true;
                default:
                    return false;
                }
            default:
                return false;
            }
        case Value.GROUP_BINARY_STRING:
            switch (vt2) {
            case Value.JAVA_OBJECT:
            case Value.GEOMETRY:
            case Value.JSON:
            case Value.UUID:
                return true;
            default:
                return false;
            }
        }
        return false;
    }

    /**
     * Determines whether two specified types have the same ordering rules.
     *
     * @param t1
     *            first data type
     * @param t2
     *            second data type
     * @return whether types are comparable
     */
    public static boolean haveSameOrdering(TypeInfo t1, TypeInfo t2) {
        int vt1 = (t1 = t1.unwrapRow()).getValueType(), vt2 = (t2 = t2.unwrapRow()).getValueType();
        if (vt1 > vt2) {
            int vt = vt1;
            vt1 = vt2;
            vt2 = vt;
            TypeInfo t = t1;
            t1 = t2;
            t2 = t;
        }
        if (vt1 <= Value.NULL) {
            return true;
        }
        if (vt1 == vt2) {
            switch (vt1) {
            case Value.ARRAY:
                return haveSameOrdering((TypeInfo) t1.getExtTypeInfo(), (TypeInfo) t2.getExtTypeInfo());
            case Value.ROW: {
                Set<Entry<String, TypeInfo>> f1 = ((ExtTypeInfoRow) t1.getExtTypeInfo()).getFields();
                Set<Entry<String, TypeInfo>> f2 = ((ExtTypeInfoRow) t2.getExtTypeInfo()).getFields();
                int degree = f1.size();
                if (f2.size() != degree) {
                    return false;
                }
                Iterator<Entry<String, TypeInfo>> i1 = f1.iterator(), i2 = f2.iterator();
                while (i1.hasNext()) {
                    if (!haveSameOrdering(i1.next().getValue(), i2.next().getValue())) {
                        return false;
                    }
                }
            }
            //$FALL-THROUGH$
            default:
                return true;
            }
        }
        byte g1 = Value.GROUPS[vt1], g2 = Value.GROUPS[vt2];
        if (g1 == g2) {
            switch (g1) {
            default:
                return true;
            case Value.GROUP_CHARACTER_STRING:
                return (vt1 == Value.VARCHAR_IGNORECASE) == (vt2 == Value.VARCHAR_IGNORECASE);
            case Value.GROUP_DATETIME:
                switch (vt1) {
                case Value.DATE:
                    return vt2 == Value.TIMESTAMP || vt2 == Value.TIMESTAMP_TZ;
                case Value.TIME:
                case Value.TIME_TZ:
                    return vt2 == Value.TIME || vt2 == Value.TIME_TZ;
                default: // TIMESTAMP TIMESTAMP_TZ
                    return true;
                }
            case Value.GROUP_OTHER:
            case Value.GROUP_COLLECTION:
                return false;
            }
        }
        if (g1 == Value.GROUP_BINARY_STRING) {
            switch (vt2) {
            case Value.JAVA_OBJECT:
            case Value.GEOMETRY:
            case Value.JSON:
            case Value.UUID:
                return true;
            default:
                return false;
            }
        }
        return false;
    }

    private TypeInfo(int valueType) {
        this.valueType = valueType;
        precision = -1L;
        scale = -1;
        extTypeInfo = null;
    }

    private TypeInfo(int valueType, long precision) {
        this.valueType = valueType;
        this.precision = precision;
        scale = -1;
        extTypeInfo = null;
    }

    private TypeInfo(int valueType, int scale) {
        this.valueType = valueType;
        precision = -1L;
        this.scale = scale;
        extTypeInfo = null;
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
     * @param extTypeInfo
     *            the extended type information, or null
     */
    public TypeInfo(int valueType, long precision, int scale, ExtTypeInfo extTypeInfo) {
        this.valueType = valueType;
        this.precision = precision;
        this.scale = scale;
        this.extTypeInfo = extTypeInfo;
    }

    /**
     * Returns this type information.
     *
     * @return this
     */
    @Override
    public TypeInfo getType() {
        return this;
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
        switch (valueType) {
        case Value.UNKNOWN:
            return -1L;
        case Value.NULL:
            return ValueNull.PRECISION;
        case Value.CHAR:
        case Value.BINARY:
            return precision >= 0L ? precision : 1L;
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.VARBINARY:
        case Value.JAVA_OBJECT:
        case Value.ENUM:
        case Value.GEOMETRY:
        case Value.JSON:
            return precision >= 0L ? precision : Constants.MAX_STRING_LENGTH;
        case Value.CLOB:
        case Value.BLOB:
            return precision >= 0L ? precision : Long.MAX_VALUE;
        case Value.BOOLEAN:
            return ValueBoolean.PRECISION;
        case Value.TINYINT:
            return ValueTinyint.PRECISION;
        case Value.SMALLINT:
            return ValueSmallint.PRECISION;
        case Value.INTEGER:
            return ValueInteger.PRECISION;
        case Value.BIGINT:
            return ValueBigint.PRECISION;
        case Value.NUMERIC:
            return precision >= 0L ? precision : Constants.MAX_NUMERIC_PRECISION;
        case Value.REAL:
            return ValueReal.PRECISION;
        case Value.DOUBLE:
            return ValueDouble.PRECISION;
        case Value.DECFLOAT:
            return precision >= 0L ? precision : Constants.MAX_NUMERIC_PRECISION;
        case Value.DATE:
            return ValueDate.PRECISION;
        case Value.TIME: {
            int s = scale >= 0 ? scale : ValueTime.DEFAULT_SCALE;
            return s == 0 ? 8 : 9 + s;
        }
        case Value.TIME_TZ: {
            int s = scale >= 0 ? scale : ValueTime.DEFAULT_SCALE;
            return s == 0 ? 14 : 15 + s;
        }
        case Value.TIMESTAMP: {
            int s = scale >= 0 ? scale : ValueTimestamp.DEFAULT_SCALE;
            return s == 0 ? 19 : 20 + s;
        }
        case Value.TIMESTAMP_TZ: {
            int s = scale >= 0 ? scale : ValueTimestamp.DEFAULT_SCALE;
            return s == 0 ? 25 : 26 + s;
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
        case Value.INTERVAL_MINUTE_TO_SECOND:
            return precision >= 0L ? precision : ValueInterval.DEFAULT_PRECISION;
        case Value.ROW:
            return Integer.MAX_VALUE;
        case Value.UUID:
            return ValueUuid.PRECISION;
        case Value.ARRAY:
            return precision >= 0L ? precision : Constants.MAX_ARRAY_CARDINALITY;
        default:
            return precision;
        }
    }

    /**
     * Returns the precision, or {@code -1L} if not specified in data type
     * definition.
     *
     * @return the precision, or {@code -1L} if not specified in data type
     *         definition
     */
    public long getDeclaredPrecision() {
        return precision;
    }

    /**
     * Returns the scale.
     *
     * @return the scale
     */
    public int getScale() {
        switch (valueType) {
        case Value.UNKNOWN:
            return -1;
        case Value.NULL:
        case Value.CHAR:
        case Value.VARCHAR:
        case Value.CLOB:
        case Value.VARCHAR_IGNORECASE:
        case Value.BINARY:
        case Value.VARBINARY:
        case Value.BLOB:
        case Value.BOOLEAN:
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT:
        case Value.REAL:
        case Value.DOUBLE:
        case Value.DECFLOAT:
        case Value.DATE:
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.JAVA_OBJECT:
        case Value.ENUM:
        case Value.GEOMETRY:
        case Value.JSON:
        case Value.UUID:
        case Value.ARRAY:
        case Value.ROW:
            return 0;
        case Value.NUMERIC:
            return scale >= 0 ? scale : 0;
        case Value.TIME:
        case Value.TIME_TZ:
            return scale >= 0 ? scale : ValueTime.DEFAULT_SCALE;
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            return scale >= 0 ? scale : ValueTimestamp.DEFAULT_SCALE;
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            return scale >= 0 ? scale : ValueInterval.DEFAULT_SCALE;
        default:
            return scale;
        }
    }

    /**
     * Returns the scale, or {@code -1} if not specified in data type
     * definition.
     *
     * @return the scale, or {@code -1} if not specified in data type definition
     */
    public int getDeclaredScale() {
        return scale;
    }

    /**
     * Returns the display size in characters.
     *
     * @return the display size
     */
    public int getDisplaySize() {
        switch (valueType) {
        case Value.UNKNOWN:
        default:
            return -1;
        case Value.NULL:
            return ValueNull.DISPLAY_SIZE;
        case Value.CHAR:
            return precision >= 0 ? (int) precision : 1;
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.JSON:
            return precision >= 0 ? (int) precision : Constants.MAX_STRING_LENGTH;
        case Value.CLOB:
            return precision >= 0 && precision <= Integer.MAX_VALUE ? (int) precision : Integer.MAX_VALUE;
        case Value.BINARY:
            return precision >= 0 ? (int) precision * 2 : 2;
        case Value.VARBINARY:
        case Value.JAVA_OBJECT:
            return precision >= 0 ? (int) precision * 2 : Constants.MAX_STRING_LENGTH * 2;
        case Value.BLOB:
            return precision >= 0 && precision <= Integer.MAX_VALUE / 2 ? (int) precision * 2 : Integer.MAX_VALUE;
        case Value.BOOLEAN:
            return ValueBoolean.DISPLAY_SIZE;
        case Value.TINYINT:
            return ValueTinyint.DISPLAY_SIZE;
        case Value.SMALLINT:
            return ValueSmallint.DISPLAY_SIZE;
        case Value.INTEGER:
            return ValueInteger.DISPLAY_SIZE;
        case Value.BIGINT:
            return ValueBigint.DISPLAY_SIZE;
        case Value.NUMERIC:
            return precision >= 0 ? (int) precision + 2 : Constants.MAX_NUMERIC_PRECISION + 2;
        case Value.REAL:
            return ValueReal.DISPLAY_SIZE;
        case Value.DOUBLE:
            return ValueDouble.DISPLAY_SIZE;
        case Value.DECFLOAT:
            return precision >= 0 ? (int) precision + 12 : Constants.MAX_NUMERIC_PRECISION + 12;
        case Value.DATE:
            return ValueDate.PRECISION;
        case Value.TIME: {
            int s = scale >= 0 ? scale : ValueTime.DEFAULT_SCALE;
            return s == 0 ? 8 : 9 + s;
        }
        case Value.TIME_TZ: {
            int s = scale >= 0 ? scale : ValueTime.DEFAULT_SCALE;
            return s == 0 ? 14 : 15 + s;
        }
        case Value.TIMESTAMP: {
            int s = scale >= 0 ? scale : ValueTimestamp.DEFAULT_SCALE;
            return s == 0 ? 19 : 20 + s;
        }
        case Value.TIMESTAMP_TZ: {
            int s = scale >= 0 ? scale : ValueTimestamp.DEFAULT_SCALE;
            return s == 0 ? 25 : 26 + s;
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
        case Value.INTERVAL_MINUTE_TO_SECOND:
            return ValueInterval.getDisplaySize(valueType,
                    precision >= 0 ? (int) precision : ValueInterval.DEFAULT_PRECISION,
                    scale >= 0 ? scale : ValueInterval.DEFAULT_SCALE);
        case Value.GEOMETRY:
        case Value.ARRAY:
        case Value.ROW:
            return Integer.MAX_VALUE;
        case Value.ENUM:
            return extTypeInfo != null ? (int) precision : Constants.MAX_STRING_LENGTH;
        case Value.UUID:
            return ValueUuid.DISPLAY_SIZE;
        }
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
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        switch (valueType) {
        case Value.CHAR:
        case Value.VARCHAR:
        case Value.CLOB:
        case Value.VARCHAR_IGNORECASE:
        case Value.BINARY:
        case Value.VARBINARY:
        case Value.BLOB:
        case Value.JAVA_OBJECT:
        case Value.JSON:
            builder.append(Value.getTypeName(valueType));
            if (precision >= 0L) {
                builder.append('(').append(precision).append(')');
            }
            break;
        case Value.NUMERIC: {
            if (extTypeInfo != null) {
                extTypeInfo.getSQL(builder, sqlFlags);
            } else {
                builder.append("NUMERIC");
            }
            boolean withPrecision = precision >= 0;
            boolean withScale = scale >= 0;
            if (withPrecision || withScale) {
                builder.append('(').append(withPrecision ? precision : Constants.MAX_NUMERIC_PRECISION);
                if (withScale) {
                    builder.append(", ").append(scale);
                }
                builder.append(')');
            }
            break;
        }
        case Value.REAL:
        case Value.DOUBLE:
            if (precision < 0) {
                builder.append(Value.getTypeName(valueType));
            } else {
                builder.append("FLOAT");
                if (precision > 0) {
                    builder.append('(').append(precision).append(')');
                }
            }
            break;
        case Value.DECFLOAT:
            builder.append("DECFLOAT");
            if (precision >= 0) {
                builder.append('(').append(precision).append(')');
            }
            break;
        case Value.TIME:
        case Value.TIME_TZ:
            builder.append("TIME");
            if (scale >= 0) {
                builder.append('(').append(scale).append(')');
            }
            if (valueType == Value.TIME_TZ) {
                builder.append(" WITH TIME ZONE");
            }
            break;
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            builder.append("TIMESTAMP");
            if (scale >= 0) {
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
            IntervalQualifier.valueOf(valueType - Value.INTERVAL_YEAR).getTypeName(builder, (int) precision, scale,
                    false);
            break;
        case Value.ENUM:
            extTypeInfo.getSQL(builder.append("ENUM"), sqlFlags);
            break;
        case Value.GEOMETRY:
            builder.append("GEOMETRY");
            if (extTypeInfo != null) {
                extTypeInfo.getSQL(builder, sqlFlags);
            }
            break;
        case Value.ARRAY:
            if (extTypeInfo != null) {
                extTypeInfo.getSQL(builder, sqlFlags).append(' ');
            }
            builder.append("ARRAY");
            if (precision >= 0L) {
                builder.append('[').append(precision).append(']');
            }
            break;
        case Value.ROW:
            builder.append("ROW");
            if (extTypeInfo != null) {
                extTypeInfo.getSQL(builder, sqlFlags);
            }
            break;
        default:
            builder.append(Value.getTypeName(valueType));
        }
        return builder;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + valueType;
        result = 31 * result + (int) (precision ^ (precision >>> 32));
        result = 31 * result + scale;
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
                && Objects.equals(extTypeInfo, other.extTypeInfo);
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
            return getTypeInfo(Value.NUMERIC, getDecimalPrecision(), 0, null);
        case Value.BIGINT:
            return TYPE_NUMERIC_BIGINT;
        case Value.NUMERIC:
            return this;
        case Value.REAL:
            // Smallest REAL value is 1.4E-45 with precision 2 and scale 46
            // Largest REAL value is 3.4028235E+38 with precision 8 and scale
            // -31
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

    /**
     * Convert this type information to compatible DECFLOAT type information.
     *
     * @return DECFLOAT type information
     */
    public TypeInfo toDecfloatType() {
        switch (valueType) {
        case Value.BOOLEAN:
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
            return getTypeInfo(Value.DECFLOAT, getDecimalPrecision(), 0, null);
        case Value.BIGINT:
            return TYPE_DECFLOAT_BIGINT;
        case Value.NUMERIC:
            return getTypeInfo(Value.DECFLOAT, getPrecision(), 0, null);
        case Value.REAL:
            return getTypeInfo(Value.DECFLOAT, ValueReal.DECIMAL_PRECISION, 0, null);
        case Value.DOUBLE:
            return getTypeInfo(Value.DECFLOAT, ValueDouble.DECIMAL_PRECISION, 0, null);
        case Value.DECFLOAT:
            return this;
        default:
            return TYPE_DECFLOAT;
        }
    }

    /**
     * Returns unwrapped data type if this data type is a row type with degree 1
     * or this type otherwise.
     *
     * @return unwrapped data type if this data type is a row type with degree 1
     *         or this type otherwise
     */
    public TypeInfo unwrapRow() {
        if (valueType == Value.ROW) {
            Set<Entry<String, TypeInfo>> fields = ((ExtTypeInfoRow) extTypeInfo).getFields();
            if (fields.size() == 1) {
                return fields.iterator().next().getValue().unwrapRow();
            }
        }
        return this;
    }

    /**
     * Returns approximate precision in decimal digits for binary numeric data
     * types and precision for all other types.
     *
     * @return precision in decimal digits
     */
    public long getDecimalPrecision() {
        switch (valueType) {
        case Value.TINYINT:
            return ValueTinyint.DECIMAL_PRECISION;
        case Value.SMALLINT:
            return ValueSmallint.DECIMAL_PRECISION;
        case Value.INTEGER:
            return ValueInteger.DECIMAL_PRECISION;
        case Value.BIGINT:
            return ValueBigint.DECIMAL_PRECISION;
        case Value.REAL:
            return ValueReal.DECIMAL_PRECISION;
        case Value.DOUBLE:
            return ValueDouble.DECIMAL_PRECISION;
        default:
            return precision;
        }
    }

    /**
     * Returns the declared name of this data type with precision, scale,
     * length, cardinality etc. parameters removed, excluding parameters of ENUM
     * data type, GEOMETRY data type, ARRAY elements, and ROW fields.
     *
     * @return the declared name
     */
    public String getDeclaredTypeName() {
        switch (valueType) {
        case Value.NUMERIC:
            return extTypeInfo != null ? "DECIMAL" : "NUMERIC";
        case Value.REAL:
        case Value.DOUBLE:
            if (extTypeInfo != null) {
                return "FLOAT";
            }
            break;
        case Value.ENUM:
        case Value.GEOMETRY:
        case Value.ROW:
            return getSQL(DEFAULT_SQL_FLAGS);
        case Value.ARRAY:
            TypeInfo typeInfo = (TypeInfo) extTypeInfo;
            // Use full type names with parameters for elements
            return typeInfo.getSQL(new StringBuilder(), DEFAULT_SQL_FLAGS).append(" ARRAY").toString();
        }
        return Value.getTypeName(valueType);
    }

}
