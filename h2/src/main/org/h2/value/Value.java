/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.ref.SoftReference;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.CastDataProvider;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.store.DataHandler;
import org.h2.util.Bits;
import org.h2.util.DateTimeUtils;
import org.h2.util.HasSQL;
import org.h2.util.IntervalUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.geometry.GeoJsonUtils;

/**
 * This is the base class for all value classes.
 * It provides conversion and comparison methods.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public abstract class Value extends VersionedValue<Value> implements HasSQL {

    /**
     * The data type is unknown at this time.
     */
    public static final int UNKNOWN = -1;

    /**
     * The value type for NULL.
     */
    public static final int NULL = 0;

    /**
     * The value type for BOOLEAN values.
     */
    public static final int BOOLEAN = 1;

    /**
     * The value type for TINYINT values.
     */
    public static final int TINYINT = 2;

    /**
     * The value type for SMALLINT values.
     */
    public static final int SMALLINT = 3;

    /**
     * The value type for INTEGER values.
     */
    public static final int INTEGER = 4;

    /**
     * The value type for BIGINT values.
     */
    public static final int BIGINT = 5;

    /**
     * The value type for NUMERIC values.
     */
    public static final int NUMERIC = 6;

    /**
     * The value type for DOUBLE PRECISION values.
     */
    public static final int DOUBLE = 7;

    /**
     * The value type for REAL values.
     */
    public static final int REAL = 8;

    /**
     * The value type for TIME values.
     */
    public static final int TIME = 9;

    /**
     * The value type for DATE values.
     */
    public static final int DATE = 10;

    /**
     * The value type for TIMESTAMP values.
     */
    public static final int TIMESTAMP = 11;

    /**
     * The value type for BINARY VARYING values.
     */
    public static final int VARBINARY = 12;

    /**
     * The value type for CHARACTER VARYING values.
     */
    public static final int VARCHAR = 13;

    /**
     * The value type for VARCHAR_IGNORECASE values.
     */
    public static final int VARCHAR_IGNORECASE = 14;

    /**
     * The value type for BLOB values.
     */
    public static final int BLOB = 15;

    /**
     * The value type for CLOB values.
     */
    public static final int CLOB = 16;

    /**
     * The value type for ARRAY values.
     */
    public static final int ARRAY = 17;

    /**
     * The value type for RESULT_SET values.
     */
    public static final int RESULT_SET = 18;

    /**
     * The value type for JAVA_OBJECT values.
     */
    public static final int JAVA_OBJECT = 19;

    /**
     * The value type for UUID values.
     */
    public static final int UUID = 20;

    /**
     * The value type for CHAR values.
     */
    public static final int CHAR = 21;

    /**
     * The value type for string values with a fixed size.
     */
    public static final int GEOMETRY = 22;

    /*
     * 23 was a short-lived experiment "TIMESTAMP UTC" which has been removed.
     */

    /**
     * The value type for TIMESTAMP WITH TIME ZONE values.
     */
    public static final int TIMESTAMP_TZ = 24;

    /**
     * The value type for ENUM values.
     */
    public static final int ENUM = 25;

    /**
     * The value type for {@code INTERVAL YEAR} values.
     */
    public static final int INTERVAL_YEAR = 26;

    /**
     * The value type for {@code INTERVAL MONTH} values.
     */
    public static final int INTERVAL_MONTH = 27;

    /**
     * The value type for {@code INTERVAL DAY} values.
     */
    public static final int INTERVAL_DAY = 28;

    /**
     * The value type for {@code INTERVAL HOUR} values.
     */
    public static final int INTERVAL_HOUR = 29;

    /**
     * The value type for {@code INTERVAL MINUTE} values.
     */
    public static final int INTERVAL_MINUTE = 30;

    /**
     * The value type for {@code INTERVAL SECOND} values.
     */
    public static final int INTERVAL_SECOND = 31;

    /**
     * The value type for {@code INTERVAL YEAR TO MONTH} values.
     */
    public static final int INTERVAL_YEAR_TO_MONTH = 32;

    /**
     * The value type for {@code INTERVAL DAY TO HOUR} values.
     */
    public static final int INTERVAL_DAY_TO_HOUR = 33;

    /**
     * The value type for {@code INTERVAL DAY TO MINUTE} values.
     */
    public static final int INTERVAL_DAY_TO_MINUTE = 34;

    /**
     * The value type for {@code INTERVAL DAY TO SECOND} values.
     */
    public static final int INTERVAL_DAY_TO_SECOND = 35;

    /**
     * The value type for {@code INTERVAL HOUR TO MINUTE} values.
     */
    public static final int INTERVAL_HOUR_TO_MINUTE = 36;

    /**
     * The value type for {@code INTERVAL HOUR TO SECOND} values.
     */
    public static final int INTERVAL_HOUR_TO_SECOND = 37;

    /**
     * The value type for {@code INTERVAL MINUTE TO SECOND} values.
     */
    public static final int INTERVAL_MINUTE_TO_SECOND = 38;

    /**
     * The value type for ROW values.
     */
    public static final int ROW = 39;

    /**
     * The value type for JSON values.
     */
    public static final int JSON = 40;

    /**
     * The value type for TIME WITH TIME ZONE values.
     */
    public static final int TIME_TZ = 41;

    /**
     * The number of value types.
     */
    public static final int TYPE_COUNT = TIME_TZ + 1;

    /**
     * Empty array of values.
     */
    public static final Value[] EMPTY_VALUES = new Value[0];

    private static SoftReference<Value[]> softCache;

    static final BigDecimal MAX_LONG_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE);

    /**
     * The smallest Long value, as a BigDecimal.
     */
    public static final BigDecimal MIN_LONG_DECIMAL = BigDecimal.valueOf(Long.MIN_VALUE);

    /**
     * Convert a value to the specified type without taking scale and precision
     * into account.
     */
    static final int CONVERT_TO = 0;

    /**
     * Cast a value to the specified type. The scale is set if applicable. The
     * value is truncated to a required precision.
     */
    static final int CAST_TO = 1;

    /**
     * Cast a value to the specified type for assignment. The scale is set if
     * applicable. If precision is too large an exception is thrown.
     */
    static final int ASSIGN_TO = 2;

    /**
     * Check the range of the parameters.
     *
     * @param zeroBasedOffset the offset (0 meaning no offset)
     * @param length the length of the target
     * @param dataSize the length of the source
     */
    static void rangeCheck(long zeroBasedOffset, long length, long dataSize) {
        if ((zeroBasedOffset | length) < 0 || length > dataSize - zeroBasedOffset) {
            if (zeroBasedOffset < 0 || zeroBasedOffset > dataSize) {
                throw DbException.getInvalidValueException("offset", zeroBasedOffset + 1);
            }
            throw DbException.getInvalidValueException("length", length);
        }
    }

    /**
     * Returns the data type.
     *
     * @return the data type
     */
    public abstract TypeInfo getType();

    /**
     * Get the value type.
     *
     * @return the value type
     */
    public abstract int getValueType();

    /**
     * Get the memory used by this object.
     *
     * @return the memory used in bytes
     */
    public int getMemory() {
        /*
         * Java 11 with -XX:-UseCompressedOops for all values up to ValueLong
         * and ValueDouble.
         */
        return 24;
    }

    /**
     * Get the value as a string.
     *
     * @return the string
     */
    public abstract String getString();

    /**
     * Get the value as an object.
     *
     * @return the object
     */
    public abstract Object getObject();

    @Override
    public abstract int hashCode();

    /**
     * Check if the two values have the same hash code. No data conversion is
     * made; this method returns false if the other object is not of the same
     * class. For some values, compareTo may return 0 even if equals return
     * false. Example: ValueDecimal 0.0 and 0.00.
     *
     * @param other the other value
     * @return true if they are equal
     */
    @Override
    public abstract boolean equals(Object other);

    /**
     * Get the order of this value type.
     *
     * @param type the value type
     * @return the order number
     */
    static int getOrder(int type) {
        switch (type) {
        case UNKNOWN:
            return 1_000;
        case NULL:
            return 2_000;
        case VARCHAR:
            return 10_000;
        case CLOB:
            return 11_000;
        case CHAR:
            return 12_000;
        case VARCHAR_IGNORECASE:
            return 13_000;
        case BOOLEAN:
            return 20_000;
        case TINYINT:
            return 21_000;
        case SMALLINT:
            return 22_000;
        case INTEGER:
            return 23_000;
        case BIGINT:
            return 24_000;
        case NUMERIC:
            return 25_000;
        case REAL:
            return 26_000;
        case DOUBLE:
            return 27_000;
        case INTERVAL_YEAR:
            return 28_000;
        case INTERVAL_MONTH:
            return 28_100;
        case INTERVAL_YEAR_TO_MONTH:
            return 28_200;
        case INTERVAL_DAY:
            return 29_000;
        case INTERVAL_HOUR:
            return 29_100;
        case INTERVAL_DAY_TO_HOUR:
            return 29_200;
        case INTERVAL_MINUTE:
            return 29_300;
        case INTERVAL_HOUR_TO_MINUTE:
            return 29_400;
        case INTERVAL_DAY_TO_MINUTE:
            return 29_500;
        case INTERVAL_SECOND:
            return 29_600;
        case INTERVAL_MINUTE_TO_SECOND:
            return 29_700;
        case INTERVAL_HOUR_TO_SECOND:
            return 29_800;
        case INTERVAL_DAY_TO_SECOND:
            return 29_900;
        case TIME:
            return 30_000;
        case TIME_TZ:
            return 30_500;
        case DATE:
            return 31_000;
        case TIMESTAMP:
            return 32_000;
        case TIMESTAMP_TZ:
            return 34_000;
        case VARBINARY:
            return 40_000;
        case BLOB:
            return 41_000;
        case JAVA_OBJECT:
            return 42_000;
        case UUID:
            return 43_000;
        case GEOMETRY:
            return 44_000;
        case ENUM:
            return 45_000;
        case JSON:
            return 46_000;
        case ARRAY:
            return 50_000;
        case ROW:
            return 51_000;
        case RESULT_SET:
            return 52_000;
        default:
            throw DbException.throwInternalError("type:"+type);
        }
    }

    /**
     * Get the higher value order type of two value types. If values need to be
     * converted to match the other operands value type, the value with the
     * lower order is converted to the value with the higher order.
     *
     * @param t1 the first value type
     * @param t2 the second value type
     * @return the higher value type of the two
     */
    public static int getHigherOrder(int t1, int t2) {
        if (t1 == UNKNOWN || t2 == UNKNOWN) {
            if (t1 == t2) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?, ?");
            } else if (t1 == NULL) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "NULL, ?");
            } else if (t2 == NULL) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?, NULL");
            }
        }
        if (t1 == t2) {
            return t1;
        }
        int o1 = getOrder(t1);
        int o2 = getOrder(t2);
        return o1 > o2 ? t1 : t2;
    }

    /**
     * Get the higher data type of two data types. If values need to be
     * converted to match the other operands data type, the value with the
     * lower order is converted to the value with the higher order.
     *
     * @param type1 the first data type
     * @param type2 the second data type
     * @return the higher data type of the two
     */
    public static TypeInfo getHigherType(TypeInfo type1, TypeInfo type2) {
        int t1 = type1.getValueType(), t2 = type2.getValueType();
        int dataType = getHigherOrder(t1, t2);
        long precision = Math.max(type1.getPrecision(), type2.getPrecision());
        int scale = Math.max(type1.getScale(), type2.getScale());
        ExtTypeInfo ext1 = type1.getExtTypeInfo();
        ExtTypeInfo ext = dataType == t1 && ext1 != null ? ext1 : dataType == t2 ? type2.getExtTypeInfo() : null;
        return TypeInfo.getTypeInfo(dataType, precision, scale, ext);
    }

    /**
     * Check if a value is in the cache that is equal to this value. If yes,
     * this value should be used to save memory. If the value is not in the
     * cache yet, it is added.
     *
     * @param v the value to look for
     * @return the value in the cache or the value passed
     */
    static Value cache(Value v) {
        if (SysProperties.OBJECT_CACHE) {
            int hash = v.hashCode();
            Value[] cache;
            if (softCache == null || (cache = softCache.get()) == null) {
                cache = new Value[SysProperties.OBJECT_CACHE_SIZE];
                softCache = new SoftReference<>(cache);
            }
            int index = hash & (SysProperties.OBJECT_CACHE_SIZE - 1);
            Value cached = cache[index];
            if (cached != null) {
                if (cached.getValueType() == v.getValueType() && v.equals(cached)) {
                    // cacheHit++;
                    return cached;
                }
            }
            // cacheMiss++;
            // cache[cacheCleaner] = null;
            // cacheCleaner = (cacheCleaner + 1) &
            //     (Constants.OBJECT_CACHE_SIZE - 1);
            cache[index] = v;
        }
        return v;
    }

    /**
     * Clear the value cache. Used for testing.
     */
    public static void clearCache() {
        softCache = null;
    }

    public boolean getBoolean() {
        return convertToBoolean().getBoolean();
    }

    public byte[] getBytes() {
        return convertTo(TypeInfo.TYPE_VARBINARY).getBytes();
    }

    public byte[] getBytesNoCopy() {
        return convertTo(TypeInfo.TYPE_VARBINARY).getBytesNoCopy();
    }

    public byte getByte() {
        return convertToTinyint(null).getByte();
    }

    public short getShort() {
        return convertToSmallint(null).getShort();
    }

    public BigDecimal getBigDecimal() {
        return convertTo(TypeInfo.TYPE_NUMERIC).getBigDecimal();
    }

    public double getDouble() {
        return convertToDouble().getDouble();
    }

    public float getFloat() {
        return convertToReal().getFloat();
    }

    public int getInt() {
        return convertToInt(null).getInt();
    }

    public long getLong() {
        return convertToBigint(null).getLong();
    }

    public InputStream getInputStream() {
        return new ByteArrayInputStream(getBytesNoCopy());
    }

    /**
     * Get the input stream
     *
     * @param oneBasedOffset the offset (1 means no offset)
     * @param length the requested length
     * @return the new input stream
     */
    public InputStream getInputStream(long oneBasedOffset, long length) {
        byte[] bytes = getBytesNoCopy();
        long zeroBasedOffset = oneBasedOffset - 1;
        rangeCheck(zeroBasedOffset, length, bytes.length);
        return new ByteArrayInputStream(bytes, (int) zeroBasedOffset, (int) length);
    }

    public Reader getReader() {
        return new StringReader(getString());
    }

    /**
     * Get the reader
     *
     * @param oneBasedOffset the offset (1 means no offset)
     * @param length the requested length
     * @return the new reader
     */
    public Reader getReader(long oneBasedOffset, long length) {
        String string = getString();
        long zeroBasedOffset = oneBasedOffset - 1;
        rangeCheck(zeroBasedOffset, length, string.length());
        int offset = (int) zeroBasedOffset;
        return new StringReader(string.substring(offset, offset + (int) length));
    }

    /**
     * Add a value and return the result.
     *
     * @param v the value to add
     * @return the result
     */
    public Value add(@SuppressWarnings("unused") Value v) {
        throw getUnsupportedExceptionForOperation("+");
    }

    public int getSignum() {
        throw getUnsupportedExceptionForOperation("SIGNUM");
    }

    /**
     * Return -value if this value support arithmetic operations.
     *
     * @return the negative
     */
    public Value negate() {
        throw getUnsupportedExceptionForOperation("NEG");
    }

    /**
     * Subtract a value and return the result.
     *
     * @param v the value to subtract
     * @return the result
     */
    public Value subtract(@SuppressWarnings("unused") Value v) {
        throw getUnsupportedExceptionForOperation("-");
    }

    /**
     * Divide by a value and return the result.
     *
     * @param v the divisor
     * @param divisorPrecision the precision of divisor
     * @return the result
     */
    public Value divide(@SuppressWarnings("unused") Value v, long divisorPrecision) {
        throw getUnsupportedExceptionForOperation("/");
    }

    /**
     * Multiply with a value and return the result.
     *
     * @param v the value to multiply with
     * @return the result
     */
    public Value multiply(@SuppressWarnings("unused") Value v) {
        throw getUnsupportedExceptionForOperation("*");
    }

    /**
     * Take the modulus with a value and return the result.
     *
     * @param v the value to take the modulus with
     * @return the result
     */
    public Value modulus(@SuppressWarnings("unused") Value v) {
        throw getUnsupportedExceptionForOperation("%");
    }

    /**
     * Convert a value to the specified type without taking scale and precision
     * into account.
     *
     * @param targetType the type of the returned value
     * @return the converted value
     */
    public final Value convertTo(int targetType) {
        return convertTo(TypeInfo.getTypeInfo(targetType), null, CONVERT_TO, null);
    }

    /**
     * Convert a value to the specified type without taking scale and precision
     * into account.
     *
     * @param targetType the type of the returned value
     * @return the converted value
     */
    public final Value convertTo(TypeInfo targetType) {
        return convertTo(targetType, null, CONVERT_TO, null);
    }

    /**
     * Convert a value to the specified type without taking scale and precision
     * into account.
     *
     * @param targetType the type of the returned value
     * @param provider the cast information provider
     * @return the converted value
     */
    public final Value convertTo(int targetType, CastDataProvider provider) {
        return convertTo(TypeInfo.getTypeInfo(targetType), provider, CONVERT_TO, null);
    }

    /**
     * Convert a value to the specified type without taking scale and precision
     * into account.
     *
     * @param targetType
     *            the type of the returned value
     * @param provider
     *            the cast information provider
     * @return the converted value
     */
    public final Value convertTo(TypeInfo targetType, CastDataProvider provider) {
        return convertTo(targetType, provider, CONVERT_TO, null);
    }

    /**
     * Convert a value to the specified type without taking scale and precision
     * into account.
     *
     * @param targetType
     *            the type of the returned value
     * @param provider
     *            the cast information provider
     * @param column
     *            the column, used to improve the error message if conversion
     *            fails
     * @return the converted value
     */
    public final Value convertTo(TypeInfo targetType, CastDataProvider provider, Object column) {
        return convertTo(targetType, provider, CONVERT_TO, column);
    }

    /**
     * Cast a value to the specified type. The scale is set if applicable. The
     * value is truncated to the required precision.
     *
     * @param targetType
     *            the type of the returned value
     * @param provider
     *            the cast information provider
     * @return the converted value
     */
    public final Value castTo(TypeInfo targetType, CastDataProvider provider) {
        return convertTo(targetType, provider, CAST_TO, null);
    }

    /**
     * Cast a value to the specified type for assignment. The scale is set if
     * applicable. If precision is too large an exception is thrown.
     *
     * @param targetType
     *            the type of the returned value
     * @param provider
     *            the cast information provider
     * @param column
     *            the column, used to improve the error message if conversion
     *            fails
     * @return the converted value
     */
    public final Value convertForAssignTo(TypeInfo targetType, CastDataProvider provider, Object column) {
        return convertTo(targetType, provider, ASSIGN_TO, column);
    }

    /**
     * Convert a value to the specified type.
     *
     * @param targetType the type of the returned value
     * @param provider the cast information provider
     * @param conversionMode conversion mode
     * @param column the column (if any), used to improve the error message if conversion fails
     * @return the converted value
     */
    private Value convertTo(TypeInfo targetType, CastDataProvider provider, int conversionMode, Object column) {
        int valueType = getValueType(), targetValueType;
        if (valueType == NULL
                || valueType == (targetValueType = targetType.getValueType()) && conversionMode == CONVERT_TO
                && targetType.getExtTypeInfo() == null) {
            return this;
        }
        switch (targetValueType) {
        case NULL:
            return ValueNull.INSTANCE;
        case BOOLEAN:
            return convertToBoolean();
        case TINYINT:
            return convertToTinyint(column);
        case SMALLINT:
            return convertToSmallint(column);
        case INTEGER:
            return convertToInt(column);
        case BIGINT:
            return convertToBigint(column);
        case NUMERIC:
            return convertToNumeric(targetType, provider, conversionMode, column);
        case DOUBLE:
            return convertToDouble();
        case REAL:
            return convertToReal();
        case DATE:
            return convertToDate(provider);
        case TIME:
            return convertToTime(targetType, provider, conversionMode);
        case TIME_TZ:
            return convertToTimeTimeZone(targetType, provider, conversionMode);
        case TIMESTAMP:
            return convertToTimestamp(targetType, provider, conversionMode);
        case TIMESTAMP_TZ:
            return convertToTimestampTimeZone(targetType, provider, conversionMode);
        case VARBINARY:
            return convertToVarbinary(targetType, conversionMode, column);
        case VARCHAR:
            return ValueVarchar.get(convertToVarchar(targetType, conversionMode, column));
        case VARCHAR_IGNORECASE:
            return ValueVarcharIgnoreCase.get(convertToVarchar(targetType, conversionMode, column));
        case CHAR:
            return convertToChar(targetType, conversionMode, column);
        case JAVA_OBJECT:
            return convertToJavaObject(targetType, conversionMode, column);
        case ENUM:
            return convertToEnum((ExtTypeInfoEnum) targetType.getExtTypeInfo());
        case BLOB:
            return convertToBlob(targetType, conversionMode, column);
        case CLOB:
            return convertToClob(targetType, conversionMode, column);
        case UUID:
            return convertToUuid();
        case GEOMETRY:
            return convertToGeometry((ExtTypeInfoGeometry) targetType.getExtTypeInfo());
        case INTERVAL_YEAR:
        case INTERVAL_MONTH:
        case INTERVAL_YEAR_TO_MONTH:
            return convertToIntervalYearMonth(targetType, conversionMode, column);
        case INTERVAL_DAY:
        case INTERVAL_HOUR:
        case INTERVAL_MINUTE:
        case INTERVAL_SECOND:
        case INTERVAL_DAY_TO_HOUR:
        case INTERVAL_DAY_TO_MINUTE:
        case INTERVAL_DAY_TO_SECOND:
        case INTERVAL_HOUR_TO_MINUTE:
        case INTERVAL_HOUR_TO_SECOND:
        case INTERVAL_MINUTE_TO_SECOND:
            return convertToIntervalDayTime(targetType, conversionMode, column);
        case JSON:
            return convertToJson();
        case ARRAY:
            return convertToArray(targetType, provider, conversionMode, column);
        case ROW:
            return convertToRow();
        case RESULT_SET:
            return convertToResultSet();
        default:
            throw getDataConversionError(targetValueType);
        }
    }

    /**
     * Converts this value to a BOOLEAN value. May not be called on a NULL
     * value.
     *
     * @return the BOOLEAN value
     */
    public final ValueBoolean convertToBoolean() {
        switch (getValueType()) {
        case BOOLEAN:
            return (ValueBoolean) this;
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case NUMERIC:
        case DOUBLE:
        case REAL:
            return ValueBoolean.get(getSignum() != 0);
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR: {
            String s = getString();
            if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("t") || s.equalsIgnoreCase("yes")
                    || s.equalsIgnoreCase("y")) {
                return ValueBoolean.TRUE;
            } else if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("f") || s.equalsIgnoreCase("no")
                    || s.equalsIgnoreCase("n")) {
                return ValueBoolean.FALSE;
            } else {
                try {
                    // convert to a number, and if it is not 0 then it is true
                    return ValueBoolean.get(new BigDecimal(s).signum() != 0);
                } catch (NumberFormatException e) {
                    // Hide this custom exception, it is meaningless for
                    // conversion to BOOLEAN
                }
            }
        }
        //$FALL-THROUGH$
        default:
            throw getDataConversionError(BOOLEAN);
        case NULL:
            throw DbException.throwInternalError();
        }
    }

    /**
     * Converts this value to a TINYINT value. May not be called on a NULL
     * value.
     *
     * @param column
     *            the column, used for to improve the error message if
     *            conversion fails
     * @return the TINYINT value
     */
    public final ValueTinyint convertToTinyint(Object column) {
        switch (getValueType()) {
        case TINYINT:
            return (ValueTinyint) this;
        case BOOLEAN:
            return ValueTinyint.get(getBoolean() ? (byte) 1 : (byte) 0);
        case SMALLINT:
        case ENUM:
        case INTEGER:
            return ValueTinyint.get(convertToByte(getInt(), column));
        case BIGINT:
        case INTERVAL_YEAR:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_HOUR:
        case INTERVAL_MINUTE:
        case INTERVAL_SECOND:
        case INTERVAL_YEAR_TO_MONTH:
        case INTERVAL_DAY_TO_HOUR:
        case INTERVAL_DAY_TO_MINUTE:
        case INTERVAL_DAY_TO_SECOND:
        case INTERVAL_HOUR_TO_MINUTE:
        case INTERVAL_HOUR_TO_SECOND:
        case INTERVAL_MINUTE_TO_SECOND:
            return ValueTinyint.get(convertToByte(getLong(), column));
        case NUMERIC:
            return ValueTinyint.get(convertToByte(convertToLong(getBigDecimal(), column), column));
        case REAL:
        case DOUBLE:
            return ValueTinyint.get(convertToByte(convertToLong(getDouble(), column), column));
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR: {
            String s = getString();
            try {
                return ValueTinyint.get(Byte.parseByte(s.trim()));
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, s);
            }
        }
        case VARBINARY: {
            byte[] bytes = getBytesNoCopy();
            if (bytes.length == 1) {
                return ValueTinyint.get(bytes[0]);
            }
        }
        //$FALL-THROUGH$
        default:
            throw getDataConversionError(TINYINT);
        case NULL:
            throw DbException.throwInternalError();
        }
    }

    /**
     * Converts this value to a SMALLINT value. May not be called on a NULL value.
     *
     * @param column
     *            the column, used for to improve the error message if
     *            conversion fails
     * @return the SMALLINT value
     */
    public final ValueSmallint convertToSmallint(Object column) {
        switch (getValueType()) {
        case SMALLINT:
            return (ValueSmallint) this;
        case BOOLEAN:
            return ValueSmallint.get(getBoolean() ? (short) 1 : (short) 0);
        case TINYINT:
            return ValueSmallint.get(getByte());
        case ENUM:
        case INTEGER:
            return ValueSmallint.get(convertToShort(getInt(), column));
        case BIGINT:
        case INTERVAL_YEAR:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_HOUR:
        case INTERVAL_MINUTE:
        case INTERVAL_SECOND:
        case INTERVAL_YEAR_TO_MONTH:
        case INTERVAL_DAY_TO_HOUR:
        case INTERVAL_DAY_TO_MINUTE:
        case INTERVAL_DAY_TO_SECOND:
        case INTERVAL_HOUR_TO_MINUTE:
        case INTERVAL_HOUR_TO_SECOND:
        case INTERVAL_MINUTE_TO_SECOND:
            return ValueSmallint.get(convertToShort(getLong(), column));
        case NUMERIC:
            return ValueSmallint.get(convertToShort(convertToLong(getBigDecimal(), column), column));
        case REAL:
        case DOUBLE:
            return ValueSmallint.get(convertToShort(convertToLong(getDouble(), column), column));
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR: {
            String s = getString();
            try {
                return ValueSmallint.get(Short.parseShort(s.trim()));
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, s);
            }
        }
        case VARBINARY: {
            byte[] bytes = getBytesNoCopy();
            if (bytes.length == 2) {
                return ValueSmallint.get((short) ((bytes[0] << 8) + (bytes[1] & 0xff)));
            }
        }
        //$FALL-THROUGH$
        default:
            throw getDataConversionError(SMALLINT);
        case NULL:
            throw DbException.throwInternalError();
        }
    }

    /**
     * Converts this value to a INT value. May not be called on a NULL value.
     *
     * @param column
     *            the column, used for to improve the error message if
     *            conversion fails
     * @return the INT value
     */
    public final ValueInteger convertToInt(Object column) {
        switch (getValueType()) {
        case INTEGER:
            return (ValueInteger) this;
        case BOOLEAN:
            return ValueInteger.get(getBoolean() ? 1 : 0);
        case TINYINT:
        case ENUM:
        case SMALLINT:
            return ValueInteger.get(getInt());
        case BIGINT:
        case INTERVAL_YEAR:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_HOUR:
        case INTERVAL_MINUTE:
        case INTERVAL_SECOND:
        case INTERVAL_YEAR_TO_MONTH:
        case INTERVAL_DAY_TO_HOUR:
        case INTERVAL_DAY_TO_MINUTE:
        case INTERVAL_DAY_TO_SECOND:
        case INTERVAL_HOUR_TO_MINUTE:
        case INTERVAL_HOUR_TO_SECOND:
        case INTERVAL_MINUTE_TO_SECOND:
            return ValueInteger.get(convertToInt(getLong(), column));
        case NUMERIC:
            return ValueInteger.get(convertToInt(convertToLong(getBigDecimal(), column), column));
        case REAL:
        case DOUBLE:
            return ValueInteger.get(convertToInt(convertToLong(getDouble(), column), column));
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR: {
            String s = getString();
            try {
                return ValueInteger.get(Integer.parseInt(s.trim()));
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, s);
            }
        }
        case VARBINARY: {
            byte[] bytes = getBytesNoCopy();
            if (bytes.length == 4) {
                return ValueInteger.get(Bits.readInt(bytes, 0));
            }
        }
        //$FALL-THROUGH$
        default:
            throw getDataConversionError(INTEGER);
        case NULL:
            throw DbException.throwInternalError();
        }
    }

    /**
     * Converts this value to a BIGINT value. May not be called on a NULL value.
     *
     * @param column
     *            the column, used for to improve the error message if
     *            conversion fails
     * @return the BIGINT value
     */
    public final ValueBigint convertToBigint(Object column) {
        switch (getValueType()) {
        case BIGINT:
            return (ValueBigint) this;
        case BOOLEAN:
            return ValueBigint.get(getBoolean() ? 1 : 0);
        case TINYINT:
        case SMALLINT:
        case ENUM:
        case INTEGER:
        case INTERVAL_YEAR:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_HOUR:
        case INTERVAL_MINUTE:
        case INTERVAL_SECOND:
        case INTERVAL_YEAR_TO_MONTH:
        case INTERVAL_DAY_TO_HOUR:
        case INTERVAL_DAY_TO_MINUTE:
        case INTERVAL_DAY_TO_SECOND:
        case INTERVAL_HOUR_TO_MINUTE:
        case INTERVAL_HOUR_TO_SECOND:
        case INTERVAL_MINUTE_TO_SECOND:
            return ValueBigint.get(getInt());
        case NUMERIC:
            return ValueBigint.get(convertToLong(getBigDecimal(), column));
        case REAL:
        case DOUBLE:
            return ValueBigint.get(convertToLong(getDouble(), column));
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR: {
            String s = getString();
            try {
                return ValueBigint.get(Long.parseLong(s.trim()));
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, s);
            }
        }
        case VARBINARY: {
            byte[] bytes = getBytesNoCopy();
            if (bytes.length == 8) {
                return ValueBigint.get(Bits.readLong(bytes, 0));
            }
        }
        //$FALL-THROUGH$
        default:
            throw getDataConversionError(BIGINT);
        case NULL:
            throw DbException.throwInternalError();
        }
    }

    private ValueNumeric convertToNumeric(TypeInfo targetType, CastDataProvider provider, int conversionMode,
            Object column) {
        ValueNumeric v;
        switch (getValueType()) {
        case NUMERIC:
            v = (ValueNumeric) this;
            break;
        case BOOLEAN:
            v = getBoolean() ? ValueNumeric.ONE : ValueNumeric.ZERO;
            break;
        case TINYINT:
        case SMALLINT:
        case ENUM:
        case INTEGER:
            v =  ValueNumeric.get(BigDecimal.valueOf(getInt()));
            break;
        case BIGINT:
            v = ValueNumeric.get(BigDecimal.valueOf(getLong()));
            break;
        case DOUBLE:
        case REAL:
        case INTERVAL_YEAR:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_HOUR:
        case INTERVAL_MINUTE:
        case INTERVAL_SECOND:
        case INTERVAL_YEAR_TO_MONTH:
        case INTERVAL_DAY_TO_HOUR:
        case INTERVAL_DAY_TO_MINUTE:
        case INTERVAL_DAY_TO_SECOND:
        case INTERVAL_HOUR_TO_MINUTE:
        case INTERVAL_HOUR_TO_SECOND:
        case INTERVAL_MINUTE_TO_SECOND:
            v = ValueNumeric.get(getBigDecimal());
            break;
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR: {
            String s = getString();
            try {
                v = ValueNumeric.get(new BigDecimal(s.trim()));
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, s);
            }
            break;
        }
        default:
            throw getDataConversionError(NUMERIC);
        }
        if (conversionMode != CONVERT_TO) {
            int targetScale = targetType.getScale();
            BigDecimal value = v.getBigDecimal();
            int scale = value.scale();
            if (scale != targetScale && (scale >= targetScale || !provider.getMode().convertOnlyToSmallerScale)) {
                v = ValueNumeric.get(ValueNumeric.setScale(value, targetScale));
            }
            if (v.getBigDecimal().precision() > targetType.getPrecision()) {
                throw v.getValueTooLongException(targetType, column);
            }
        }
        return v;
    }

    /**
     * Converts this value to a DOUBLE value. May not be called on a NULL value.
     *
     * @return the DOUBLE value
     */
    public final ValueDouble convertToDouble() {
        switch (getValueType()) {
        case DOUBLE:
            return (ValueDouble) this;
        case BOOLEAN:
            return getBoolean() ? ValueDouble.ONE : ValueDouble.ZERO;
        case TINYINT:
        case SMALLINT:
        case INTEGER:
            return ValueDouble.get(getInt());
        case BIGINT:
        case INTERVAL_YEAR:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_HOUR:
        case INTERVAL_MINUTE:
            return ValueDouble.get(getLong());
        case NUMERIC:
        case INTERVAL_SECOND:
        case INTERVAL_YEAR_TO_MONTH:
        case INTERVAL_DAY_TO_HOUR:
        case INTERVAL_DAY_TO_MINUTE:
        case INTERVAL_DAY_TO_SECOND:
        case INTERVAL_HOUR_TO_MINUTE:
        case INTERVAL_HOUR_TO_SECOND:
        case INTERVAL_MINUTE_TO_SECOND:
            return ValueDouble.get(getBigDecimal().doubleValue());
        case REAL:
            return ValueDouble.get(getFloat());
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR: {
            String s = getString();
            try {
                return ValueDouble.get(Double.parseDouble(s.trim()));
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, s);
            }
        }
        default:
            throw getDataConversionError(DOUBLE);
        case NULL:
            throw DbException.throwInternalError();
        }
    }

    /**
     * Converts this value to a REAL value. May not be called on a NULL value.
     *
     * @return the REAL value
     */
    public final ValueReal convertToReal() {
        switch (getValueType()) {
        case REAL:
            return (ValueReal) this;
        case BOOLEAN:
            return getBoolean() ? ValueReal.ONE : ValueReal.ZERO;
        case TINYINT:
        case SMALLINT:
        case INTEGER:
            return ValueReal.get(getInt());
        case BIGINT:
        case INTERVAL_YEAR:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_HOUR:
        case INTERVAL_MINUTE:
            return ValueReal.get(getLong());
        case NUMERIC:
        case INTERVAL_SECOND:
        case INTERVAL_YEAR_TO_MONTH:
        case INTERVAL_DAY_TO_HOUR:
        case INTERVAL_DAY_TO_MINUTE:
        case INTERVAL_DAY_TO_SECOND:
        case INTERVAL_HOUR_TO_MINUTE:
        case INTERVAL_HOUR_TO_SECOND:
        case INTERVAL_MINUTE_TO_SECOND:
            return ValueReal.get(getBigDecimal().floatValue());
        case DOUBLE:
            return ValueReal.get((float) getDouble());
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR: {
            String s = getString();
            try {
                return ValueReal.get(Float.parseFloat(s.trim()));
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, s);
            }
        }
        default:
            throw getDataConversionError(REAL);
        case NULL:
            throw DbException.throwInternalError();
        }
    }

    /**
     * Converts this value to a DATE value. May not be called on a NULL value.
     *
     * @param provider
     *            the cast information provider
     * @return the DATE value
     */
    public final ValueDate convertToDate(CastDataProvider provider) {
        switch (getValueType()) {
        case DATE:
            return (ValueDate) this;
        case TIMESTAMP:
            return ValueDate.fromDateValue(((ValueTimestamp) this).getDateValue());
        case TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) this;
            long timeNanos = ts.getTimeNanos();
            long epochSeconds = DateTimeUtils.getEpochSeconds(ts.getDateValue(), timeNanos,
                    ts.getTimeZoneOffsetSeconds());
            return ValueDate.fromDateValue(DateTimeUtils
                    .dateValueFromLocalSeconds(epochSeconds
                            + provider.currentTimeZone().getTimeZoneOffsetUTC(epochSeconds)));
        }
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            return ValueDate.parse(getString().trim());
        default:
            throw getDataConversionError(DATE);
        case NULL:
            throw DbException.throwInternalError();
        }
    }

    private ValueTime convertToTime(TypeInfo targetType, CastDataProvider provider, int conversionMode) {
        ValueTime v;
        switch (getValueType()) {
        case TIME:
            v = (ValueTime) this;
            break;
        case TIME_TZ:
            v = ValueTime.fromNanos(getLocalTimeNanos(provider));
            break;
        case TIMESTAMP:
            v = ValueTime.fromNanos(((ValueTimestamp) this).getTimeNanos());
            break;
        case TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) this;
            long timeNanos = ts.getTimeNanos();
            long epochSeconds = DateTimeUtils.getEpochSeconds(ts.getDateValue(), timeNanos,
                    ts.getTimeZoneOffsetSeconds());
            v = ValueTime.fromNanos(
                    DateTimeUtils.nanosFromLocalSeconds(epochSeconds
                            + provider.currentTimeZone().getTimeZoneOffsetUTC(epochSeconds))
                            + timeNanos % DateTimeUtils.NANOS_PER_SECOND);
            break;
        }
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            v = ValueTime.parse(getString().trim());
            break;
        default:
            throw getDataConversionError(TIME);
        }
        if (conversionMode != CONVERT_TO) {
            int targetScale = targetType.getScale();
            if (targetScale < ValueTime.MAXIMUM_SCALE) {
                long n = v.getNanos();
                long n2 = DateTimeUtils.convertScale(n, targetScale, DateTimeUtils.NANOS_PER_DAY);
                if (n2 != n) {
                    v = ValueTime.fromNanos(n2);
                }
            }
        }
        return v;
    }

    private ValueTimeTimeZone convertToTimeTimeZone(TypeInfo targetType, CastDataProvider provider,
            int conversionMode) {
        ValueTimeTimeZone v;
        switch (getValueType()) {
        case TIME_TZ:
            v = (ValueTimeTimeZone) this;
            break;
        case TIME:
            v = ValueTimeTimeZone.fromNanos(((ValueTime) this).getNanos(),
                    provider.currentTimestamp().getTimeZoneOffsetSeconds());
            break;
        case TIMESTAMP: {
            ValueTimestamp ts = (ValueTimestamp) this;
            long timeNanos = ts.getTimeNanos();
            v = ValueTimeTimeZone.fromNanos(timeNanos,
                    provider.currentTimeZone().getTimeZoneOffsetLocal(ts.getDateValue(), timeNanos));
            break;
        }
        case TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) this;
            v = ValueTimeTimeZone.fromNanos(ts.getTimeNanos(), ts.getTimeZoneOffsetSeconds());
            break;
        }
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            v = ValueTimeTimeZone.parse(getString().trim());
            break;
        default:
            throw getDataConversionError(TIME_TZ);
        }
        if (conversionMode != CONVERT_TO) {
            int targetScale = targetType.getScale();
            if (targetScale < ValueTime.MAXIMUM_SCALE) {
                long n = v.getNanos();
                long n2 = DateTimeUtils.convertScale(n, targetScale, DateTimeUtils.NANOS_PER_DAY);
                if (n2 != n) {
                    v = ValueTimeTimeZone.fromNanos(n2, v.getTimeZoneOffsetSeconds());
                }
            }
        }
        return v;
    }

    private ValueTimestamp convertToTimestamp(TypeInfo targetType, CastDataProvider provider, int conversionMode) {
        ValueTimestamp v;
        switch (getValueType()) {
        case TIMESTAMP:
            v = (ValueTimestamp) this;
            break;
        case TIME:
            v = ValueTimestamp.fromDateValueAndNanos(provider.currentTimestamp().getDateValue(),
                    ((ValueTime) this).getNanos());
            break;
        case TIME_TZ:
            v = ValueTimestamp.fromDateValueAndNanos(provider.currentTimestamp().getDateValue(),
                    getLocalTimeNanos(provider));
            break;
        case DATE:
            // Scale is always 0
            return ValueTimestamp.fromDateValueAndNanos(((ValueDate) this).getDateValue(), 0);
        case TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) this;
            long timeNanos = ts.getTimeNanos();
            long epochSeconds = DateTimeUtils.getEpochSeconds(ts.getDateValue(), timeNanos,
                    ts.getTimeZoneOffsetSeconds());
            epochSeconds += provider.currentTimeZone().getTimeZoneOffsetUTC(epochSeconds);
            v = ValueTimestamp.fromDateValueAndNanos(DateTimeUtils.dateValueFromLocalSeconds(epochSeconds),
                    DateTimeUtils.nanosFromLocalSeconds(epochSeconds) + timeNanos % DateTimeUtils.NANOS_PER_SECOND);
            break;
        }
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            v = ValueTimestamp.parse(getString().trim(), provider);
            break;
        default:
            throw getDataConversionError(TIMESTAMP);
        }
        if (conversionMode != CONVERT_TO) {
            int targetScale = targetType.getScale();
            if (targetScale < ValueTimestamp.MAXIMUM_SCALE) {
                long dv = v.getDateValue(), n = v.getTimeNanos();
                long n2 = DateTimeUtils.convertScale(n, targetScale,
                        dv == DateTimeUtils.MAX_DATE_VALUE ? DateTimeUtils.NANOS_PER_DAY : Long.MAX_VALUE);
                if (n2 != n) {
                    if (n2 >= DateTimeUtils.NANOS_PER_DAY) {
                        n2 -= DateTimeUtils.NANOS_PER_DAY;
                        dv = DateTimeUtils.incrementDateValue(dv);
                    }
                    v = ValueTimestamp.fromDateValueAndNanos(dv, n2);
                }
            }
        }
        return v;
    }

    private long getLocalTimeNanos(CastDataProvider provider) {
        ValueTimeTimeZone ts = (ValueTimeTimeZone) this;
        int localOffset = provider.currentTimestamp().getTimeZoneOffsetSeconds();
        return DateTimeUtils.normalizeNanosOfDay(ts.getNanos() +
                (ts.getTimeZoneOffsetSeconds() - localOffset) * DateTimeUtils.NANOS_PER_DAY);
    }

    private ValueTimestampTimeZone convertToTimestampTimeZone(TypeInfo targetType, CastDataProvider provider,
            int conversionMode) {
        ValueTimestampTimeZone v;
        switch (getValueType()) {
        case TIMESTAMP_TZ:
            v = (ValueTimestampTimeZone) this;
            break;
        case TIME: {
            long dateValue = provider.currentTimestamp().getDateValue();
            long timeNanos = ((ValueTime) this).getNanos();
            v = ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos,
                    provider.currentTimeZone().getTimeZoneOffsetLocal(dateValue, timeNanos));
            break;
        }
        case TIME_TZ: {
            ValueTimeTimeZone t = (ValueTimeTimeZone) this;
            v = ValueTimestampTimeZone.fromDateValueAndNanos(provider.currentTimestamp().getDateValue(),
                    t.getNanos(), t.getTimeZoneOffsetSeconds());
            break;
        }
        case DATE: {
            long dateValue = ((ValueDate) this).getDateValue();
            // Scale is always 0
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, 0L,
                    provider.currentTimeZone().getTimeZoneOffsetLocal(dateValue, 0L));
        }
        case TIMESTAMP: {
            ValueTimestamp ts = (ValueTimestamp) this;
            long dateValue = ts.getDateValue();
            long timeNanos = ts.getTimeNanos();
            v = ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos,
                    provider.currentTimeZone().getTimeZoneOffsetLocal(dateValue, timeNanos));
            break;
        }
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            v = ValueTimestampTimeZone.parse(getString().trim(), provider);
            break;
        default:
            throw getDataConversionError(TIMESTAMP_TZ);
        }
        if (conversionMode != CONVERT_TO) {
            int targetScale = targetType.getScale();
            if (targetScale < ValueTimestamp.MAXIMUM_SCALE) {
                long dv = v.getDateValue();
                long n = v.getTimeNanos();
                long n2 = DateTimeUtils.convertScale(n, targetScale,
                        dv == DateTimeUtils.MAX_DATE_VALUE ? DateTimeUtils.NANOS_PER_DAY : Long.MAX_VALUE);
                if (n2 != n) {
                    if (n2 >= DateTimeUtils.NANOS_PER_DAY) {
                        n2 -= DateTimeUtils.NANOS_PER_DAY;
                        dv = DateTimeUtils.incrementDateValue(dv);
                    }
                    v = ValueTimestampTimeZone.fromDateValueAndNanos(dv, n2, v.getTimeZoneOffsetSeconds());
                }
            }
        }
        return v;
    }

    private ValueVarbinary convertToVarbinary(TypeInfo targetType, int conversionMode, Object column) {
        ValueVarbinary v;
        switch (getValueType()) {
        case VARBINARY:
            v = (ValueVarbinary) this;
            break;
        case JAVA_OBJECT:
        case BLOB:
        case GEOMETRY:
        case JSON:
            v = ValueVarbinary.getNoCopy(getBytesNoCopy());
            break;
        case UUID:
            v = ValueVarbinary.getNoCopy(getBytes());
            break;
        case TINYINT:
            v = ValueVarbinary.getNoCopy(new byte[] { getByte() });
            break;
        case SMALLINT: {
            int x = getShort();
            v = ValueVarbinary.getNoCopy(new byte[] { (byte) (x >> 8), (byte) x });
            break;
        }
        case INTEGER: {
            byte[] b = new byte[4];
            Bits.writeInt(b, 0, getInt());
            v = ValueVarbinary.getNoCopy(b);
            break;
        }
        case BIGINT: {
            byte[] b = new byte[8];
            Bits.writeLong(b, 0, getLong());
            v = ValueVarbinary.getNoCopy(b);
            break;
        }
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            v = ValueVarbinary.getNoCopy(getString().getBytes(StandardCharsets.UTF_8));
            break;
        default:
            throw getDataConversionError(VARBINARY);
        }
        if (conversionMode != CONVERT_TO) {
            byte[] value = v.getBytesNoCopy();
            int length = value.length;
            if (conversionMode == CAST_TO) {
                int p = MathUtils.convertLongToInt(targetType.getPrecision());
                if (length > p) {
                    v = ValueVarbinary.getNoCopy(Arrays.copyOf(value, p));
                }
            } else if (length > targetType.getPrecision()) {
                throw v.getValueTooLongException(targetType, column);
            }
        }
        return v;
    }

    private String convertToVarchar(TypeInfo targetType, int conversionMode, Object column) {
        switch (getValueType()) {
        case BLOB:
        case JAVA_OBJECT:
            throw getDataConversionError(targetType.getValueType());
        }
        String s = getString();
        if (conversionMode != CONVERT_TO) {
            int p = MathUtils.convertLongToInt(targetType.getPrecision());
            if (s.length() > p) {
                if (conversionMode != CAST_TO) {
                    throw getValueTooLongException(targetType, column);
                }
                s = s.substring(0, p);
            }
        }
        return s;
    }

    private ValueChar convertToChar(TypeInfo targetType, int conversionMode, Object column) {
        switch (getValueType()) {
        case BLOB:
        case JAVA_OBJECT:
            throw getDataConversionError(targetType.getValueType());
        }
        String s = getString();
        int p = MathUtils.convertLongToInt(targetType.getPrecision()), l = s.length();
        if (conversionMode == CAST_TO && l > p) {
            l = p;
        }
        while (l > 0 && s.charAt(l - 1) == ' ') {
            l--;
        }
        if (conversionMode == ASSIGN_TO && l > p) {
            throw getValueTooLongException(targetType, column);
        }
        return ValueChar.get(s.substring(0, l));
    }

    /**
     * Converts this value to a JAVA_OBJECT value. May not be called on a NULL
     * value.
     *
     * @param targetType
     *            the type of the returned value
     * @param conversionMode
     *            conversion mode
     * @param column
     *            the column (if any), used to improve the error message if
     *            conversion fails
     * @return the JAVA_OBJECT value
     */
    public final ValueJavaObject convertToJavaObject(TypeInfo targetType, int conversionMode, Object column) {
        ValueJavaObject v;
        switch (getValueType()) {
        case JAVA_OBJECT:
            v = (ValueJavaObject) this;
            break;
        case VARBINARY:
            v = ValueJavaObject.getNoCopy(getBytesNoCopy());
            break;
        default:
            throw getDataConversionError(JAVA_OBJECT);
        case NULL:
            throw DbException.throwInternalError();
        }
        if (conversionMode != CONVERT_TO && v.getBytesNoCopy().length > targetType.getPrecision()) {
            throw v.getValueTooLongException(targetType, column);
        }
        return v;
    }

    /**
     * Converts this value to an ENUM value. May not be called on a NULL value.
     *
     * @param extTypeInfo
     *            the extended data type information
     * @return the ENUM value
     */
    public final ValueEnum convertToEnum(ExtTypeInfoEnum extTypeInfo) {
        switch (getValueType()) {
        case ENUM: {
            ValueEnum v = (ValueEnum) this;
            if (extTypeInfo.equals(v.getEnumerators())) {
                return v;
            }
            return extTypeInfo.getValue(v.getString());
        }
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case NUMERIC:
            return extTypeInfo.getValue(getInt());
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            return extTypeInfo.getValue(getString());
        default:
            throw getDataConversionError(ENUM);
        case NULL:
            throw DbException.throwInternalError();
        }
    }

    private ValueLob convertToBlob(TypeInfo targetType, int conversionMode, Object column) {
        ValueLob v;
        switch (getValueType()) {
        case BLOB:
            v = (ValueLob) this;
            break;
        case CLOB: {
            v = (ValueLob) this;
            DataHandler handler = v.getDataHandler();
            if (handler != null) {
                v = handler.getLobStorage().createBlob(v.getInputStream(), -1);
            } else {
                v = ValueLob.createSmallLob(BLOB, v.getBytesNoCopy());
            }
            break;
        }
        case VARBINARY:
        case GEOMETRY:
        case JSON:
            v = ValueLob.createSmallLob(BLOB, getBytesNoCopy());
            break;
        case UUID:
            v = ValueLob.createSmallLob(BLOB, getBytes());
            break;
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            v = ValueLob.createSmallLob(BLOB, getString().getBytes(StandardCharsets.UTF_8));
            break;
        default:
            throw getDataConversionError(BLOB);
        }
        if (conversionMode != CONVERT_TO) {
            if (conversionMode == CAST_TO) {
                v = v.convertPrecision(targetType.getPrecision());
            } else if (v.getPrecision() > targetType.getPrecision()) {
                throw v.getValueTooLongException(targetType, column);
            }
        }
        return v;
    }

    private ValueLob convertToClob(TypeInfo targetType, int conversionMode, Object column) {
        ValueLob v;
        switch (getValueType()) {
        case CLOB:
            v = (ValueLob) this;
            break;
        case JAVA_OBJECT:
            throw getDataConversionError(targetType.getValueType());
        case BLOB: {
            v = (ValueLob) this;
            DataHandler handler = v.getDataHandler();
            if (handler != null) {
                v = handler.getLobStorage().createClob(v.getReader(), -1);
                break;
            }
            // Try to reuse the array, if possible
            byte[] small = v.getSmall();
            if (small != null) {
                byte[] bytes = new String(small, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8);
                if (Arrays.equals(bytes, small)) {
                    bytes = small;
                }
                v = ValueLob.createSmallLob(CLOB, bytes);
                break;
            }
        }
        //$FALL-THROUGH$
        default:
            v = ValueLob.createSmallLob(CLOB, getString().getBytes(StandardCharsets.UTF_8));
        } 
        if (conversionMode != CONVERT_TO) {
            if (conversionMode == CAST_TO) {
                v = v.convertPrecision(targetType.getPrecision());
            } else if (v.getPrecision() > targetType.getPrecision()) {
                throw v.getValueTooLongException(targetType, column);
            }
        }
        return v;
    }

    /**
     * Converts this value to a UUID value. May not be called on a NULL value.
     *
     * @return the UUID value
     */
    public final ValueUuid convertToUuid() {
        switch (getValueType()) {
        case UUID:
            return (ValueUuid) this;
        case VARBINARY:
            return ValueUuid.get(getBytesNoCopy());
        case JAVA_OBJECT:
            return JdbcUtils.deserializeUuid(getBytesNoCopy());
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            return ValueUuid.get(getString());
        default:
            throw getDataConversionError(UUID);
        case NULL:
            throw DbException.throwInternalError();
        }
    }

    /**
     * Converts this value to a GEOMETRY value. May not be called on a NULL
     * value.
     *
     * @param extTypeInfo
     *            the extended data type information, or null
     * @return the GEOMETRY value
     */
    public final ValueGeometry convertToGeometry(ExtTypeInfoGeometry extTypeInfo) {
        ValueGeometry result;
        switch (getValueType()) {
        case GEOMETRY:
            result = (ValueGeometry) this;
            break;
        case VARBINARY:
            result = ValueGeometry.getFromEWKB(getBytesNoCopy());
            break;
        case JSON: {
            int srid = 0;
            if (extTypeInfo != null) {
                Integer s = extTypeInfo.getSrid();
                if (s != null) {
                    srid = s;
                }
            }
            try {
                result = ValueGeometry.get(GeoJsonUtils.geoJsonToEwkb(getBytesNoCopy(), srid));
            } catch (RuntimeException ex) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, getTraceSQL());
            }
            break;
        }
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            result = ValueGeometry.get(getString());
            break;
        default:
            throw getDataConversionError(GEOMETRY);
        case NULL:
            throw DbException.throwInternalError();
        }
        if (extTypeInfo != null) {
            int type = extTypeInfo.getType();
            Integer srid = extTypeInfo.getSrid();
            if (type != 0 && result.getTypeAndDimensionSystem() != type || srid != null && result.getSRID() != srid) {
                throw DbException.get(ErrorCode.CHECK_CONSTRAINT_VIOLATED_1,
                        ExtTypeInfoGeometry.toSQL(result.getTypeAndDimensionSystem(), result.getSRID())
                                + " <> " + extTypeInfo.toString());
            }
        }
        return result;
    }

    private ValueInterval convertToIntervalYearMonth(TypeInfo targetType, int conversionMode, Object column) {
        ValueInterval v = convertToIntervalYearMonth(targetType.getValueType(), column);
        if (conversionMode != CONVERT_TO) {
            if (!v.checkPrecision(targetType.getPrecision())) {
                throw v.getValueTooLongException(targetType, column);
            }
        }
        return v;
    }

    private ValueInterval convertToIntervalYearMonth(int targetType, Object column) {
        long leading;
        switch (getValueType()) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
            leading = getInt();
            break;
        case BIGINT:
            leading = getLong();
            break;
        case REAL:
        case DOUBLE:
            if (targetType == INTERVAL_YEAR_TO_MONTH) {
                return IntervalUtils.intervalFromAbsolute(IntervalQualifier.YEAR_TO_MONTH, getBigDecimal()
                        .multiply(BigDecimal.valueOf(12)).setScale(0, RoundingMode.HALF_UP).toBigInteger());
            }
            leading = convertToLong(getDouble(), column);
            break;
        case NUMERIC:
            if (targetType == INTERVAL_YEAR_TO_MONTH) {
                return IntervalUtils.intervalFromAbsolute(IntervalQualifier.YEAR_TO_MONTH, getBigDecimal()
                        .multiply(BigDecimal.valueOf(12)).setScale(0, RoundingMode.HALF_UP).toBigInteger());
            }
            leading = convertToLong(getBigDecimal(), column);
            break;
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR: {
            String s = getString();
            try {
                return (ValueInterval) IntervalUtils
                        .parseFormattedInterval(IntervalQualifier.valueOf(targetType - INTERVAL_YEAR), s)
                        .convertTo(targetType);
            } catch (Exception e) {
                throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, e, "INTERVAL", s);
            }
        }
        case INTERVAL_YEAR:
        case INTERVAL_MONTH:
        case INTERVAL_YEAR_TO_MONTH:
            return IntervalUtils.intervalFromAbsolute(IntervalQualifier.valueOf(targetType - INTERVAL_YEAR),
                    IntervalUtils.intervalToAbsolute((ValueInterval) this));
        default:
            throw getDataConversionError(targetType);
        }
        boolean negative = false;
        if (leading < 0) {
            negative = true;
            leading = -leading;
        }
        return ValueInterval.from(IntervalQualifier.valueOf(targetType - INTERVAL_YEAR), negative, leading,
                0L);
    }

    private ValueInterval convertToIntervalDayTime(TypeInfo targetType, int conversionMode, Object column) {
        ValueInterval v = convertToIntervalDayTime(targetType.getValueType(), column);
        if (conversionMode != CONVERT_TO) {
            v = v.setPrecisionAndScale(targetType, column);
        }
        return v;
    }

    private ValueInterval convertToIntervalDayTime(int targetType, Object column) {
        long leading;
        switch (getValueType()) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
            leading = getInt();
            break;
        case BIGINT:
            leading = getLong();
            break;
        case REAL:
        case DOUBLE:
            if (targetType > INTERVAL_MINUTE) {
                return convertToIntervalDayTime(getBigDecimal(), targetType);
            }
            leading = convertToLong(getDouble(), column);
            break;
        case NUMERIC:
            if (targetType > INTERVAL_MINUTE) {
                return convertToIntervalDayTime(getBigDecimal(), targetType);
            }
            leading = convertToLong(getBigDecimal(), column);
            break;
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR: {
            String s = getString();
            try {
                return (ValueInterval) IntervalUtils
                        .parseFormattedInterval(IntervalQualifier.valueOf(targetType - INTERVAL_YEAR), s)
                        .convertTo(targetType);
            } catch (Exception e) {
                throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, e, "INTERVAL", s);
            }
        }
        case INTERVAL_DAY:
        case INTERVAL_HOUR:
        case INTERVAL_MINUTE:
        case INTERVAL_SECOND:
        case INTERVAL_DAY_TO_HOUR:
        case INTERVAL_DAY_TO_MINUTE:
        case INTERVAL_DAY_TO_SECOND:
        case INTERVAL_HOUR_TO_MINUTE:
        case INTERVAL_HOUR_TO_SECOND:
        case INTERVAL_MINUTE_TO_SECOND:
            return IntervalUtils.intervalFromAbsolute(IntervalQualifier.valueOf(targetType - INTERVAL_YEAR),
                    IntervalUtils.intervalToAbsolute((ValueInterval) this));
        default:
            throw getDataConversionError(targetType);
        }
        boolean negative = false;
        if (leading < 0) {
            negative = true;
            leading = -leading;
        }
        return ValueInterval.from(IntervalQualifier.valueOf(targetType - INTERVAL_YEAR), negative, leading,
                0L);
    }

    private ValueInterval convertToIntervalDayTime(BigDecimal bigDecimal, int targetType) {
        long multiplier;
        switch (targetType) {
        case INTERVAL_SECOND:
            multiplier = DateTimeUtils.NANOS_PER_SECOND;
            break;
        case INTERVAL_DAY_TO_HOUR:
        case INTERVAL_DAY_TO_MINUTE:
        case INTERVAL_DAY_TO_SECOND:
            multiplier = DateTimeUtils.NANOS_PER_DAY;
            break;
        case INTERVAL_HOUR_TO_MINUTE:
        case INTERVAL_HOUR_TO_SECOND:
            multiplier = DateTimeUtils.NANOS_PER_HOUR;
            break;
        case INTERVAL_MINUTE_TO_SECOND:
            multiplier = DateTimeUtils.NANOS_PER_MINUTE;
            break;
        default:
            throw getDataConversionError(targetType);
        }
        return IntervalUtils.intervalFromAbsolute(IntervalQualifier.valueOf(targetType - INTERVAL_YEAR),
                bigDecimal.multiply(BigDecimal.valueOf(multiplier)).setScale(0, RoundingMode.HALF_UP).toBigInteger());
    }

    private Value convertToJson() {
        switch (getValueType()) {
        case JSON:
            return this;
        case BOOLEAN:
            return ValueJson.get(getBoolean());
        case TINYINT:
        case SMALLINT:
        case INTEGER:
            return ValueJson.get(getInt());
        case BIGINT:
            return ValueJson.get(getLong());
        case REAL:
        case DOUBLE:
        case NUMERIC:
            return ValueJson.get(getBigDecimal());
        case VARBINARY:
            return ValueJson.fromJson(getBytesNoCopy());
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            return ValueJson.get(getString());
        case GEOMETRY: {
            ValueGeometry vg = (ValueGeometry) this;
            return ValueJson.getInternal(GeoJsonUtils.ewkbToGeoJson(vg.getBytesNoCopy(), vg.getDimensionSystem()));
        }
        default:
            throw getDataConversionError(JSON);
        }
    }

    private ValueArray convertToArray(TypeInfo targetType, CastDataProvider provider, int conversionMode,
            Object column) {
        ExtTypeInfoArray extTypeInfo = (ExtTypeInfoArray) targetType.getExtTypeInfo();
        int valueType = getValueType();
        ValueArray v;
        if (valueType == ARRAY) {
            v = (ValueArray) this;
        } else {
            Value[] a;
            switch (valueType) {
            case ROW:
                a = ((ValueRow) this).getList();
                break;
            case BLOB:
                a = new Value[] { ValueVarbinary.get(getBytesNoCopy()) };
                break;
            case CLOB:
            case RESULT_SET:
                a = new Value[] { ValueVarchar.get(getString()) };
                break;
            default:
                a = new Value[] { this };
            }
            v = ValueArray.get(a);
        }
        if (extTypeInfo != null) {
            TypeInfo componentType = extTypeInfo.getComponentType();
            Value[] values = v.getList();
            int length = values.length;
            loop: for (int i = 0; i < length; i++) {
                Value v1 = values[i];
                Value v2 = v1.convertTo(componentType, provider, conversionMode, column);
                if (v1 != v2) {
                    Value[] newValues = new Value[length];
                    System.arraycopy(values, 0, newValues, 0, i);
                    newValues[i] = v2;
                    while (++i < length) {
                        newValues[i] = values[i].convertTo(componentType, provider, conversionMode, column);
                    }
                    v = ValueArray.get(newValues);
                    break loop;
                }
            }
        }
        if (conversionMode != CONVERT_TO) {
            Value[] values = v.getList();
            int cardinality = values.length;
            if (conversionMode == CAST_TO) {
                int p = MathUtils.convertLongToInt(targetType.getPrecision());
                if (cardinality > p) {
                    v = ValueArray.get(v.getComponentType(), Arrays.copyOf(values, p));
                }
            } else if (cardinality > targetType.getPrecision()) {
                throw v.getValueTooLongException(targetType, column);
            }
        }
        return v;
    }

    private Value convertToRow() {
        Value[] a;
        switch (getValueType()) {
        case ROW:
            return this;
        case RESULT_SET:
            ResultInterface result = getResult();
            if (result.hasNext()) {
                a = result.currentRow();
                if (result.hasNext()) {
                    throw DbException.get(ErrorCode.SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW);
                }
            } else {
                return ValueNull.INSTANCE;
            }
            break;
        default:
            a = new Value[] { this };
            break;
        }
        return ValueRow.get(a);
    }

    /**
     * Converts this value to a RESULT_SET value. May not be called on a NULL
     * value.
     *
     * @return the RESULT_SET value
     */
    public final ValueResultSet convertToResultSet() {
        SimpleResult result;
        switch (getValueType()) {
        case RESULT_SET:
            return (ValueResultSet) this;
        case ROW:
            result = new SimpleResult();
            Value[] values = ((ValueRow) this).getList();
            for (int i = 0; i < values.length;) {
                Value v = values[i++];
                String columnName = "C" + i;
                result.addColumn(columnName, columnName, v.getType());
            }
            result.addRow(values);
            break;
        default:
            result = new SimpleResult();
            result.addColumn("X", "X", getType());
            result.addRow(this);
            break;
        case NULL:
            throw DbException.throwInternalError();
        }
        return ValueResultSet.get(result);
    }

    /**
     * Creates new instance of the DbException for data conversion error.
     *
     * @param targetType Target data type.
     * @return instance of the DbException.
     */
    final DbException getDataConversionError(int targetType) {
        DataType from = DataType.getDataType(getValueType());
        DataType to = DataType.getDataType(targetType);
        throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, (from != null ? from.name : "type=" + getValueType())
                + " to " + (to != null ? to.name : "type=" + targetType));
    }

    final DbException getValueTooLongException(TypeInfo targetType, Object column) {
        String s = getTraceSQL();
        if (s.length() > 127) {
            s = s.substring(0, 128) + "...";
        }
        StringBuilder builder = new StringBuilder();
        if (column != null) {
            builder.append(column).append(' ');
        }
        targetType.getSQL(builder);
        return DbException.get(ErrorCode.VALUE_TOO_LONG_2, builder.toString(),
                s + " (" + getType().getPrecision() + ')');
    }

    /**
     * Compare this value against another value given that the values are of the
     * same data type.
     *
     * @param v the other value
     * @param mode the compare mode
     * @param provider the cast information provider
     * @return 0 if both values are equal, -1 if the other value is smaller, and
     *         1 otherwise
     */
    public abstract int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider);

    /**
     * Compare this value against another value using the specified compare
     * mode.
     *
     * @param v the other value
     * @param provider the cast information provider
     * @param compareMode the compare mode
     * @return 0 if both values are equal, -1 if this value is smaller, and
     *         1 otherwise
     */
    public final int compareTo(Value v, CastDataProvider provider, CompareMode compareMode) {
        if (this == v) {
            return 0;
        }
        if (this == ValueNull.INSTANCE) {
            return -1;
        } else if (v == ValueNull.INSTANCE) {
            return 1;
        }
        return compareToNotNullable(v, provider, compareMode);
    }

    private int compareToNotNullable(Value v, CastDataProvider provider, CompareMode compareMode) {
        Value l = this;
        int leftType = l.getValueType();
        int rightType = v.getValueType();
        if (leftType != rightType || leftType == ENUM) {
            int dataType = getHigherOrder(leftType, rightType);
            if (dataType == ENUM) {
                ExtTypeInfoEnum enumerators = ExtTypeInfoEnum.getEnumeratorsForBinaryOperation(l, v);
                l = l.convertToEnum(enumerators);
                v = v.convertToEnum(enumerators);
            } else {
                l = l.convertTo(dataType, provider);
                v = v.convertTo(dataType, provider);
            }
        }
        return l.compareTypeSafe(v, compareMode, provider);
    }

    /**
     * Compare this value against another value using the specified compare
     * mode.
     *
     * @param v the other value
     * @param forEquality perform only check for equality
     * @param provider the cast information provider
     * @param compareMode the compare mode
     * @return 0 if both values are equal, -1 if this value is smaller, 1
     *         if other value is larger, {@link Integer#MIN_VALUE} if order is
     *         not defined due to NULL comparison
     */
    public int compareWithNull(Value v, boolean forEquality, CastDataProvider provider,
            CompareMode compareMode) {
        if (this == ValueNull.INSTANCE || v == ValueNull.INSTANCE) {
            return Integer.MIN_VALUE;
        }
        return compareToNotNullable(v, provider, compareMode);
    }

    /**
     * Returns true if this value is NULL or contains NULL value.
     *
     * @return true if this value is NULL or contains NULL value
     */
    public boolean containsNull() {
        return false;
    }

    private static byte convertToByte(long x, Object column) {
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE) {
            throw DbException.get(
                    ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_2, Long.toString(x), getColumnName(column));
        }
        return (byte) x;
    }

    private static short convertToShort(long x, Object column) {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE) {
            throw DbException.get(
                    ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_2, Long.toString(x), getColumnName(column));
        }
        return (short) x;
    }

    /**
     * Convert to integer, throwing exception if out of range.
     *
     * @param x integer value.
     * @param column Column info.
     * @return x
     */
    public static int convertToInt(long x, Object column) {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            throw DbException.get(
                    ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_2, Long.toString(x), getColumnName(column));
        }
        return (int) x;
    }

    private static long convertToLong(double x, Object column) {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE) {
            // TODO document that +Infinity, -Infinity throw an exception and
            // NaN returns 0
            throw DbException.get(
                    ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_2, Double.toString(x), getColumnName(column));
        }
        return Math.round(x);
    }

    private static long convertToLong(BigDecimal x, Object column) {
        if (x.compareTo(MAX_LONG_DECIMAL) > 0 ||
                x.compareTo(MIN_LONG_DECIMAL) < 0) {
            throw DbException.get(
                    ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_2, x.toString(), getColumnName(column));
        }
        return x.setScale(0, RoundingMode.HALF_UP).longValue();
    }

    private static String getColumnName(Object column) {
        return column == null ? "" : column.toString();
    }

    @Override
    public String toString() {
        return getTraceSQL();
    }

    /**
     * Create an exception meaning the specified operation is not supported for
     * this data type.
     *
     * @param op the operation
     * @return the exception
     */
    protected final DbException getUnsupportedExceptionForOperation(String op) {
        return DbException.getUnsupportedException(
                DataType.getDataType(getValueType()).name + " " + op);
    }

    /**
     * Returns result for result set value, or single-row result with this value
     * in column X for other values.
     *
     * @return result
     */
    public ResultInterface getResult() {
        SimpleResult rs = new SimpleResult();
        rs.addColumn("X", "X", getType());
        rs.addRow(this);
        return rs;
    }

}
