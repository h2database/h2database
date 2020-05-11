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
    public static final int NULL = UNKNOWN + 1;

    /**
     * The value type for CHAR values.
     */
    public static final int CHAR = NULL + 1;

    /**
     * The value type for CHARACTER VARYING values.
     */
    public static final int VARCHAR = CHAR + 1;

    /**
     * The value type for CLOB values.
     */
    public static final int CLOB = VARCHAR + 1;

    /**
     * The value type for VARCHAR_IGNORECASE values.
     */
    public static final int VARCHAR_IGNORECASE = CLOB + 1;

    /**
     * The value type for BINARY values.
     */
    public static final int BINARY = VARCHAR_IGNORECASE + 1;

    /**
     * The value type for BINARY VARYING values.
     */
    public static final int VARBINARY = BINARY + 1;

    /**
     * The value type for BLOB values.
     */
    public static final int BLOB = VARBINARY + 1;

    /**
     * The value type for BOOLEAN values.
     */
    public static final int BOOLEAN = BLOB + 1;

    /**
     * The value type for TINYINT values.
     */
    public static final int TINYINT = BOOLEAN + 1;

    /**
     * The value type for SMALLINT values.
     */
    public static final int SMALLINT = TINYINT + 1;

    /**
     * The value type for INTEGER values.
     */
    public static final int INTEGER = SMALLINT + 1;

    /**
     * The value type for BIGINT values.
     */
    public static final int BIGINT = INTEGER + 1;

    /**
     * The value type for NUMERIC values.
     */
    public static final int NUMERIC = BIGINT + 1;

    /**
     * The value type for REAL values.
     */
    public static final int REAL = NUMERIC + 1;

    /**
     * The value type for DOUBLE PRECISION values.
     */
    public static final int DOUBLE = REAL + 1;

    /**
     * The value type for DATE values.
     */
    public static final int DATE = DOUBLE + 1;

    /**
     * The value type for TIME values.
     */
    public static final int TIME = DATE + 1;

    /**
     * The value type for TIME WITH TIME ZONE values.
     */
    public static final int TIME_TZ = TIME + 1;

    /**
     * The value type for TIMESTAMP values.
     */
    public static final int TIMESTAMP = TIME_TZ + 1;

    /**
     * The value type for TIMESTAMP WITH TIME ZONE values.
     */
    public static final int TIMESTAMP_TZ = TIMESTAMP + 1;

    /**
     * The value type for {@code INTERVAL YEAR} values.
     */
    public static final int INTERVAL_YEAR = TIMESTAMP_TZ + 1;

    /**
     * The value type for {@code INTERVAL MONTH} values.
     */
    public static final int INTERVAL_MONTH = INTERVAL_YEAR + 1;

    /**
     * The value type for {@code INTERVAL DAY} values.
     */
    public static final int INTERVAL_DAY = INTERVAL_MONTH + 1;

    /**
     * The value type for {@code INTERVAL HOUR} values.
     */
    public static final int INTERVAL_HOUR = INTERVAL_DAY + 1;

    /**
     * The value type for {@code INTERVAL MINUTE} values.
     */
    public static final int INTERVAL_MINUTE = INTERVAL_HOUR + 1;

    /**
     * The value type for {@code INTERVAL SECOND} values.
     */
    public static final int INTERVAL_SECOND = INTERVAL_MINUTE + 1;

    /**
     * The value type for {@code INTERVAL YEAR TO MONTH} values.
     */
    public static final int INTERVAL_YEAR_TO_MONTH = INTERVAL_SECOND + 1;

    /**
     * The value type for {@code INTERVAL DAY TO HOUR} values.
     */
    public static final int INTERVAL_DAY_TO_HOUR = INTERVAL_YEAR_TO_MONTH + 1;

    /**
     * The value type for {@code INTERVAL DAY TO MINUTE} values.
     */
    public static final int INTERVAL_DAY_TO_MINUTE = INTERVAL_DAY_TO_HOUR + 1;

    /**
     * The value type for {@code INTERVAL DAY TO SECOND} values.
     */
    public static final int INTERVAL_DAY_TO_SECOND = INTERVAL_DAY_TO_MINUTE + 1;

    /**
     * The value type for {@code INTERVAL HOUR TO MINUTE} values.
     */
    public static final int INTERVAL_HOUR_TO_MINUTE = INTERVAL_DAY_TO_SECOND + 1;

    /**
     * The value type for {@code INTERVAL HOUR TO SECOND} values.
     */
    public static final int INTERVAL_HOUR_TO_SECOND = INTERVAL_HOUR_TO_MINUTE + 1;

    /**
     * The value type for {@code INTERVAL MINUTE TO SECOND} values.
     */
    public static final int INTERVAL_MINUTE_TO_SECOND = INTERVAL_HOUR_TO_SECOND + 1;

    /**
     * The value type for JAVA_OBJECT values.
     */
    public static final int JAVA_OBJECT = INTERVAL_MINUTE_TO_SECOND + 1;

    /**
     * The value type for ENUM values.
     */
    public static final int ENUM = JAVA_OBJECT + 1;

    /**
     * The value type for string values with a fixed size.
     */
    public static final int GEOMETRY = ENUM + 1;

    /**
     * The value type for JSON values.
     */
    public static final int JSON = GEOMETRY + 1;

    /**
     * The value type for UUID values.
     */
    public static final int UUID = JSON + 1;

    /**
     * The value type for ARRAY values.
     */
    public static final int ARRAY = UUID + 1;

    /**
     * The value type for ROW values.
     */
    public static final int ROW = ARRAY + 1;

    /**
     * The value type for RESULT_SET values.
     */
    public static final int RESULT_SET = ROW + 1;

    /**
     * The number of value types.
     */
    public static final int TYPE_COUNT = RESULT_SET + 1;

    /**
     * Group for untyped NULL data type.
     */
    private static final int GROUP_NULL = 0;

    /**
     * Group for character string data types.
     */
    private static final int GROUP_CHARACTER_STRING = GROUP_NULL + 1;

    /**
     * Group for binary string data types.
     */
    private static final int GROUP_BINARY_STRING = GROUP_CHARACTER_STRING + 1;

    /**
     * Group for BINARY data type.
     */
    private static final int GROUP_BOOLEAN = GROUP_BINARY_STRING + 1;

    /**
     * Group for numeric data types.
     */
    private static final int GROUP_NUMERIC = GROUP_BOOLEAN + 1;

    /**
     * Group for datetime data types.
     */
    private static final int GROUP_DATETIME = GROUP_NUMERIC + 1;

    /**
     * Group for year-month interval data types.
     */
    private static final int GROUP_INTERVAL_YM = GROUP_DATETIME + 1;

    /**
     * Group for day-time interval data types.
     */
    private static final int GROUP_INTERVAL_DT = GROUP_INTERVAL_YM + 1;

    /**
     * Group for other data types (JAVA_OBJECT, UUID, GEOMETRY, ENUM, JSON).
     */
    private static final int GROUP_OTHER = GROUP_INTERVAL_DT + 1;

    /**
     * Group for collection data types (ARRAY, RESULT_SET, ROW).
     */
    private static final int GROUP_COLLECTION = GROUP_OTHER + 1;

    private static final byte GROUPS[] = {
            // NULL
            GROUP_NULL,
            // CHAR, VARCHAR, CLOB, VARCHAR_IGNORECASE
            GROUP_CHARACTER_STRING, GROUP_CHARACTER_STRING, GROUP_CHARACTER_STRING, GROUP_CHARACTER_STRING,
            // BINARY, VARBINARY, BLOB
            GROUP_BINARY_STRING, GROUP_BINARY_STRING, GROUP_BINARY_STRING,
            // BOOLEAN
            GROUP_BOOLEAN,
            // TINYINT, SMALLINT, INTEGER, BIGINT, NUMERIC, REAL, DOUBLE
            GROUP_NUMERIC, GROUP_NUMERIC, GROUP_NUMERIC, GROUP_NUMERIC, GROUP_NUMERIC, GROUP_NUMERIC, GROUP_NUMERIC,
            // DATE, TIME, TIME_TZ, TIMESTAMP, TIMESTAMP_TZ
            GROUP_DATETIME, GROUP_DATETIME, GROUP_DATETIME, GROUP_DATETIME, GROUP_DATETIME,
            // INTERVAL_YEAR, INTERVAL_MONTH
            GROUP_INTERVAL_YM, GROUP_INTERVAL_YM,
            // INTERVAL_DAY, INTERVAL_HOUR, INTERVAL_MINUTE, INTERVAL_SECOND
            GROUP_INTERVAL_DT, GROUP_INTERVAL_DT, GROUP_INTERVAL_DT, GROUP_INTERVAL_DT,
            // INTERVAL_YEAR_TO_MONTH
            GROUP_INTERVAL_YM,
            // INTERVAL_DAY_TO_HOUR, INTERVAL_DAY_TO_MINUTE,
            // INTERVAL_DAY_TO_SECOND, INTERVAL_HOUR_TO_MINUTE,
            // INTERVAL_HOUR_TO_SECOND, INTERVAL_MINUTE_TO_SECOND
            GROUP_INTERVAL_DT, GROUP_INTERVAL_DT, GROUP_INTERVAL_DT, GROUP_INTERVAL_DT, GROUP_INTERVAL_DT,
            GROUP_INTERVAL_DT,
            // JAVA_OBJECT, ENUM, GEOMETRY, JSON, UUID
            GROUP_OTHER, GROUP_OTHER, GROUP_OTHER, GROUP_OTHER, GROUP_OTHER,
            // ARRAY, ROW, RESULT_SET
            GROUP_COLLECTION, GROUP_COLLECTION, GROUP_COLLECTION,
            //
    };

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
     * Get the higher value order type of two value types. If values need to be
     * converted to match the other operands value type, the value with the
     * lower order is converted to the value with the higher order.
     *
     * @param t1 the first value type
     * @param t2 the second value type
     * @return the higher value type of the two
     */
    public static int getHigherOrder(int t1, int t2) {
        if (t1 == t2) {
            if (t1 == UNKNOWN) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?, ?");
            }
            return t1;
        }
        if (t1 < t2) {
            int t = t1;
            t1 = t2;
            t2 = t;
        }
        if (t1 == UNKNOWN) {
            if (t2 == NULL) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?, NULL");
            }
            return t2;
        } else if (t2 == UNKNOWN) {
            if (t1 == NULL) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "NULL, ?");
            }
            return t1;
        }
        if (t2 == NULL) {
            return t1;
        }
        int g1 = GROUPS[t1], g2 = GROUPS[t2];
        switch (g1) {
        case GROUP_BOOLEAN:
            if (g2 == GROUP_BINARY_STRING) {
                throw getDataTypeCombinationException(BOOLEAN, t2);
            }
            break;
        case GROUP_NUMERIC:
            return getHigherNumeric(t1, t2, g2);
        case GROUP_DATETIME:
            return getHigherDateTime(t1, t2, g2);
        case GROUP_INTERVAL_YM:
            return getHigherIntervalYearMonth(t1, t2, g2);
        case GROUP_INTERVAL_DT:
            return getHigherIntervalDayTime(t1, t2, g2);
        case GROUP_OTHER:
            return getHigherOther(t1, t2, g2);
        }
        return t1;
    }

    private static int getHigherNumeric(int t1, int t2, int g2) {
        if (g2 == GROUP_NUMERIC) {
            if (t1 == NUMERIC || t2 == NUMERIC) {
                return NUMERIC;
            }
            if (t1 == REAL) {
                if (t2 == INTEGER) {
                    return DOUBLE;
                }
                if (t2 == BIGINT) {
                    return NUMERIC;
                }
            } else if (t1 == DOUBLE && t2 == BIGINT) {
                return NUMERIC;
            }
        } else if (g2 == GROUP_BINARY_STRING) {
            throw getDataTypeCombinationException(t1, t2);
        }
        return t1;
    }

    private static int getHigherDateTime(int t1, int t2, int g2) {
        if (g2 == GROUP_CHARACTER_STRING) {
            return t1;
        }
        if (g2 != GROUP_DATETIME) {
            throw getDataTypeCombinationException(t1, t2);
        }
        switch (t1) {
        case TIME:
            if (t2 == DATE) {
                return TIMESTAMP;
            }
            break;
        case TIME_TZ:
            if (t2 == DATE) {
                return TIMESTAMP_TZ;
            }
            break;
        case TIMESTAMP:
            if (t2 == TIME_TZ) {
                return TIMESTAMP_TZ;
            }
        }
        return t1;
    }

    private static int getHigherIntervalYearMonth(int t1, int t2, int g2) {
        switch (g2) {
        case GROUP_INTERVAL_YM:
            if (t1 == INTERVAL_MONTH && t2 == INTERVAL_YEAR) {
                return INTERVAL_YEAR_TO_MONTH;
            }
            //$FALL-THROUGH$
        case GROUP_CHARACTER_STRING:
        case GROUP_NUMERIC:
            return t1;
        default:
            throw getDataTypeCombinationException(t1, t2);
        }
    }

    private static int getHigherIntervalDayTime(int t1, int t2, int g2) {
        switch (g2) {
        case GROUP_INTERVAL_DT:
            break;
        case GROUP_CHARACTER_STRING:
        case GROUP_NUMERIC:
            return t1;
        default:
            throw getDataTypeCombinationException(t1, t2);
        }
        switch (t1) {
        case INTERVAL_HOUR:
            return INTERVAL_DAY_TO_HOUR;
        case INTERVAL_MINUTE:
            if (t2 == INTERVAL_DAY) {
                return INTERVAL_DAY_TO_MINUTE;
            }
            return INTERVAL_HOUR_TO_MINUTE;
        case INTERVAL_SECOND:
            if (t2 == INTERVAL_DAY) {
                return INTERVAL_DAY_TO_SECOND;
            }
            if (t2 == INTERVAL_HOUR) {
                return INTERVAL_HOUR_TO_SECOND;
            }
            return INTERVAL_MINUTE_TO_SECOND;
        case INTERVAL_DAY_TO_HOUR:
            if (t2 == INTERVAL_MINUTE) {
                return INTERVAL_DAY_TO_MINUTE;
            }
            if (t2 == INTERVAL_SECOND) {
                return INTERVAL_DAY_TO_SECOND;
            }
            break;
        case INTERVAL_DAY_TO_MINUTE:
            if (t1 == INTERVAL_SECOND) {
                return INTERVAL_DAY_TO_SECOND;
            }
            break;
        case INTERVAL_HOUR_TO_MINUTE:
            switch (t2) {
            case INTERVAL_DAY:
            case INTERVAL_DAY_TO_HOUR:
            case INTERVAL_DAY_TO_MINUTE:
                return INTERVAL_DAY_TO_MINUTE;
            case INTERVAL_SECOND:
                return INTERVAL_HOUR_TO_SECOND;
            case INTERVAL_DAY_TO_SECOND:
                return INTERVAL_DAY_TO_SECOND;
            }
            break;
        case INTERVAL_HOUR_TO_SECOND:
            switch (t2) {
            case INTERVAL_DAY:
            case INTERVAL_DAY_TO_HOUR:
            case INTERVAL_DAY_TO_MINUTE:
            case INTERVAL_DAY_TO_SECOND:
                return INTERVAL_DAY_TO_SECOND;
            }
            break;
        case INTERVAL_MINUTE_TO_SECOND:
            switch (t2) {
            case INTERVAL_DAY:
            case INTERVAL_DAY_TO_HOUR:
            case INTERVAL_DAY_TO_MINUTE:
            case INTERVAL_DAY_TO_SECOND:
                return INTERVAL_DAY_TO_SECOND;
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_TO_MINUTE:
            case INTERVAL_HOUR_TO_SECOND:
                return INTERVAL_HOUR_TO_SECOND;
            }
        }
        return t1;
    }

    private static int getHigherOther(int t1, int t2, int g2) {
        switch (t1) {
        case JAVA_OBJECT:
            if (g2 != GROUP_BINARY_STRING) {
                throw getDataTypeCombinationException(t1, t2);
            }
            break;
        case ENUM:
            if (g2 != GROUP_CHARACTER_STRING && (g2 != GROUP_NUMERIC || t2 > INTEGER)) {
                throw getDataTypeCombinationException(t1, t2);
            }
            break;
        case GEOMETRY:
            if (g2 != GROUP_CHARACTER_STRING && g2 != GROUP_BINARY_STRING) {
                throw getDataTypeCombinationException(t1, t2);
            }
            break;
        case JSON:
            switch (g2) {
            case GROUP_DATETIME:
            case GROUP_INTERVAL_YM:
            case GROUP_INTERVAL_DT:
            case GROUP_OTHER:
                throw getDataTypeCombinationException(t1, t2);
            }
            break;
        case UUID:
            switch (g2) {
            case GROUP_CHARACTER_STRING:
            case GROUP_BINARY_STRING:
                break;
            case GROUP_OTHER:
                if (t2 == JAVA_OBJECT) {
                    break;
                }
                //$FALL-THROUGH$
            default:
                throw getDataTypeCombinationException(t1, t2);
            }
        }
        return t1;
    }

    private static DbException getDataTypeCombinationException(int t1, int t2) {
        return DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1,
                DataType.getDataType(t1).name + ", " + DataType.getDataType(t2).name);
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
        case CHAR:
            return convertToChar(targetType, conversionMode, column);
        case VARCHAR:
            return ValueVarchar.get(convertToVarchar(targetType, conversionMode, column));
        case CLOB:
            return convertToClob(targetType, conversionMode, column);
        case VARCHAR_IGNORECASE:
            return ValueVarcharIgnoreCase.get(convertToVarchar(targetType, conversionMode, column));
        case BINARY:
            return convertToBinary(targetType, conversionMode, column);
        case VARBINARY:
            return convertToVarbinary(targetType, conversionMode, column);
        case BLOB:
            return convertToBlob(targetType, conversionMode, column);
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
        case REAL:
            return convertToReal();
        case DOUBLE:
            return convertToDouble();
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
        case JAVA_OBJECT:
            return convertToJavaObject(targetType, conversionMode, column);
        case ENUM:
            return convertToEnum((ExtTypeInfoEnum) targetType.getExtTypeInfo());
        case GEOMETRY:
            return convertToGeometry((ExtTypeInfoGeometry) targetType.getExtTypeInfo());
        case JSON:
            return convertToJson();
        case UUID:
            return convertToUuid();
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
        case BINARY:
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
            int p = MathUtils.convertLongToInt(targetType.getPrecision());
            if (conversionMode == CAST_TO) {
                if (length > p) {
                    v = ValueVarbinary.getNoCopy(Arrays.copyOf(value, p));
                }
            } else if (length > p) {
                throw v.getValueTooLongException(targetType, column);
            }
        }
        return v;
    }

    private ValueBinary convertToBinary(TypeInfo targetType, int conversionMode, Object column) {
        ValueBinary v;
        switch (getValueType()) {
        case BINARY:
            v = (ValueBinary) this;
            break;
        case VARBINARY:
        case JAVA_OBJECT:
        case BLOB:
        case GEOMETRY:
        case JSON:
            v = ValueBinary.getNoCopy(getBytesNoCopy());
            break;
        case UUID:
            v = ValueBinary.getNoCopy(getBytes());
            break;
        case TINYINT:
            v = ValueBinary.getNoCopy(new byte[] { getByte() });
            break;
        case SMALLINT: {
            int x = getShort();
            v = ValueBinary.getNoCopy(new byte[] { (byte) (x >> 8), (byte) x });
            break;
        }
        case INTEGER: {
            byte[] b = new byte[4];
            Bits.writeInt(b, 0, getInt());
            v = ValueBinary.getNoCopy(b);
            break;
        }
        case BIGINT: {
            byte[] b = new byte[8];
            Bits.writeLong(b, 0, getLong());
            v = ValueBinary.getNoCopy(b);
            break;
        }
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            v = ValueBinary.getNoCopy(getString().getBytes(StandardCharsets.UTF_8));
            break;
        default:
            throw getDataConversionError(VARBINARY);
        }
        if (conversionMode != CONVERT_TO) {
            byte[] value = v.getBytesNoCopy();
            int length = value.length;
            int p = MathUtils.convertLongToInt(targetType.getPrecision());
            if (length != p) {
                if (conversionMode == ASSIGN_TO && length > p) {
                    throw v.getValueTooLongException(targetType, column);
                }
                v = ValueBinary.getNoCopy(Arrays.copyOf(value, p));
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
            if (v instanceof ValueLobInMemory) {
                v = ValueLobInMemory.createSmallLob(BLOB, v.getBytesNoCopy());
            } else if (v instanceof ValueLobDatabase) {
                v = ((ValueLobDatabase) v).getDataHandler().getLobStorage().createBlob(v.getInputStream(), -1);
            }
            break;
        }
        case VARBINARY:
        case GEOMETRY:
        case JSON:
            v = ValueLobInMemory.createSmallLob(BLOB, getBytesNoCopy());
            break;
        case UUID:
            v = ValueLobInMemory.createSmallLob(BLOB, getBytes());
            break;
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            v = ValueLobInMemory.createSmallLob(BLOB, getString().getBytes(StandardCharsets.UTF_8));
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
            // Try to reuse the array, if possible
            if (v instanceof ValueLobInMemory) {
                byte[] small = ((ValueLobInMemory)v).getSmall();
                byte[] bytes = new String(small, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8);
                if (Arrays.equals(bytes, small)) {
                    bytes = small;
                }
                v = ValueLobInMemory.createSmallLob(CLOB, bytes);
                break;
            } else if (v instanceof ValueLobDatabase) {
                v = ((ValueLobDatabase) v).getDataHandler().getLobStorage().createClob(v.getReader(), -1);
                break;
            }
        }
        //$FALL-THROUGH$
        default:
            v = ValueLobInMemory.createSmallLob(CLOB, getString().getBytes(StandardCharsets.UTF_8));
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
                StringBuilder builder = ExtTypeInfoGeometry
                        .toSQL(new StringBuilder(), result.getTypeAndDimensionSystem(), result.getSRID())
                        .append(" -> ");
                extTypeInfo.getSQL(builder);
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, builder.toString());
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
        TypeInfo componentType = (TypeInfo) targetType.getExtTypeInfo();
        int valueType = getValueType();
        ValueArray v;
        if (valueType == ARRAY) {
            v = (ValueArray) this;
        } else {
            Value[] a;
            switch (valueType) {
            case ROW:
                a = ((ValueRow) this).getList().clone();
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
            v = ValueArray.get(a, provider);
        }
        if (componentType != null) {
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
                    v = ValueArray.get(newValues, provider);
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
                    v = ValueArray.get(v.getComponentType(), Arrays.copyOf(values, p), provider);
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
                if (dataType <= BLOB) {
                    if (dataType <= CLOB) {
                        if (leftType == CHAR || rightType == CHAR) {
                            dataType = CHAR;
                        }
                    } else if (dataType >= BINARY && (leftType == BINARY || rightType == BINARY)) {
                        dataType = BINARY;
                    }
                }
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
