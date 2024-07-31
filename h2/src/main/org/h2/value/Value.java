/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import static org.h2.util.Bits.INT_VH_BE;
import static org.h2.util.Bits.LONG_VH_BE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.ref.SoftReference;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Mode.CharPadding;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.util.DateTimeUtils;
import org.h2.util.HasSQL;
import org.h2.util.IntervalUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.geometry.GeoJsonUtils;
import org.h2.util.json.JsonConstructorUtils;
import org.h2.value.lob.LobData;
import org.h2.value.lob.LobDataDatabase;
import org.h2.value.lob.LobDataInMemory;

/**
 * This is the base class for all value classes.
 * It provides conversion and comparison methods.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public abstract class Value extends VersionedValue<Value> implements HasSQL, Typed {

    /**
     * The data type is unknown at this time.
     */
    public static final int UNKNOWN = -1;

    /**
     * The value type for NULL.
     */
    public static final int NULL = UNKNOWN + 1;

    /**
     * The value type for CHARACTER values.
     */
    public static final int CHAR = NULL + 1;

    /**
     * The value type for CHARACTER VARYING values.
     */
    public static final int VARCHAR = CHAR + 1;

    /**
     * The value type for CHARACTER LARGE OBJECT values.
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
     * The value type for BINARY LARGE OBJECT values.
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
     * The value type for DECFLOAT values.
     */
    public static final int DECFLOAT = DOUBLE + 1;

    /**
     * The value type for DATE values.
     */
    public static final int DATE = DECFLOAT + 1;

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
     * The number of value types.
     */
    public static final int TYPE_COUNT = ROW + 1;

    /**
     * Group for untyped NULL data type.
     */
    static final int GROUP_NULL = 0;

    /**
     * Group for character string data types.
     */
    static final int GROUP_CHARACTER_STRING = GROUP_NULL + 1;

    /**
     * Group for binary string data types.
     */
    static final int GROUP_BINARY_STRING = GROUP_CHARACTER_STRING + 1;

    /**
     * Group for BINARY data type.
     */
    static final int GROUP_BOOLEAN = GROUP_BINARY_STRING + 1;

    /**
     * Group for numeric data types.
     */
    static final int GROUP_NUMERIC = GROUP_BOOLEAN + 1;

    /**
     * Group for datetime data types.
     */
    static final int GROUP_DATETIME = GROUP_NUMERIC + 1;

    /**
     * Group for year-month interval data types.
     */
    static final int GROUP_INTERVAL_YM = GROUP_DATETIME + 1;

    /**
     * Group for day-time interval data types.
     */
    static final int GROUP_INTERVAL_DT = GROUP_INTERVAL_YM + 1;

    /**
     * Group for other data types (JAVA_OBJECT, UUID, GEOMETRY, ENUM, JSON).
     */
    static final int GROUP_OTHER = GROUP_INTERVAL_DT + 1;

    /**
     * Group for collection data types (ARRAY, ROW).
     */
    static final int GROUP_COLLECTION = GROUP_OTHER + 1;

    static final byte GROUPS[] = {
            // NULL
            GROUP_NULL,
            // CHAR, VARCHAR, CLOB, VARCHAR_IGNORECASE
            GROUP_CHARACTER_STRING, GROUP_CHARACTER_STRING, GROUP_CHARACTER_STRING, GROUP_CHARACTER_STRING,
            // BINARY, VARBINARY, BLOB
            GROUP_BINARY_STRING, GROUP_BINARY_STRING, GROUP_BINARY_STRING,
            // BOOLEAN
            GROUP_BOOLEAN,
            // TINYINT, SMALLINT, INTEGER, BIGINT, NUMERIC, REAL, DOUBLE, DECFLOAT
            GROUP_NUMERIC, GROUP_NUMERIC, GROUP_NUMERIC, GROUP_NUMERIC, GROUP_NUMERIC, GROUP_NUMERIC, GROUP_NUMERIC,
            GROUP_NUMERIC,
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
            // ARRAY, ROW
            GROUP_COLLECTION, GROUP_COLLECTION,
            //
    };

    private static final String NAMES[] = {
            "UNKNOWN",
            "NULL", //
            "CHARACTER", "CHARACTER VARYING", "CHARACTER LARGE OBJECT", "VARCHAR_IGNORECASE", //
            "BINARY", "BINARY VARYING", "BINARY LARGE OBJECT", //
            "BOOLEAN", //
            "TINYINT", "SMALLINT", "INTEGER", "BIGINT", //
            "NUMERIC", "REAL", "DOUBLE PRECISION", "DECFLOAT", //
            "DATE", "TIME", "TIME WITH TIME ZONE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", //
            "INTERVAL YEAR", "INTERVAL MONTH", //
            "INTERVAL DAY", "INTERVAL HOUR", "INTERVAL MINUTE", "INTERVAL SECOND", //
            "INTERVAL YEAR TO MONTH", //
            "INTERVAL DAY TO HOUR", "INTERVAL DAY TO MINUTE", "INTERVAL DAY TO SECOND", //
            "INTERVAL HOUR TO MINUTE", "INTERVAL HOUR TO SECOND", "INTERVAL MINUTE TO SECOND", //
            "JAVA_OBJECT", "ENUM", "GEOMETRY", "JSON", "UUID", //
            "ARRAY", "ROW", //
    };

    /**
     * Empty array of values.
     */
    public static final Value[] EMPTY_VALUES = new Value[0];

    private static SoftReference<Value[]> softCache;

    /**
     * The largest BIGINT value, as a BigDecimal.
     */
    public static final BigDecimal MAX_LONG_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE);

    /**
     * The smallest BIGINT value, as a BigDecimal.
     */
    public static final BigDecimal MIN_LONG_DECIMAL = BigDecimal.valueOf(Long.MIN_VALUE);

    /**
     * Convert a value to the specified type without taking scale and precision
     * into account.
     */
    public static final int CONVERT_TO = 0;

    /**
     * Cast a value to the specified type. The scale is set if applicable. The
     * value is truncated to a required precision.
     */
    public static final int CAST_TO = 1;

    /**
     * Cast a value to the specified type for assignment. The scale is set if
     * applicable. If precision is too large an exception is thrown.
     */
    public static final int ASSIGN_TO = 2;

    /**
     * Returns name of the specified data type.
     *
     * @param valueType
     *            the value type
     * @return the name
     */
    public static String getTypeName(int valueType) {
        return NAMES[valueType + 1];
    }

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

    @Override
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
         * Java 11 with -XX:-UseCompressedOops for all values up to ValueBigint
         * and ValueDouble.
         */
        return 24;
    }

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
        return getHigherOrderKnown(t1, t2);
    }

    private static int getHigherOrderNonNull(int t1, int t2) {
        if (t1 == t2) {
            return t1;
        }
        if (t1 < t2) {
            int t = t1;
            t1 = t2;
            t2 = t;
        }
        return getHigherOrderKnown(t1, t2);
    }

    static int getHigherOrderKnown(int t1, int t2) {
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
            switch (t1) {
            case REAL:
                switch (t2) {
                case INTEGER:
                    return DOUBLE;
                case BIGINT:
                case NUMERIC:
                    return DECFLOAT;
                }
                break;
            case DOUBLE:
                switch (t2) {
                case BIGINT:
                case NUMERIC:
                    return DECFLOAT;
                }
                break;
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
            if (t2 == INTERVAL_SECOND) {
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
        return DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, getTypeName(t1) + ", " + getTypeName(t2));
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

    /**
     * Get the value as a string.
     *
     * @return the string
     */
    public abstract String getString();

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

    public byte[] getBytes() {
        throw getDataConversionError(VARBINARY);
    }

    public byte[] getBytesNoCopy() {
        return getBytes();
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

    /**
     * Returns this value as a Java {@code boolean} value.
     *
     * @throws DbException
     *             if this value is {@code NULL} or cannot be casted to
     *             {@code BOOLEAN}
     * @return value
     * @see #isTrue()
     * @see #isFalse()
     */
    public boolean getBoolean() {
        return convertToBoolean().getBoolean();
    }

    /**
     * Returns this value as a Java {@code byte} value.
     *
     * @throws DbException
     *             if this value is {@code NULL} or cannot be casted to
     *             {@code TINYINT}
     * @return value
     */
    public byte getByte() {
        return convertToTinyint(null).getByte();
    }

    /**
     * Returns this value as a Java {@code short} value.
     *
     * @throws DbException
     *             if this value is {@code NULL} or cannot be casted to
     *             {@code SMALLINT}
     * @return value
     */
    public short getShort() {
        return convertToSmallint(null).getShort();
    }

    /**
     * Returns this value as a Java {@code int} value.
     *
     * @throws DbException
     *             if this value is {@code NULL} or cannot be casted to
     *             {@code INTEGER}
     * @return value
     */
    public int getInt() {
        return convertToInt(null).getInt();
    }

    /**
     * Returns this value as a Java {@code long} value.
     *
     * @throws DbException
     *             if this value is {@code NULL} or cannot be casted to
     *             {@code BIGINT}
     * @return value
     */
    public long getLong() {
        return convertToBigint(null).getLong();
    }

    public BigDecimal getBigDecimal() {
        throw getDataConversionError(NUMERIC);
    }

    /**
     * Returns this value as a Java {@code float} value.
     *
     * @throws DbException
     *             if this value is {@code NULL} or cannot be casted to
     *             {@code REAL}
     * @return value
     */
    public float getFloat() {
        throw getDataConversionError(REAL);
    }

    /**
     * Returns this value as a Java {@code double} value.
     *
     * @throws DbException
     *             if this value is {@code NULL} or cannot be casted to
     *             {@code DOUBLE PRECISION}
     * @return value
     */
    public double getDouble() {
        throw getDataConversionError(DOUBLE);
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
     * @param quotientType the type of quotient (used only to read precision and scale
     *            when applicable)
     * @return the result
     */
    public Value divide(@SuppressWarnings("unused") Value v, TypeInfo quotientType) {
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
        return convertTo(targetType, null);
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
        switch (targetType) {
        case ARRAY:
            return convertToAnyArray(provider);
        case ROW:
            return convertToAnyRow();
        default:
            return convertTo(TypeInfo.getTypeInfo(targetType), provider, CONVERT_TO, null);
        }
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
     * Convert this value to JSON data type.
     *
     * @return a JSON value
     */
    public final ValueJson convertToAnyJson() {
        return this != ValueNull.INSTANCE ? convertToJson(TypeInfo.TYPE_JSON, CONVERT_TO, null) : ValueJson.NULL;
    }

    /**
     * Convert this value to any ARRAY data type.
     *
     * @param provider
     *            the cast information provider
     * @return a row value
     */
    public final ValueArray convertToAnyArray(CastDataProvider provider) {
        if (getValueType() == Value.ARRAY) {
            return (ValueArray) this;
        }
        return ValueArray.get(this.getType(), new Value[] { this }, provider);
    }

    /**
     * Convert this value to any ROW data type.
     *
     * @return a row value
     */
    public final ValueRow convertToAnyRow() {
        if (getValueType() == Value.ROW) {
            return (ValueRow) this;
        }
        return ValueRow.get(new Value[] { this });
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
                && targetType.getExtTypeInfo() == null && valueType != CHAR) {
            return this;
        }
        switch (targetValueType) {
        case NULL:
            return ValueNull.INSTANCE;
        case CHAR:
            return convertToChar(targetType, provider, conversionMode, column);
        case VARCHAR:
            return convertToVarchar(targetType, provider, conversionMode, column);
        case CLOB:
            return convertToClob(targetType, conversionMode, column);
        case VARCHAR_IGNORECASE:
            return convertToVarcharIgnoreCase(targetType, conversionMode, column);
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
        case DECFLOAT:
            return convertToDecfloat(targetType, conversionMode);
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
            return convertToEnum((ExtTypeInfoEnum) targetType.getExtTypeInfo(), provider);
        case GEOMETRY:
            return convertToGeometry((ExtTypeInfoGeometry) targetType.getExtTypeInfo());
        case JSON:
            return convertToJson(targetType, conversionMode, column);
        case UUID:
            return convertToUuid();
        case ARRAY:
            return convertToArray(targetType, provider, conversionMode, column);
        case ROW:
            return convertToRow(targetType, provider, conversionMode, column);
        default:
            throw getDataConversionError(targetValueType);
        }
    }

    /**
     * Converts this value to a CHAR value. May not be called on a NULL value.
     *
     * @return a CHAR value.
     */
    public ValueChar convertToChar() {
        return convertToChar(TypeInfo.getTypeInfo(CHAR), null, CONVERT_TO, null);
    }

    private ValueChar convertToChar(TypeInfo targetType, CastDataProvider provider, int conversionMode, //
            Object column) {
        int valueType = getValueType();
        switch (valueType) {
        case BLOB:
        case JAVA_OBJECT:
            throw getDataConversionError(targetType.getValueType());
        }
        String s = getString();
        int length = s.length(), newLength = length;
        if (conversionMode == CONVERT_TO) {
            while (newLength > 0 && s.charAt(newLength - 1) == ' ') {
                newLength--;
            }
        } else {
            int p = MathUtils.convertLongToInt(targetType.getPrecision());
            if (provider == null || provider.getMode().charPadding == CharPadding.ALWAYS) {
                if (newLength != p) {
                    if (newLength < p) {
                        return ValueChar.get(StringUtils.pad(s, p, null, true));
                    } else if (conversionMode == CAST_TO) {
                        newLength = p;
                    } else {
                        do {
                            if (s.charAt(--newLength) != ' ') {
                                throw getValueTooLongException(targetType, column);
                            }
                        } while (newLength > p);
                    }
                }
            } else {
                if (conversionMode == CAST_TO && newLength > p) {
                    newLength = p;
                }
                while (newLength > 0 && s.charAt(newLength - 1) == ' ') {
                    newLength--;
                }
                if (conversionMode == ASSIGN_TO && newLength > p) {
                    throw getValueTooLongException(targetType, column);
                }
            }
        }
        if (length != newLength) {
            s = s.substring(0, newLength);
        } else if (valueType == CHAR) {
            return (ValueChar) this;
        }
        return ValueChar.get(s);
    }

    private Value convertToVarchar(TypeInfo targetType, CastDataProvider provider, int conversionMode, Object column) {
        int valueType = getValueType();
        switch (valueType) {
        case BLOB:
        case JAVA_OBJECT:
            throw getDataConversionError(targetType.getValueType());
        }
        if (conversionMode != CONVERT_TO) {
            String s = getString();
            int p = MathUtils.convertLongToInt(targetType.getPrecision());
            if (s.length() > p) {
                if (conversionMode != CAST_TO) {
                    throw getValueTooLongException(targetType, column);
                }
                return ValueVarchar.get(s.substring(0, p), provider);
            }
        }
        return valueType == Value.VARCHAR ? this : ValueVarchar.get(getString(), provider);
    }

    private ValueClob convertToClob(TypeInfo targetType, int conversionMode, Object column) {
        ValueClob v;
        switch (getValueType()) {
        case CLOB:
            v = (ValueClob) this;
            break;
        case JAVA_OBJECT:
            throw getDataConversionError(targetType.getValueType());
        case BLOB: {
            LobData data = ((ValueBlob) this).lobData;
            // Try to reuse the array, if possible
            if (data instanceof LobDataInMemory) {
                byte[] small = ((LobDataInMemory) data).getSmall();
                byte[] bytes = new String(small, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8);
                if (Arrays.equals(bytes, small)) {
                    bytes = small;
                }
                v = ValueClob.createSmall(bytes);
                break;
            } else if (data instanceof LobDataDatabase) {
                v = data.getDataHandler().getLobStorage().createClob(getReader(), -1);
                break;
            }
        }
        //$FALL-THROUGH$
        default:
            v = ValueClob.createSmall(getString());
        }
        if (conversionMode != CONVERT_TO) {
            if (conversionMode == CAST_TO) {
                v = v.convertPrecision(targetType.getPrecision());
            } else if (v.charLength() > targetType.getPrecision()) {
                throw v.getValueTooLongException(targetType, column);
            }
        }
        return v;
    }

    private Value convertToVarcharIgnoreCase(TypeInfo targetType, int conversionMode, Object column) {
        int valueType = getValueType();
        switch (valueType) {
        case BLOB:
        case JAVA_OBJECT:
            throw getDataConversionError(targetType.getValueType());
        }
        if (conversionMode != CONVERT_TO) {
            String s = getString();
            int p = MathUtils.convertLongToInt(targetType.getPrecision());
            if (s.length() > p) {
                if (conversionMode != CAST_TO) {
                    throw getValueTooLongException(targetType, column);
                }
                return ValueVarcharIgnoreCase.get(s.substring(0, p));
            }
        }
        return valueType == Value.VARCHAR_IGNORECASE ? this : ValueVarcharIgnoreCase.get(getString());
    }

    private ValueBinary convertToBinary(TypeInfo targetType, int conversionMode, Object column) {
        ValueBinary v;
        if (getValueType() == BINARY) {
            v = (ValueBinary) this;
        } else {
            try {
                v = ValueBinary.getNoCopy(getBytesNoCopy());
            } catch (DbException e) {
                if (e.getErrorCode() == ErrorCode.DATA_CONVERSION_ERROR_1) {
                    throw getDataConversionError(BINARY);
                }
                throw e;
            }
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

    private ValueVarbinary convertToVarbinary(TypeInfo targetType, int conversionMode, Object column) {
        ValueVarbinary v;
        if (getValueType() == VARBINARY) {
            v = (ValueVarbinary) this;
        } else {
            v = ValueVarbinary.getNoCopy(getBytesNoCopy());
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

    private ValueBlob convertToBlob(TypeInfo targetType, int conversionMode, Object column) {
        ValueBlob v;
        switch (getValueType()) {
        case BLOB:
            v = (ValueBlob) this;
            break;
        case CLOB:
            DataHandler handler = ((ValueLob) this).lobData.getDataHandler();
            if (handler != null) {
                v = handler.getLobStorage().createBlob(getInputStream(), -1);
                break;
            }
            //$FALL-THROUGH$
        default:
            try {
                v = ValueBlob.createSmall(getBytesNoCopy());
            } catch (DbException e) {
                if (e.getErrorCode() == ErrorCode.DATA_CONVERSION_ERROR_1) {
                    throw getDataConversionError(BLOB);
                }
                throw e;
            }
            break;
        }
        if (conversionMode != CONVERT_TO) {
            if (conversionMode == CAST_TO) {
                v = v.convertPrecision(targetType.getPrecision());
            } else if (v.octetLength() > targetType.getPrecision()) {
                throw v.getValueTooLongException(targetType, column);
            }
        }
        return v;
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
        case CHAR:
        case VARCHAR:
        case VARCHAR_IGNORECASE:
            return ValueBoolean.get(getBoolean());
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case NUMERIC:
        case DOUBLE:
        case REAL:
        case DECFLOAT:
            return ValueBoolean.get(getSignum() != 0);
        default:
            throw getDataConversionError(BOOLEAN);
        case NULL:
            throw DbException.getInternalError();
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
        case CHAR:
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case BOOLEAN:
            return ValueTinyint.get(getByte());
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
        case DECFLOAT:
            return ValueTinyint.get(convertToByte(convertToLong(getBigDecimal(), column), column));
        case REAL:
        case DOUBLE:
            return ValueTinyint.get(convertToByte(convertToLong(getDouble(), column), column));
        case BINARY:
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
            throw DbException.getInternalError();
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
        case CHAR:
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case BOOLEAN:
        case TINYINT:
            return ValueSmallint.get(getShort());
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
        case DECFLOAT:
            return ValueSmallint.get(convertToShort(convertToLong(getBigDecimal(), column), column));
        case REAL:
        case DOUBLE:
            return ValueSmallint.get(convertToShort(convertToLong(getDouble(), column), column));
        case BINARY:
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
            throw DbException.getInternalError();
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
        case CHAR:
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case BOOLEAN:
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
        case DECFLOAT:
            return ValueInteger.get(convertToInt(convertToLong(getBigDecimal(), column), column));
        case REAL:
        case DOUBLE:
            return ValueInteger.get(convertToInt(convertToLong(getDouble(), column), column));
        case BINARY:
        case VARBINARY: {
            byte[] bytes = getBytesNoCopy();
            if (bytes.length == 4) {
                return ValueInteger.get((int) INT_VH_BE.get(bytes, 0));
            }
        }
        //$FALL-THROUGH$
        default:
            throw getDataConversionError(INTEGER);
        case NULL:
            throw DbException.getInternalError();
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
        case CHAR:
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case BOOLEAN:
        case TINYINT:
        case SMALLINT:
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
        case ENUM:
            return ValueBigint.get(getLong());
        case NUMERIC:
        case DECFLOAT:
            return ValueBigint.get(convertToLong(getBigDecimal(), column));
        case REAL:
        case DOUBLE:
            return ValueBigint.get(convertToLong(getDouble(), column));
        case BINARY:
        case VARBINARY: {
            byte[] bytes = getBytesNoCopy();
            if (bytes.length == 8) {
                return ValueBigint.get((long) LONG_VH_BE.get(bytes, 0));
            }
        }
        //$FALL-THROUGH$
        default:
            throw getDataConversionError(BIGINT);
        case NULL:
            throw DbException.getInternalError();
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
        default: {
            BigDecimal value = getBigDecimal();
            int targetScale = targetType.getScale();
            int scale = value.scale();
            if (scale < 0 || scale > ValueNumeric.MAXIMUM_SCALE || conversionMode != CONVERT_TO && scale != targetScale
                    && (scale >= targetScale || !provider.getMode().convertOnlyToSmallerScale)) {
                value = ValueNumeric.setScale(value, targetScale);
            }
            if (conversionMode != CONVERT_TO
                    && value.precision() > targetType.getPrecision() - targetScale + value.scale()) {
                throw getValueTooLongException(targetType, column);
            }
            return ValueNumeric.get(value);
        }
        case NULL:
            throw DbException.getInternalError();
        }
        if (conversionMode != CONVERT_TO) {
            int targetScale = targetType.getScale();
            BigDecimal value = v.getBigDecimal();
            int scale = value.scale();
            if (scale != targetScale && (scale >= targetScale || !provider.getMode().convertOnlyToSmallerScale)) {
                v = ValueNumeric.get(ValueNumeric.setScale(value, targetScale));
            }
            BigDecimal bd = v.getBigDecimal();
            if (bd.precision() > targetType.getPrecision() - targetScale + bd.scale()) {
                throw v.getValueTooLongException(targetType, column);
            }
        }
        return v;
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
        default:
            return ValueReal.get(getFloat());
        case NULL:
            throw DbException.getInternalError();
        }
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
        default:
            return ValueDouble.get(getDouble());
        case NULL:
            throw DbException.getInternalError();
        }
    }

    private ValueDecfloat convertToDecfloat(TypeInfo targetType, int conversionMode) {
        ValueDecfloat v;
        switch (getValueType()) {
        case DECFLOAT:
            v = (ValueDecfloat) this;
            if (v.value == null) {
                return v;
            }
            break;
        case CHAR:
        case VARCHAR:
        case VARCHAR_IGNORECASE: {
            String s = getString().trim();
            try {
                v = ValueDecfloat.get(new BigDecimal(s));
            } catch (NumberFormatException e) {
                switch (s) {
                case "-Infinity":
                    return ValueDecfloat.NEGATIVE_INFINITY;
                case "Infinity":
                case "+Infinity":
                    return ValueDecfloat.POSITIVE_INFINITY;
                case "NaN":
                case "-NaN":
                case "+NaN":
                    return ValueDecfloat.NAN;
                default:
                    throw getDataConversionError(DECFLOAT);
                }
            }
            break;
        }
        case BOOLEAN:
            v = getBoolean() ? ValueDecfloat.ONE : ValueDecfloat.ZERO;
            break;
        case REAL: {
            float value = getFloat();
            if (Float.isFinite(value)) {
                v = ValueDecfloat.get(new BigDecimal(Float.toString(value)));
            } else if (value == Float.POSITIVE_INFINITY) {
                return ValueDecfloat.POSITIVE_INFINITY;
            } else if (value == Float.NEGATIVE_INFINITY) {
                return ValueDecfloat.NEGATIVE_INFINITY;
            } else {
                return ValueDecfloat.NAN;
            }
            break;
        }
        case DOUBLE: {
            double value = getDouble();
            if (Double.isFinite(value)) {
                v = ValueDecfloat.get(new BigDecimal(Double.toString(value)));
            } else if (value == Double.POSITIVE_INFINITY) {
                return ValueDecfloat.POSITIVE_INFINITY;
            } else if (value == Double.NEGATIVE_INFINITY) {
                return ValueDecfloat.NEGATIVE_INFINITY;
            } else {
                return ValueDecfloat.NAN;
            }
            break;
        }
        default:
            try {
                v = ValueDecfloat.get(getBigDecimal());
            } catch (DbException e) {
                if (e.getErrorCode() == ErrorCode.DATA_CONVERSION_ERROR_1) {
                    throw getDataConversionError(DECFLOAT);
                }
                throw e;
            }
            break;
        case NULL:
            throw DbException.getInternalError();
        }
        if (conversionMode != CONVERT_TO) {
            BigDecimal bd = v.value;
            int precision = bd.precision(), targetPrecision = (int) targetType.getPrecision();
            if (precision > targetPrecision) {
                v = ValueDecfloat.get(bd.setScale(bd.scale() - precision + targetPrecision, RoundingMode.HALF_UP));
            }
        }
        return v;
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
            throw DbException.getInternalError();
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
            v = ValueTime.parse(getString().trim(), provider);
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
            v = ValueTimeTimeZone.parse(getString().trim(), provider);
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
                (localOffset - ts.getTimeZoneOffsetSeconds()) * DateTimeUtils.NANOS_PER_SECOND);
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
        case DECFLOAT:
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
        case DECFLOAT:
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
        case BINARY:
        case VARBINARY:
        case BLOB:
            v = ValueJavaObject.getNoCopy(getBytesNoCopy());
            break;
        default:
            throw getDataConversionError(JAVA_OBJECT);
        case NULL:
            throw DbException.getInternalError();
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
     * @param provider
     *            the cast information provider
     * @return the ENUM value
     */
    public final ValueEnum convertToEnum(ExtTypeInfoEnum extTypeInfo, CastDataProvider provider) {
        switch (getValueType()) {
        case ENUM: {
            ValueEnum v = (ValueEnum) this;
            if (extTypeInfo.equals(v.getEnumerators())) {
                return v;
            }
            return extTypeInfo.getValue(v.getString(), provider);
        }
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case NUMERIC:
        case DECFLOAT:
            return extTypeInfo.getValue(getInt(), provider);
        case VARCHAR:
        case VARCHAR_IGNORECASE:
        case CHAR:
            return extTypeInfo.getValue(getString(), provider);
        default:
            throw getDataConversionError(ENUM);
        case NULL:
            throw DbException.getInternalError();
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
        case BINARY:
        case VARBINARY:
        case BLOB:
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
        case CHAR:
        case VARCHAR:
        case CLOB:
        case VARCHAR_IGNORECASE:
            result = ValueGeometry.get(getString());
            break;
        default:
            throw getDataConversionError(GEOMETRY);
        case NULL:
            throw DbException.getInternalError();
        }
        if (extTypeInfo != null) {
            int type = extTypeInfo.getType();
            Integer srid = extTypeInfo.getSrid();
            if (type != 0 && result.getTypeAndDimensionSystem() != type || srid != null && result.getSRID() != srid) {
                StringBuilder builder = ExtTypeInfoGeometry
                        .toSQL(new StringBuilder(), result.getTypeAndDimensionSystem(), result.getSRID())
                        .append(" -> ");
                extTypeInfo.getSQL(builder, TRACE_SQL_FLAGS);
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, builder.toString());
            }
        }
        return result;
    }

    /**
     * Converts this value to a JSON value. May not be called on a NULL
     * value.
     *
     * @param targetType
     *            the type of the returned value
     * @param conversionMode
     *            conversion mode
     * @param column
     *            the column (if any), used to improve the error message if
     *            conversion fails
     * @return the JSON value
     */
    public ValueJson convertToJson(TypeInfo targetType, int conversionMode, Object column) {
        ValueJson v;
        switch (getValueType()) {
        case JSON:
            v = (ValueJson) this;
            break;
        case BOOLEAN:
            v = ValueJson.get(getBoolean());
            break;
        case TINYINT:
        case SMALLINT:
        case INTEGER:
            v = ValueJson.get(getInt());
            break;
        case BIGINT:
            v = ValueJson.get(getLong());
            break;
        case REAL:
        case DOUBLE:
        case NUMERIC:
        case DECFLOAT:
            v = ValueJson.get(getBigDecimal());
            break;
        case BINARY:
        case VARBINARY:
        case BLOB:
            v = ValueJson.fromJson(getBytesNoCopy());
            break;
        case CHAR:
        case VARCHAR:
        case CLOB:
        case VARCHAR_IGNORECASE:
        case DATE:
        case TIME:
        case TIME_TZ:
        case ENUM:
        case UUID:
            v = ValueJson.get(getString());
            break;
        case TIMESTAMP:
            v = ValueJson.get(((ValueTimestamp) this).getISOString());
            break;
        case TIMESTAMP_TZ:
            v = ValueJson.get(((ValueTimestampTimeZone) this).getISOString());
            break;
        case GEOMETRY: {
            ValueGeometry vg = (ValueGeometry) this;
            v = ValueJson.getInternal(GeoJsonUtils.ewkbToGeoJson(vg.getBytesNoCopy(), vg.getDimensionSystem()));
            break;
        }
        case ARRAY: {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write('[');
            for (Value e : ((ValueArray) this).getList()) {
                JsonConstructorUtils.jsonArrayAppend(baos, e, 0);
            }
            baos.write(']');
            v = ValueJson.getInternal(baos.toByteArray());
            break;
        }
        default:
            throw getDataConversionError(JSON);
        }
        if (conversionMode != CONVERT_TO && v.getBytesNoCopy().length > targetType.getPrecision()) {
            throw v.getValueTooLongException(targetType, column);
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
        case BINARY:
        case VARBINARY:
            return ValueUuid.get(getBytesNoCopy());
        case JAVA_OBJECT:
            return JdbcUtils.deserializeUuid(getBytesNoCopy());
        case CHAR:
        case VARCHAR:
        case VARCHAR_IGNORECASE:
            return ValueUuid.get(getString());
        default:
            throw getDataConversionError(UUID);
        case NULL:
            throw DbException.getInternalError();
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
            case BLOB:
                a = new Value[] { ValueVarbinary.get(getBytesNoCopy()) };
                break;
            case CLOB:
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
                    v = ValueArray.get(componentType, newValues, provider);
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

    private Value convertToRow(TypeInfo targetType, CastDataProvider provider, int conversionMode,
            Object column) {
        ValueRow v;
        if (getValueType() == ROW) {
            v = (ValueRow) this;
        } else {
            v = ValueRow.get(new Value[] { this });
        }
        ExtTypeInfoRow ext = (ExtTypeInfoRow) targetType.getExtTypeInfo();
        if (ext != null) {
            Value[] values = v.getList();
            int length = values.length;
            Set<Map.Entry<String, TypeInfo>> fields = ext.getFields();
            if (length != fields.size()) {
                throw getDataConversionError(targetType);
            }
            Iterator<Map.Entry<String, TypeInfo>> iter = fields.iterator();
            loop: for (int i = 0; i < length; i++) {
                Value v1 = values[i];
                TypeInfo componentType = iter.next().getValue();
                Value v2 = v1.convertTo(componentType, provider, conversionMode, column);
                if (v1 != v2) {
                    Value[] newValues = new Value[length];
                    System.arraycopy(values, 0, newValues, 0, i);
                    newValues[i] = v2;
                    while (++i < length) {
                        newValues[i] = values[i].convertTo(componentType, provider, conversionMode, column);
                    }
                    v = ValueRow.get(targetType, newValues);
                    break loop;
                }
            }
        }
        return v;
    }

    /**
     * Creates new instance of the DbException for data conversion error.
     *
     * @param targetType Target data type.
     * @return instance of the DbException.
     */
    final DbException getDataConversionError(int targetType) {
        throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, getTypeName(getValueType()) + " to "
                + getTypeName(targetType));
    }

    /**
     * Creates new instance of the DbException for data conversion error.
     *
     * @param targetType target data type.
     * @return instance of the DbException.
     */
    final DbException getDataConversionError(TypeInfo targetType) {
        throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, getTypeName(getValueType()) + " to "
                + targetType.getTraceSQL());
    }

    final DbException getValueTooLongException(TypeInfo targetType, Object column) {
        StringBuilder builder = new StringBuilder();
        if (column != null) {
            builder.append(column).append(' ');
        }
        targetType.getSQL(builder, TRACE_SQL_FLAGS);
        return DbException.getValueTooLongException(builder.toString(), getTraceSQL(), getType().getPrecision());
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
        return compareToNotNullable(this, v, provider, compareMode);
    }

    private static int compareToNotNullable(Value l, Value r, CastDataProvider provider, CompareMode compareMode) {
        int leftType = l.getValueType();
        int rightType = r.getValueType();
        if (leftType != rightType || leftType == ENUM) {
            int dataType = getHigherOrderNonNull(leftType, rightType);
            if (DataType.isNumericType(dataType)) {
                return compareNumeric(l, r, leftType, rightType, dataType);
            }
            if (dataType == ENUM) {
                ExtTypeInfoEnum enumerators = ExtTypeInfoEnum.getEnumeratorsForBinaryOperation(l, r);
                return Integer.compare(l.convertToEnum(enumerators, provider).getInt(),
                        r.convertToEnum(enumerators, provider).getInt());
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
                r = r.convertTo(dataType, provider);
            }
        }
        return l.compareTypeSafe(r, compareMode, provider);
    }

    private static int compareNumeric(Value l, Value r, int leftType, int rightType, int dataType) {
        if (DataType.isNumericType(leftType) && DataType.isNumericType(rightType)) {
            switch (dataType) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return Integer.compare(l.getInt(), r.getInt());
            case BIGINT:
                return Long.compare(l.getLong(), r.getLong());
            case NUMERIC:
                return l.getBigDecimal().compareTo(r.getBigDecimal());
            case REAL:
                return Float.compare(l.getFloat(), r.getFloat());
            case DOUBLE:
                return Double.compare(l.getDouble(), r.getDouble());
            }
        }
        return l.convertToDecfloat(null, CONVERT_TO).compareTypeSafe( //
                r.convertToDecfloat(null, CONVERT_TO), null, null);
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
        return compareToNotNullable(this, v, provider, compareMode);
    }

    /**
     * Returns true if this value is NULL or contains NULL value.
     *
     * @return true if this value is NULL or contains NULL value
     */
    public boolean containsNull() {
        return false;
    }

    /**
     * Scans this and specified values until a first NULL occurrence and returns
     * a value where NULL appears earlier, or {@code null} if these two values
     * have first NULL on the same position.
     *
     * @param v
     *            a value of the same data type as this value, must be neither
     *            equal to nor smaller than nor greater than this value
     * @return this value, the specified value, or {@code null}
     */
    public Value getValueWithFirstNull(Value v) {
        return this == ValueNull.INSTANCE ? v == ValueNull.INSTANCE ? null : ValueNull.INSTANCE
                : v == ValueNull.INSTANCE ? ValueNull.INSTANCE : getValueWithFirstNullImpl(v);
    }

    Value getValueWithFirstNullImpl(Value v) {
        return this;
    }

    private static byte convertToByte(long x, Object column) {
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE) {
            throw getOutOfRangeException(Long.toString(x), column);
        }
        return (byte) x;
    }

    private static short convertToShort(long x, Object column) {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE) {
            throw getOutOfRangeException(Long.toString(x), column);
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
            throw getOutOfRangeException(Long.toString(x), column);
        }
        return (int) x;
    }

    private static long convertToLong(double x, Object column) {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE) {
            // TODO document that +Infinity, -Infinity throw an exception and
            // NaN returns 0
            throw getOutOfRangeException(Double.toString(x), column);
        }
        return Math.round(x);
    }

    /**
     * Convert to long, throwing exception if out of range.
     *
     * @param x long value.
     * @param column Column info.
     * @return x
     */
    public static long convertToLong(BigDecimal x, Object column) {
        if (x.compareTo(MAX_LONG_DECIMAL) > 0 || x.compareTo(MIN_LONG_DECIMAL) < 0) {
            throw getOutOfRangeException(x.toString(), column);
        }
        return x.setScale(0, RoundingMode.HALF_UP).longValue();
    }

    private static DbException getOutOfRangeException(String string, Object column) {
        return column != null
                ? DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_2, string, column.toString())
                : DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, string);
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
        return DbException.getUnsupportedException(getTypeName(getValueType()) + ' ' + op);
    }

    /**
     * Returns length of this value in characters.
     *
     * @return length of this value in characters
     * @throws NullPointerException if this value is {@code NULL}
     */
    public long charLength() {
        return getString().length();
    }

    /**
     * Returns length of this value in bytes.
     *
     * @return length of this value in bytes
     * @throws NullPointerException if this value is {@code NULL}
     */
    public long octetLength() {
        return getBytesNoCopy().length;
    }

    /**
     * Returns whether this value {@code IS TRUE}.
     *
     * @return {@code true} if it is. For {@code BOOLEAN} values returns
     *         {@code true} for {@code TRUE} and {@code false} for {@code FALSE}
     *         and {@code UNKNOWN} ({@code NULL}).
     * @see #getBoolean()
     * @see #isFalse()
     */
    public final boolean isTrue() {
        return this != ValueNull.INSTANCE ? getBoolean() : false;
    }

    /**
     * Returns whether this value {@code IS FALSE}.
     *
     * @return {@code true} if it is. For {@code BOOLEAN} values returns
     *         {@code true} for {@code FALSE} and {@code false} for {@code TRUE}
     *         and {@code UNKNOWN} ({@code NULL}).
     * @see #getBoolean()
     * @see #isTrue()
     */
    public final boolean isFalse() {
        return this != ValueNull.INSTANCE && !getBoolean();
    }

}
