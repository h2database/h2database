/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.lang.ref.SoftReference;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.store.DataHandler;
import org.h2.tools.SimpleResultSet;
import org.h2.util.ByteUtils;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * This is the base class for all value classes.
 * It provides conversion and comparison methods.
 */
public abstract class Value {

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
     * The value type for BYTE values.
     */
    public static final int BYTE = 2;

    /**
     * The value type for SHORT values.
     */
    public static final int SHORT = 3;

    /**
     * The value type for INT values.
     */
    public static final int INT = 4;

    /**
     * The value type for LONG values.
     */
    public static final int LONG = 5;

    /**
     * The value type for DECIMAL values.
     */
    public static final int DECIMAL = 6;

    /**
     * The value type for DOUBLE values.
     */
    public static final int DOUBLE = 7;

    /**
     * The value type for FLOAT values.
     */
    public static final int FLOAT = 8;

    /**
     * The value type for INT values.
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
     * The value type for BYTES values.
     */
    public static final int BYTES = 12;

    /**
     * The value type for STRING values.
     */
    public static final int STRING = 13;

    /**
     * The value type for case insensitive STRING values.
     */
    public static final int STRING_IGNORECASE = 14;

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
     * The value type for string values with a fixed size.
     */
    public static final int STRING_FIXED = 21;

    /**
     * The number of value types.
     */
    public static final int TYPE_COUNT = STRING_FIXED + 1;

    private static SoftReference<Value[]> softCache = new SoftReference<Value[]>(null);
    private static final BigDecimal MAX_LONG_DECIMAL = new BigDecimal("" + Long.MAX_VALUE);
    private static final BigDecimal MIN_LONG_DECIMAL = new BigDecimal("" + Long.MIN_VALUE);

    /**
     * Get the SQL expression for this value.
     *
     * @return the SQL expression
     */
    public abstract String getSQL();

    /**
     * Get the value type.
     *
     * @return the type
     */
    public abstract int getType();

    /**
     * Get the precision.
     *
     * @return the precision
     */
    public abstract long getPrecision();

    /**
     * Get the display size in characters.
     *
     * @return the display size
     */
    public abstract int getDisplaySize();

    /**
     * Get the memory used by this object.
     *
     * @return the memory used in bytes
     */
    public int getMemory() {
        return DataType.getDataType(getType()).memory * 4;
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

    /**
     * Set the value as a parameter in a prepared statement.
     *
     * @param prep the prepared statement
     * @param parameterIndex the parameter index
     */
    public abstract void set(PreparedStatement prep, int parameterIndex) throws SQLException;

    /**
     * Compare the value with another value of the same type.
     *
     * @param v the other value
     * @param mode the compare mode
     * @return 0 if both values are equal, -1 if the other value is smaller, and
     *         1 otherwise
     */
    protected abstract int compareSecure(Value v, CompareMode mode) throws SQLException;

    public abstract int hashCode();

    /**
     * Check if the two values are equal.
     * No data conversion is made; this method returns false
     * if the other object is not of the same class.
     *
     * @param other the other value
     * @return true if they are equal
     */
    public abstract boolean equals(Object other);

    /**
     * Get the order of this value type.
     *
     * @param type the value type
     * @return the order number
     */
    static int getOrder(int type) {
        switch(type) {
        case UNKNOWN:
            return 1;
        case NULL:
            return 2;
        case STRING:
            return 10;
        case CLOB:
            return 11;
        case STRING_FIXED:
            return 12;
        case STRING_IGNORECASE:
            return 13;
        case BOOLEAN:
            return 20;
        case BYTE:
            return 21;
        case SHORT:
            return 22;
        case INT:
            return 23;
        case LONG:
            return 24;
        case DECIMAL:
            return 25;
        case FLOAT:
            return 26;
        case DOUBLE:
            return 27;
        case TIME:
            return 30;
        case DATE:
            return 31;
        case TIMESTAMP:
            return 32;
        case BYTES:
            return 40;
        case BLOB:
            return 41;
        case UUID:
            return 42;
        case JAVA_OBJECT:
            return 43;
        case ARRAY:
            return 50;
        case RESULT_SET:
            return 51;
        default:
            throw Message.throwInternalError("type:"+type);
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
    public static int getHigherOrder(int t1, int t2) throws SQLException {
        if (t1 == t2) {
            if (t1 == Value.UNKNOWN) {
                throw Message.getSQLException(ErrorCode.UNKNOWN_DATA_TYPE_1, "?, ?");
            }
            return t1;
        }
        int o1 = getOrder(t1);
        int o2 = getOrder(t2);
        return o1 > o2 ? t1 : t2;
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
            if (softCache == null) {
                softCache = new SoftReference<Value[]>(null);
            }
            Value[] cache = softCache.get();
            if (cache == null) {
                cache = new Value[SysProperties.OBJECT_CACHE_SIZE];
                softCache = new SoftReference<Value[]>(cache);
            }
            int index = hash & (SysProperties.OBJECT_CACHE_SIZE - 1);
            Value cached = cache[index];
            if (cached != null) {
                if (cached.getType() == v.getType() && v.equals(cached)) {
                    // cacheHit++;
                    return cached;
                }
            }
            // cacheMiss++;
            // cache[cacheCleaner] = null;
            // cacheCleaner = (cacheCleaner + 1) & (Constants.OBJECT_CACHE_SIZE - 1);
            cache[index] = v;
        }
        return v;
    }

    public Boolean getBoolean() throws SQLException {
        return ((ValueBoolean) convertTo(Value.BOOLEAN)).getBoolean();
    }

    public Date getDate() throws SQLException {
        return ((ValueDate) convertTo(Value.DATE)).getDate();
    }

    public Date getDateNoCopy() throws SQLException {
        return ((ValueDate) convertTo(Value.DATE)).getDateNoCopy();
    }

    public Time getTime() throws SQLException {
        return ((ValueTime) convertTo(Value.TIME)).getTime();
    }

    public Time getTimeNoCopy() throws SQLException {
        return ((ValueTime) convertTo(Value.TIME)).getTimeNoCopy();
    }

    public Timestamp getTimestamp() throws SQLException {
        return ((ValueTimestamp) convertTo(Value.TIMESTAMP)).getTimestamp();
    }

    public Timestamp getTimestampNoCopy() throws SQLException {
        return ((ValueTimestamp) convertTo(Value.TIMESTAMP)).getTimestampNoCopy();
    }

    public byte[] getBytes() throws SQLException {
        return ((ValueBytes) convertTo(Value.BYTES)).getBytes();
    }

    public byte[] getBytesNoCopy() throws SQLException {
        return ((ValueBytes) convertTo(Value.BYTES)).getBytesNoCopy();
    }

    public byte getByte() throws SQLException {
        return ((ValueByte) convertTo(Value.BYTE)).getByte();
    }

    public short getShort() throws SQLException {
        return ((ValueShort) convertTo(Value.SHORT)).getShort();
    }

    public BigDecimal getBigDecimal() throws SQLException {
        return ((ValueDecimal) convertTo(Value.DECIMAL)).getBigDecimal();
    }

    public double getDouble() throws SQLException {
        return ((ValueDouble) convertTo(Value.DOUBLE)).getDouble();
    }

    public float getFloat() throws SQLException {
        return ((ValueFloat) convertTo(Value.FLOAT)).getFloat();
    }

    public int getInt() throws SQLException {
        return ((ValueInt) convertTo(Value.INT)).getInt();
    }

    public long getLong() throws SQLException {
        return ((ValueLong) convertTo(Value.LONG)).getLong();
    }

    public InputStream getInputStream() throws SQLException {
        return new ByteArrayInputStream(getBytesNoCopy());
    }

    public Reader getReader() {
        return IOUtils.getReader(getString());
    }

    /**
     * Add a value and return the result.
     *
     * @param v the value to add
     * @return the result
     */
    public Value add(Value v) throws SQLException {
        throw throwUnsupportedExceptionForType();
    }

    public int getSignum() throws SQLException {
        throw throwUnsupportedExceptionForType();
    }

    /**
     * Return -value if this value support arithmetic operations.
     *
     * @return the negative
     */
    public Value negate() throws SQLException {
        throw throwUnsupportedExceptionForType();
    }

    /**
     * Subtract a value and return the result.
     *
     * @param v the value to subtract
     * @return the result
     */
    public Value subtract(Value v) throws SQLException {
        throw throwUnsupportedExceptionForType();
    }

    /**
     * Divide by a value and return the result.
     *
     * @param v the value to divide by
     * @return the result
     */
    public Value divide(Value v) throws SQLException {
        throw throwUnsupportedExceptionForType();
    }

    /**
     * Multiply with a value and return the result.
     *
     * @param v the value to multiply with
     * @return the result
     */
    public Value multiply(Value v) throws SQLException {
        throw throwUnsupportedExceptionForType();
    }

    /**
     * Compare a value to the specified type.
     *
     * @param type the value type
     * @return the value
     */
    public Value convertTo(int type) throws SQLException {
        // converting NULL done in ValueNull
        // converting BLOB to CLOB and vice versa is done in ValueLob
        if (getType() == type) {
            return this;
        }
        // decimal conversion
        switch (type) {
        case BOOLEAN: {
            switch (getType()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
                return ValueBoolean.get(getSignum() != 0);
            case TIME:
            case DATE:
            case TIMESTAMP:
            case BYTES:
            case JAVA_OBJECT:
            case UUID:
                throw Message.getSQLException(ErrorCode.DATA_CONVERSION_ERROR_1, getString());
            }
            break;
        }
        case BYTE: {
            switch (getType()) {
            case BOOLEAN:
                return ValueByte.get(getBoolean().booleanValue() ? (byte) 1 : (byte) 0);
            case SHORT:
                return ValueByte.get(convertToByte(getShort()));
            case INT:
                return ValueByte.get(convertToByte(getInt()));
            case LONG:
                return ValueByte.get(convertToByte(getLong()));
            case DECIMAL:
                return ValueByte.get(convertToByte(convertToLong(getBigDecimal())));
            case DOUBLE:
                return ValueByte.get(convertToByte(convertToLong(getDouble())));
            case FLOAT:
                return ValueByte.get(convertToByte(convertToLong(getFloat())));
            }
            break;
        }
        case SHORT: {
            switch (getType()) {
            case BOOLEAN:
                return ValueShort.get(getBoolean().booleanValue() ? (short) 1 : (short) 0);
            case BYTE:
                return ValueShort.get(getByte());
            case INT:
                return ValueShort.get(convertToShort(getInt()));
            case LONG:
                return ValueShort.get(convertToShort(getLong()));
            case DECIMAL:
                return ValueShort.get(convertToShort(convertToLong(getBigDecimal())));
            case DOUBLE:
                return ValueShort.get(convertToShort(convertToLong(getDouble())));
            case FLOAT:
                return ValueShort.get(convertToShort(convertToLong(getFloat())));
            }
            break;
        }
        case INT: {
            switch (getType()) {
            case BOOLEAN:
                return ValueInt.get(getBoolean().booleanValue() ? 1 : 0);
            case BYTE:
                return ValueInt.get(getByte());
            case SHORT:
                return ValueInt.get(getShort());
            case LONG:
                return ValueInt.get(convertToInt(getLong()));
            case DECIMAL:
                return ValueInt.get(convertToInt(convertToLong(getBigDecimal())));
            case DOUBLE:
                return ValueInt.get(convertToInt(convertToLong(getDouble())));
            case FLOAT:
                return ValueInt.get(convertToInt(convertToLong(getFloat())));
            }
            break;
        }
        case LONG: {
            switch (getType()) {
            case BOOLEAN:
                return ValueLong.get(getBoolean().booleanValue() ? 1 : 0);
            case BYTE:
                return ValueLong.get(getByte());
            case SHORT:
                return ValueLong.get(getShort());
            case INT:
                return ValueLong.get(getInt());
            case DECIMAL:
                return ValueLong.get(convertToLong(getBigDecimal()));
            case DOUBLE:
                return ValueLong.get(convertToLong(getDouble()));
            case FLOAT:
                return ValueLong.get(convertToLong(getFloat()));
            }
            break;
        }
        case DECIMAL: {
            // convert to string is required for JDK 1.4
            switch (getType()) {
            case BOOLEAN:
                return ValueDecimal.get(new BigDecimal(getBoolean().booleanValue() ? "1" : "0"));
            case BYTE:
                return ValueDecimal.get(new BigDecimal("" + getByte()));
            case SHORT:
                return ValueDecimal.get(new BigDecimal("" + getShort()));
            case INT:
                return ValueDecimal.get(new BigDecimal("" + getInt()));
            case LONG:
                return ValueDecimal.get(new BigDecimal("" + getLong()));
            case DOUBLE: {
                double d = getDouble();
                if (Double.isInfinite(d) || Double.isNaN(d)) {
                    throw Message.getSQLException(ErrorCode.DATA_CONVERSION_ERROR_1, "" + d);
                }
                return ValueDecimal.get(new BigDecimal(d));
            }
            case FLOAT: {
                float f = getFloat();
                if (Float.isInfinite(f) || Float.isNaN(f)) {
                    throw Message.getSQLException(ErrorCode.DATA_CONVERSION_ERROR_1, ""+f);
                }
                return ValueDecimal.get(new BigDecimal(f));
            }
            }
            break;
        }
        case DOUBLE: {
            switch (getType()) {
            case BOOLEAN:
                return ValueDouble.get(getBoolean().booleanValue() ? 1 : 0);
            case BYTE:
                return ValueDouble.get(getByte());
            case SHORT:
                return ValueDouble.get(getShort());
            case INT:
                return ValueDouble.get(getInt());
            case LONG:
                return ValueDouble.get(getLong());
            case DECIMAL:
                return ValueDouble.get(getBigDecimal().doubleValue());
            case FLOAT:
                return ValueDouble.get(getFloat());
            }
            break;
        }
        case FLOAT: {
            switch (getType()) {
            case BOOLEAN:
                return ValueFloat.get(getBoolean().booleanValue() ? 1 : 0);
            case BYTE:
                return ValueFloat.get(getByte());
            case SHORT:
                return ValueFloat.get(getShort());
            case INT:
                return ValueFloat.get(getInt());
            case LONG:
                return ValueFloat.get(getLong());
            case DECIMAL:
                return ValueFloat.get(getBigDecimal().floatValue());
            case DOUBLE:
                return ValueFloat.get((float) getDouble());
            }
            break;
        }
        case DATE: {
            switch (getType()) {
            case TIME:
                return ValueDate.get(new Date(getTimeNoCopy().getTime()));
            case TIMESTAMP:
                return ValueDate.get(new Date(getTimestampNoCopy().getTime()));
            }
            break;
        }
        case TIME: {
            switch (getType()) {
            case DATE:
                // need to normalize the year, month and day
                return ValueTime.get(new Time(getDateNoCopy().getTime()));
            case TIMESTAMP:
                // need to normalize the year, month and day
                return ValueTime.get(new Time(getTimestampNoCopy().getTime()));
            }
            break;
        }
        case TIMESTAMP: {
            switch (getType()) {
            case TIME:
                return ValueTimestamp.getNoCopy(new Timestamp(getTimeNoCopy().getTime()));
            case DATE:
                return ValueTimestamp.getNoCopy(new Timestamp(getDateNoCopy().getTime()));
            }
            break;
        }
        case BYTES: {
            switch(getType()) {
            case JAVA_OBJECT:
            case BLOB:
                return ValueBytes.getNoCopy(getBytesNoCopy());
            case UUID:
                return ValueBytes.getNoCopy(getBytes());
            }
            break;
        }
        case JAVA_OBJECT: {
            switch(getType()) {
            case BYTES:
            case BLOB:
                return ValueBytes.getNoCopy(getBytesNoCopy());
            }
            break;
        }
        case BLOB: {
            switch(getType()) {
            case BYTES:
                return ValueLob.createSmallLob(Value.BLOB, getBytesNoCopy());
            }
            break;
        }
        case UUID: {
            switch(getType()) {
            case BYTES:
                return ValueUuid.get(getBytesNoCopy());
            }
        }
        }
        // conversion by parsing the string value
        String s = getString();
        try {
            switch (type) {
            case NULL:
                return ValueNull.INSTANCE;
            case BOOLEAN: {
                if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("t") || s.equalsIgnoreCase("yes") || s.equalsIgnoreCase("y")) {
                    return ValueBoolean.get(true);
                } else if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("f") || s.equalsIgnoreCase("no") || s.equalsIgnoreCase("n")) {
                    return ValueBoolean.get(false);
                } else {
                    // convert to a number, and if it is not 0 then it is true
                    return ValueBoolean.get(new BigDecimal(s).signum() != 0);
                }
            }
            case BYTE:
                return ValueByte.get(MathUtils.decodeByte(s.trim()));
            case SHORT:
                return ValueShort.get(MathUtils.decodeShort(s.trim()));
            case INT:
                return ValueInt.get(MathUtils.decodeInt(s.trim()));
            case LONG:
                return ValueLong.get(MathUtils.decodeLong(s.trim()));
            case DECIMAL:
                return ValueDecimal.get(new BigDecimal(s.trim()));
            case TIME:
                return ValueTime.getNoCopy(ValueTime.parseTime(s.trim()));
            case DATE:
                return ValueDate.getNoCopy(ValueDate.parseDate(s.trim()));
            case TIMESTAMP:
                return ValueTimestamp.getNoCopy(ValueTimestamp.parseTimestamp(s.trim()));
            case BYTES:
                return ValueBytes.getNoCopy(ByteUtils.convertStringToBytes(s.trim()));
            case JAVA_OBJECT:
                return ValueJavaObject.getNoCopy(ByteUtils.convertStringToBytes(s.trim()));
            case STRING:
                return ValueString.get(s);
            case STRING_IGNORECASE:
                return ValueStringIgnoreCase.get(s);
            case STRING_FIXED:
                return ValueStringFixed.get(s);
            case DOUBLE:
                return ValueDouble.get(Double.parseDouble(s.trim()));
            case FLOAT:
                return ValueFloat.get(Float.parseFloat(s.trim()));
            case CLOB:
                return ValueLob.createSmallLob(CLOB, StringUtils.utf8Encode(s));
            case BLOB:
                return ValueLob.createSmallLob(BLOB, ByteUtils.convertStringToBytes(s.trim()));
            case ARRAY:
                return ValueArray.get(new Value[]{ValueString.get(s)});
            case RESULT_SET: {
                SimpleResultSet rs = new SimpleResultSet();
                rs.addColumn("X", Types.VARCHAR, s.length(), 0);
                rs.addRow(s);
                return ValueResultSet.get(rs);
            }
            case UUID:
                return ValueUuid.get(s);
            default:
                throw Message.throwInternalError("type=" + type);
            }
        } catch (NumberFormatException e) {
            throw Message.getSQLException(ErrorCode.DATA_CONVERSION_ERROR_1, e, s);
        }
    }

    /**
     * Compare this value against another value given that the values are of the
     * same data type.
     *
     * @param v the other value
     * @param mode the compare mode
     * @return 0 if both values are equal, -1 if the other value is smaller, and
     *         1 otherwise
     */
    public final int compareTypeSave(Value v, CompareMode mode) throws SQLException {
        if (this == ValueNull.INSTANCE) {
            return v == ValueNull.INSTANCE ? 0 : -1;
        } else if (v == ValueNull.INSTANCE) {
            return 1;
        }
        return compareSecure(v, mode);
    }

    /**
     * Compare two values and return true if they contain the same data.
     *
     * @param v the value to compare against
     * @return true if both values are the same     * @throws SQLException
     */
    public final boolean compareEqual(Value v) throws SQLException {
        if (this == ValueNull.INSTANCE) {
            return v == ValueNull.INSTANCE;
        } else if (v == ValueNull.INSTANCE) {
            return false;
        }
        if (getType() == v.getType()) {
            return equals(v);
        }
        int t2 = Value.getHigherOrder(getType(), v.getType());
        return convertTo(t2).equals(v.convertTo(t2));
    }

    /**
     * Compare this value against another value using the specified compare
     * mode.
     *
     * @param v the other value
     * @param mode the compare mode
     * @return 0 if both values are equal, -1 if the other value is smaller, and
     *         1 otherwise
     */
    public final int compareTo(Value v, CompareMode mode) throws SQLException {
        if (this == v) {
            return 0;
        }
        if (this == ValueNull.INSTANCE) {
            return v == ValueNull.INSTANCE ? 0 : -1;
        } else if (v == ValueNull.INSTANCE) {
            return 1;
        }
        if (getType() == v.getType()) {
            return compareSecure(v, mode);
        }
        int t2 = Value.getHigherOrder(getType(), v.getType());
        return convertTo(t2).compareSecure(v.convertTo(t2), mode);
    }

    public int getScale() {
        return 0;
    }

    /**
     * Convert the scale.
     *
     * @param onlyToSmallerScale if the scale should not reduced
     * @param targetScale the requested scale
     * @return the value
     * @throws SQLException
     */
    public Value convertScale(boolean onlyToSmallerScale, int targetScale) throws SQLException {
        return this;
    }

    /**
     * Convert the precision to the requested value.
     *
     * @param precision the new precision
     * @return the new value
     * @throws SQLException
     */
    public Value convertPrecision(long precision) throws SQLException {
        return this;
    }

    private byte convertToByte(long x) throws SQLException {
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE) {
            throw Message.getSQLException(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE);
        }
        return (byte) x;
    }

    private short convertToShort(long x) throws SQLException {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE) {
            throw Message.getSQLException(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE);
        }
        return (short) x;
    }

    private int convertToInt(long x) throws SQLException {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            throw Message.getSQLException(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE);
        }
        return (int) x;
    }

    private long convertToLong(double x) throws SQLException {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE) {
            // TODO document that +Infinity, -Infinity throw an exception and NaN returns 0
            throw Message.getSQLException(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE);
        }
        return Math.round(x);
    }

    private long convertToLong(BigDecimal x) throws SQLException {
        if (x.compareTo(MAX_LONG_DECIMAL) > 0 || x.compareTo(Value.MIN_LONG_DECIMAL) < 0) {
            throw Message.getSQLException(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE);
        }
        return x.setScale(0, BigDecimal.ROUND_HALF_UP).longValue();
    }

    /**
     * Link a large value to a given table. For values that are kept fully in
     * memory this method has no effect.
     *
     * @param handler the data handler
     * @param tableId the table to link to
     * @return the new value or itself
     * @throws SQLException
     */
    public Value link(DataHandler handler, int tableId) throws SQLException {
        return this;
    }

    /**
     * Check if this value is linked to a specific table. For values that are
     * kept fully in memory, this method returns false.
     *
     * @return true if it is
     */
    public boolean isLinked() {
        return false;
    }

    /**
     * Mark any underlying resource as 'not linked to any table'. For values
     * that are kept fully in memory this method has no effect.
     *
     * @throws SQLException
     */
    public void unlink() throws SQLException {
        // nothing to do
    }

    /**
     * Check if this value is stored in it's own file. For values that are
     * kept fully in memory, this method returns false.
     *
     * @return true if it is
     */
    public boolean isFileBased() {
        return false;
    }

    /**
     * Close the underlying resource, if any. For values that are kept fully in
     * memory this method has no effect.
     *
     * @throws SQLException
     */
    public void close() throws SQLException {
        // nothing to do
    }

    /**
     * Check if the precision is smaller or equal than the given precision.
     *
     * @param precision the maximum precision
     * @return true if the precision of this value is smaller or equal to the
     *         given precision
     */
    public boolean checkPrecision(long precision) {
        return getPrecision() <= precision;
    }

    /**
     * Get a medium size SQL expression for debugging or tracing. If the precision is
     * too large, only a subset of the value is returned.
     *
     * @return the SQL expression
     */
    public String getTraceSQL() {
        return getSQL();
    }

    public String toString() {
        return getTraceSQL();
    }

    /**
     * Throw the exception that the feature is not support for the given data type.
     *
     * @return the exception
     * @throws SQLException
     */
    protected SQLException throwUnsupportedExceptionForType() throws SQLException {
        throw Message.getUnsupportedException(DataType.getDataType(getType()).name);
    }

}
