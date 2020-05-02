/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import static org.h2.mvstore.DataUtils.readString;
import static org.h2.mvstore.DataUtils.readVarInt;
import static org.h2.mvstore.DataUtils.readVarLong;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Database;
import org.h2.engine.Mode;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.rtree.SpatialDataType;
import org.h2.mvstore.rtree.SpatialKey;
import org.h2.mvstore.type.BasicDataType;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.MetaType;
import org.h2.mvstore.type.StatefulDataType;
import org.h2.result.ResultInterface;
import org.h2.result.RowFactory;
import org.h2.result.SearchRow;
import org.h2.result.SimpleResult;
import org.h2.result.SortOrder;
import org.h2.store.DataHandler;
import org.h2.util.DateTimeUtils;
import org.h2.util.Utils;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBinary;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueChar;
import org.h2.value.ValueCollectionBase;
import org.h2.value.ValueDate;
import org.h2.value.ValueDouble;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueInteger;
import org.h2.value.ValueInterval;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueJson;
import org.h2.value.ValueLob;
import org.h2.value.ValueLobDatabase;
import org.h2.value.ValueLobInMemory;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueReal;
import org.h2.value.ValueResultSet;
import org.h2.value.ValueRow;
import org.h2.value.ValueSmallint;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueTinyint;
import org.h2.value.ValueUuid;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;
import org.h2.value.ValueVarcharIgnoreCase;

/**
 * A row type.
 */
public final class ValueDataType extends BasicDataType<Value> implements StatefulDataType<Database> {

    private static final byte NULL = 0;
    private static final byte TINYINT = 2;
    private static final byte SMALLINT = 3;
    private static final byte INTEGER = 4;
    private static final byte BIGINT = 5;
    private static final byte NUMERIC = 6;
    private static final byte DOUBLE = 7;
    private static final byte REAL = 8;
    private static final byte TIME = 9;
    private static final byte DATE = 10;
    private static final byte TIMESTAMP = 11;
    private static final byte VARBINARY = 12;
    private static final byte VARCHAR = 13;
    private static final byte VARCHAR_IGNORECASE = 14;
    private static final byte BLOB = 15;
    private static final byte CLOB = 16;
    private static final byte ARRAY = 17;
    private static final byte RESULT_SET = 18;
    private static final byte JAVA_OBJECT = 19;
    private static final byte UUID = 20;
    private static final byte CHAR = 21;
    private static final byte GEOMETRY = 22;
    private static final byte TIMESTAMP_TZ = 24;
    private static final byte ENUM = 25;
    private static final byte INTERVAL = 26;
    private static final byte ROW = 27;
    private static final byte INT_0_15 = 32;
    private static final byte BIGINT_0_7 = 48;
    private static final byte NUMERIC_0_1 = 56;
    private static final byte NUMERIC_SMALL_0 = 58;
    private static final byte NUMERIC_SMALL = 59;
    private static final byte DOUBLE_0_1 = 60;
    private static final byte REAL_0_1 = 62;
    private static final byte BOOLEAN_FALSE = 64;
    private static final byte BOOLEAN_TRUE = 65;
    private static final byte INT_NEG = 66;
    private static final byte BIGINT_NEG = 67;
    private static final byte VARCHAR_0_31 = 68;
    private static final int VARBINARY_0_31 = 100;
    private static final int SPATIAL_KEY_2D = 132;
    // 133 was used for CUSTOM_DATA_TYPE
    private static final int JSON = 134;
    private static final int TIMESTAMP_TZ_2 = 135;
    private static final int TIME_TZ = 136;
    private static final int BINARY = 137;

    final DataHandler handler;
    final CastDataProvider provider;
    final CompareMode compareMode;
    protected final Mode mode;
    final int[] sortTypes;
    SpatialDataType spatialType;
    private RowFactory rowFactory;

    public ValueDataType() {
        this(null, CompareMode.getInstance(null, 0), null, null, null);
    }

    public ValueDataType(Database database, int[] sortTypes) {
        this(database, database.getCompareMode(), database.getMode(), database, sortTypes);
    }

    public ValueDataType(CastDataProvider provider, CompareMode compareMode, Mode mode, DataHandler handler,
            int[] sortTypes) {
        this.provider = provider;
        this.compareMode = compareMode;
        this.mode = mode;
        this.handler = handler;
        this.sortTypes = sortTypes;
    }

    public RowFactory getRowFactory() {
        return rowFactory;
    }

    public void setRowFactory(RowFactory rowFactory) {
        this.rowFactory = rowFactory;
    }

    private SpatialDataType getSpatialDataType() {
        if (spatialType == null) {
            spatialType = new SpatialDataType(2);
        }
        return spatialType;
    }

    @Override
    public Value[] createStorage(int size) {
        return new Value[size];
    }

    @Override
    public int compare(Value a, Value b) {
        if (a == b) {
            return 0;
        }
        if (a instanceof SearchRow && b instanceof SearchRow) {
            return compare((SearchRow)a, (SearchRow)b);
        } else if (a instanceof ValueCollectionBase && b instanceof ValueCollectionBase) {
            Value[] ax = ((ValueCollectionBase) a).getList();
            Value[] bx = ((ValueCollectionBase) b).getList();
            int al = ax.length;
            int bl = bx.length;
            int len = Math.min(al, bl);
            for (int i = 0; i < len; i++) {
                int sortType = sortTypes == null ? SortOrder.ASCENDING : sortTypes[i];
                Value one = ax[i];
                Value two = bx[i];
                if (one == null || two == null) {
                    return compareValues(ax[len - 1], bx[len - 1], SortOrder.ASCENDING);
                }

                int comp = compareValues(one, two, sortType);
                if (comp != 0) {
                    return comp;
                }
            }
            if (len < al) {
                return -1;
            } else if (len < bl) {
                return 1;
            }
            return 0;
        }
        return compareValues(a, b, SortOrder.ASCENDING);
    }

    private int compare(SearchRow a, SearchRow b) {
        if (a == b) {
            return 0;
        }
        int[] indexes = rowFactory.getIndexes();
        if (indexes == null) {
            int len = a.getColumnCount();
            assert len == b.getColumnCount() : len + " != " + b.getColumnCount();
            for (int i = 0; i < len; i++) {
                int comp = compareValues(a.getValue(i), b.getValue(i), sortTypes[i]);
                if (comp != 0) {
                    return comp;
                }
            }
            return 0;
        } else {
            assert sortTypes.length == indexes.length;
            for (int i = 0; i < indexes.length; i++) {
                int index = indexes[i];
                Value v1 = a.getValue(index);
                Value v2 = b.getValue(index);
                if (v1 == null || v2 == null) {
                    // can't compare further
                    break;
                }
                int comp = compareValues(a.getValue(index), b.getValue(index), sortTypes[i]);
                if (comp != 0) {
                    return comp;
                }
            }
            long aKey = a.getKey();
            long bKey = b.getKey();
            return aKey == SearchRow.MATCH_ALL_ROW_KEY || bKey == SearchRow.MATCH_ALL_ROW_KEY ?
                    0 : Long.compare(aKey, bKey);
        }
    }

    public int compareValues(Value a, Value b, int sortType) {
        if (a == b) {
            return 0;
        }
        boolean aNull = a == ValueNull.INSTANCE;
        if (aNull || b == ValueNull.INSTANCE) {
            return SortOrder.compareNull(aNull, sortType);
        }

        int comp = a.compareTo(b, provider, compareMode);

        if ((sortType & SortOrder.DESCENDING) != 0) {
            comp = -comp;
        }
        return comp;
    }

    @Override
    public int getMemory(Value v) {
        if (v instanceof SpatialKey) {
            return getSpatialDataType().getMemory((SpatialKey) v);
        }
        return v == null ? 0 : v.getMemory();
    }

    @Override
    public Value read(ByteBuffer buff) {
        return readValue(buff, true);
    }

    @Override
    public void write(WriteBuffer buff, Value obj) {
        if (obj instanceof SpatialKey) {
            buff.put((byte) SPATIAL_KEY_2D);
            getSpatialDataType().write(buff, (SpatialKey) obj);
            return;
        }
        writeValue(buff, obj, true);
    }

    private void writeValue(WriteBuffer buff, Value v, boolean rowAsRow) {
        if (v == ValueNull.INSTANCE) {
            buff.put((byte) 0);
            return;
        }
        int type = v.getValueType();
        switch (type) {
        case Value.BOOLEAN:
            buff.put(v.getBoolean() ? BOOLEAN_TRUE : BOOLEAN_FALSE);
            break;
        case Value.TINYINT:
            buff.put(TINYINT).put(v.getByte());
            break;
        case Value.SMALLINT:
            buff.put(SMALLINT).putShort(v.getShort());
            break;
        case Value.ENUM:
        case Value.INTEGER: {
            int x = v.getInt();
            if (x < 0) {
                buff.put(INT_NEG).putVarInt(-x);
            } else if (x < 16) {
                buff.put((byte) (INT_0_15 + x));
            } else {
                buff.put(type == Value.INTEGER ? INTEGER : ENUM).putVarInt(x);
            }
            break;
        }
        case Value.BIGINT: {
            long x = v.getLong();
            writeLong(buff, x);
            break;
        }
        case Value.NUMERIC: {
            BigDecimal x = v.getBigDecimal();
            if (BigDecimal.ZERO.equals(x)) {
                buff.put(NUMERIC_0_1);
            } else if (BigDecimal.ONE.equals(x)) {
                buff.put((byte) (NUMERIC_0_1 + 1));
            } else {
                int scale = x.scale();
                BigInteger b = x.unscaledValue();
                int bits = b.bitLength();
                if (bits <= 63) {
                    if (scale == 0) {
                        buff.put(NUMERIC_SMALL_0).
                            putVarLong(b.longValue());
                    } else {
                        buff.put(NUMERIC_SMALL).
                            putVarInt(scale).
                            putVarLong(b.longValue());
                    }
                } else {
                    byte[] bytes = b.toByteArray();
                    buff.put(NUMERIC).
                        putVarInt(scale).
                        putVarInt(bytes.length).
                        put(bytes);
                }
            }
            break;
        }
        case Value.TIME: {
            ValueTime t = (ValueTime) v;
            long nanos = t.getNanos();
            long millis = nanos / 1_000_000;
            nanos -= millis * 1_000_000;
            buff.put(TIME).
                putVarLong(millis).
                putVarInt((int) nanos);
            break;
        }
        case Value.TIME_TZ: {
            ValueTimeTimeZone t = (ValueTimeTimeZone) v;
            long nanosOfDay = t.getNanos();
            buff.put((byte) TIME_TZ).
                putVarInt((int) (nanosOfDay / DateTimeUtils.NANOS_PER_SECOND)).
                putVarInt((int) (nanosOfDay % DateTimeUtils.NANOS_PER_SECOND));
            writeTimeZone(buff, t.getTimeZoneOffsetSeconds());
            break;
        }
        case Value.DATE: {
            long x = ((ValueDate) v).getDateValue();
            buff.put(DATE).putVarLong(x);
            break;
        }
        case Value.TIMESTAMP: {
            ValueTimestamp ts = (ValueTimestamp) v;
            long dateValue = ts.getDateValue();
            long nanos = ts.getTimeNanos();
            long millis = nanos / 1_000_000;
            nanos -= millis * 1_000_000;
            buff.put(TIMESTAMP).
                putVarLong(dateValue).
                putVarLong(millis).
                putVarInt((int) nanos);
            break;
        }
        case Value.TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) v;
            long dateValue = ts.getDateValue();
            long nanos = ts.getTimeNanos();
            long millis = nanos / 1_000_000;
            nanos -= millis * 1_000_000;
            int timeZoneOffset = ts.getTimeZoneOffsetSeconds();
            if (timeZoneOffset % 60 == 0) {
                buff.put(TIMESTAMP_TZ).
                    putVarLong(dateValue).
                    putVarLong(millis).
                    putVarInt((int) nanos).
                    putVarInt(timeZoneOffset / 60);
            } else {
                buff.put((byte) TIMESTAMP_TZ_2).
                    putVarLong(dateValue).
                    putVarLong(millis).
                    putVarInt((int) nanos);
                writeTimeZone(buff, timeZoneOffset);
            }
            break;
        }
        case Value.JAVA_OBJECT: {
            byte[] b = v.getBytesNoCopy();
            buff.put(JAVA_OBJECT).
                putVarInt(b.length).
                put(b);
            break;
        }
        case Value.VARBINARY: {
            byte[] b = v.getBytesNoCopy();
            int len = b.length;
            if (len < 32) {
                buff.put((byte) (VARBINARY_0_31 + len)).
                    put(b);
            } else {
                buff.put(VARBINARY).
                    putVarInt(len).
                    put(b);
            }
            break;
        }
        case Value.BINARY: {
            byte[] b = v.getBytesNoCopy();
            buff.put((byte) BINARY).
                putVarInt(b.length).
                put(b);
            break;
        }
        case Value.UUID: {
            ValueUuid uuid = (ValueUuid) v;
            buff.put(UUID).
                putLong(uuid.getHigh()).
                putLong(uuid.getLow());
            break;
        }
        case Value.VARCHAR: {
            String s = v.getString();
            int len = s.length();
            if (len < 32) {
                buff.put((byte) (VARCHAR_0_31 + len)).
                    putStringData(s, len);
            } else {
                buff.put(VARCHAR);
                writeString(buff, s);
            }
            break;
        }
        case Value.VARCHAR_IGNORECASE:
            buff.put(VARCHAR_IGNORECASE);
            writeString(buff, v.getString());
            break;
        case Value.CHAR:
            buff.put(CHAR);
            writeString(buff, v.getString());
            break;
        case Value.DOUBLE: {
            double x = v.getDouble();
            if (x == 1.0d) {
                buff.put((byte) (DOUBLE_0_1 + 1));
            } else {
                long d = Double.doubleToLongBits(x);
                if (d == ValueDouble.ZERO_BITS) {
                    buff.put(DOUBLE_0_1);
                } else {
                    buff.put(DOUBLE).
                        putVarLong(Long.reverse(d));
                }
            }
            break;
        }
        case Value.REAL: {
            float x = v.getFloat();
            if (x == 1.0f) {
                buff.put((byte) (REAL_0_1 + 1));
            } else {
                int f = Float.floatToIntBits(x);
                if (f == ValueReal.ZERO_BITS) {
                    buff.put(REAL_0_1);
                } else {
                    buff.put(REAL).
                        putVarInt(Integer.reverse(f));
                }
            }
            break;
        }
        case Value.BLOB:
        case Value.CLOB: {
            buff.put(type == Value.BLOB ? BLOB : CLOB);
            ValueLob lob = (ValueLob) v;
            if (lob instanceof ValueLobDatabase) {
                ValueLobDatabase lobDb = (ValueLobDatabase) lob;
                buff.putVarInt(-3).
                    putVarInt(lobDb.getTableId()).
                    putVarLong(lobDb.getLobId()).
                    putVarLong(lob.getType().getPrecision());
            } else {
                byte[] small = ((ValueLobInMemory)lob).getSmall();
                buff.putVarInt(small.length).
                    put(small);
            }
            break;
        }
        case Value.ARRAY:
            if (rowAsRow && rowFactory != null && v instanceof SearchRow) {
                SearchRow row = (SearchRow) v;
                int[] indexes = rowFactory.getIndexes();
                writeRow(buff, row, indexes);
                break;
            }
            //$FALL-THROUGH$
        case Value.ROW: {
            Value[] list = ((ValueCollectionBase) v).getList();
            buff.put(type == Value.ARRAY ? ARRAY : ROW)
                    .putVarInt(list.length);
            for (Value x : list) {
                writeValue(buff, x, false);
            }
            break;
        }
        case Value.RESULT_SET: {
            buff.put(RESULT_SET);
            ResultInterface result = v.getResult();
            int columnCount = result.getVisibleColumnCount();
            buff.putVarInt(columnCount);
            for (int i = 0; i < columnCount; i++) {
                writeString(buff, result.getAlias(i));
                writeString(buff, result.getColumnName(i));
                TypeInfo columnType = result.getColumnType(i);
                buff.putVarInt(columnType.getValueType()).
                    putVarLong(columnType.getPrecision()).
                    putVarInt(columnType.getScale());
            }
            while (result.next()) {
                buff.put((byte) 1);
                Value[] row = result.currentRow();
                for (int i = 0; i < columnCount; i++) {
                    writeValue(buff, row[i], false);
                }
            }
            buff.put((byte) 0);
            break;
        }
        case Value.GEOMETRY: {
            byte[] b = v.getBytes();
            int len = b.length;
            buff.put(GEOMETRY).
                putVarInt(len).
                put(b);
            break;
        }
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE: {
            ValueInterval interval = (ValueInterval) v;
            int ordinal = type - Value.INTERVAL_YEAR;
            if (interval.isNegative()) {
                ordinal = ~ordinal;
            }
            buff.put(INTERVAL).
                put((byte) ordinal).
                putVarLong(interval.getLeading());
            break;
        }
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND: {
            ValueInterval interval = (ValueInterval) v;
            int ordinal = type - Value.INTERVAL_YEAR;
            if (interval.isNegative()) {
                ordinal = ~ordinal;
            }
            buff.put(INTERVAL).
                put((byte) (ordinal)).
                putVarLong(interval.getLeading()).
                putVarLong(interval.getRemaining());
            break;
        }
        case Value.JSON:{
            byte[] b = v.getBytesNoCopy();
            buff.put((byte) JSON).putVarInt(b.length).put(b);
            break;
        }
        default:
            throw DbException.throwInternalError("type=" + v.getValueType());
        }
    }

    public void writeRow(WriteBuffer buff, SearchRow row, int[] indexes) {
        buff.put(ARRAY);
        if (indexes == null) {
            int columnCount = row.getColumnCount();
            buff.putVarInt(columnCount + 1);
            for (int i = 0; i < columnCount; i++) {
                writeValue(buff, row.getValue(i), false);
            }
        } else {
            buff.putVarInt(indexes.length + 1);
            for (int i : indexes) {
                writeValue(buff, row.getValue(i), false);
            }
        }
        writeValue(buff, ValueBigint.get(row.getKey()), false);
    }

    public static void writeLong(WriteBuffer buff, long x) {
        if (x < 0) {
            buff.put(BIGINT_NEG).putVarLong(-x);
        } else if (x < 8) {
            buff.put((byte) (BIGINT_0_7 + x));
        } else {
            buff.put(BIGINT).putVarLong(x);
        }
    }

    private static void writeString(WriteBuffer buff, String s) {
        int len = s.length();
        buff.putVarInt(len).putStringData(s, len);
    }

    private static void writeTimeZone(WriteBuffer buff, int timeZoneOffset) {
        // Valid JSR-310 offsets are -64,800..64,800
        // Use 1 byte for common time zones (including +8:45 etc.)
        if (timeZoneOffset % 900 == 0) {
            // -72..72
            buff.put((byte) (timeZoneOffset / 900));
        } else if (timeZoneOffset > 0) {
            buff.put(Byte.MAX_VALUE).putVarInt(timeZoneOffset);
        } else {
            buff.put(Byte.MIN_VALUE).putVarInt(-timeZoneOffset);
        }
    }

    /**
     * Read a value.
     *
     * @return the value
     */
    private Value readValue(ByteBuffer buff, boolean rowAsRow) {
        int type = buff.get() & 255;
        switch (type) {
        case NULL:
            return ValueNull.INSTANCE;
        case BOOLEAN_TRUE:
            return ValueBoolean.TRUE;
        case BOOLEAN_FALSE:
            return ValueBoolean.FALSE;
        case INT_NEG:
            return ValueInteger.get(-readVarInt(buff));
        case ENUM:
        case INTEGER:
            return ValueInteger.get(readVarInt(buff));
        case BIGINT_NEG:
            return ValueBigint.get(-readVarLong(buff));
        case BIGINT:
            return ValueBigint.get(readVarLong(buff));
        case TINYINT:
            return ValueTinyint.get(buff.get());
        case SMALLINT:
            return ValueSmallint.get(buff.getShort());
        case NUMERIC_0_1:
            return ValueNumeric.ZERO;
        case NUMERIC_0_1 + 1:
            return ValueNumeric.ONE;
        case NUMERIC_SMALL_0:
            return ValueNumeric.get(BigDecimal.valueOf(
                    readVarLong(buff)));
        case NUMERIC_SMALL: {
            int scale = readVarInt(buff);
            return ValueNumeric.get(BigDecimal.valueOf(
                    readVarLong(buff), scale));
        }
        case NUMERIC: {
            int scale = readVarInt(buff);
            int len = readVarInt(buff);
            byte[] buff2 = Utils.newBytes(len);
            buff.get(buff2, 0, len);
            BigInteger b = new BigInteger(buff2);
            return ValueNumeric.get(new BigDecimal(b, scale));
        }
        case DATE: {
            return ValueDate.fromDateValue(readVarLong(buff));
        }
        case TIME: {
            long nanos = readVarLong(buff) * 1_000_000 + readVarInt(buff);
            return ValueTime.fromNanos(nanos);
        }
        case TIME_TZ:
            return ValueTimeTimeZone.fromNanos(readVarInt(buff) * DateTimeUtils.NANOS_PER_SECOND + readVarInt(buff),
                    readTimeZone(buff));
        case TIMESTAMP: {
            long dateValue = readVarLong(buff);
            long nanos = readVarLong(buff) * 1_000_000 + readVarInt(buff);
            return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
        }
        case TIMESTAMP_TZ: {
            long dateValue = readVarLong(buff);
            long nanos = readVarLong(buff) * 1_000_000 + readVarInt(buff);
            int tz = readVarInt(buff) * 60;
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, nanos, tz);
        }
        case TIMESTAMP_TZ_2: {
            long dateValue = readVarLong(buff);
            long nanos = readVarLong(buff) * 1_000_000 + readVarInt(buff);
            int tz = readTimeZone(buff);
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, nanos, tz);
        }
        case VARBINARY: {
            int len = readVarInt(buff);
            byte[] b = Utils.newBytes(len);
            buff.get(b, 0, len);
            return ValueVarbinary.getNoCopy(b);
        }
        case BINARY: {
            int len = readVarInt(buff);
            byte[] b = Utils.newBytes(len);
            buff.get(b, 0, len);
            return ValueBinary.getNoCopy(b);
        }
        case JAVA_OBJECT: {
            int len = readVarInt(buff);
            byte[] b = Utils.newBytes(len);
            buff.get(b, 0, len);
            return ValueJavaObject.getNoCopy(b);
        }
        case UUID:
            return ValueUuid.get(buff.getLong(), buff.getLong());
        case VARCHAR:
            return ValueVarchar.get(readString(buff));
        case VARCHAR_IGNORECASE:
            return ValueVarcharIgnoreCase.get(readString(buff));
        case CHAR:
            return ValueChar.get(readString(buff));
        case INTERVAL: {
            int ordinal = buff.get();
            boolean negative = ordinal < 0;
            if (negative) {
                ordinal = ~ordinal;
            }
            return ValueInterval.from(IntervalQualifier.valueOf(ordinal), negative, readVarLong(buff),
                    ordinal < 5 ? 0 : readVarLong(buff));
        }
        case REAL_0_1:
            return ValueReal.ZERO;
        case REAL_0_1 + 1:
            return ValueReal.ONE;
        case DOUBLE_0_1:
            return ValueDouble.ZERO;
        case DOUBLE_0_1 + 1:
            return ValueDouble.ONE;
        case DOUBLE:
            return ValueDouble.get(Double.longBitsToDouble(Long.reverse(readVarLong(buff))));
        case REAL:
            return ValueReal.get(Float.intBitsToFloat(Integer.reverse(readVarInt(buff))));
        case BLOB:
        case CLOB: {
            int smallLen = readVarInt(buff);
            if (smallLen >= 0) {
                byte[] small = Utils.newBytes(smallLen);
                buff.get(small, 0, smallLen);
                return ValueLobInMemory.createSmallLob(type == BLOB ? Value.BLOB : Value.CLOB, small);
            } else if (smallLen == -3) {
                int tableId = readVarInt(buff);
                long lobId = readVarLong(buff);
                long precision = readVarLong(buff);
                return ValueLobDatabase.create(type == BLOB ? Value.BLOB : Value.CLOB,
                        handler, tableId, lobId, precision);
            } else {
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1,
                        "lob type: " + smallLen);
            }
        }
        case ARRAY:
            if (rowFactory != null && rowAsRow) {
                int valueCount = readVarInt(buff) - 1;
                SearchRow row = rowFactory.createRow();
                int[] indexes = rowFactory.getIndexes();
                boolean hasRowKey;
                if (indexes == null) {
                    int columnCount = row.getColumnCount();
                    for (int i = 0; i < columnCount; i++) {
                        row.setValue(i, readValue(buff, false));
                    }
                    hasRowKey = valueCount == columnCount;
                } else {
                    for (int i : indexes) {
                        row.setValue(i, readValue(buff, false));
                    }
                    hasRowKey = valueCount == indexes.length;
                }
                if (hasRowKey) {
                    row.setKey(readValue(buff, false).getLong());
                }
                return row;
            }
            //$FALL-THROUGH$
        case ROW: {
            int len = readVarInt(buff);
            Value[] list = new Value[len];
            for (int i = 0; i < len; i++) {
                list[i] = readValue(buff, false);
            }
            return type == ARRAY && !rowAsRow ? ValueArray.get(list, provider) : ValueRow.get(list);
        }
        case RESULT_SET: {
            SimpleResult rs = new SimpleResult();
            int columns = readVarInt(buff);
            for (int i = 0; i < columns; i++) {
                rs.addColumn(readString(buff), readString(buff), readVarInt(buff), readVarLong(buff),
                        readVarInt(buff));
            }
            while (buff.get() != 0) {
                Value[] o = new Value[columns];
                for (int i = 0; i < columns; i++) {
                    o[i] = readValue(buff, false);
                }
                rs.addRow(o);
            }
            return ValueResultSet.get(rs);
        }
        case GEOMETRY: {
            int len = readVarInt(buff);
            byte[] b = Utils.newBytes(len);
            buff.get(b, 0, len);
            return ValueGeometry.get(b);
        }
        case SPATIAL_KEY_2D:
            return getSpatialDataType().read(buff);
        case JSON: {
            int len = readVarInt(buff);
            byte[] b = Utils.newBytes(len);
            buff.get(b, 0, len);
            return ValueJson.getInternal(b);
        }
        default:
            if (type >= INT_0_15 && type < INT_0_15 + 16) {
                return ValueInteger.get(type - INT_0_15);
            } else if (type >= BIGINT_0_7 && type < BIGINT_0_7 + 8) {
                return ValueBigint.get(type - BIGINT_0_7);
            } else if (type >= VARBINARY_0_31 && type < VARBINARY_0_31 + 32) {
                int len = type - VARBINARY_0_31;
                byte[] b = Utils.newBytes(len);
                buff.get(b, 0, len);
                return ValueVarbinary.getNoCopy(b);
            } else if (type >= VARCHAR_0_31 && type < VARCHAR_0_31 + 32) {
                return ValueVarchar.get(readString(buff, type - VARCHAR_0_31));
            }
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "type: " + type);
        }
    }

    private static int readTimeZone(ByteBuffer buff) {
        byte b = buff.get();
        if (b == Byte.MAX_VALUE) {
            return readVarInt(buff);
        } else if (b == Byte.MIN_VALUE) {
            return -readVarInt(buff);
        } else {
            return b * 900;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof ValueDataType)) {
            return false;
        }
        ValueDataType v = (ValueDataType) obj;
        if (!compareMode.equals(v.compareMode)) {
            return false;
        }
        int[] indexes = rowFactory == null ? null : rowFactory.getIndexes();
        int[] indexes2 = v.rowFactory == null ? null : v.rowFactory.getIndexes();
        return Arrays.equals(sortTypes, v.sortTypes)
            && Arrays.equals(indexes, indexes2);
    }

    @Override
    public int hashCode() {
        int[] indexes = rowFactory == null ? null : rowFactory.getIndexes();
        return super.hashCode() ^ Arrays.hashCode(indexes)
            ^ compareMode.hashCode() ^ Arrays.hashCode(sortTypes);
    }

    @Override
    public void save(WriteBuffer buff, MetaType<Database> metaType) {
        writeIntArray(buff, sortTypes);
        int columnCount = rowFactory == null ? 0 : rowFactory.getColumnCount();
        buff.putVarInt(columnCount);
        int[] indexes = rowFactory == null ? null : rowFactory.getIndexes();
        writeIntArray(buff, indexes);
    }

    private static void writeIntArray(WriteBuffer buff, int[] array) {
        if(array == null) {
            buff.putVarInt(0);
        } else {
            buff.putVarInt(array.length + 1);
            for (int i : array) {
                buff.putVarInt(i);
            }
        }
    }

    @Override
    public Factory getFactory() {
        return FACTORY;
    }



    private static final Factory FACTORY = new Factory();

    public static final class Factory implements StatefulDataType.Factory<Database> {

        @Override
        public DataType<?> create(ByteBuffer buff, MetaType<Database> metaType, Database database) {
            int[] sortTypes = readIntArray(buff);
            int columnCount = DataUtils.readVarInt(buff);
            int[] indexes = readIntArray(buff);
            CompareMode compareMode = database == null ? CompareMode.getInstance(null, 0) : database.getCompareMode();
            Mode mode = database == null ? Mode.getRegular() : database.getMode();
            if (database == null) {
                return new ValueDataType();
            } else if (sortTypes == null) {
                return new ValueDataType(database, null);
            }
            RowFactory rowFactory = RowFactory.getDefaultRowFactory()
                    .createRowFactory(database, compareMode, mode, database, sortTypes, indexes, columnCount);
            return rowFactory.getRowDataType();
        }

        private static int[] readIntArray(ByteBuffer buff) {
            int len = DataUtils.readVarInt(buff) - 1;
            if(len < 0) {
                return null;
            }
            int[] res = new int[len];
            for (int i = 0; i < res.length; i++) {
                res[i] = DataUtils.readVarInt(buff);
            }
            return res;
        }
    }

}
