/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.type.DataType;
import org.h2.result.SortOrder;
import org.h2.store.Data;
import org.h2.store.DataHandler;
import org.h2.store.LobStorageFrontend;
import org.h2.store.LobStorageInterface;
import org.h2.tools.SimpleResultSet;
import org.h2.util.DateTimeUtils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueInt;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueLob;
import org.h2.value.ValueLobDb;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueResultSet;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueStringFixed;
import org.h2.value.ValueStringIgnoreCase;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;

/**
 * A row type.
 */
public class ValueDataType implements DataType {

    static final String PREFIX = ValueDataType.class.getName();

    private static final int INT_0_15 = 32;
    private static final int LONG_0_7 = 48;
    private static final int DECIMAL_0_1 = 56;
    private static final int DECIMAL_SMALL_0 = 58;
    private static final int DECIMAL_SMALL = 59;
    private static final int DOUBLE_0_1 = 60;
    private static final int FLOAT_0_1 = 62;
    private static final int BOOLEAN_FALSE = 64;
    private static final int BOOLEAN_TRUE = 65;
    private static final int INT_NEG = 66;
    private static final int LONG_NEG = 67;
    private static final int STRING_0_31 = 68;
    private static final int BYTES_0_31 = 100;
    private static final int LOCAL_TIME = 132;
    private static final int LOCAL_DATE = 133;
    private static final int LOCAL_TIMESTAMP = 134;

    private static final long MILLIS_PER_MINUTE = 1000 * 60;

    final DataHandler handler;
    final CompareMode compareMode;
    final int[] sortTypes;

    public ValueDataType(CompareMode compareMode, DataHandler handler, int[] sortTypes) {
        this.compareMode = compareMode;
        this.handler = handler;
        this.sortTypes = sortTypes;
    }

    @Override
    public int compare(Object a, Object b) {
        if (a == b) {
            return 0;
        }
        if (a instanceof ValueArray && b instanceof ValueArray) {
            Value[] ax = ((ValueArray) a).getList();
            Value[] bx = ((ValueArray) b).getList();
            int al = ax.length;
            int bl = bx.length;
            int len = Math.min(al, bl);
            for (int i = 0; i < len; i++) {
                int comp = compareValues(ax[i], bx[i], sortTypes[i]);
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
        return compareValues((Value) a, (Value) b, SortOrder.ASCENDING);
    }

    private int compareValues(Value a, Value b, int sortType) {
        if (a == b) {
            return 0;
        }
        boolean aNull = a == null, bNull = b == null;
        if (aNull || bNull) {
            return SortOrder.compareNull(aNull, sortType);
        }
        int comp = compareTypeSave(a, b);
        if ((sortType & SortOrder.DESCENDING) != 0) {
            comp = -comp;
        }
        return comp;
    }

    private int compareTypeSave(Value a, Value b) {
        if (a == b) {
            return 0;
        }
        int dataType = Value.getHigherOrder(a.getType(), b.getType());
        a = a.convertTo(dataType);
        b = b.convertTo(dataType);
        return a.compareTypeSave(b, compareMode);
    }

    @Override
    public int getMemory(Object obj) {
        return getMemory((Value) obj);
    }

    private static int getMemory(Value v) {
        return v == null ? 0 : v.getMemory();
    }

    @Override
    public Value read(ByteBuffer buff) {
        return readValue(buff);
    }

    @Override
    public ByteBuffer write(ByteBuffer buff, Object obj) {
        Value x = (Value) obj;
        buff = DataUtils.ensureCapacity(buff, 0);
        buff = writeValue(buff, x);
        return buff;
    }

    private ByteBuffer writeValue(ByteBuffer buff, Value v) {
        int start = buff.position();
        if (v == ValueNull.INSTANCE) {
            buff.put((byte) 0);
            return buff;
        }
        int type = v.getType();
        switch (type) {
        case Value.BOOLEAN:
            buff.put((byte) (v.getBoolean().booleanValue() ? BOOLEAN_TRUE : BOOLEAN_FALSE));
            break;
        case Value.BYTE:
            buff.put((byte) type);
            buff.put(v.getByte());
            break;
        case Value.SHORT:
            buff.put((byte) type);
            buff.putShort(v.getShort());
            break;
        case Value.INT: {
            int x = v.getInt();
            if (x < 0) {
                buff.put((byte) INT_NEG);
                writeVarInt(buff, -x);
            } else if (x < 16) {
                buff.put((byte) (INT_0_15 + x));
            } else {
                buff.put((byte) type);
                writeVarInt(buff, x);
            }
            break;
        }
        case Value.LONG: {
            long x = v.getLong();
            if (x < 0) {
                buff.put((byte) LONG_NEG);
                writeVarLong(buff, -x);
            } else if (x < 8) {
                buff.put((byte) (LONG_0_7 + x));
            } else {
                buff.put((byte) type);
                writeVarLong(buff, x);
            }
            break;
        }
        case Value.DECIMAL: {
            BigDecimal x = v.getBigDecimal();
            if (BigDecimal.ZERO.equals(x)) {
                buff.put((byte) DECIMAL_0_1);
            } else if (BigDecimal.ONE.equals(x)) {
                buff.put((byte) (DECIMAL_0_1 + 1));
            } else {
                int scale = x.scale();
                BigInteger b = x.unscaledValue();
                int bits = b.bitLength();
                if (bits <= 63) {
                    if (scale == 0) {
                        buff.put((byte) DECIMAL_SMALL_0);
                        writeVarLong(buff, b.longValue());
                    } else {
                        buff.put((byte) DECIMAL_SMALL);
                        writeVarInt(buff, scale);
                        writeVarLong(buff, b.longValue());
                    }
                } else {
                    buff.put((byte) type);
                    writeVarInt(buff, scale);
                    byte[] bytes = b.toByteArray();
                    writeVarInt(buff, bytes.length);
                    buff = DataUtils.ensureCapacity(buff, bytes.length);
                    buff.put(bytes, 0, bytes.length);
                }
            }
            break;
        }
        case Value.TIME:
            if (SysProperties.STORE_LOCAL_TIME) {
                buff.put((byte) LOCAL_TIME);
                ValueTime t = (ValueTime) v;
                long nanos = t.getNanos();
                long millis = nanos / 1000000;
                nanos -= millis * 1000000;
                writeVarLong(buff, millis);
                writeVarLong(buff, nanos);
            } else {
                buff.put((byte) type);
                writeVarLong(buff, DateTimeUtils.getTimeLocalWithoutDst(v.getTime()));
            }
            break;
        case Value.DATE: {
            if (SysProperties.STORE_LOCAL_TIME) {
                buff.put((byte) LOCAL_DATE);
                long x = ((ValueDate) v).getDateValue();
                writeVarLong(buff, x);
            } else {
                buff.put((byte) type);
                long x = DateTimeUtils.getTimeLocalWithoutDst(v.getDate());
                writeVarLong(buff, x / MILLIS_PER_MINUTE);
            }
            break;
        }
        case Value.TIMESTAMP: {
            if (SysProperties.STORE_LOCAL_TIME) {
                buff.put((byte) LOCAL_TIMESTAMP);
                ValueTimestamp ts = (ValueTimestamp) v;
                long dateValue = ts.getDateValue();
                writeVarLong(buff, dateValue);
                long nanos = ts.getNanos();
                long millis = nanos / 1000000;
                nanos -= millis * 1000000;
                writeVarLong(buff, millis);
                writeVarLong(buff, nanos);
            } else {
                Timestamp ts = v.getTimestamp();
                buff.put((byte) type);
                writeVarLong(buff, DateTimeUtils.getTimeLocalWithoutDst(ts));
                writeVarInt(buff, ts.getNanos());
            }
            break;
        }
        case Value.JAVA_OBJECT: {
            buff.put((byte) type);
            byte[] b = v.getBytesNoCopy();
            writeVarInt(buff, b.length);
            buff = DataUtils.ensureCapacity(buff, b.length);
            buff.put(b, 0, b.length);
            break;
        }
        case Value.BYTES: {
            byte[] b = v.getBytesNoCopy();
            int len = b.length;
            if (len < 32) {
                buff.put((byte) (BYTES_0_31 + len));
                buff.put(b, 0, b.length);
            } else {
                buff.put((byte) type);
                writeVarInt(buff, b.length);
                buff = DataUtils.ensureCapacity(buff, b.length);
                buff.put(b, 0, b.length);
            }
            break;
        }
        case Value.UUID: {
            buff.put((byte) type);
            ValueUuid uuid = (ValueUuid) v;
            buff.putLong(uuid.getHigh());
            buff.putLong(uuid.getLow());
            break;
        }
        case Value.STRING: {
            String s = v.getString();
            int len = s.length();
            if (len < 32) {
                buff.put((byte) (STRING_0_31 + len));
                buff = writeStringWithoutLength(buff, s, len);
            } else {
                buff.put((byte) type);
                buff = writeString(buff, s);
            }
            break;
        }
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            buff.put((byte) type);
            buff = writeString(buff, v.getString());
            break;
        case Value.DOUBLE: {
            double x = v.getDouble();
            if (x == 1.0d) {
                buff.put((byte) (DOUBLE_0_1 + 1));
            } else {
                long d = Double.doubleToLongBits(x);
                if (d == ValueDouble.ZERO_BITS) {
                    buff.put((byte) DOUBLE_0_1);
                } else {
                    buff.put((byte) type);
                    writeVarLong(buff, Long.reverse(d));
                }
            }
            break;
        }
        case Value.FLOAT: {
            float x = v.getFloat();
            if (x == 1.0f) {
                buff.put((byte) (FLOAT_0_1 + 1));
            } else {
                int f = Float.floatToIntBits(x);
                if (f == ValueFloat.ZERO_BITS) {
                    buff.put((byte) FLOAT_0_1);
                } else {
                    buff.put((byte) type);
                    writeVarInt(buff, Integer.reverse(f));
                }
            }
            break;
        }
        case Value.BLOB:
        case Value.CLOB: {
            buff.put((byte) type);
            if (v instanceof ValueLob) {
                ValueLob lob = (ValueLob) v;
                lob.convertToFileIfRequired(handler);
                byte[] small = lob.getSmall();
                if (small == null) {
                    int t = -1;
                    if (!lob.isLinked()) {
                        t = -2;
                    }
                    writeVarInt(buff, t);
                    writeVarInt(buff, lob.getTableId());
                    writeVarInt(buff, lob.getObjectId());
                    writeVarLong(buff, lob.getPrecision());
                    buff.put((byte) (lob.useCompression() ? 1 : 0));
                    if (t == -2) {
                        buff = writeString(buff, lob.getFileName());
                    }
                } else {
                    writeVarInt(buff, small.length);
                    buff = DataUtils.ensureCapacity(buff, small.length);
                    buff.put(small, 0, small.length);
                }
            } else {
                ValueLobDb lob = (ValueLobDb) v;
                byte[] small = lob.getSmall();
                if (small == null) {
                    writeVarInt(buff, -3);
                    writeVarInt(buff, lob.getTableId());
                    writeVarLong(buff, lob.getLobId());
                    writeVarLong(buff, lob.getPrecision());
                } else {
                    writeVarInt(buff, small.length);
                    buff = DataUtils.ensureCapacity(buff, small.length);
                    buff.put(small, 0, small.length);
                }
            }
            break;
        }
        case Value.ARRAY: {
            buff.put((byte) type);
            Value[] list = ((ValueArray) v).getList();
            writeVarInt(buff, list.length);
            for (Value x : list) {
                buff = DataUtils.ensureCapacity(buff, 0);
                buff = writeValue(buff, x);
            }
            break;
        }
        case Value.RESULT_SET: {
            buff.put((byte) type);
            try {
                ResultSet rs = ((ValueResultSet) v).getResultSet();
                rs.beforeFirst();
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                writeVarInt(buff, columnCount);
                for (int i = 0; i < columnCount; i++) {
                    buff = DataUtils.ensureCapacity(buff, 0);
                    buff = writeString(buff, meta.getColumnName(i + 1));
                    writeVarInt(buff, meta.getColumnType(i + 1));
                    writeVarInt(buff, meta.getPrecision(i + 1));
                    writeVarInt(buff, meta.getScale(i + 1));
                }
                while (rs.next()) {
                    buff.put((byte) 1);
                    for (int i = 0; i < columnCount; i++) {
                        int t = org.h2.value.DataType.convertSQLTypeToValueType(meta.getColumnType(i + 1));
                        Value val = org.h2.value.DataType.readValue(null, rs, i + 1, t);
                        buff = writeValue(buff, val);
                    }
                }
                buff.put((byte) 0);
                rs.beforeFirst();
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
            break;
        }
        case Value.GEOMETRY: {
            buff.put((byte) type);
            byte[] b = v.getBytes();
            int len = b.length;
            writeVarInt(buff, len);
            buff = DataUtils.ensureCapacity(buff, len);
            buff.put(b, 0, len);
            break;
        }
        default:
            DbException.throwInternalError("type=" + v.getType());
        }
        if (SysProperties.CHECK2) {
            if (buff.position() - start != Data.getValueLen(v, handler)) {
                throw DbException
                        .throwInternalError("value size error: got " + (buff.position() - start) + " expected " + Data.getValueLen(v, handler));
            }
        }
        return buff;
    }

    private static void writeVarInt(ByteBuffer buff, int x) {
        while ((x & ~0x7f) != 0) {
            buff.put((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        buff.put((byte) x);
    }

    private static void writeVarLong(ByteBuffer buff, long x) {
        while ((x & ~0x7f) != 0) {
            buff.put((byte) ((x & 0x7f) | 0x80));
            x >>>= 7;
        }
        buff.put((byte) x);
    }

    private static ByteBuffer writeString(ByteBuffer buff, String s) {
        int len = s.length();
        writeVarInt(buff, len);
        return writeStringWithoutLength(buff, s, len);
    }

    private static ByteBuffer writeStringWithoutLength(ByteBuffer buff, String s, int len) {
        buff = DataUtils.ensureCapacity(buff, 3 * len);
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (c < 0x80) {
                buff.put((byte) c);
            } else if (c >= 0x800) {
                buff.put((byte) (0xe0 | (c >> 12)));
                buff.put((byte) (((c >> 6) & 0x3f)));
                buff.put((byte) (c & 0x3f));
            } else {
                buff.put((byte) (0xc0 | (c >> 6)));
                buff.put((byte) (c & 0x3f));
            }
        }
        return buff;
    }

    /**
     * Read a value.
     *
     * @return the value
     */
    private Value readValue(ByteBuffer buff) {
        int type = buff.get() & 255;
        switch (type) {
        case Value.NULL:
            return ValueNull.INSTANCE;
        case BOOLEAN_TRUE:
            return ValueBoolean.get(true);
        case BOOLEAN_FALSE:
            return ValueBoolean.get(false);
        case INT_NEG:
            return ValueInt.get(-readVarInt(buff));
        case Value.INT:
            return ValueInt.get(readVarInt(buff));
        case LONG_NEG:
            return ValueLong.get(-readVarLong(buff));
        case Value.LONG:
            return ValueLong.get(readVarLong(buff));
        case Value.BYTE:
            return ValueByte.get(buff.get());
        case Value.SHORT:
            return ValueShort.get(buff.getShort());
        case DECIMAL_0_1:
            return (ValueDecimal) ValueDecimal.ZERO;
        case DECIMAL_0_1 + 1:
            return (ValueDecimal) ValueDecimal.ONE;
        case DECIMAL_SMALL_0:
            return ValueDecimal.get(BigDecimal.valueOf(readVarLong(buff)));
        case DECIMAL_SMALL: {
            int scale = readVarInt(buff);
            return ValueDecimal.get(BigDecimal.valueOf(readVarLong(buff), scale));
        }
        case Value.DECIMAL: {
            int scale = readVarInt(buff);
            int len = readVarInt(buff);
            byte[] buff2 = DataUtils.newBytes(len);
            buff.get(buff2, 0, len);
            BigInteger b = new BigInteger(buff2);
            return ValueDecimal.get(new BigDecimal(b, scale));
        }
        case LOCAL_DATE: {
            return ValueDate.fromDateValue(readVarLong(buff));
        }
        case Value.DATE: {
            long x = readVarLong(buff) * MILLIS_PER_MINUTE;
            return ValueDate.get(new Date(DateTimeUtils.getTimeUTCWithoutDst(x)));
        }
        case LOCAL_TIME: {
            long nanos = readVarLong(buff) * 1000000 + readVarLong(buff);
            return ValueTime.fromNanos(nanos);
        }
        case Value.TIME:
            // need to normalize the year, month and day
            return ValueTime.get(new Time(DateTimeUtils.getTimeUTCWithoutDst(readVarLong(buff))));
        case LOCAL_TIMESTAMP: {
            long dateValue = readVarLong(buff);
            long nanos = readVarLong(buff) * 1000000 + readVarLong(buff);
            return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
        }
        case Value.TIMESTAMP: {
            Timestamp ts = new Timestamp(DateTimeUtils.getTimeUTCWithoutDst(readVarLong(buff)));
            ts.setNanos(readVarInt(buff));
            return ValueTimestamp.get(ts);
        }
        case Value.BYTES: {
            int len = readVarInt(buff);
            byte[] b = DataUtils.newBytes(len);
            buff.get(b, 0, len);
            return ValueBytes.getNoCopy(b);
        }
        case Value.JAVA_OBJECT: {
            int len = readVarInt(buff);
            byte[] b = DataUtils.newBytes(len);
            buff.get(b, 0, len);
            return ValueJavaObject.getNoCopy(null, b);
        }
        case Value.UUID:
            return ValueUuid.get(buff.getLong(), buff.getLong());
        case Value.STRING:
            return ValueString.get(readString(buff));
        case Value.STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(readString(buff));
        case Value.STRING_FIXED:
            return ValueStringFixed.get(readString(buff));
        case FLOAT_0_1:
            return ValueFloat.get(0);
        case FLOAT_0_1 + 1:
            return ValueFloat.get(1);
        case DOUBLE_0_1:
            return ValueDouble.get(0);
        case DOUBLE_0_1 + 1:
            return ValueDouble.get(1);
        case Value.DOUBLE:
            return ValueDouble.get(Double.longBitsToDouble(Long.reverse(readVarLong(buff))));
        case Value.FLOAT:
            return ValueFloat.get(Float.intBitsToFloat(Integer.reverse(readVarInt(buff))));
        case Value.BLOB:
        case Value.CLOB: {
            int smallLen = readVarInt(buff);
            if (smallLen >= 0) {
                byte[] small = DataUtils.newBytes(smallLen);
                buff.get(small, 0, smallLen);
                return LobStorageFrontend.createSmallLob(type, small);
            } else if (smallLen == -3) {
                int tableId = readVarInt(buff);
                long lobId = readVarLong(buff);
                long precision = readVarLong(buff);
                LobStorageInterface lobStorage = handler.getLobStorage();
                ValueLobDb lob = ValueLobDb.create(type, lobStorage, tableId, lobId, null, precision);
                return lob;
            } else {
                int tableId = readVarInt(buff);
                int objectId = readVarInt(buff);
                long precision = 0;
                boolean compression = false;
                // -1: regular
                // -2: regular, but not linked (in this case: including file name)
                if (smallLen == -1 || smallLen == -2) {
                    precision = readVarLong(buff);
                    compression = buff.get() == 1;
                }
                if (smallLen == -2) {
                    String filename = readString(buff);
                    return ValueLob.openUnlinked(type, handler, tableId, objectId, precision, compression, filename);
                }
                ValueLob lob = ValueLob.openLinked(type, handler, tableId, objectId, precision, compression);
                return lob;
            }
        }
        case Value.ARRAY: {
            int len = readVarInt(buff);
            Value[] list = new Value[len];
            for (int i = 0; i < len; i++) {
                list[i] = readValue(buff);
            }
            return ValueArray.get(list);
        }
        case Value.RESULT_SET: {
            SimpleResultSet rs = new SimpleResultSet();
            int columns = readVarInt(buff);
            for (int i = 0; i < columns; i++) {
                rs.addColumn(readString(buff), readVarInt(buff), readVarInt(buff), readVarInt(buff));
            }
            while (true) {
                if (buff.get() == 0) {
                    break;
                }
                Object[] o = new Object[columns];
                for (int i = 0; i < columns; i++) {
                    o[i] = readValue(buff).getObject();
                }
                rs.addRow(o);
            }
            return ValueResultSet.get(rs);
        }
        case Value.GEOMETRY: {
            int len = readVarInt(buff);
            byte[] b = DataUtils.newBytes(len);
            buff.get(b, 0, len);
            return ValueGeometry.get(b);
        }
        default:
            if (type >= INT_0_15 && type < INT_0_15 + 16) {
                return ValueInt.get(type - INT_0_15);
            } else if (type >= LONG_0_7 && type < LONG_0_7 + 8) {
                return ValueLong.get(type - LONG_0_7);
            } else if (type >= BYTES_0_31 && type < BYTES_0_31 + 32) {
                int len = type - BYTES_0_31;
                byte[] b = DataUtils.newBytes(len);
                buff.get(b, 0, len);
                return ValueBytes.getNoCopy(b);
            } else if (type >= STRING_0_31 && type < STRING_0_31 + 32) {
                return ValueString.get(readString(buff, type - STRING_0_31));
            }
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "type: " + type);
        }
    }

    private static int readVarInt(ByteBuffer buff) {
        return DataUtils.readVarInt(buff);
    }

    private static long readVarLong(ByteBuffer buff) {
        return DataUtils.readVarLong(buff);
    }

    private static String readString(ByteBuffer buff, int len) {
        return DataUtils.readString(buff, len);
    }

    private static String readString(ByteBuffer buff) {
        int len = readVarInt(buff);
        return DataUtils.readString(buff, len);
    }

}
