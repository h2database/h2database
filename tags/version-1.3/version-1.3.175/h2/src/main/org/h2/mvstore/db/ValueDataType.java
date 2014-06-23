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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.rtree.SpatialDataType;
import org.h2.mvstore.rtree.SpatialKey;
import org.h2.mvstore.type.DataType;
import org.h2.result.SortOrder;
import org.h2.store.DataHandler;
import org.h2.store.LobStorageFrontend;
import org.h2.tools.SimpleResultSet;
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
    private static final int SPATIAL_KEY_2D = 132;

    final DataHandler handler;
    final CompareMode compareMode;
    final int[] sortTypes;
    final SpatialDataType spatialType = new SpatialDataType(2);

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
                int sortType = sortTypes[i];
                int comp = compareValues(ax[i], bx[i], sortType);
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
        return a.compareTypeSave(b, compareMode);
    }

    @Override
    public int getMemory(Object obj) {
        if (obj instanceof SpatialKey) {
            return spatialType.getMemory(obj);
        }
        return getMemory((Value) obj);
    }

    private static int getMemory(Value v) {
        return v == null ? 0 : v.getMemory();
    }

    @Override
    public Object read(ByteBuffer buff) {
        return readValue(buff);
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        if (obj instanceof SpatialKey) {
            buff.put((byte) SPATIAL_KEY_2D);
            spatialType.write(buff, obj);
            return;
        }
        Value x = (Value) obj;
        writeValue(buff, x);
    }

    private void writeValue(WriteBuffer buff, Value v) {
        if (v == ValueNull.INSTANCE) {
            buff.put((byte) 0);
            return;
        }
        int type = v.getType();
        switch (type) {
        case Value.BOOLEAN:
            buff.put((byte) (v.getBoolean().booleanValue() ? BOOLEAN_TRUE : BOOLEAN_FALSE));
            break;
        case Value.BYTE:
            buff.put((byte) type).put(v.getByte());
            break;
        case Value.SHORT:
            buff.put((byte) type).putShort(v.getShort());
            break;
        case Value.INT: {
            int x = v.getInt();
            if (x < 0) {
                buff.put((byte) INT_NEG).putVarInt(-x);
            } else if (x < 16) {
                buff.put((byte) (INT_0_15 + x));
            } else {
                buff.put((byte) type).putVarInt(x);
            }
            break;
        }
        case Value.LONG: {
            long x = v.getLong();
            if (x < 0) {
                buff.put((byte) LONG_NEG).putVarLong(-x);
            } else if (x < 8) {
                buff.put((byte) (LONG_0_7 + x));
            } else {
                buff.put((byte) type).putVarLong(x);
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
                        buff.put((byte) DECIMAL_SMALL_0).
                            putVarLong(b.longValue());
                    } else {
                        buff.put((byte) DECIMAL_SMALL).
                            putVarInt(scale).
                            putVarLong(b.longValue());
                    }
                } else {
                    byte[] bytes = b.toByteArray();
                    buff.put((byte) type).
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
            long millis = nanos / 1000000;
            nanos -= millis * 1000000;
            buff.put((byte) type).
                putVarLong(millis).
                putVarLong(nanos);
            break;
        }
        case Value.DATE: {
            long x = ((ValueDate) v).getDateValue();
            buff.put((byte) type).putVarLong(x);
            break;
        }
        case Value.TIMESTAMP: {
            ValueTimestamp ts = (ValueTimestamp) v;
            long dateValue = ts.getDateValue();
            long nanos = ts.getNanos();
            long millis = nanos / 1000000;
            nanos -= millis * 1000000;
            buff.put((byte) type).
                putVarLong(dateValue).
                putVarLong(millis).
                putVarLong(nanos);
            break;
        }
        case Value.JAVA_OBJECT: {
            byte[] b = v.getBytesNoCopy();
            buff.put((byte) type).
                putVarInt(b.length).
                put(b);
            break;
        }
        case Value.BYTES: {
            byte[] b = v.getBytesNoCopy();
            int len = b.length;
            if (len < 32) {
                buff.put((byte) (BYTES_0_31 + len)).
                    put(b);
            } else {
                buff.put((byte) type).
                    putVarInt(b.length).
                    put(b);
            }
            break;
        }
        case Value.UUID: {
            ValueUuid uuid = (ValueUuid) v;
            buff.put((byte) type).
                putLong(uuid.getHigh()).
                putLong(uuid.getLow());
            break;
        }
        case Value.STRING: {
            String s = v.getString();
            int len = s.length();
            if (len < 32) {
                buff.put((byte) (STRING_0_31 + len)).
                    putStringData(s, len);
            } else {
                buff.put((byte) type);
                writeString(buff, s);
            }
            break;
        }
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            buff.put((byte) type);
            writeString(buff, v.getString());
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
                    buff.put((byte) type).
                        putVarLong(Long.reverse(d));
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
                    buff.put((byte) type).
                        putVarInt(Integer.reverse(f));
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
                    buff.putVarInt(t).
                        putVarInt(lob.getTableId()).
                        putVarInt(lob.getObjectId()).
                        putVarLong(lob.getPrecision()).
                        put((byte) (lob.isCompressed() ? 1 : 0));
                    if (t == -2) {
                        writeString(buff, lob.getFileName());
                    }
                } else {
                    buff.putVarInt(small.length).put(small);
                }
            } else {
                ValueLobDb lob = (ValueLobDb) v;
                byte[] small = lob.getSmall();
                if (small == null) {
                    buff.putVarInt(-3).
                        putVarInt(lob.getTableId()).
                        putVarLong(lob.getLobId()).
                        putVarLong(lob.getPrecision());
                } else {
                    buff.putVarInt(small.length).
                        put(small);
                }
            }
            break;
        }
        case Value.ARRAY: {
            Value[] list = ((ValueArray) v).getList();
            buff.put((byte) type).putVarInt(list.length);
            for (Value x : list) {
                writeValue(buff, x);
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
                buff.putVarInt(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    writeString(buff, meta.getColumnName(i + 1));
                    buff.putVarInt(meta.getColumnType(i + 1)).
                        putVarInt(meta.getPrecision(i + 1)).
                        putVarInt(meta.getScale(i + 1));
                }
                while (rs.next()) {
                    buff.put((byte) 1);
                    for (int i = 0; i < columnCount; i++) {
                        int t = org.h2.value.DataType.getValueTypeFromResultSet(meta, i + 1);
                        Value val = org.h2.value.DataType.readValue(null, rs, i + 1, t);
                        writeValue(buff, val);
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
            byte[] b = v.getBytes();
            int len = b.length;
            buff.put((byte) type).
                putVarInt(len).
                put(b);
            break;
        }
        default:
            DbException.throwInternalError("type=" + v.getType());
        }
    }

    private static void writeString(WriteBuffer buff, String s) {
        int len = s.length();
        buff.putVarInt(len).putStringData(s, len);
    }

    /**
     * Read a value.
     *
     * @return the value
     */
    private Object readValue(ByteBuffer buff) {
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
            return ValueDecimal.ZERO;
        case DECIMAL_0_1 + 1:
            return ValueDecimal.ONE;
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
        case Value.DATE: {
            return ValueDate.fromDateValue(readVarLong(buff));
        }
        case Value.TIME: {
            long nanos = readVarLong(buff) * 1000000 + readVarLong(buff);
            return ValueTime.fromNanos(nanos);
        }
        case Value.TIMESTAMP: {
            long dateValue = readVarLong(buff);
            long nanos = readVarLong(buff) * 1000000 + readVarLong(buff);
            return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
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
            return ValueJavaObject.getNoCopy(null, b, handler);
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
                ValueLobDb lob = ValueLobDb.create(type, handler, tableId, lobId, null, precision);
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
                list[i] = (Value) readValue(buff);
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
                    o[i] = ((Value) readValue(buff)).getObject();
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
        case SPATIAL_KEY_2D:
            return spatialType.read(buff);
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
