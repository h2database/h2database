/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 *
 * The variable size number format code is a port from SQLite,
 * but stored in reverse order (least significant bits in the first byte).
 */
package org.h2.store;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;
import org.h2.util.Utils;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueLob;
import org.h2.value.ValueLobDb;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueStringFixed;
import org.h2.value.ValueStringIgnoreCase;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;

/**
 * This class represents a byte buffer that contains persistent data of a page.
 */
public class Data {

    /**
     * The length of an integer value.
     */
    public static final int LENGTH_INT = 4;

    private static final boolean TEST = false;
    private static final int TEST_OFFSET = 0;

    /**
     * The length of a long value.
     */
    private static final int LENGTH_LONG = 8;

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

    private static final long MILLIS_PER_MINUTE = 1000 * 60;

    /**
     * The data itself.
     */
    protected byte[] data;

    /**
     * The current write or read position.
     */
    private int pos;

    /**
     * The data handler responsible for lob objects.
     */
    private final DataHandler handler;

    protected Data(DataHandler handler, byte[] data) {
        this.handler = handler;
        this.data = data;
    }

    /**
     * Update an integer at the given position.
     * The current position is not change.
     *
     * @param pos the position
     * @param x the value
     */
    public void setInt(int pos, int x) {
        byte[] buff = data;
        buff[pos] = (byte) (x >> 24);
        buff[pos + 1] = (byte) (x >> 16);
        buff[pos + 2] = (byte) (x >> 8);
        buff[pos + 3] = (byte) x;
    }

    /**
     * Write an integer at the current position.
     * The current position is incremented.
     *
     * @param x the value
     */
    public void writeInt(int x) {
        byte[] buff = data;
        buff[pos] = (byte) (x >> 24);
        buff[pos + 1] = (byte) (x >> 16);
        buff[pos + 2] = (byte) (x >> 8);
        buff[pos + 3] = (byte) x;
        pos += 4;
    }

    /**
     * Read an integer at the current position.
     * The current position is incremented.
     *
     * @return the value
     */
    public int readInt() {
        byte[] buff = data;
        int x = (buff[pos] << 24) + ((buff[pos+1] & 0xff) << 16) + ((buff[pos+2] & 0xff) << 8) + (buff[pos+3] & 0xff);
        pos += 4;
        return x;
    }

    /**
     * Get the length of a String. This includes the bytes required to encode
     * the length.
     *
     * @param s the string
     * @return the number of bytes required
     */
    public static int getStringLen(String s) {
        int len = s.length();
        return getStringWithoutLengthLen(s, len) + getVarIntLen(len);
    }

    /**
     * Calculate the length of String, excluding the bytes required to encode
     * the length.
     * <p>
     * For performance reasons the internal representation of a String is
     * similar to UTF-8, but not exactly UTF-8.
     *
     * @param s the string
     * @param len the length of the string
     * @return the number of bytes required
     */
    public static int getStringWithoutLengthLen(String s, int len) {
        int plus = 0;
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c >= 0x800) {
                plus += 2;
            } else if (c >= 0x80) {
                plus++;
            }
        }
        return len + plus;
    }

    /**
     * Read a String value.
     * The current position is incremented.
     *
     * @return the value
     */
    public String readString() {
        int len = readVarInt();
        return readString(len);
    }

    /**
     * Read a String from the byte array.
     * <p>
     * For performance reasons the internal representation of a String is
     * similar to UTF-8, but not exactly UTF-8.
     *
     * @param len
     * @return the String
     */
    private String readString(int len) {
        byte[] buff = data;
        int p = pos;
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            int x = buff[p++] & 0xff;
            if (x < 0x80) {
                chars[i] = (char) x;
            } else if (x >= 0xe0) {
                chars[i] = (char) (((x & 0xf) << 12) + ((buff[p++] & 0x3f) << 6) + (buff[p++] & 0x3f));
            } else {
                chars[i] = (char) (((x & 0x1f) << 6) + (buff[p++] & 0x3f));
            }
        }
        pos = p;
        return new String(chars);
    }

    /**
     * Write a String.
     * The current position is incremented.
     *
     * @param s the value
     */
    public void writeString(String s) {
        int len = s.length();
        writeVarInt(len);
        writeStringWithoutLength(s, len);
    }

    /**
     * Write a String.
     * <p>
     * For performance reasons the internal representation of a String is
     * similar to UTF-8, but not exactly UTF-8.
     *
     * @param s the string
     * @param len the number of characters to write
     */
    private void writeStringWithoutLength(String s, int len) {
        int p = pos;
        byte[] buff = data;
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (c < 0x80) {
                buff[p++] = (byte) c;
            } else if (c >= 0x800) {
                buff[p++] = (byte) (0xe0 | (c >> 12));
                buff[p++] = (byte) (((c >> 6) & 0x3f));
                buff[p++] = (byte) (c & 0x3f);
            } else {
                buff[p++] = (byte) (0xc0 | (c >> 6));
                buff[p++] = (byte) (c & 0x3f);
            }
        }
        pos = p;
    }

    /**
     * Increase the size to the given length.
     * The current position is set to the given value.
     *
     * @param len the new length
     */
    public void fill(int len) {
        pos = len;
        if (data.length < len) {
            checkCapacity(len - data.length);
        }
    }

    /**
     * Create a new buffer for the given handler. The
     * handler will decide what type of buffer is created.
     *
     * @param handler the data handler
     * @param capacity the initial capacity of the buffer
     * @return the buffer
     */
    public static Data create(DataHandler handler, int capacity) {
        return new Data(handler, new byte[capacity]);
    }

    /**
     * Create a new buffer using the given data for the given handler. The
     * handler will decide what type of buffer is created.
     *
     * @param handler the data handler
     * @param buff the data
     * @return the buffer
     */
    public static Data create(DataHandler handler, byte[] buff) {
        return new Data(handler, buff);
    }

    /**
     * Get the current write position of this buffer, which is the current
     * length.
     *
     * @return the length
     */
    public int length() {
        return pos;
    }

    /**
     * Get the byte array used for this page.
     *
     * @return the byte array
     */
    public byte[] getBytes() {
        return data;
    }

    /**
     * Set the position to 0.
     */
    public void reset() {
        pos = 0;
    }

    /**
     * Append a number of bytes to this buffer.
     *
     * @param buff the data
     * @param off the offset in the data
     * @param len the length in bytes
     */
    public void write(byte[] buff, int off, int len) {
        System.arraycopy(buff, off, data, pos, len);
        pos += len;
    }

    /**
     * Append a number of bytes to this buffer.
     *
     * @param buff the data
     */
    public void write(byte[] buff) {
        write(buff, 0, buff.length);
    }

    /**
     * Copy a number of bytes to the given buffer from the current position. The
     * current position is incremented accordingly.
     *
     * @param buff the output buffer
     * @param off the offset in the output buffer
     * @param len the number of bytes to copy
     */
    public void read(byte[] buff, int off, int len) {
        System.arraycopy(data, pos, buff, off, len);
        pos += len;
    }

    /**
     * Copy a number of bytes to the given buffer from the current position. The
     * current position is incremented accordingly.
     *
     * @param buff the output buffer
     */
    public void read(byte[] buff) {
        read(buff, 0, buff.length);
    }

    /**
     * Append one single byte.
     *
     * @param x the value
     */
    public void writeByte(byte x) {
        data[pos++] = x;
    }

    /**
     * Read one single byte.
     *
     * @return the value
     */
    public byte readByte() {
        return data[pos++];
    }

    /**
     * Read a long value. This method reads two int values and combines them.
     *
     * @return the long value
     */
    public long readLong() {
        return ((long) (readInt()) << 32) + (readInt() & 0xffffffffL);
    }

    /**
     * Append a long value. This method writes two int values.
     *
     * @param x the value
     */
    public void writeLong(long x) {
        writeInt((int) (x >>> 32));
        writeInt((int) x);
    }

    /**
     * Append a value.
     *
     * @param v the value
     */
    public void writeValue(Value v) {
        int start = pos;
        if (TEST) {
            pos += TEST_OFFSET;
        }
        if (v == ValueNull.INSTANCE) {
            data[pos++] = 0;
            return;
        }
        int type = v.getType();
        switch (type) {
        case Value.BOOLEAN:
            writeByte((byte) (v.getBoolean().booleanValue() ? BOOLEAN_TRUE : BOOLEAN_FALSE));
            break;
        case Value.BYTE:
            writeByte((byte) type);
            writeByte(v.getByte());
            break;
        case Value.SHORT:
            writeByte((byte) type);
            writeShortInt(v.getShort());
            break;
        case Value.INT: {
            int x = v.getInt();
            if (x < 0) {
                writeByte((byte) INT_NEG);
                writeVarInt(-x);
            } else if (x < 16) {
                writeByte((byte) (INT_0_15 + x));
            } else {
                writeByte((byte) type);
                writeVarInt(x);
            }
            break;
        }
        case Value.LONG: {
            long x = v.getLong();
            if (x < 0) {
                writeByte((byte) LONG_NEG);
                writeVarLong(-x);
            } else if (x < 8) {
                writeByte((byte) (LONG_0_7 + x));
            } else {
                writeByte((byte) type);
                writeVarLong(x);
            }
            break;
        }
        case Value.DECIMAL: {
            BigDecimal x = v.getBigDecimal();
            if (BigDecimal.ZERO.equals(x)) {
                writeByte((byte) DECIMAL_0_1);
            } else if (BigDecimal.ONE.equals(x)) {
                writeByte((byte) (DECIMAL_0_1 + 1));
            } else {
                int scale = x.scale();
                BigInteger b = x.unscaledValue();
                int bits = b.bitLength();
                if (bits <= 63) {
                    if (scale == 0) {
                        writeByte((byte) DECIMAL_SMALL_0);
                        writeVarLong(b.longValue());
                    } else {
                        writeByte((byte) DECIMAL_SMALL);
                        writeVarInt(scale);
                        writeVarLong(b.longValue());
                    }
                } else {
                    writeByte((byte) type);
                    writeVarInt(scale);
                    byte[] bytes = b.toByteArray();
                    writeVarInt(bytes.length);
                    write(bytes, 0, bytes.length);
                }
            }
            break;
        }
        case Value.TIME:
            writeByte((byte) type);
            writeVarLong(DateTimeUtils.getTimeLocal(v.getTimeNoCopy()));
            break;
        case Value.DATE: {
            writeByte((byte) type);
            long x = DateTimeUtils.getTimeLocal(v.getDateNoCopy());
            writeVarLong(x / MILLIS_PER_MINUTE);
            break;
        }
        case Value.TIMESTAMP: {
            writeByte((byte) type);
            Timestamp ts = v.getTimestampNoCopy();
            writeVarLong(DateTimeUtils.getTimeLocal(ts));
            writeVarInt(ts.getNanos());
            break;
        }
        case Value.JAVA_OBJECT: {
            writeByte((byte) type);
            byte[] b = v.getBytesNoCopy();
            writeVarInt(b.length);
            write(b, 0, b.length);
            break;
        }
        case Value.BYTES: {
            byte[] b = v.getBytesNoCopy();
            int len = b.length;
            if (len < 32) {
                writeByte((byte) (BYTES_0_31 + len));
                write(b, 0, b.length);
            } else {
                writeByte((byte) type);
                writeVarInt(b.length);
                write(b, 0, b.length);
            }
            break;
        }
        case Value.UUID: {
            writeByte((byte) type);
            ValueUuid uuid = (ValueUuid) v;
            writeLong(uuid.getHigh());
            writeLong(uuid.getLow());
            break;
        }
        case Value.STRING: {
            String s = v.getString();
            int len = s.length();
            if (len < 32) {
                writeByte((byte) (STRING_0_31 + len));
                writeStringWithoutLength(s, len);
            } else {
                writeByte((byte) type);
                writeString(s);
            }
            break;
        }
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            writeByte((byte) type);
            writeString(v.getString());
            break;
        case Value.DOUBLE: {
            double x = v.getDouble();
            if (x == 0.0 || x == 1.0) {
                writeByte((byte) (DOUBLE_0_1 + x));
            } else {
                writeByte((byte) type);
                writeVarLong(MathUtils.reverseLong(Double.doubleToLongBits(x)));
            }
            break;
        }
        case Value.FLOAT: {
            float x = v.getFloat();
            if (x == 0.0f || x == 1.0f) {
                writeByte((byte) (FLOAT_0_1 + x));
            } else {
                writeByte((byte) type);
                writeVarInt(MathUtils.reverseInt(Float.floatToIntBits(v.getFloat())));
            }
            break;
        }
        case Value.BLOB:
        case Value.CLOB: {
            writeByte((byte) type);
            if (v instanceof ValueLob) {
                ValueLob lob = (ValueLob) v;
                lob.convertToFileIfRequired(handler);
                byte[] small = lob.getSmall();
                if (small == null) {
                    int t = -1;
                    if (!lob.isLinked()) {
                        t = -2;
                    }
                    writeVarInt(t);
                    writeVarInt(lob.getTableId());
                    writeVarInt(lob.getObjectId());
                    writeVarLong(lob.getPrecision());
                    writeByte((byte) (lob.useCompression() ? 1 : 0));
                    if (t == -2) {
                        writeString(lob.getFileName());
                    }
                } else {
                    writeVarInt(small.length);
                    write(small, 0, small.length);
                }
            } else {
                ValueLobDb lob = (ValueLobDb) v;
                byte[] small = lob.getSmall();
                if (small == null) {
                    writeVarInt(-3);
                    writeVarInt(lob.getTableId());
                    writeVarLong(lob.getLobId());
                    writeVarLong(lob.getPrecision());
                } else {
                    writeVarInt(small.length);
                    write(small, 0, small.length);
                }
            }
            break;
        }
        case Value.ARRAY: {
            writeByte((byte) type);
            Value[] list = ((ValueArray) v).getList();
            writeVarInt(list.length);
            for (Value x : list) {
                writeValue(x);
            }
            break;
        }
        default:
            DbException.throwInternalError("type=" + v.getType());
        }
        if (SysProperties.CHECK2) {
            if (pos - start != getValueLen(v, handler)) {
                throw DbException
                        .throwInternalError("value size error: got " + (pos - start) + " expected " + getValueLen(v, handler));
            }
        }
    }

    /**
     * Read a value.
     *
     * @return the value
     */
    public Value readValue() {
        if (TEST) {
            pos += TEST_OFFSET;
        }
        int type = data[pos++] & 255;
        switch (type) {
        case Value.NULL:
            return ValueNull.INSTANCE;
        case BOOLEAN_TRUE:
            return ValueBoolean.get(true);
        case BOOLEAN_FALSE:
            return ValueBoolean.get(false);
        case INT_NEG:
            return ValueInt.get(-readVarInt());
        case Value.INT:
            return ValueInt.get(readVarInt());
        case LONG_NEG:
            return ValueLong.get(-readVarLong());
        case Value.LONG:
            return ValueLong.get(readVarLong());
        case Value.BYTE:
            return ValueByte.get(readByte());
        case Value.SHORT:
            return ValueShort.get(readShortInt());
        case DECIMAL_0_1:
            return (ValueDecimal) ValueDecimal.ZERO;
        case DECIMAL_0_1 + 1:
            return (ValueDecimal) ValueDecimal.ONE;
        case DECIMAL_SMALL_0:
            return ValueDecimal.get(BigDecimal.valueOf(readVarLong()));
        case DECIMAL_SMALL: {
            int scale = readVarInt();
            return ValueDecimal.get(BigDecimal.valueOf(readVarLong(), scale));
        }
        case Value.DECIMAL: {
            int scale = readVarInt();
            int len = readVarInt();
            byte[] buff = Utils.newBytes(len);
            read(buff, 0, len);
            BigInteger b = new BigInteger(buff);
            return ValueDecimal.get(new BigDecimal(b, scale));
        }
        case Value.DATE: {
            long x = readVarLong() * MILLIS_PER_MINUTE;
            return ValueDate.getNoCopy(new Date(DateTimeUtils.getTimeGMT(x)));
        }
        case Value.TIME:
            // need to normalize the year, month and day
            return ValueTime.get(new Time(DateTimeUtils.getTimeGMT(readVarLong())));
        case Value.TIMESTAMP: {
            Timestamp ts = new Timestamp(DateTimeUtils.getTimeGMT(readVarLong()));
            ts.setNanos(readVarInt());
            return ValueTimestamp.getNoCopy(ts);
        }
        case Value.BYTES: {
            int len = readVarInt();
            byte[] b = Utils.newBytes(len);
            read(b, 0, len);
            return ValueBytes.getNoCopy(b);
        }
        case Value.JAVA_OBJECT: {
            int len = readVarInt();
            byte[] b = Utils.newBytes(len);
            read(b, 0, len);
            return ValueJavaObject.getNoCopy(b);
        }
        case Value.UUID:
            return ValueUuid.get(readLong(), readLong());
        case Value.STRING:
            return ValueString.get(readString());
        case Value.STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(readString());
        case Value.STRING_FIXED:
            return ValueStringFixed.get(readString());
        case FLOAT_0_1:
            return ValueFloat.get(0);
        case FLOAT_0_1 + 1:
            return ValueFloat.get(1);
        case DOUBLE_0_1:
            return ValueDouble.get(0);
        case DOUBLE_0_1 + 1:
            return ValueDouble.get(1);
        case Value.DOUBLE:
            return ValueDouble.get(Double.longBitsToDouble(MathUtils.reverseLong(readVarLong())));
        case Value.FLOAT:
            return ValueFloat.get(Float.intBitsToFloat(MathUtils.reverseInt(readVarInt())));
        case Value.BLOB:
        case Value.CLOB: {
            int smallLen = readVarInt();
            if (smallLen >= 0) {
                byte[] small = Utils.newBytes(smallLen);
                read(small, 0, smallLen);
                return LobStorage.createSmallLob(type, small);
            } else if (smallLen == -3) {
                int tableId = readVarInt();
                long lobId = readVarLong();
                long precision = readVarLong();
                LobStorage lobStorage = handler.getLobStorage();
                ValueLobDb lob = ValueLobDb.create(type, lobStorage, null, tableId, lobId, precision);
                return lob;
            } else {
                int tableId = readVarInt();
                int objectId = readVarInt();
                long precision = 0;
                boolean compression = false;
                // -1: regular
                // -2: regular, but not linked (in this case: including file name)
                if (smallLen == -1 || smallLen == -2) {
                    precision = readVarLong();
                    compression = readByte() == 1;
                }
                ValueLob lob = ValueLob.open(type, handler, tableId, objectId, precision, compression);
                if (smallLen == -2) {
                    lob.setFileName(readString(), false);
                }
                return lob;
            }
        }
        case Value.ARRAY: {
            int len = readVarInt();
            Value[] list = new Value[len];
            for (int i = 0; i < len; i++) {
                list[i] = readValue();
            }
            return ValueArray.get(list);
        }
        default:
            if (type >= INT_0_15 && type < INT_0_15 + 16) {
                return ValueInt.get(type - INT_0_15);
            } else if (type >= LONG_0_7 && type < LONG_0_7 + 8) {
                return ValueLong.get(type - LONG_0_7);
            } else if (type >= BYTES_0_31 && type < BYTES_0_31 + 32) {
                int len = type - BYTES_0_31;
                byte[] b = Utils.newBytes(len);
                read(b, 0, len);
                return ValueBytes.getNoCopy(b);
            } else if (type >= STRING_0_31 && type < STRING_0_31 + 32) {
                return ValueString.get(readString(type - STRING_0_31));
            }
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "type: " + type);
        }
    }

    /**
     * Calculate the number of bytes required to encode the given value.
     *
     * @param v the value
     * @return the number of bytes required to store this value
     */
    public int getValueLen(Value v) {
        return getValueLen(v, handler);
    }

    /**
     * Calculate the number of bytes required to encode the given value.
     *
     * @param v the value
     * @param handler the data handler for lobs
     * @return the number of bytes required to store this value
     */
    public static int getValueLen(Value v, DataHandler handler) {
        return getValueLen2(v, handler) + TEST_OFFSET;
    }

    private static int getValueLen2(Value v, DataHandler handler) {
        if (v == ValueNull.INSTANCE) {
            return 1;
        }
        switch (v.getType()) {
        case Value.BOOLEAN:
            return 1;
        case Value.BYTE:
            return 2;
        case Value.SHORT:
            return 3;
        case Value.INT: {
            int x = v.getInt();
            if (x < 0) {
                return 1 + getVarIntLen(-x);
            } else if (x < 16) {
                return 1;
            } else {
                return 1 + getVarIntLen(x);
            }
        }
        case Value.LONG: {
            long x = v.getLong();
            if (x < 0) {
                return 1 + getVarLongLen(-x);
            } else if (x < 8) {
                return 1;
            } else {
                return 1 + getVarLongLen(x);
            }
        }
        case Value.DOUBLE: {
            double x = v.getDouble();
            if (x == 0.0 || x == 1.0) {
                return 1;
            }
            return 1 + getVarLongLen(MathUtils.reverseLong(Double.doubleToLongBits(x)));
        }
        case Value.FLOAT: {
            float x = v.getFloat();
            if (x == 0.0f || x == 1.0f) {
                return 1;
            }
            return 1 + getVarIntLen(MathUtils.reverseInt(Float.floatToIntBits(v.getFloat())));
        }
        case Value.STRING: {
            String s = v.getString();
            int len = s.length();
            if (len < 32) {
                return 1 + getStringWithoutLengthLen(s, len);
            }
            return 1 + getStringLen(s);
        }
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            return 1 + getStringLen(v.getString());
        case Value.DECIMAL: {
            BigDecimal x = v.getBigDecimal();
            if (BigDecimal.ZERO.equals(x)) {
                return 1;
            } else if (BigDecimal.ONE.equals(x)) {
                return 1;
            }
            int scale = x.scale();
            BigInteger b = x.unscaledValue();
            int bits = b.bitLength();
            if (bits <= 63) {
                if (scale == 0) {
                    return 1 + getVarLongLen(b.longValue());
                }
                return 1 + getVarIntLen(scale) + getVarLongLen(b.longValue());
            }
            byte[] bytes = b.toByteArray();
            return 1 + getVarIntLen(scale) + getVarIntLen(bytes.length) + bytes.length;
        }
        case Value.TIME:
            return 1 + getVarLongLen(DateTimeUtils.getTimeLocal(v.getTimeNoCopy()));
        case Value.DATE: {
            long x = DateTimeUtils.getTimeLocal(v.getDateNoCopy());
            return 1 + getVarLongLen(x / MILLIS_PER_MINUTE);
        }
        case Value.TIMESTAMP: {
            Timestamp ts = v.getTimestampNoCopy();
            return 1 + getVarLongLen(DateTimeUtils.getTimeLocal(ts)) + getVarIntLen(ts.getNanos());
        }
        case Value.JAVA_OBJECT: {
            byte[] b = v.getBytesNoCopy();
            return 1 + getVarIntLen(b.length) + b.length;
        }
        case Value.BYTES: {
            byte[] b = v.getBytesNoCopy();
            int len = b.length;
            if (len < 32) {
                return 1 + b.length;
            }
            return 1 + getVarIntLen(b.length) + b.length;
        }
        case Value.UUID:
            return 1 + LENGTH_LONG + LENGTH_LONG;
        case Value.BLOB:
        case Value.CLOB: {
            int len = 1;
            if (v instanceof ValueLob) {
                ValueLob lob = (ValueLob) v;
                lob.convertToFileIfRequired(handler);
                byte[] small = lob.getSmall();
                if (small == null) {
                    int t = -1;
                    if (!lob.isLinked()) {
                        t = -2;
                    }
                    len += getVarIntLen(t);
                    len += getVarIntLen(lob.getTableId());
                    len += getVarIntLen(lob.getObjectId());
                    len += getVarLongLen(lob.getPrecision());
                    len += 1;
                    if (t == -2) {
                        len += getStringLen(lob.getFileName());
                    }
                } else {
                    len += getVarIntLen(small.length);
                    len += small.length;
                }
            } else {
                ValueLobDb lob = (ValueLobDb) v;
                byte[] small = lob.getSmall();
                if (small == null) {
                    len += getVarIntLen(-3);
                    len += getVarIntLen(lob.getTableId());
                    len += getVarLongLen(lob.getLobId());
                    len += getVarLongLen(lob.getPrecision());
                } else {
                    len += getVarIntLen(small.length);
                    len += small.length;
                }
            }
            return len;
        }
        case Value.ARRAY: {
            Value[] list = ((ValueArray) v).getList();
            int len = 1 + getVarIntLen(list.length);
            for (Value x : list) {
                len += getValueLen(x, handler);
            }
            return len;
        }
        default:
            throw DbException.throwInternalError("type=" + v.getType());
        }
    }

    /**
     * Set the current read / write position.
     *
     * @param pos the new position
     */
    public void setPos(int pos) {
        this.pos = pos;
    }

    /**
     * Write a short integer at the current position.
     * The current position is incremented.
     *
     * @param x the value
     */
    public void writeShortInt(int x) {
        byte[] buff = data;
        buff[pos++] = (byte) (x >> 8);
        buff[pos++] = (byte) x;
    }

    /**
     * Read an short integer at the current position.
     * The current position is incremented.
     *
     * @return the value
     */
    public short readShortInt() {
        byte[] buff = data;
        return (short) (((buff[pos++] & 0xff) << 8) + (buff[pos++] & 0xff));
    }

    /**
     * Shrink the array to this size.
     *
     * @param size the new size
     */
    public void truncate(int size) {
        if (pos > size) {
            byte[] buff = new byte[size];
            System.arraycopy(data, 0, buff, 0, size);
            this.pos = size;
            data = buff;
        }
    }

    /**
     * The number of bytes required for a variable size int.
     *
     * @param x the value
     * @return the len
     */
    public static int getVarIntLen(int x) {
        if ((x & (-1 << 7)) == 0) {
            return 1;
        } else if ((x & (-1 << 14)) == 0) {
            return 2;
        } else if ((x & (-1 << 21)) == 0) {
            return 3;
        } else if ((x & (-1 << 28)) == 0) {
            return 4;
        }
        return 5;
    }

    /**
     * Write a variable size int.
     *
     * @param x the value
     */
    public void writeVarInt(int x) {
        while ((x & ~0x7f) != 0) {
            data[pos++] = (byte) (0x80 | (x & 0x7f));
            x >>>= 7;
        }
        data[pos++] = (byte) x;
    }

    /**
     * Read a variable size int.
     *
     * @return the value
     */
    public int readVarInt() {
        int b = data[pos];
        if (b >= 0) {
            pos++;
            return b;
        }
        int x = b & 0x7f;
        b = data[pos + 1];
        if (b >= 0) {
            pos += 2;
            return x | (b << 7);
        }
        x |= (b & 0x7f) << 7;
        b = data[pos + 2];
        if (b >= 0) {
            pos += 3;
            return x | (b << 14);
        }
        x |= (b & 0x7f) << 14;
        b = data[pos + 3];
        if (b >= 0) {
            pos += 4;
            return x | b << 21;
        }
        x |= ((b & 0x7f) << 21) | (data[pos + 4] << 28);
        pos += 5;
        return x;
    }

    /**
     * The number of bytes required for a variable size long.
     *
     * @param x the value
     * @return the len
     */
    public static int getVarLongLen(long x) {
        int i = 1;
        while (true) {
            x >>>= 7;
            if (x == 0) {
                return i;
            }
            i++;
        }
    }

    /**
     * Write a variable size long.
     *
     * @param x the value
     */
    public void writeVarLong(long x) {
        while ((x & ~0x7f) != 0) {
            data[pos++] = (byte) ((x & 0x7f) | 0x80);
            x >>>= 7;
        }
        data[pos++] = (byte) x;
    }

    /**
     * Read a variable size long.
     *
     * @return the value
     */
    public long readVarLong() {
        long x = data[pos++];
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7;; s += 7) {
            long b = data[pos++];
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                return x;
            }
        }
    }

    /**
     * Check if there is still enough capacity in the buffer.
     * This method extends the buffer if required.
     *
     * @param plus the number of additional bytes required
     */
    public void checkCapacity(int plus) {
        if (pos + plus >= data.length) {
            // a separate method to simplify inlining
            expand(plus);
        }
    }

    private void expand(int plus) {
        byte[] d = Utils.newBytes((data.length + plus) * 2);
        // must copy everything, because pos could be 0 and data may be
        // still required
        System.arraycopy(data, 0, d, 0, data.length);
        data = d;
    }

    /**
     * Fill up the buffer with empty space and an (initially empty) checksum
     * until the size is a multiple of Constants.FILE_BLOCK_SIZE.
     */
    public void fillAligned() {
        // 0..6 > 8, 7..14 > 16, 15..22 > 24, ...
        fill(MathUtils.roundUpInt(pos + 2, Constants.FILE_BLOCK_SIZE));
    }

}
