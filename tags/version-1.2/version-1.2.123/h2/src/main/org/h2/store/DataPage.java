/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

package org.h2.store;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.util.MathUtils;
import org.h2.util.MemoryUtils;
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
 * A data page is a byte buffer that contains persistent data of a row or index
 * page.
 */
public class DataPage {

    /**
     * The space required for the checksum and additional fillers.
     */
    public static final int LENGTH_FILLER = 2;

    /**
     * The length of an integer value.
     */
    public static final int LENGTH_INT = 4;

    /**
     * The length of a long value.
     */
    public static final int LENGTH_LONG = 8;

    /**
     * Whether calculating (and checking) the checksum is enabled.
     */
    private static final boolean CHECKSUM = true;

    /**
     * The data handler responsible for lob objects.
     */
    protected DataHandler handler;

    /**
     * The data itself.
     */
    protected byte[] data;

    /**
     * The current write or read position.
     */
    protected int pos;

    protected DataPage(DataHandler handler, byte[] data) {
        this.handler = handler;
        this.data = data;
    }

    /**
     * Calculate the checksum and write.
     *
     */
    public void updateChecksum() {
        if (CHECKSUM) {
            int x = handler.getChecksum(data, 0, pos - 2);
            data[pos - 2] = (byte) x;
        }
    }

    /**
     * Test if the checksum is correct.
     *
     * @param len the number of bytes
     * @throws SQLException if the checksum does not match
     */
    public void check(int len) throws SQLException {
        if (CHECKSUM) {
            int x = handler.getChecksum(data, 0, len - 2);
            if (data[len - 2] == (byte) x) {
                return;
            }
            handler.handleInvalidChecksum();
        }
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
        buff[pos++] = (byte) (x >> 24);
        buff[pos++] = (byte) (x >> 16);
        buff[pos++] = (byte) (x >> 8);
        buff[pos++] = (byte) x;
    }

    /**
     * Read an integer at the current position.
     * The current position is incremented.
     *
     * @return the value
     */
    public int readInt() {
        byte[] buff = data;
        return (buff[pos++] << 24) + ((buff[pos++] & 0xff) << 16) + ((buff[pos++] & 0xff) << 8) + (buff[pos++] & 0xff);
    }

    /**
     * Get the length of a String value.
     *
     * @param s the value
     * @return the length
     */
    public int getStringLen(String s) {
        return getStringLenUTF8(s);
    }

    /**
     * Read a String value.
     * The current position is incremented.
     *
     * @return the value
     */
    public String readString() {
        byte[] buff = data;
        int p = pos;
        int len = ((buff[p++] & 0xff) << 24) + ((buff[p++] & 0xff) << 16) + ((buff[p++] & 0xff) << 8)
                + (buff[p++] & 0xff);
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
     * Write a String value.
     * The current position is incremented.
     *
     * @param s the value
     */
    public void writeString(String s) {
        int len = s.length();
        checkCapacity(len * 3 + 4);
        int p = pos;
        byte[] buff = data;
        buff[p++] = (byte) (len >> 24);
        buff[p++] = (byte) (len >> 16);
        buff[p++] = (byte) (len >> 8);
        buff[p++] = (byte) len;
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (c > 0 && c < 0x80) {
                buff[p++] = (byte) c;
            } else if (c >= 0x800) {
                buff[p++] = (byte) (0xe0 | (c >> 12));
                buff[p++] = (byte) (0x80 | ((c >> 6) & 0x3f));
                buff[p++] = (byte) (0x80 | (c & 0x3f));
            } else {
                buff[p++] = (byte) (0xc0 | (c >> 6));
                buff[p++] = (byte) (0x80 | (c & 0x3f));
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
        if (pos > len) {
            pos = len;
        }
        checkCapacity(len - pos);
        pos = len;
    }

    /**
     * Create a new data page for the given handler. The
     * handler will decide what type of buffer is created.
     *
     * @param handler the data handler
     * @param capacity the initial capacity of the buffer
     * @return the data page
     */
    public static DataPage create(DataHandler handler, int capacity) {
        return new DataPage(handler, new byte[capacity]);
    }

    /**
     * Create a new data page using the given data for the given handler. The
     * handler will decide what type of buffer is created.
     *
     * @param handler the data handler
     * @param buff the data
     * @return the data page
     */
    public static DataPage create(DataHandler handler, byte[] buff) {
        return new DataPage(handler, buff);
    }

    /**
     * Check if there is still enough capacity in the buffer.
     * This method extends the buffer if required.
     *
     * @param plus the number of additional bytes required
     */
    public void checkCapacity(int plus) {
        if (pos + plus >= data.length) {
            byte[] d = MemoryUtils.newBytes((data.length + plus) * 2);
            // must copy everything, because pos could be 0 and data may be
            // still required
            System.arraycopy(data, 0, d, 0, data.length);
            data = d;
        }
    }

    /**
     * Get the current write position of this data page, which is the current
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
     * Append the contents of the given data page to this page.
     * The filler is not appended.
     *
     * @param page the page that will be appended
     */
    public void writeDataPageNoSize(DataPage page) {
        checkCapacity(page.pos);
        // don't write filler
        int len = page.pos - LENGTH_FILLER;
        System.arraycopy(page.data, 0, data, pos, len);
        pos += len;
    }

    /**
     * Read a data page from this page. The data from the current position to
     * the end of the page is copied.
     *
     * @return the new page
     */
    public DataPage readDataPageNoSize() {
        int len = data.length - pos;
        DataPage page = DataPage.create(handler, len);
        System.arraycopy(data, pos, page.data, 0, len);
        page.pos = len;
        return page;
    }

    /**
     * Append a number of bytes to this data page.
     *
     * @param buff the data
     * @param off the offset in the data
     * @param len the length in bytes
     */
    public void write(byte[] buff, int off, int len) {
        checkCapacity(len);
        System.arraycopy(buff, off, data, pos, len);
        pos += len;
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
    public int readByte() {
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
    public void writeValue(Value v) throws SQLException {
        if (SysProperties.CHECK) {
            checkCapacity(8);
        }
        // TODO text output: could be in the Value... classes
        if (v == ValueNull.INSTANCE) {
            data[pos++] = '-';
            return;
        }
        int start = pos;
        data[pos++] = (byte) (v.getType() + 'a');
        switch (v.getType()) {
        case Value.BOOLEAN:
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
            writeInt(v.getInt());
            break;
        case Value.LONG:
            writeLong(v.getLong());
            break;
        case Value.DECIMAL:
            String s = v.getString();
            writeString(s);
            break;
        case Value.TIME:
            writeLong(v.getTimeNoCopy().getTime());
            break;
        case Value.DATE:
            writeLong(v.getDateNoCopy().getTime());
            break;
        case Value.TIMESTAMP: {
            Timestamp ts = v.getTimestampNoCopy();
            writeLong(ts.getTime());
            writeInt(ts.getNanos());
            break;
        }
        case Value.JAVA_OBJECT:
        case Value.BYTES: {
            byte[] b = v.getBytesNoCopy();
            writeInt(b.length);
            write(b, 0, b.length);
            break;
        }
        case Value.UUID: {
            ValueUuid uuid = (ValueUuid) v;
            writeLong(uuid.getHigh());
            writeLong(uuid.getLow());
            break;
        }
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            writeString(v.getString());
            break;
        case Value.DOUBLE:
            writeLong(Double.doubleToLongBits(v.getDouble()));
            break;
        case Value.FLOAT:
            writeInt(Float.floatToIntBits(v.getFloat()));
            break;
        case Value.BLOB:
        case Value.CLOB: {
            ValueLob lob = (ValueLob) v;
            lob.convertToFileIfRequired(handler);
            byte[] small = lob.getSmall();
            if (small == null) {
                // -2 for historical reasons (-1 didn't store precision)
                int type = -2;
                if (!lob.isLinked()) {
                    type = -3;
                }
                writeInt(type);
                writeInt(lob.getTableId());
                writeInt(lob.getObjectId());
                writeLong(lob.getPrecision());
                writeByte((byte) (lob.useCompression() ? 1 : 0));
                if (type == -3) {
                    writeString(lob.getFileName());
                }
            } else {
                writeInt(small.length);
                write(small, 0, small.length);
            }
            break;
        }
        case Value.ARRAY: {
            Value[] list = ((ValueArray) v).getList();
            writeInt(list.length);
            for (Value x : list) {
                writeValue(x);
            }
            break;
        }
        default:
            Message.throwInternalError("type=" + v.getType());
        }
        if (SysProperties.CHECK2) {
            if (pos - start != getValueLen(v)) {
                throw Message
                        .throwInternalError("value size error: got " + (pos - start) + " expected " + getValueLen(v));
            }
        }
    }

    /**
     * Calculate the number of bytes required to encode the given value.
     *
     * @param v the value
     * @return the number of bytes required to store this value
     */
    public int getValueLen(Value v) throws SQLException {
        if (v == ValueNull.INSTANCE) {
            return 1;
        }
        switch (v.getType()) {
        case Value.BOOLEAN:
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
            return 1 + LENGTH_INT;
        case Value.LONG:
            return 1 + LENGTH_LONG;
        case Value.DOUBLE:
            return 1 + LENGTH_LONG;
        case Value.FLOAT:
            return 1 + LENGTH_INT;
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            return 1 + getStringLen(v.getString());
        case Value.DECIMAL:
            return 1 + getStringLen(v.getString());
        case Value.JAVA_OBJECT:
        case Value.BYTES: {
            int len = v.getBytesNoCopy().length;
            return 1 + LENGTH_INT + len;
        }
        case Value.UUID:
            return 1 + LENGTH_LONG + LENGTH_LONG;
        case Value.TIME:
            return 1 + LENGTH_LONG;
        case Value.DATE:
            return 1 + LENGTH_LONG;
        case Value.TIMESTAMP:
            return 1 + LENGTH_LONG + LENGTH_INT;
        case Value.BLOB:
        case Value.CLOB: {
            int len = 1;
            ValueLob lob = (ValueLob) v;
            lob.convertToFileIfRequired(handler);
            byte[] small = lob.getSmall();
            if (small != null) {
                len += LENGTH_INT + small.length;
            } else {
                len += LENGTH_INT + LENGTH_INT + LENGTH_INT + LENGTH_LONG + 1;
                if (!lob.isLinked()) {
                    len += getStringLen(lob.getFileName());
                }
            }
            return len;
        }
        case Value.ARRAY: {
            Value[] list = ((ValueArray) v).getList();
            int len = 1 + LENGTH_INT;
            for (Value x : list) {
                len += getValueLen(x);
            }
            return len;
        }
        default:
            throw Message.throwInternalError("type=" + v.getType());
        }
    }

    /**
     * Read a value.
     *
     * @return the value
     */
    public Value readValue() throws SQLException {
        int dataType = data[pos++];
        if (dataType == '-') {
            return ValueNull.INSTANCE;
        }
        dataType = dataType - 'a';
        switch (dataType) {
        case Value.BOOLEAN:
            return ValueBoolean.get(readInt() == 1);
        case Value.BYTE:
            return ValueByte.get((byte) readInt());
        case Value.SHORT:
            return ValueShort.get((short) readInt());
        case Value.INT:
            return ValueInt.get(readInt());
        case Value.LONG:
            return ValueLong.get(readLong());
        case Value.DECIMAL:
            return ValueDecimal.get(new BigDecimal(readString()));
        case Value.DATE:
            return ValueDate.getNoCopy(new Date(readLong()));
        case Value.TIME:
            // need to normalize the year, month and day
            return ValueTime.get(new Time(readLong()));
        case Value.TIMESTAMP: {
            Timestamp ts = new Timestamp(readLong());
            ts.setNanos(readInt());
            return ValueTimestamp.getNoCopy(ts);
        }
        case Value.JAVA_OBJECT: {
            int len = readInt();
            byte[] b = MemoryUtils.newBytes(len);
            read(b, 0, len);
            return ValueJavaObject.getNoCopy(b);
        }
        case Value.BYTES: {
            int len = readInt();
            byte[] b = MemoryUtils.newBytes(len);
            read(b, 0, len);
            return ValueBytes.getNoCopy(b);
        }
        case Value.UUID:
            return ValueUuid.get(readLong(), readLong());
        case Value.STRING:
            return ValueString.get(readString());
        case Value.STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(readString());
        case Value.STRING_FIXED:
            return ValueStringFixed.get(readString());
        case Value.DOUBLE:
            return ValueDouble.get(Double.longBitsToDouble(readLong()));
        case Value.FLOAT:
            return ValueFloat.get(Float.intBitsToFloat(readInt()));
        case Value.BLOB:
        case Value.CLOB: {
            int smallLen = readInt();
            if (smallLen >= 0) {
                byte[] small = MemoryUtils.newBytes(smallLen);
                read(small, 0, smallLen);
                return ValueLob.createSmallLob(dataType, small);
            }
            int tableId = readInt();
            int objectId = readInt();
            long precision = 0;
            boolean compression = false;
            // -1: historical (didn't store precision)
            // -2: regular
            // -3: regular, but not linked (in this case: including file name)
            if (smallLen == -2 || smallLen == -3) {
                precision = readLong();
                compression = readByte() == 1;
            }
            ValueLob lob = ValueLob.open(dataType, handler, tableId, objectId, precision, compression);
            if (smallLen == -3) {
                lob.setFileName(readString(), false);
            }
            return lob;
        }
        case Value.ARRAY: {
            int len = readInt();
            Value[] list = new Value[len];
            for (int i = 0; i < len; i++) {
                list[i] = readValue();
            }
            return ValueArray.get(list);
        }
        default:
            throw Message.throwInternalError("type=" + dataType);
        }
    }

    /**
     * Fill up the buffer with empty space and an (initially empty) checksum
     * until the size is a multiple of Constants.FILE_BLOCK_SIZE.
     */
    public void fillAligned() {
        // TODO datapage: fillAligned should not use a fixed constant '2'
        // 0..6 > 8, 7..14 > 16, 15..22 > 24, ...
        fill(MathUtils.roundUp(pos + 2, Constants.FILE_BLOCK_SIZE));
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
    public int readShortInt() {
        byte[] buff = data;
        return ((buff[pos++] & 0xff) << 8) + (buff[pos++] & 0xff);
    }

    private static int getStringLenUTF8(String s) {
        int plus = 4, len = s.length();
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c >= 0x800) {
                plus += 2;
            } else if (c == 0 || c >= 0x80) {
                plus++;
            }
        }
        return len + plus;
    }

}
