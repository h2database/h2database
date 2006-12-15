/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.util.MathUtils;
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
import org.h2.value.ValueStringIgnoreCase;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;

/**
 * @author Thomas
 */
public abstract class DataPage {
    
    static final boolean CHECKSUM = true;
    protected DataHandler handler;
    protected byte[] data;
    protected int pos;
    
    public static DataPage create(DataHandler handler, int capacity) {
        if(handler.getTextStorage()) {
            return new DataPageText(handler, new byte[capacity]);
        } else {
            return new DataPageBinary(handler, new byte[capacity]);
        }
    }

    public static DataPage create(DataHandler handler, byte[] buff) {
        if(handler.getTextStorage()) {
            return new DataPageText(handler, buff);
        } else {
            return new DataPageBinary(handler, buff);
        }
    }

    protected DataPage(DataHandler handler, int capacity) {
        this(handler, new byte[capacity]);
    }
    
    protected DataPage(DataHandler handler, byte[] data) {
        this.handler = handler;
        this.data = data;
    }

    public void checkCapacity(int plus) {
        if (pos + plus >= data.length) {
            byte[] d = new byte[(data.length+plus) * 2];
            // must copy everything, because pos could be 0 and data may be still required
            System.arraycopy(data, 0, d, 0, data.length);
            data = d;
        }
    }
    
    public abstract void updateChecksum();
    public abstract void check(int len) throws SQLException;
    public abstract int getFillerLength();
    public abstract void setInt(int pos, int x);

    public int length() {
        return pos;
    }

    public byte[] getBytes() {
        return data;
    }

    public void reset() {
        pos = 0;
    }

    public void writeDataPageNoSize(DataPage page) {
        checkCapacity(page.pos);
        // don't write filler
        int len = page.pos - getFillerLength();
        System.arraycopy(page.data, 0, data, pos, len);
        pos += len;
    }

    public DataPage readDataPageNoSize() {
        int len = data.length - pos;
        DataPage page = DataPage.create(handler, len);
        System.arraycopy(data, pos, page.data, 0, len);
        page.pos = len;
        return page;
    }
    
    public void write(byte[] buff, int off, int len) {
        checkCapacity(len);
        System.arraycopy(buff, 0, data, pos, len);
        pos += len;
    }
    
    public void read(byte[] buff, int off, int len) {
        System.arraycopy(data, pos, buff, off, len);
        pos += len;
    }

    public void writeByte(byte x) {
        data[pos++] = x;
    }

    public int readByte() {
        return data[pos++];
    }

    public abstract void writeInt(int x);
    public abstract int readInt();
    public abstract int getIntLen();
    public abstract int getLongLen(long x);
    public abstract int getStringLen(String s);
    public abstract String readString();
    public abstract void writeString(String s);

    public long readLong() {
        return ((long)(readInt()) << 32) + (readInt() & 0xffffffffL);
    }

    public void writeLong(long x) {
        writeInt((int)(x >>> 32));
        writeInt((int)x);
    }    

    public void writeValue(Value v) throws SQLException {
        if(Constants.CHECK) {
            checkCapacity(8);
        }
        // TODO text output: could be in the Value... classes
        if (v == ValueNull.INSTANCE) {
            data[pos++] = '-';
            return;
        }
        int start = pos;
        data[pos++] = (byte)(v.getType() + 'a');
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
            writeLong(v.getTime().getTime());
            break;
        case Value.DATE:
            writeLong(v.getDate().getTime());
            break;
        case Value.TIMESTAMP: {
            Timestamp ts = v.getTimestamp();
            writeLong(ts.getTime());
            writeInt(ts.getNanos());
            break;
        }
        case Value.JAVA_OBJECT:
        case Value.BYTES: {
            byte[] b = v.getBytes();
            writeInt(b.length);
            write(b, 0, b.length);
            break;
        }
        case Value.UUID: {
            ValueUuid uuid = (ValueUuid)v;
            writeLong(uuid.getHigh());
            writeLong(uuid.getLow());
            break;
        }
        case Value.STRING:
        case Value.STRING_IGNORECASE:
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
            ValueLob lob = (ValueLob)v;
            lob.convertToFileIfRequired(handler);
            byte[] small = lob.getSmall();
            if(small == null) {
                // TODO lob: currently use -2 for historical reasons (-1 didn't store precision)
                writeInt(-2);
                writeInt(lob.getTableId());
                writeInt(lob.getObjectId());
                writeLong(lob.getPrecision());
                writeByte((byte)(lob.useCompression() ? 1 : 0)); // compression flag
            } else {
                writeInt(small.length);
                write(small, 0, small.length);
            }
            break;
        }
        case Value.ARRAY: {
            Value[] list = ((ValueArray)v).getList();
            writeInt(list.length);
            for(int i=0; i<list.length; i++) {
                writeValue(list[i]);
            }
            break;
        }
        default:
            throw Message.getInternalError("type=" + v.getType());
        }
        if(Constants.CHECK) {
            if(pos - start != getValueLen(v)) {
                throw Message.getInternalError("value size error: got " + (pos-start) + " expected " + getValueLen(v));
            }
        }
    }
    
    public int getValueLen(Value v) throws SQLException {
        if (v == ValueNull.INSTANCE) {
            return 1;
        }
        switch (v.getType()) {
        case Value.BOOLEAN:
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
            return 1 + getIntLen();
        case Value.LONG:
            return 1 + getLongLen(v.getLong());
        case Value.DOUBLE:
            return 1 + getLongLen(Double.doubleToLongBits(v.getDouble()));
        case Value.FLOAT:
            return 1 + getIntLen();
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.DECIMAL:
            return 1 + getStringLen(v.getString());
        case Value.JAVA_OBJECT:
        case Value.BYTES: {
            int len = v.getBytes().length;
            return 1 + getIntLen() + len;
        }
        case Value.UUID: {
            ValueUuid uuid = (ValueUuid) v;
            return 1 + getLongLen(uuid.getHigh()) + getLongLen(uuid.getLow());
        }
        case Value.TIME:
            return 1 + getLongLen(v.getTime().getTime());
        case Value.DATE:
            return 1 + getLongLen(v.getDate().getTime());
        case Value.TIMESTAMP: {
            Timestamp ts = v.getTimestamp();
            return 1 + getLongLen(ts.getTime()) + getIntLen();
        }
        case Value.BLOB: 
        case Value.CLOB:{
            int len = 1;
            ValueLob lob = (ValueLob)v;
            lob.convertToFileIfRequired(handler);            
            byte[] small = lob.getSmall();
            if(small != null) {
                len += getIntLen() + small.length;
            } else {
                len += getIntLen() + getIntLen() + getIntLen() + getLongLen(lob.getPrecision()) + 1;
            }
            return len;
        }
        case Value.ARRAY: {
            Value[] list = ((ValueArray)v).getList();
            int len = 1 + getIntLen();
            for(int i=0; i<list.length; i++) {
                len += getValueLen(list[i]);
            }
            return len;
        }            
        default:
            throw Message.getInternalError("type=" + v.getType());
        }
    }

    public Value readValue() throws SQLException {
        int type = data[pos++];
        if(type == '-') {
            return ValueNull.INSTANCE;
        }      
        type = (type - 'a');
        switch (type) {
        case Value.BOOLEAN:
            return ValueBoolean.get(readInt() == 1);
        case Value.BYTE:
            return ValueByte.get((byte)readInt());
        case Value.SHORT:
            return ValueShort.get((short)readInt());
        case Value.INT:
            return ValueInt.get(readInt());
        case Value.LONG:
            return ValueLong.get(readLong());
        case Value.DECIMAL:
            return ValueDecimal.get(new BigDecimal(readString()));
        case Value.DATE:
            return ValueDate.get(new Date(readLong()));
        case Value.TIME:
            return ValueTime.get(new Time(readLong()));
        case Value.TIMESTAMP: {
            Timestamp ts = new Timestamp(readLong());
            ts.setNanos(readInt());
            return ValueTimestamp.get(ts);
        }
        case Value.JAVA_OBJECT: {
            int len = readInt();
            byte[] b = new byte[len];
            read(b, 0, len);
            return ValueJavaObject.get(b);
        }
        case Value.BYTES: {
            int len = readInt();
            byte[] b = new byte[len];
            read(b, 0, len);
            return ValueBytes.get(b);
        }
        case Value.UUID:
            return ValueUuid.get(readLong(), readLong());
        case Value.STRING:
            return ValueString.get(readString());
        case Value.STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(readString());
        case Value.DOUBLE:
            return ValueDouble.get(Double.longBitsToDouble(readLong()));
        case Value.FLOAT:
            return ValueFloat.get(Float.intBitsToFloat(readInt()));
        case Value.BLOB:
        case Value.CLOB: {
            int smallLen = readInt();
            if(smallLen >= 0) {
                byte[] small = new byte[smallLen];
                read(small, 0, smallLen);
                return ValueLob.createSmallLob(type, small);
            } else {
                int tableId = readInt();
                int objectId = readInt();
                long precision = 0;
                boolean compression = false;
                // TODO lob: -2 is for historical reasons (-1 didn't store precision)
                if(smallLen == -2) {
                    precision = readLong();
                    compression = readByte() == 1;
                }
                return ValueLob.open(type, handler, tableId, objectId, precision, compression);
            }
        }
        case Value.ARRAY: {
            int len = readInt();
            Value[] list = new Value[len];
            for(int i=0; i<len; i++) {
                list[i] = readValue();
            }
            return ValueArray.get(list);
        }
        default:
            throw Message.getInternalError("type=" + type);
        }
    }
    
    public abstract void fill(int len);

    public void fillAligned() {
        // TODO datapage: fillAligned should not use a fixed constant '2'
        // 0..6 > 8, 7..14 > 16, 15..22 > 24, ...
        fill(MathUtils.roundUp(pos+2, Constants.FILE_BLOCK_SIZE));
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

}
