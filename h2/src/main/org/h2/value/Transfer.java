/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Map.Entry;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.security.SHA256;
import org.h2.store.Data;
import org.h2.store.DataReader;
import org.h2.util.Bits;
import org.h2.util.DateTimeUtils;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.NetUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.lob.LobData;
import org.h2.value.lob.LobDataDatabase;
import org.h2.value.lob.LobDataFetchOnDemand;

/**
 * The transfer class is used to send and receive Value objects.
 * It is used on both the client side, and on the server side.
 */
public final class Transfer {

    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int LOB_MAGIC = 0x1234;
    private static final int LOB_MAC_SALT_LENGTH = 16;

    private static final int NULL = 0;
    private static final int BOOLEAN = 1;
    private static final int TINYINT = 2;
    private static final int SMALLINT = 3;
    private static final int INTEGER = 4;
    private static final int BIGINT = 5;
    private static final int NUMERIC = 6;
    private static final int DOUBLE = 7;
    private static final int REAL = 8;
    private static final int TIME = 9;
    private static final int DATE = 10;
    private static final int TIMESTAMP = 11;
    private static final int VARBINARY = 12;
    private static final int VARCHAR = 13;
    private static final int VARCHAR_IGNORECASE = 14;
    private static final int BLOB = 15;
    private static final int CLOB = 16;
    private static final int ARRAY = 17;
    private static final int JAVA_OBJECT = 19;
    private static final int UUID = 20;
    private static final int CHAR = 21;
    private static final int GEOMETRY = 22;
    // 1.4.192
    private static final int TIMESTAMP_TZ = 24;
    // 1.4.195
    private static final int ENUM = 25;
    // 1.4.198
    private static final int INTERVAL = 26;
    private static final int ROW = 27;
    // 1.4.200
    private static final int JSON = 28;
    private static final int TIME_TZ = 29;
    // 2.0.202
    private static final int BINARY = 30;
    private static final int DECFLOAT = 31;

    private static final int[] VALUE_TO_TI = new int[Value.TYPE_COUNT + 1];
    private static final int[] TI_TO_VALUE = new int[45];

    static {
        addType(-1, Value.UNKNOWN);
        addType(NULL, Value.NULL);
        addType(BOOLEAN, Value.BOOLEAN);
        addType(TINYINT, Value.TINYINT);
        addType(SMALLINT, Value.SMALLINT);
        addType(INTEGER, Value.INTEGER);
        addType(BIGINT, Value.BIGINT);
        addType(NUMERIC, Value.NUMERIC);
        addType(DOUBLE, Value.DOUBLE);
        addType(REAL, Value.REAL);
        addType(TIME, Value.TIME);
        addType(DATE, Value.DATE);
        addType(TIMESTAMP, Value.TIMESTAMP);
        addType(VARBINARY, Value.VARBINARY);
        addType(VARCHAR, Value.VARCHAR);
        addType(VARCHAR_IGNORECASE, Value.VARCHAR_IGNORECASE);
        addType(BLOB, Value.BLOB);
        addType(CLOB, Value.CLOB);
        addType(ARRAY, Value.ARRAY);
        addType(JAVA_OBJECT, Value.JAVA_OBJECT);
        addType(UUID, Value.UUID);
        addType(CHAR, Value.CHAR);
        addType(GEOMETRY, Value.GEOMETRY);
        addType(TIMESTAMP_TZ, Value.TIMESTAMP_TZ);
        addType(ENUM, Value.ENUM);
        addType(26, Value.INTERVAL_YEAR);
        addType(27, Value.INTERVAL_MONTH);
        addType(28, Value.INTERVAL_DAY);
        addType(29, Value.INTERVAL_HOUR);
        addType(30, Value.INTERVAL_MINUTE);
        addType(31, Value.INTERVAL_SECOND);
        addType(32, Value.INTERVAL_YEAR_TO_MONTH);
        addType(33, Value.INTERVAL_DAY_TO_HOUR);
        addType(34, Value.INTERVAL_DAY_TO_MINUTE);
        addType(35, Value.INTERVAL_DAY_TO_SECOND);
        addType(36, Value.INTERVAL_HOUR_TO_MINUTE);
        addType(37, Value.INTERVAL_HOUR_TO_SECOND);
        addType(38, Value.INTERVAL_MINUTE_TO_SECOND);
        addType(39, Value.ROW);
        addType(40, Value.JSON);
        addType(41, Value.TIME_TZ);
        addType(42, Value.BINARY);
        addType(43, Value.DECFLOAT);
    }

    private static void addType(int typeInformationType, int valueType) {
        VALUE_TO_TI[valueType + 1] = typeInformationType;
        TI_TO_VALUE[typeInformationType + 1] = valueType;
    }

    private final ReentrantLock lock = new ReentrantLock();

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private Session session;
    private boolean ssl;
    private int version;
    private byte[] lobMacSalt;

    /**
     * Create a new transfer object for the specified session.
     *
     * @param session the session
     * @param s the socket
     */
    public Transfer(Session session, Socket s) {
        this.session = session;
        this.socket = s;
    }

    /**
     * Locks this object with a reentrant lock.
     *
     * <pre>
     * lock();
     * try {
     *     ...
     * } finally {
     *     unlock();
     * }
     * </pre>
     */
    private void lock() {
        lock.lock();
    }

    /**
     * Unlocks this object.
     *
     * @see #lock()
     */
    private void unlock() {
        lock.unlock();
    }

    /**
     * Initialize the transfer object. This method will try to open an input and
     * output stream.
     * @throws IOException on failure
     */
    public void init() throws IOException {
        lock();
        try {
            if (socket != null) {
                in = new DataInputStream(
                        new BufferedInputStream(
                                socket.getInputStream(), Transfer.BUFFER_SIZE));
                out = new DataOutputStream(
                        new BufferedOutputStream(
                                socket.getOutputStream(), Transfer.BUFFER_SIZE));
            }
        } finally {
            unlock();
        }
    }

    /**
     * Write pending changes.
     * @throws IOException on failure
     */
    public void flush() throws IOException {
        out.flush();
    }

    /**
     * Write a boolean.
     *
     * @param x the value
     * @return itself
     * @throws IOException on failure
     */
    public Transfer writeBoolean(boolean x) throws IOException {
        out.writeByte((byte) (x ? 1 : 0));
        return this;
    }

    /**
     * Read a boolean.
     *
     * @return the value
     * @throws IOException on failure
     */
    public boolean readBoolean() throws IOException {
        return in.readByte() != 0;
    }

    /**
     * Write a byte.
     *
     * @param x the value
     * @return itself
     * @throws IOException on failure
     */
    public Transfer writeByte(byte x) throws IOException {
        out.writeByte(x);
        return this;
    }

    /**
     * Read a byte.
     *
     * @return the value
     * @throws IOException on failure
     */
    public byte readByte() throws IOException {
        return in.readByte();
    }

    /**
     * Write a short.
     *
     * @param x the value
     * @return itself
     * @throws IOException on failure
     */
    private Transfer writeShort(short x) throws IOException {
        out.writeShort(x);
        return this;
    }

    /**
     * Read a short.
     *
     * @return the value
     * @throws IOException on failure
     */
    private short readShort() throws IOException {
        return in.readShort();
    }

    /**
     * Write an int.
     *
     * @param x the value
     * @return itself
     * @throws IOException on failure
     */
    public Transfer writeInt(int x) throws IOException {
        out.writeInt(x);
        return this;
    }

    /**
     * Read an int.
     *
     * @return the value
     * @throws IOException on failure
     */
    public int readInt() throws IOException {
        return in.readInt();
    }

    /**
     * Write a long.
     *
     * @param x the value
     * @return itself
     * @throws IOException on failure
     */
    public Transfer writeLong(long x) throws IOException {
        out.writeLong(x);
        return this;
    }

    /**
     * Read a long.
     *
     * @return the value
     * @throws IOException on failure
     */
    public long readLong() throws IOException {
        return in.readLong();
    }

    /**
     * Write a double.
     *
     * @param i the value
     * @return itself
     * @throws IOException on failure
     */
    private Transfer writeDouble(double i) throws IOException {
        out.writeDouble(i);
        return this;
    }

    /**
     * Write a float.
     *
     * @param i the value
     * @return itself
     */
    private Transfer writeFloat(float i) throws IOException {
        out.writeFloat(i);
        return this;
    }

    /**
     * Read a double.
     *
     * @return the value
     * @throws IOException on failure
     */
    private double readDouble() throws IOException {
        return in.readDouble();
    }

    /**
     * Read a float.
     *
     * @return the value
     * @throws IOException on failure
     */
    private float readFloat() throws IOException {
        return in.readFloat();
    }

    /**
     * Write a string. The maximum string length is Integer.MAX_VALUE.
     *
     * @param s the value
     * @return itself
     * @throws IOException on failure
     */
    public Transfer writeString(String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(s.length());
            out.writeChars(s);
        }
        return this;
    }

    /**
     * Read a string.
     *
     * @return the value
     * @throws IOException on failure
     */
    public String readString() throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        StringBuilder buff = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            buff.append(in.readChar());
        }
        String s = buff.toString();
        s = StringUtils.cache(s);
        return s;
    }

    /**
     * Write a byte array.
     *
     * @param data the value
     * @return itself
     * @throws IOException on failure
     */
    public Transfer writeBytes(byte[] data) throws IOException {
        if (data == null) {
            writeInt(-1);
        } else {
            writeInt(data.length);
            out.write(data);
        }
        return this;
    }

    /**
     * Write a number of bytes.
     *
     * @param buff the value
     * @param off the offset
     * @param len the length
     * @return itself
     * @throws IOException on failure
     */
    public Transfer writeBytes(byte[] buff, int off, int len) throws IOException {
        out.write(buff, off, len);
        return this;
    }

    /**
     * Read a byte array.
     *
     * @return the value
     * @throws IOException on failure
     */
    public byte[] readBytes() throws IOException {
        int len = readInt();
        if (len == -1) {
            return null;
        }
        byte[] b = Utils.newBytes(len);
        in.readFully(b);
        return b;
    }

    /**
     * Read a number of bytes.
     *
     * @param buff the target buffer
     * @param off the offset
     * @param len the number of bytes to read
     * @throws IOException on failure
     */
    public void readBytes(byte[] buff, int off, int len) throws IOException {
        in.readFully(buff, off, len);
    }

    /**
     * Close the transfer object and the socket.
     */
    public void close() {
        lock();
        try {
            if (socket != null) {
                try {
                    if (out != null) {
                        out.flush();
                    }
                    socket.close();
                } catch (IOException e) {
                    DbException.traceThrowable(e);
                } finally {
                    socket = null;
                }
            }
        } finally {
            unlock();
        }
    }

    /**
     * Write value type, precision, and scale.
     *
     * @param type data type information
     * @return itself
     * @throws IOException on failure
     */
    public Transfer writeTypeInfo(TypeInfo type) throws IOException {
        if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
            writeTypeInfo20(type);
        } else {
            writeTypeInfo19(type);
        }
        return this;
    }

    private void writeTypeInfo20(TypeInfo type) throws IOException {
        int valueType = type.getValueType();
        writeInt(VALUE_TO_TI[valueType + 1]);
        switch (valueType) {
        case Value.UNKNOWN:
        case Value.NULL:
        case Value.BOOLEAN:
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT:
        case Value.DATE:
        case Value.UUID:
            break;
        case Value.CHAR:
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.BINARY:
        case Value.VARBINARY:
        case Value.DECFLOAT:
        case Value.JAVA_OBJECT:
        case Value.JSON:
            writeInt((int) type.getDeclaredPrecision());
            break;
        case Value.CLOB:
        case Value.BLOB:
            writeLong(type.getDeclaredPrecision());
            break;
        case Value.NUMERIC:
            writeInt((int) type.getDeclaredPrecision());
            writeInt(type.getDeclaredScale());
            writeBoolean(type.getExtTypeInfo() != null);
            break;
        case Value.REAL:
        case Value.DOUBLE:
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_MINUTE:
            writeBytePrecisionWithDefault(type.getDeclaredPrecision());
            break;
        case Value.TIME:
        case Value.TIME_TZ:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            writeByteScaleWithDefault(type.getDeclaredScale());
            break;
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            writeBytePrecisionWithDefault(type.getDeclaredPrecision());
            writeByteScaleWithDefault(type.getDeclaredScale());
            break;
        case Value.ENUM:
            writeTypeInfoEnum(type);
            break;
        case Value.GEOMETRY:
            writeTypeInfoGeometry(type);
            break;
        case Value.ARRAY:
            writeInt((int) type.getDeclaredPrecision());
            writeTypeInfo((TypeInfo) type.getExtTypeInfo());
            break;
        case Value.ROW:
            writeTypeInfoRow(type);
            break;
        default:
            throw DbException.getUnsupportedException("value type " + valueType);
        }
    }

    private void writeBytePrecisionWithDefault(long precision) throws IOException {
        writeByte(precision >= 0 ? (byte) precision : -1);
    }

    private void writeByteScaleWithDefault(int scale) throws IOException {
        writeByte(scale >= 0 ? (byte) scale : -1);
    }

    private void writeTypeInfoEnum(TypeInfo type) throws IOException {
        ExtTypeInfoEnum ext = (ExtTypeInfoEnum) type.getExtTypeInfo();
        if (ext != null) {
            int c = ext.getCount();
            writeInt(c);
            for (int i = 0; i < c; i++) {
                writeString(ext.getEnumerator(i));
            }
        } else {
            writeInt(0);
        }
    }

    private void writeTypeInfoGeometry(TypeInfo type) throws IOException {
        ExtTypeInfoGeometry ext = (ExtTypeInfoGeometry) type.getExtTypeInfo();
        if (ext == null) {
            writeByte((byte) 0);
        } else {
            int t = ext.getType();
            Integer srid = ext.getSrid();
            if (t == 0) {
                if (srid == null) {
                    writeByte((byte) 0);
                } else {
                    writeByte((byte) 2);
                    writeInt(srid);
                }
            } else {
                if (srid == null) {
                    writeByte((byte) 1);
                    writeShort((short) t);
                } else {
                    writeByte((byte) 3);
                    writeShort((short) t);
                    writeInt(srid);
                }
            }
        }
    }

    private void writeTypeInfoRow(TypeInfo type) throws IOException {
        Set<Map.Entry<String, TypeInfo>> fields = ((ExtTypeInfoRow) type.getExtTypeInfo()).getFields();
        writeInt(fields.size());
        for (Map.Entry<String, TypeInfo> field : fields) {
            writeString(field.getKey()).writeTypeInfo(field.getValue());
        }
    }

    private void writeTypeInfo19(TypeInfo type) throws IOException {
        int valueType = type.getValueType();
        switch (valueType) {
        case Value.BINARY:
            valueType = Value.VARBINARY;
            break;
        case Value.DECFLOAT:
            valueType = Value.NUMERIC;
            break;
        }
        writeInt(VALUE_TO_TI[valueType + 1]).writeLong(type.getPrecision()).writeInt(type.getScale());
    }

    /**
     * Read a type information.
     *
     * @return the type information
     * @throws IOException on failure
     */
    public TypeInfo readTypeInfo() throws IOException {
        if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
            return readTypeInfo20();
        } else {
            return readTypeInfo19();
        }
    }

    private TypeInfo readTypeInfo20() throws IOException {
        int valueType = TI_TO_VALUE[readInt() + 1];
        long precision = -1L;
        int scale = -1;
        ExtTypeInfo ext = null;
        switch (valueType) {
        case Value.UNKNOWN:
        case Value.NULL:
        case Value.BOOLEAN:
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT:
        case Value.DATE:
        case Value.UUID:
            break;
        case Value.CHAR:
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.BINARY:
        case Value.VARBINARY:
        case Value.DECFLOAT:
        case Value.JAVA_OBJECT:
        case Value.JSON:
            precision = readInt();
            break;
        case Value.CLOB:
        case Value.BLOB:
            precision = readLong();
            break;
        case Value.NUMERIC:
            precision = readInt();
            scale = readInt();
            if (readBoolean()) {
                ext = ExtTypeInfoNumeric.DECIMAL;
            }
            break;
        case Value.REAL:
        case Value.DOUBLE:
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_MINUTE:
            precision = readByte();
            break;
        case Value.TIME:
        case Value.TIME_TZ:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            scale = readByte();
            break;
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            precision = readByte();
            scale = readByte();
            break;
        case Value.ENUM:
            ext = readTypeInfoEnum();
            break;
        case Value.GEOMETRY:
            ext = readTypeInfoGeometry();
            break;
        case Value.ARRAY:
            precision = readInt();
            ext = readTypeInfo();
            break;
        case Value.ROW:
            ext = readTypeInfoRow();
            break;
        default:
            throw DbException.getUnsupportedException("value type " + valueType);
        }
        return TypeInfo.getTypeInfo(valueType, precision, scale, ext);
    }

    private ExtTypeInfo readTypeInfoEnum() throws IOException {
        ExtTypeInfo ext;
        int c = readInt();
        if (c > 0) {
            String[] enumerators = new String[c];
            for (int i = 0; i < c; i++) {
                enumerators[i] = readString();
            }
            ext = new ExtTypeInfoEnum(enumerators);
        } else {
            ext = null;
        }
        return ext;
    }

    private ExtTypeInfo readTypeInfoGeometry() throws IOException {
        ExtTypeInfo ext;
        int e = readByte();
        switch (e) {
        case 0:
            ext = null;
            break;
        case 1:
            ext = new ExtTypeInfoGeometry(readShort(), null);
            break;
        case 2:
            ext = new ExtTypeInfoGeometry(0, readInt());
            break;
        case 3:
            ext = new ExtTypeInfoGeometry(readShort(), readInt());
            break;
        default:
            throw DbException.getUnsupportedException("GEOMETRY type encoding " + e);
        }
        return ext;
    }

    private ExtTypeInfo readTypeInfoRow() throws IOException {
        LinkedHashMap<String, TypeInfo> fields = new LinkedHashMap<>();
        for (int i = 0, l = readInt(); i < l; i++) {
            String name = readString();
            if (fields.putIfAbsent(name, readTypeInfo()) != null) {
                throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, name);
            }
        }
        return new ExtTypeInfoRow(fields);
    }

    private TypeInfo readTypeInfo19() throws IOException {
        return TypeInfo.getTypeInfo(TI_TO_VALUE[readInt() + 1], readLong(), readInt(), null);
    }

    /**
     * Write a value.
     *
     * @param v the value
     * @throws IOException on failure
     */
    public void writeValue(Value v) throws IOException {
        int type = v.getValueType();
        switch (type) {
        case Value.NULL:
            writeInt(NULL);
            break;
        case Value.BINARY:
            if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
                writeInt(BINARY);
                writeBytes(v.getBytesNoCopy());
                break;
            }
            //$FALL-THROUGH$
        case Value.VARBINARY:
            writeInt(VARBINARY);
            writeBytes(v.getBytesNoCopy());
            break;
        case Value.JAVA_OBJECT:
            writeInt(JAVA_OBJECT);
            writeBytes(v.getBytesNoCopy());
            break;
        case Value.UUID: {
            writeInt(UUID);
            ValueUuid uuid = (ValueUuid) v;
            writeLong(uuid.getHigh());
            writeLong(uuid.getLow());
            break;
        }
        case Value.BOOLEAN:
            writeInt(BOOLEAN);
            writeBoolean(v.getBoolean());
            break;
        case Value.TINYINT:
            writeInt(TINYINT);
            writeByte(v.getByte());
            break;
        case Value.TIME:
            writeInt(TIME);
            writeLong(((ValueTime) v).getNanos());
            break;
        case Value.TIME_TZ: {
            ValueTimeTimeZone t = (ValueTimeTimeZone) v;
            if (version >= Constants.TCP_PROTOCOL_VERSION_19) {
                writeInt(TIME_TZ);
                writeLong(t.getNanos());
                writeInt(t.getTimeZoneOffsetSeconds());
            } else {
                writeInt(TIME);
                /*
                 * Don't call SessionRemote.currentTimestamp(), it may require
                 * own remote call and old server will not return custom time
                 * zone anyway.
                 */
                ValueTimestampTimeZone current = session.isRemote()
                        ? DateTimeUtils.currentTimestamp(DateTimeUtils.getTimeZone()) : session.currentTimestamp();
                writeLong(DateTimeUtils.normalizeNanosOfDay(t.getNanos() +
                        (t.getTimeZoneOffsetSeconds() - current.getTimeZoneOffsetSeconds())
                        * DateTimeUtils.NANOS_PER_DAY));
            }
            break;
        }
        case Value.DATE:
            writeInt(DATE);
            writeLong(((ValueDate) v).getDateValue());
            break;
        case Value.TIMESTAMP: {
            writeInt(TIMESTAMP);
            ValueTimestamp ts = (ValueTimestamp) v;
            writeLong(ts.getDateValue());
            writeLong(ts.getTimeNanos());
            break;
        }
        case Value.TIMESTAMP_TZ: {
            writeInt(TIMESTAMP_TZ);
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) v;
            writeLong(ts.getDateValue());
            writeLong(ts.getTimeNanos());
            int timeZoneOffset = ts.getTimeZoneOffsetSeconds();
            writeInt(version >= Constants.TCP_PROTOCOL_VERSION_19 //
                    ? timeZoneOffset : timeZoneOffset / 60);
            break;
        }
        case Value.DECFLOAT:
            if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
                writeInt(DECFLOAT);
                writeString(v.getString());
                break;
            }
        //$FALL-THROUGH$
        case Value.NUMERIC:
            writeInt(NUMERIC);
            writeString(v.getString());
            break;
        case Value.DOUBLE:
            writeInt(DOUBLE);
            writeDouble(v.getDouble());
            break;
        case Value.REAL:
            writeInt(REAL);
            writeFloat(v.getFloat());
            break;
        case Value.INTEGER:
            writeInt(INTEGER);
            writeInt(v.getInt());
            break;
        case Value.BIGINT:
            writeInt(BIGINT);
            writeLong(v.getLong());
            break;
        case Value.SMALLINT:
            writeInt(SMALLINT);
            if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
                writeShort(v.getShort());
            } else {
                writeInt(v.getShort());
            }
            break;
        case Value.VARCHAR:
            writeInt(VARCHAR);
            writeString(v.getString());
            break;
        case Value.VARCHAR_IGNORECASE:
            writeInt(VARCHAR_IGNORECASE);
            writeString(v.getString());
            break;
        case Value.CHAR:
            writeInt(CHAR);
            writeString(v.getString());
            break;
        case Value.BLOB: {
            writeInt(BLOB);
            ValueBlob lob = (ValueBlob) v;
            LobData lobData = lob.getLobData();
            long length = lob.octetLength();
            if (lobData instanceof LobDataDatabase) {
                LobDataDatabase lobDataDatabase = (LobDataDatabase) lobData;
                writeLong(-1);
                writeInt(lobDataDatabase.getTableId());
                writeLong(lobDataDatabase.getLobId());
                writeBytes(calculateLobMac(lobDataDatabase.getLobId()));
                writeLong(length);
                break;
            }
            if (length < 0) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "length=" + length);
            }
            writeLong(length);
            long written = IOUtils.copyAndCloseInput(lob.getInputStream(), out);
            if (written != length) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "length:" + length + " written:" + written);
            }
            writeInt(LOB_MAGIC);
            break;
        }
        case Value.CLOB: {
            writeInt(CLOB);
            ValueClob lob = (ValueClob) v;
            LobData lobData = lob.getLobData();
            long charLength = lob.charLength();
            if (lobData instanceof LobDataDatabase) {
                LobDataDatabase lobDataDatabase = (LobDataDatabase) lobData;
                writeLong(-1);
                writeInt(lobDataDatabase.getTableId());
                writeLong(lobDataDatabase.getLobId());
                writeBytes(calculateLobMac(lobDataDatabase.getLobId()));
                if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
                    writeLong(lob.octetLength());
                }
                writeLong(charLength);
                break;
            }
            if (charLength < 0) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "length=" + charLength);
            }
            writeLong(charLength);
            Reader reader = lob.getReader();
            Data.copyString(reader, out);
            writeInt(LOB_MAGIC);
            break;
        }
        case Value.ARRAY: {
            writeInt(ARRAY);
            ValueArray va = (ValueArray) v;
            Value[] list = va.getList();
            int len = list.length;
            writeInt(len);
            for (Value value : list) {
                writeValue(value);
            }
            break;
        }
        case Value.ROW: {
            writeInt(version >= Constants.TCP_PROTOCOL_VERSION_18 ? ROW : ARRAY);
            ValueRow va = (ValueRow) v;
            Value[] list = va.getList();
            int len = list.length;
            writeInt(len);
            for (Value value : list) {
                writeValue(value);
            }
            break;
        }
        case Value.ENUM: {
            writeInt(ENUM);
            writeInt(v.getInt());
            if (version < Constants.TCP_PROTOCOL_VERSION_20) {
                writeString(v.getString());
            }
            break;
        }
        case Value.GEOMETRY:
            writeInt(GEOMETRY);
            writeBytes(v.getBytesNoCopy());
            break;
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
            if (version >= Constants.TCP_PROTOCOL_VERSION_18) {
                ValueInterval interval = (ValueInterval) v;
                int ordinal = type - Value.INTERVAL_YEAR;
                if (interval.isNegative()) {
                    ordinal = ~ordinal;
                }
                writeInt(INTERVAL);
                writeByte((byte) ordinal);
                writeLong(interval.getLeading());
            } else {
                writeInt(VARCHAR);
                writeString(v.getString());
            }
            break;
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            if (version >= Constants.TCP_PROTOCOL_VERSION_18) {
                ValueInterval interval = (ValueInterval) v;
                int ordinal = type - Value.INTERVAL_YEAR;
                if (interval.isNegative()) {
                    ordinal = ~ordinal;
                }
                writeInt(INTERVAL);
                writeByte((byte) ordinal);
                writeLong(interval.getLeading());
                writeLong(interval.getRemaining());
            } else {
                writeInt(VARCHAR);
                writeString(v.getString());
            }
            break;
        case Value.JSON: {
            writeInt(JSON);
            writeBytes(v.getBytesNoCopy());
            break;
        }
        default:
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "type=" + type);
        }
    }

    /**
     * Read a value.
     *
     * @param columnType the data type of value, or {@code null}
     * @return the value
     * @throws IOException on failure
     */
    public Value readValue(TypeInfo columnType) throws IOException {
        int type = readInt();
        switch (type) {
        case NULL:
            return ValueNull.INSTANCE;
        case VARBINARY:
            return ValueVarbinary.getNoCopy(readBytes());
        case BINARY:
            return ValueBinary.getNoCopy(readBytes());
        case UUID:
            return ValueUuid.get(readLong(), readLong());
        case JAVA_OBJECT:
            return ValueJavaObject.getNoCopy(readBytes());
        case BOOLEAN:
            return ValueBoolean.get(readBoolean());
        case TINYINT:
            return ValueTinyint.get(readByte());
        case DATE:
            return ValueDate.fromDateValue(readLong());
        case TIME:
            return ValueTime.fromNanos(readLong());
        case TIME_TZ:
            return ValueTimeTimeZone.fromNanos(readLong(), readInt());
        case TIMESTAMP:
            return ValueTimestamp.fromDateValueAndNanos(readLong(), readLong());
        case TIMESTAMP_TZ: {
            long dateValue = readLong(), timeNanos = readLong();
            int timeZoneOffset = readInt();
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos,
                    version >= Constants.TCP_PROTOCOL_VERSION_19 ? timeZoneOffset : timeZoneOffset * 60);
        }
        case NUMERIC:
            return ValueNumeric.get(new BigDecimal(readString()));
        case DOUBLE:
            return ValueDouble.get(readDouble());
        case REAL:
            return ValueReal.get(readFloat());
        case ENUM: {
            int ordinal = readInt();
            if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
                return ((ExtTypeInfoEnum) columnType.getExtTypeInfo()).getValue(ordinal, session);
            }
            return ValueEnumBase.get(readString(), ordinal);
        }
        case INTEGER:
            return ValueInteger.get(readInt());
        case BIGINT:
            return ValueBigint.get(readLong());
        case SMALLINT:
            if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
                return ValueSmallint.get(readShort());
            } else {
                return ValueSmallint.get((short) readInt());
            }
        case VARCHAR:
            return ValueVarchar.get(readString());
        case VARCHAR_IGNORECASE:
            return ValueVarcharIgnoreCase.get(readString());
        case CHAR:
            return ValueChar.get(readString());
        case BLOB: {
            long length = readLong();
            if (length == -1) {
                // fetch-on-demand LOB
                int tableId = readInt();
                long id = readLong();
                byte[] hmac = readBytes();
                long precision = readLong();
                return new ValueBlob(new LobDataFetchOnDemand(session.getDataHandler(), tableId, id, hmac), precision);
            }
            Value v = session.getDataHandler().getLobStorage().createBlob(in, length);
            int magic = readInt();
            if (magic != LOB_MAGIC) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "magic=" + magic);
            }
            return v;
        }
        case CLOB: {
            long charLength = readLong();
            if (charLength == -1) {
                // fetch-on-demand LOB
                int tableId = readInt();
                long id = readLong();
                byte[] hmac = readBytes();
                long octetLength = version >= Constants.TCP_PROTOCOL_VERSION_20 ? readLong() : -1L;
                charLength = readLong();
                return new ValueClob(new LobDataFetchOnDemand(session.getDataHandler(), tableId, id, hmac),
                        octetLength, charLength);
            }
            if (charLength < 0) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "length="+ charLength);
            }
            Value v = session.getDataHandler().getLobStorage().
                    createClob(new DataReader(in), charLength);
            int magic = readInt();
            if (magic != LOB_MAGIC) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "magic=" + magic);
            }
            return v;
        }
        case ARRAY: {
            int len = readInt();
            if (len < 0) {
                // Unlikely, but possible with H2 1.4.200 and older versions
                len = ~len;
                readString();
            }
            if (columnType != null) {
                TypeInfo elementType = (TypeInfo) columnType.getExtTypeInfo();
                return ValueArray.get(elementType, readArrayElements(len, elementType), session);
            }
            return ValueArray.get(readArrayElements(len, null), session);
        }
        case ROW: {
            int len = readInt();
            Value[] list = new Value[len];
            if (columnType != null) {
                ExtTypeInfoRow extTypeInfoRow = (ExtTypeInfoRow) columnType.getExtTypeInfo();
                Iterator<Entry<String, TypeInfo>> fields = extTypeInfoRow.getFields().iterator();
                for (int i = 0; i < len; i++) {
                    list[i] = readValue(fields.next().getValue());
                }
                return ValueRow.get(columnType, list);
            }
            for (int i = 0; i < len; i++) {
                list[i] = readValue(null);
            }
            return ValueRow.get(list);
        }
        case GEOMETRY:
            return ValueGeometry.get(readBytes());
        case INTERVAL: {
            int ordinal = readByte();
            boolean negative = ordinal < 0;
            if (negative) {
                ordinal = ~ordinal;
            }
            return ValueInterval.from(IntervalQualifier.valueOf(ordinal), negative, readLong(),
                    ordinal < 5 ? 0 : readLong());
        }
        case JSON:
            // Do not trust the value
            return ValueJson.fromJson(readBytes());
        case DECFLOAT: {
            String s = readString();
            switch (s) {
            case "-Infinity":
                return ValueDecfloat.NEGATIVE_INFINITY;
            case "Infinity":
                return ValueDecfloat.POSITIVE_INFINITY;
            case "NaN":
                return ValueDecfloat.NAN;
            default:
                return ValueDecfloat.get(new BigDecimal(s));
            }
        }
        default:
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "type=" + type);
        }
    }

    private Value[] readArrayElements(int len, TypeInfo elementType) throws IOException {
        Value[] list = new Value[len];
        for (int i = 0; i < len; i++) {
            list[i] = readValue(elementType);
        }
        return list;
    }

    /**
     * Read a row count.
     *
     * @return the row count
     * @throws IOException on failure
     */
    public long readRowCount() throws IOException {
        return version >= Constants.TCP_PROTOCOL_VERSION_20 ? readLong() : readInt();
    }

    /**
     * Write a row count.
     *
     * @param rowCount the row count
     * @return itself
     * @throws IOException on failure
     */
    public Transfer writeRowCount(long rowCount) throws IOException {
        return version >= Constants.TCP_PROTOCOL_VERSION_20 ? writeLong(rowCount)
                : writeInt(rowCount < Integer.MAX_VALUE ? (int) rowCount : Integer.MAX_VALUE);
    }

    /**
     * Get the socket.
     *
     * @return the socket
     */
    public Socket getSocket() {
        return socket;
    }

    /**
     * Set the session.
     *
     * @param session the session
     */
    public void setSession(Session session) {
        this.session = session;
    }

    /**
     * Enable or disable SSL.
     *
     * @param ssl the new value
     */
    public void setSSL(boolean ssl) {
        this.ssl = ssl;
    }

    /**
     * Open a new connection to the same address and port as this one.
     *
     * @return the new transfer object
     * @throws IOException on failure
     */
    public Transfer openNewConnection() throws IOException {
        InetAddress address = socket.getInetAddress();
        int port = socket.getPort();
        Socket s2 = NetUtils.createSocket(address, port, ssl);
        Transfer trans = new Transfer(null, s2);
        trans.setSSL(ssl);
        return trans;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    public boolean isClosed() {
        lock();
        try {
            return socket == null || socket.isClosed();
        } finally {
            unlock();
        }
    }

    /**
     * Verify the HMAC.
     *
     * @param hmac the message authentication code
     * @param lobId the lobId
     * @throws DbException if the HMAC does not match
     */
    public void verifyLobMac(byte[] hmac, long lobId) {
        byte[] result = calculateLobMac(lobId);
        if (!Utils.compareSecure(hmac,  result)) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                    "Invalid lob hmac; possibly the connection was re-opened internally");
        }
    }

    private byte[] calculateLobMac(long lobId) {
        if (lobMacSalt == null) {
            lobMacSalt = MathUtils.secureRandomBytes(LOB_MAC_SALT_LENGTH);
        }
        byte[] data = new byte[8];
        Bits.writeLong(data, 0, lobId);
        return SHA256.getHashWithSalt(data, lobMacSalt);
    }

}
