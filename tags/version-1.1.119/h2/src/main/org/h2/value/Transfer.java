/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.Socket;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.SessionInterface;
import org.h2.message.Message;
import org.h2.message.TraceSystem;
import org.h2.tools.SimpleResultSet;
import org.h2.util.ExactUTF8InputStreamReader;
import org.h2.util.IOUtils;
import org.h2.util.MemoryUtils;
import org.h2.util.NetUtils;
import org.h2.util.StringCache;

/**
 * The transfer class is used to send and receive Value objects.
 * It is used on both the client side, and on the server side.
 */
public class Transfer {

    private static final int BUFFER_SIZE = 16 * 1024;
    private static final int LOB_MAGIC = 0x1234;

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private SessionInterface session;
    private boolean ssl;

    /**
     * Create a new transfer object for the specified session.
     *
     * @param session the session
     */
    public Transfer(SessionInterface session) {
        this.session = session;
    }

    /**
     * Set the socket this object uses.
     *
     * @param s the socket
     */
    public void setSocket(Socket s) {
        socket = s;
    }

    /**
     * Initialize the transfer object. This method will try to open an input and
     * output stream.
     */
    public void init() throws IOException {
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), Transfer.BUFFER_SIZE));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), Transfer.BUFFER_SIZE));
    }

    /**
     * Write pending changes.
     */
    public void flush() throws IOException {
        out.flush();
    }

    /**
     * Write a boolean.
     *
     * @param x the value
     * @return itself
     */
    public Transfer writeBoolean(boolean x) throws IOException {
        out.writeByte((byte) (x ? 1 : 0));
        return this;
    }

    /**
     * Read a boolean.
     *
     * @return the value
     */
    public boolean readBoolean() throws IOException {
        return in.readByte() == 1;
    }

    /**
     * Write a byte.
     *
     * @param x the value
     * @return itself
     */
    private Transfer writeByte(byte x) throws IOException {
        out.writeByte(x);
        return this;
    }

    /**
     * Read a byte.
     *
     * @return the value
     */
    private byte readByte() throws IOException {
        return in.readByte();
    }

    /**
     * Write an int.
     *
     * @param x the value
     * @return itself
     */
    public Transfer writeInt(int x) throws IOException {
        out.writeInt(x);
        return this;
    }

    /**
     * Read an int.
     *
     * @return the value
     */
    public int readInt() throws IOException {
        return in.readInt();
    }

    /**
     * Write a long.
     *
     * @param x the value
     * @return itself
     */
    public Transfer writeLong(long x) throws IOException {
        out.writeLong(x);
        return this;
    }

    /**
     * Read a long.
     *
     * @return the value
     */
    public long readLong() throws IOException {
        return in.readLong();
    }

    /**
     * Write a double.
     *
     * @param x the value
     * @return itself
     */
    private Transfer writeDouble(double i) throws IOException {
        out.writeDouble(i);
        return this;
    }

    /**
     * Write a float.
     *
     * @param x the value
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
     */
    private double readDouble() throws IOException {
        return in.readDouble();
    }

    /**
     * Read a float.
     *
     * @return the value
     */
    private float readFloat() throws IOException {
        return in.readFloat();
    }

    /**
     * Write a string. The maximum string length is Integer.MAX_VALUE.
     *
     * @param s the value
     * @return itself
     */
    public Transfer writeString(String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
        } else {
            int len = s.length();
            out.writeInt(len);
            for (int i = 0; i < len; i++) {
                out.writeChar(s.charAt(i));
            }
        }
        return this;
    }

    /**
     * Read a string.
     *
     * @return the value
     */
    public String readString() throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        // TODO optimize: StringBuilder is synchronized, maybe use a char array
        // (but that means more memory)
        StringBuilder buff = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            buff.append(in.readChar());
        }
        String s = buff.toString();
        s = StringCache.get(s);
        return s;
    }

    /**
     * Write a byte array.
     *
     * @param data the value
     * @return itself
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
     * Read a byte array.
     *
     * @return the value
     */
    public byte[] readBytes() throws IOException {
        int len = readInt();
        if (len == -1) {
            return null;
        }
        byte[] b = MemoryUtils.newBytes(len);
        in.readFully(b);
        return b;
    }

    /**
     * Close the transfer object and the socket.
     */
    public void close() {
        if (socket != null) {
            try {
                out.flush();
                if (socket != null) {
                    socket.close();
                }
            } catch (IOException e) {
                TraceSystem.traceThrowable(e);
            } finally {
                socket = null;
            }
        }
    }

    /**
     * Write a value.
     *
     * @param v the value
     */
    public void writeValue(Value v) throws IOException, SQLException {
        int type = v.getType();
        writeInt(type);
        switch (type) {
        case Value.NULL:
            break;
        case Value.BYTES:
        case Value.JAVA_OBJECT:
            writeBytes(v.getBytesNoCopy());
            break;
        case Value.UUID: {
            ValueUuid uuid = (ValueUuid) v;
            writeLong(uuid.getHigh());
            writeLong(uuid.getLow());
            break;
        }
        case Value.BOOLEAN:
            writeBoolean(v.getBoolean().booleanValue());
            break;
        case Value.BYTE:
            writeByte(v.getByte());
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
        case Value.DECIMAL:
            writeString(v.getString());
            break;
        case Value.DOUBLE:
            writeDouble(v.getDouble());
            break;
        case Value.FLOAT:
            writeFloat(v.getFloat());
            break;
        case Value.INT:
            writeInt(v.getInt());
            break;
        case Value.LONG:
            writeLong(v.getLong());
            break;
        case Value.SHORT:
            writeInt(v.getShort());
            break;
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            writeString(v.getString());
            break;
        case Value.BLOB: {
            long length = v.getPrecision();
            if (SysProperties.CHECK && length < 0) {
                Message.throwInternalError("length: " + length);
            }
            writeLong(length);
            InputStream in = v.getInputStream();
            long written = IOUtils.copyAndCloseInput(in, out);
            if (SysProperties.CHECK && written != length) {
                Message.throwInternalError("length:" + length + " written:" + written);
            }
            writeInt(LOB_MAGIC);
            break;
        }
        case Value.CLOB: {
            long length = v.getPrecision();
            if (SysProperties.CHECK && length < 0) {
                Message.throwInternalError("length: " + length);
            }
            writeLong(length);
            Reader reader = v.getReader();
            // below, writer.flush needs to be called to ensure the buffer is written
            // but, this will also flush the output stream, and this slows things down
            // so construct an output stream that will ignore this chained flush call
            java.io.OutputStream out2 = new java.io.FilterOutputStream(out) {
                public void flush() {
                    // do nothing
                }
            };
            Writer writer = new BufferedWriter(new OutputStreamWriter(out2, Constants.UTF8));
            long written = IOUtils.copyAndCloseInput(reader, writer);
            if (SysProperties.CHECK && written != length) {
                Message.throwInternalError("length:" + length + " written:" + written);
            }
            writer.flush();
            writeInt(LOB_MAGIC);
            break;
        }
        case Value.ARRAY: {
            Value[] list = ((ValueArray) v).getList();
            writeInt(list.length);
            for (Value value : list) {
                writeValue(value);
            }
            break;
        }
        case Value.RESULT_SET: {
            ResultSet rs = ((ValueResultSet) v).getResultSet();
            rs.beforeFirst();
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            writeInt(columnCount);
            for (int i = 0; i < columnCount; i++) {
                writeString(meta.getColumnName(i + 1));
                writeInt(meta.getColumnType(i + 1));
                writeInt(meta.getPrecision(i + 1));
                writeInt(meta.getScale(i + 1));
            }
            while (rs.next()) {
                writeBoolean(true);
                for (int i = 0; i < columnCount; i++) {
                    int t = DataType.convertSQLTypeToValueType(meta.getColumnType(i + 1));
                    Value val = DataType.readValue(session, rs, i + 1, t);
                    writeValue(val);
                }
            }
            writeBoolean(false);
            rs.beforeFirst();
            break;
        }
        default:
            Message.throwInternalError("type=" + type);
        }
    }

    /**
     * Read a value.
     *
     * @return the value
     */
    public Value readValue() throws IOException, SQLException {
        int type = readInt();
        switch(type) {
        case Value.NULL:
            return ValueNull.INSTANCE;
        case Value.BYTES:
            return ValueBytes.getNoCopy(readBytes());
        case Value.UUID:
            return ValueUuid.get(readLong(), readLong());
        case Value.JAVA_OBJECT:
            return ValueJavaObject.getNoCopy(readBytes());
        case Value.BOOLEAN:
            return ValueBoolean.get(readBoolean());
        case Value.BYTE:
            return ValueByte.get(readByte());
        case Value.DATE:
            return ValueDate.getNoCopy(new Date(readLong()));
        case Value.TIME:
            return ValueTime.getNoCopy(new Time(readLong()));
        case Value.TIMESTAMP: {
            Timestamp ts = new Timestamp(readLong());
            ts.setNanos(readInt());
            return ValueTimestamp.getNoCopy(ts);
        }
        case Value.DECIMAL:
            return ValueDecimal.get(new BigDecimal(readString()));
        case Value.DOUBLE:
            return ValueDouble.get(readDouble());
        case Value.FLOAT:
            return ValueFloat.get(readFloat());
        case Value.INT:
            return ValueInt.get(readInt());
        case Value.LONG:
            return ValueLong.get(readLong());
        case Value.SHORT:
            return ValueShort.get((short) readInt());
        case Value.STRING:
            return ValueString.get(readString());
        case Value.STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(readString());
        case Value.STRING_FIXED:
            return ValueStringFixed.get(readString());
        case Value.BLOB: {
            long length = readLong();
            ValueLob v = ValueLob.createBlob(in, length, session.getDataHandler());
            if (readInt() != LOB_MAGIC) {
                throw Message.getSQLException(ErrorCode.CONNECTION_BROKEN);
            }
            return v;
        }
        case Value.CLOB: {
            long length = readLong();
            ValueLob v = ValueLob.createClob(new ExactUTF8InputStreamReader(in), length, session.getDataHandler());
            if (readInt() != LOB_MAGIC) {
                throw Message.getSQLException(ErrorCode.CONNECTION_BROKEN);
            }
            return v;
        }
        case Value.ARRAY: {
            int len = readInt();
            Value[] list = new Value[len];
            for (int i = 0; i < len; i++) {
                list[i] = readValue();
            }
            return ValueArray.get(list);
        }
        case Value.RESULT_SET: {
            SimpleResultSet rs = new SimpleResultSet();
            int columns = readInt();
            for (int i = 0; i < columns; i++) {
                rs.addColumn(readString(), readInt(), readInt(), readInt());
            }
            while (true) {
                if (!readBoolean()) {
                    break;
                }
                Object[] o = new Object[columns];
                for (int i = 0; i < columns; i++) {
                    o[i] = readValue().getObject();
                }
                rs.addRow(o);
            }
            return ValueResultSet.get(rs);
        }
        default:
            throw Message.throwInternalError("type="+type);
        }
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
    public void setSession(SessionInterface session) {
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
     * Open a new new connection to the same address and port as this one.
     *
     * @return the new transfer object
     */
    public Transfer openNewConnection() throws IOException {
        InetAddress address = socket.getInetAddress();
        int port = socket.getPort();
        Socket socket = NetUtils.createSocket(address, port, ssl);
        Transfer trans = new Transfer(null);
        trans.setSocket(socket);
        trans.setSSL(ssl);
        return trans;
    }

}
