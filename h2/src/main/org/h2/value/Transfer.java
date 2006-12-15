/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.net.Socket;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.h2.engine.Constants;
import org.h2.engine.SessionInterface;
import org.h2.message.Message;
import org.h2.message.TraceSystem;
import org.h2.tools.SimpleResultSet;
import org.h2.util.ExactUTF8InputStreamReader;
import org.h2.util.IOUtils;
import org.h2.util.StringCache;

/**
 * @author Thomas
 */
public class Transfer {

    private static final int BUFFER_SIZE = 16 * 1024;
    private static final int LOB_MAGIC = 0x1234;

    protected Socket socket;
    protected DataInputStream in;
    protected DataOutputStream out;
    private Exception stackTrace = new Exception();
    private SessionInterface session;
    
    public Transfer(SessionInterface session) {
        this.session = session;
    }

    public void finalize() {
        if (!Constants.RUN_FINALIZERS) {
            return;
        }
        if(socket != null) {
            throw Message.getInternalError("not closed", stackTrace);
        }
    }

    public void setSocket(Socket s) {
        socket = s;
    }

    public void init() throws IOException {
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), Transfer.BUFFER_SIZE));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), Transfer.BUFFER_SIZE));
    }

    public void flush() throws IOException {
        out.flush();
    }

    public Transfer writeBoolean(boolean x) throws IOException {
        out.writeByte((byte)(x ? 1 : 0));
        return this;
    }

    public boolean readBoolean() throws IOException {
        return in.readByte() == 1;
    }

    public Transfer writeByte(byte x) throws IOException {
        out.writeByte(x);
        return this;
    }

    public byte readByte() throws IOException {
        return in.readByte();
    }

    public Transfer writeInt(int i) throws IOException {
        out.writeInt(i);
        return this;
    }

    public int readInt() throws IOException {
        return in.readInt();
    }

    public Transfer writeLong(long i) throws IOException {
        out.writeLong(i);
        return this;
    }

    public long readLong() throws IOException {
        return in.readLong();
    }

    public Transfer writeDouble(double i) throws IOException {
        out.writeDouble(i);
        return this;
    }

    public Transfer writeFloat(float i) throws IOException {
        out.writeFloat(i);
        return this;
    }

    public double readDouble() throws IOException {
        return in.readDouble();
    }

    public float readFloat() throws IOException {
        return in.readFloat();
    }

    public Transfer writeString(String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
        } else {
            int len = s.length();
            out.writeInt(len);
            for(int i=0; i<len; i++) {
                out.writeChar(s.charAt(i));
            }
        }
        return this;
    }

    public String readString() throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        // TODO optimize: StringBuffer is synchronized, maybe use a char array (but that means more memory)
        StringBuffer buff = new StringBuffer(len);
        for(int i=0; i<len; i++) {
            buff.append(in.readChar());
        }
        String s = buff.toString();
        s = StringCache.get(s);
        return s;
    }

    public Transfer writeBytes(byte[] data) throws IOException {
        if(data == null) {
            writeInt(-1);
        } else {
            writeInt(data.length);
            out.write(data);
        }
        return this;
    }

    public byte[] readBytes() throws IOException {
        int len = readInt();
        if(len == -1) {
            return null;
        }
        byte[] b = new byte[len];
        in.readFully(b);
        return b;
    }

    public void close() {
        if(socket != null) {
            try {
                out.flush();
                if(socket != null) {
                    socket.close();
                }
            } catch(IOException e) {
                TraceSystem.traceThrowable(e);
            } finally {
                socket = null;
            }
        }
    }

    public void writeValue(Value v) throws IOException, SQLException {
        int type = v.getType();
        writeInt(type);
        switch(type) {
        case Value.NULL:
            break;
        case Value.BYTES:
        case Value.JAVA_OBJECT:
            writeBytes(v.getBytes());
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
            writeString(v.getString());
            break;
        case Value.BLOB: {
            long length = v.getPrecision();
            if(Constants.CHECK && length < 0) {
                throw Message.getInternalError("length: " + length);
            }
            writeLong(length);
            InputStream in = v.getInputStream();
            long written = IOUtils.copyAndCloseInput(in, out);
            if(Constants.CHECK && written != length) {
                Message.getInternalError("length:" + length + " written:" + written);
            }
            writeInt(LOB_MAGIC);
            break;
        }
        case Value.CLOB: {
            long length = v.getPrecision();
            if(Constants.CHECK && length < 0) {
                throw Message.getInternalError("length: " + length);
            }
            writeLong(length);
            Reader reader = v.getReader();
            Writer writer = new OutputStreamWriter(out, Constants.UTF8);
            long written = IOUtils.copyAndCloseInput(reader, writer);
            if(Constants.CHECK && written != length) {
                Message.getInternalError("length:" + length + " written:" + written);
            }
            writer.flush();
            writeInt(LOB_MAGIC);
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
        case Value.RESULT_SET: {
            ResultSet rs = ((ValueResultSet)v).getResultSet();
            rs.beforeFirst();
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            writeInt(columnCount);
            for(int i=0; i<columnCount; i++) {
                writeString(meta.getColumnName(i + 1));
                writeInt(meta.getColumnType(i + 1));
                writeInt(meta.getPrecision(i + 1));
                writeInt(meta.getScale(i + 1));
            }
            while(rs.next()) {
                writeBoolean(true);
                for(int i=0; i<columnCount; i++) {
                    int t = DataType.convertSQLTypeToValueType(meta.getColumnType(i + 1));
                    Value val = DataType.readValue(session, rs, i+1, t);
                    writeValue(val);
                }
            }
            writeBoolean(false);
            rs.beforeFirst();
            break;
        }
        default:
            throw Message.getInternalError("type="+type);
        }
    }

    public Value readValue() throws IOException, SQLException {
        int type = readInt();
        switch(type) {
        case Value.NULL:
            return ValueNull.INSTANCE;
        case Value.BYTES:
            return ValueBytes.get(readBytes());
        case Value.UUID:
            return ValueUuid.get(readLong(), readLong());
        case Value.JAVA_OBJECT:
            return ValueJavaObject.get(readBytes());
        case Value.BOOLEAN:
            return ValueBoolean.get(readBoolean());
        case Value.BYTE:
            return ValueByte.get(readByte());
        case Value.DATE:
            return ValueDate.get(new Date(readLong()));
        case Value.TIME:
            return ValueTime.get(new Time(readLong()));
        case Value.TIMESTAMP: {
            Timestamp ts = new Timestamp(readLong());
            ts.setNanos(readInt());
            return ValueTimestamp.get(ts);
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
            return ValueShort.get((short)readInt());
        case Value.STRING:
            return ValueString.get(readString());
        case Value.STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(readString());
        case Value.BLOB: {
            long length = readLong();
            ValueLob v = ValueLob.createBlob(in, length, session.getDataHandler());
            if(readInt() != LOB_MAGIC) {
                throw Message.getSQLException(Message.CONNECTION_BROKEN);
            }
            return v;
        }
        case Value.CLOB: {
            long length = readLong();
            ValueLob v = ValueLob.createClob(new ExactUTF8InputStreamReader(in), length, session.getDataHandler());
            if(readInt() != LOB_MAGIC) {
                throw Message.getSQLException(Message.CONNECTION_BROKEN);
            }
            return v;
        }
        case Value.ARRAY: {
            int len = readInt();
            Value[] list = new Value[len];
            for(int i=0; i<len; i++) {
                list[i] = readValue();
            }
            return ValueArray.get(list);
        }
        case Value.RESULT_SET: {
            SimpleResultSet rs = new SimpleResultSet();
            int columns = readInt();
            for(int i=0; i<columns; i++) {
                rs.addColumn(readString(), readInt(), readInt(), readInt());
            }
            while(true) {
                if(!readBoolean()) {
                    break;
                }
                Object[] o = new Object[columns];
                for(int i=0; i<columns; i++) {
                    o[i] = readValue().getObject();
                }
                rs.addRow(o);
            }
            return ValueResultSet.get(rs);
        }
        default:
            throw Message.getInternalError("type="+type);
        }
    }

    public Socket getSocket() {
        return socket;
    }

    public void setSession(SessionInterface session) {
        this.session = session;
    }

}
