/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thomas
 */

public class OdbcTransfer {
    static final int BUFFER_SIZE = 1024;

    private DataInputStream in;
    private DataOutputStream out;

    OdbcTransfer(DataInputStream in, DataOutputStream out) {
        this.in = in;
        this.out = out;
    }

    OdbcTransfer writeBoolean(boolean x) throws IOException {
        writeInt(x ? 1 : 0);
        return this;
    }

    OdbcTransfer writeOk() throws IOException {
        writeBoolean(true);
        return this;
    }

    boolean readBoolean() throws IOException {
        return readInt() == 1;
    }

    OdbcTransfer writeByte(byte x) throws IOException {
        out.write(x);
        return this;
    }

    int readByte() throws IOException {
        return in.read();
    }

    OdbcTransfer writeShort(short x) throws IOException {
        return writeInt(x);
    }

    short readShort() throws IOException {
        return (short) readInt();
    }

    OdbcTransfer writeInt(int i) throws IOException {
        out.writeInt(i);
        return this;
    }

    int readInt() throws IOException {
        return in.readInt();
    }

    OdbcTransfer writeLong(long i) throws IOException {
        out.writeLong(i);
        return this;
    }

    long readLong() throws IOException {
        return in.readLong();
    }

    OdbcTransfer writeFloat(float i) throws IOException {
        out.writeFloat(i);
        return this;
    }

    float readFloat() throws IOException {
        return in.readFloat();
    }

    OdbcTransfer writeDouble(double i) throws IOException {
        out.writeDouble(i);
        return this;
    }

    double readDouble() throws IOException {
        return in.readDouble();

    }

    OdbcTransfer writeString(String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(s.length());
            for(int i=0; i<s.length(); i++) {
                out.write(s.charAt(i));
            }
        }
        return this;
    }

    String readString() throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        char[] chars = new char[len];
        for(int i=0; i<len; i++) {
            chars[i] = (char)in.readByte();
        }
        return new String(chars);
    }

    OdbcTransfer writeDate(java.sql.Date x) throws IOException {
        if (x == null) {
            writeString(null);
        } else {
            writeString(x.toString());
        }
        return this;
    }

    OdbcTransfer writeTime(java.sql.Time x) throws IOException {
        if (x == null) {
            writeString(null);
        } else {
            writeString(x.toString());
        }
        return this;
    }

    OdbcTransfer writeTimestamp(java.sql.Timestamp x) throws IOException {
        if (x == null) {
            writeString(null);
        } else {
            writeString(x.toString());
        }
        return this;
    }

    java.sql.Date readDate() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        return java.sql.Date.valueOf(s);
    }

    java.sql.Time readTime() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        return java.sql.Time.valueOf(s);
    }

    java.sql.Timestamp readTimestamp() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        return java.sql.Timestamp.valueOf(s);
    }

    OdbcTransfer writeByteArray(byte[] data) throws IOException {
        if (data == null) {
            writeInt(-1);
        } else {
            writeInt(data.length);
        }
        out.write(data);

        return this;
    }

    byte[] readByteArray() throws IOException {
        int len = readInt();
        if (len == -1) {
            return null;
        }

        byte[] b = new byte[len];
        in.readFully(b);
        return b;

    }

    OdbcTransfer writeIntArray(int[] s) throws IOException {
        if (s == null) {
            writeInt(-1);
        } else {
            writeInt(s.length);
            for (int i = 0; i < s.length; i++) {
                writeInt(s[i]);
            }
        }
        return this;
    }

    int[] readIntArray() throws IOException {
        int len = readInt();
        if (len == -1) {
            return null;
        }
        int[] s = new int[len];
        for (int i = 0; i < len; i++) {
            s[i] = readInt();
        }
        return s;
    }

    OdbcTransfer writeStringArray(String[] s) throws IOException {
        if (s == null) {
            writeInt(-1);
        } else {
            writeInt(s.length);
            for (int i = 0; i < s.length; i++) {
                writeString(s[i]);
            }
        }
        return this;
    }


    String[] readStringArray() throws IOException {
        int len = readInt();
        if (len == -1) {
            return null;
        }
        String[] s = new String[len];
        for (int i = 0; i < len; i++) {
            s[i] = readString();
        }
        return s;
    }

    // buffer - cannot be null
    OdbcTransfer writeBuffer(byte[] buffer) throws IOException {
        out.write(buffer);
        return this;
    }

}
