/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.util.IOUtils;
import org.h2.value.Value;

//## Java 1.6 begin ##
import java.sql.NClob;
//## Java 1.6 end ##

/**
 * Represents a CLOB value.
 */
public class JdbcClob extends TraceObject implements Clob
//## Java 1.6 begin ##
    , NClob
//## Java 1.6 end ##
{

    private Value value;
    private JdbcConnection conn;

    /**
     * INTERNAL
     */
    public JdbcClob(JdbcConnection conn, Value value, int id) {
        setTrace(conn.getSession().getTrace(), TraceObject.CLOB, id);
        this.conn = conn;
        this.value = value;
    }

    /**
     * Returns the length.
     *
     * @return the length
     */
    public long length() throws SQLException {
        try {
            debugCodeCall("length");
            checkClosed();
            if (value.getType() == Value.CLOB) {
                long precision = value.getPrecision();
                if (precision > 0) {
                    return precision;
                }
            }
            Reader in = value.getReader();
            try {
                long size = 0;
                char[] buff = new char[Constants.IO_BUFFER_SIZE];
                while (true) {
                    int len = in.read(buff, 0, Constants.IO_BUFFER_SIZE);
                    if (len <= 0) {
                        break;
                    }
                    size += len;
                }
                return size;
            } finally {
                in.close();
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Truncates the object.
     */
    public void truncate(long len) throws SQLException {
        throw unsupported("LOB update");
    }

    /**
     * Returns the input stream.
     *
     * @return the input stream
     */
    public InputStream getAsciiStream() throws SQLException {
        try {
            debugCodeCall("getAsciiStream");
            checkClosed();
            String s = value.getString();
            return IOUtils.getInputStream(s);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Returns an output  stream.
     */
    public OutputStream setAsciiStream(long pos) throws SQLException {
        throw unsupported("LOB update");
    }

    /**
     * Returns the reader.
     *
     * @return the reader
     */
    public Reader getCharacterStream() throws SQLException {
        try {
            debugCodeCall("getCharacterStream");
            checkClosed();
            return value.getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Returns a writer starting from a given position.
     */
    public Writer setCharacterStream(long pos) throws SQLException {
        throw unsupported("LOB update");
    }

    /**
     * Returns a substring.
     *
     * @param pos the position (the first character is at position 1)
     * @param length the number of characters
     * @return the string
     */
    public String getSubString(long pos, int length) throws SQLException {
        try {
            debugCode("getSubString(" + pos + ", " + length + ");");
            checkClosed();
            if (pos < 1) {
                throw DbException.getInvalidValueException("pos", "" + pos);
            }
            if (length < 0) {
                throw DbException.getInvalidValueException("length", "" + length);
            }
            StringBuilder buff = new StringBuilder(Math.min(4096, length));
            Reader reader = value.getReader();
            try {
                IOUtils.skipFully(reader, pos - 1);
                for (int i = 0; i < length; i++) {
                    int ch = reader.read();
                    if (ch < 0) {
                        break;
                    }
                    buff.append((char) ch);
                }
            } finally {
                reader.close();
            }
            return buff.toString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Sets a substring.
     */
    public int setString(long pos, String str) throws SQLException {
        throw unsupported("LOB update");
    }

    /**
     * [Not supported] Sets a substring.
     */
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        throw unsupported("LOB update");
    }

    /**
     * [Not supported] Searches a pattern and return the position.
     */
    public long position(String pattern, long start) throws SQLException {
        throw unsupported("LOB search");
    }

    /**
     * [Not supported] Searches a pattern and return the position.
     */
    public long position(Clob clobPattern, long start) throws SQLException {
        throw unsupported("LOB search");
    }

    /**
     * Release all resources of this object.
     */
    public void free() {
        debugCodeCall("free");
        value = null;
    }

    /**
     * [Not supported] Returns the reader, starting from an offset.
     */
//## Java 1.6 begin ##
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        throw unsupported("LOB subset");
    }
//## Java 1.6 end ##

    private void checkClosed() throws SQLException {
        conn.checkClosed();
        if (value == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
    }

    /**
     * INTERNAL
     */
    public String toString() {
        return getTraceObjectName() + ": " + value.getTraceSQL();
    }

}
