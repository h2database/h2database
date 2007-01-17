/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.SessionInterface;
import org.h2.message.Message;
import org.h2.message.TraceObject;
import org.h2.util.IOUtils;
import org.h2.value.Value;

//#ifdef JDK16
/*
import java.sql.NClob;
*/
//#endif

/**
 * Represents a CLOB value.
 */
public class JdbcClob extends TraceObject implements Clob
//#ifdef JDK16
/*
, NClob
*/
//#endif
{

    private Value value;
    private JdbcConnection conn;

    public JdbcClob(SessionInterface session, JdbcConnection conn, Value value, int id) {
        setTrace(session.getTrace(), TraceObject.CLOB, id);
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
            if(value.getType() == Value.CLOB) {
                long precision = value.getPrecision();
                if(precision > 0) {
                    return precision;
                }
            }
            Reader in = value.getReader();
            try {
                long size = 0;
                char[] buff = new char[Constants.FILE_BLOCK_SIZE];
                while(true) {
                    int len = in.read(buff, 0, Constants.FILE_BLOCK_SIZE);
                    if(len <= 0) {
                        break;
                    }
                    size += len;
                }
                return size;
            } finally {
                in.close();
            }
        } catch(Throwable e) {
            throw Message.convert(e);
        }
    }

    /**
     * Truncates the object.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public void truncate(long len) throws SQLException {
        debugCodeCall("truncate", len);
        throw Message.getUnsupportedException();
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns an output  stream.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public OutputStream setAsciiStream(long pos) throws SQLException {
        debugCodeCall("setAsciiStream", pos);
        throw Message.getUnsupportedException();
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a writer.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public Writer setCharacterStream(long pos) throws SQLException {
        debugCodeCall("setCharacterStream", pos);
        throw Message.getUnsupportedException();
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
            debugCode("getSubString("+pos+", "+length+");");
            checkClosed();
            if(pos < 1) {
                throw Message.getInvalidValueException("pos", ""+pos);
            }
            if(length < 0) {
                throw Message.getInvalidValueException("length", ""+length);
            }
            StringBuffer buff = new StringBuffer(length);
            Reader reader = value.getReader();
            try {
                IOUtils.skipFully(reader, pos - 1);
                for(int i=0; i<length; i++) {
                    int ch = reader.read();
                    if(ch < 0) {
                        break;
                    }
                    buff.append((char) ch);
                }
            } finally {
                reader.close();
            }
            return buff.toString();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets a substring.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public int setString(long pos, String str) throws SQLException {
        debugCode("setString("+pos+", "+quote(str)+");");
        throw Message.getUnsupportedException();
    }

    /**
     * Sets a substring.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        debugCode("setString("+pos+", "+quote(str)+", "+offset+", "+len+");");
        throw Message.getUnsupportedException();
    }

    /**
     * Searches a pattern and return the position.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public long position(String pattern, long start) throws SQLException {
        debugCode("position("+quote(pattern)+", "+start+");");
        throw Message.getUnsupportedException();
    }

    /**
     * Searches a pattern and return the position.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public long position(Clob clobPattern, long start) throws SQLException {
        debugCode("position(clobPattern, "+start+");");
        throw Message.getUnsupportedException();
    }

    /**
     * Release all resources of this object.
     */
    public void free() throws SQLException {
        debugCodeCall("free");
        value = null;
    }

    /**
     * Returns the reader, starting from an offset.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        debugCode("getCharacterStream("+pos+", "+length+");");
        throw Message.getUnsupportedException();
    }

    private void checkClosed() throws SQLException {
        conn.checkClosed();
        if (value == null) {
            throw Message.getSQLException(Message.OBJECT_CLOSED);
        }
    }

}
