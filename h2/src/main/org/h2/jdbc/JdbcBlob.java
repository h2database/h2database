/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.message.TraceObject;
import org.h2.util.IOUtils;
import org.h2.value.Value;

/**
 * Represents a BLOB value.
 */
public class JdbcBlob extends TraceObject implements Blob {

    private Value value;
    private JdbcConnection conn;

    /**
     * INTERNAL
     */
    public JdbcBlob(JdbcConnection conn, Value value, int id) {
        setTrace(conn.getSession().getTrace(), TraceObject.BLOB, id);
        this.conn = conn;
        this.value = value;
    }

    /**
     * Returns the length.
     *
     * @return the length
     * @throws SQLException
     */
    public long length() throws SQLException {
        try {
            debugCodeCall("length");
            checkClosed();
            if (value.getType() == Value.BLOB) {
                long precision = value.getPrecision();
                if (precision > 0) {
                    return precision;
                }
            }
            long size = 0;
            InputStream in = value.getInputStream();
            try {
                byte[] buff = new byte[Constants.FILE_BLOCK_SIZE];
                while (true) {
                    int len = in.read(buff, 0, Constants.FILE_BLOCK_SIZE);
                    if (len <= 0) {
                        break;
                    }
                    size += len;
                }
            } finally {
                in.close();
            }
            return size;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Truncates the object.
     *
     * @param len the new length
     * @throws SQLException
     */
    public void truncate(long len) throws SQLException {
        debugCodeCall("truncate", len);
        throw Message.getUnsupportedException("LOB update");
    }

    /**
     * Returns some bytes of the object.
     *
     * @param pos the index, the first byte is at position 1
     * @param length the number of bytes
     * @return the bytes, at most length bytes
     * @throws SQLException
     */
    public byte[] getBytes(long pos, int length) throws SQLException {
        try {
            debugCode("getBytes("+pos+", "+length+");");
            checkClosed();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InputStream in = value.getInputStream();
            try {
                IOUtils.skipFully(in, pos - 1);
                while (length > 0) {
                    int x = in.read();
                    if (x < 0) {
                        break;
                    }
                    out.write(x);
                    length--;
                }
            } finally {
                in.close();
            }
            return out.toByteArray();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Sets some bytes of the object.
     *
     * @param pos the write position
     * @param bytes the bytes to set
     * @return how many bytes have been written
     * @throws SQLException
     */
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        debugCode("setBytes("+pos+", bytes);");
        throw Message.getUnsupportedException("LOB update");
    }

    /**
     * [Not supported] Sets some bytes of the object.
     *
     * @param pos the write position
     * @param bytes the bytes to set
     * @param offset the bytes offset
     * @param len the number of bytes to write
     * @return how many bytes have been written
     * @throws SQLException
     */
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        debugCode("setBytes("+pos+", bytes, "+offset+", "+len+");");
        throw Message.getUnsupportedException("LOB update");
    }

    /**
     * Returns the input stream.
     *
     * @return the input stream
     * @throws SQLException
     */
    public InputStream getBinaryStream() throws SQLException {
        try {
            debugCodeCall("getBinaryStream");
            checkClosed();
            return value.getInputStream();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Returns an output stream.
     *
     * @param pos where to start writing
     * @return the output stream to write into
     * @throws SQLException
     */
    public OutputStream setBinaryStream(long pos) throws SQLException {
        debugCodeCall("setBinaryStream", pos);
        throw Message.getUnsupportedException("LOB update");
    }

    /**
     * [Not supported] Searches a pattern and return the position.
     *
     * @param pattern the pattern to search
     * @param start the index, the first byte is at position 1
     * @return the position (first byte is at position 1), or -1 for not found
     * @throws SQLException
     */
    public long position(byte[] pattern, long start) throws SQLException {
        debugCode("position(pattern, "+start+");");
        if (Constants.BLOB_SEARCH) {
            try {
                debugCode("position(pattern, " + start + ");");
                if (pattern == null) {
                    return -1;
                }
                if (pattern.length == 0) {
                    return 1;
                }
                // TODO performance: blob pattern search is slow
                BufferedInputStream in = new BufferedInputStream(value.getInputStream());
                IOUtils.skipFully(in, start - 1);
                int pos = 0;
                int patternPos = 0;
                while (true) {
                    int x = in.read();
                    if (x < 0) {
                        break;
                    }
                    if (x == (pattern[patternPos] & 0xff)) {
                        if (patternPos == 0) {
                            in.mark(pattern.length);
                        }
                        if (patternPos == pattern.length) {
                            return pos - patternPos;
                        }
                        patternPos++;
                    } else {
                        if (patternPos > 0) {
                            in.reset();
                            pos -= patternPos;
                        }
                    }
                    pos++;
                }
                return -1;
            } catch (Exception e) {
                throw logAndConvert(e);
            }
        }
        throw Message.getUnsupportedException("LOB search");
    }

    /**
     * [Not supported] Searches a pattern and return the position.
     *
     * @param blobPattern the pattern to search
     * @param start the index, the first byte is at position 1
     * @return the position (first byte is at position 1), or -1 for not found
     * @throws SQLException
     */
    public long position(Blob blobPattern, long start) throws SQLException {
        debugCode("position(blobPattern, "+start+");");
        if (Constants.BLOB_SEARCH) {
            try {
                debugCode("position(blobPattern, " + start + ");");
                if (blobPattern == null) {
                    return -1;
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                InputStream in = blobPattern.getBinaryStream();
                while (true) {
                    int x = in.read();
                    if (x < 0) {
                        break;
                    }
                    out.write(x);
                }
                return position(out.toByteArray(), start);
            } catch (Exception e) {
                throw logAndConvert(e);
            }
        }
        throw Message.getUnsupportedException("LOB subset");
    }

    /**
     * Release all resources of this object.
     */
    public void free() {
        debugCodeCall("free");
        value = null;
    }

    /**
     * [Not supported] Returns the input stream, starting from an offset.
     *
     * @param pos where to start reading
     * @param length the number of bytes that will be read
     * @return the input stream to read
     * @throws SQLException
     */
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        debugCode("getBinaryStream("+pos+", "+length+");");
        throw Message.getUnsupportedException("LOB update");
    }

    private void checkClosed() throws SQLException {
        conn.checkClosed();
        if (value == null) {
            throw Message.getSQLException(ErrorCode.OBJECT_CLOSED);
        }
    }

    /**
     * INTERNAL
     */
    public String toString() {
        return getTraceObjectName() + ": " + value.getTraceSQL();
    }

}
