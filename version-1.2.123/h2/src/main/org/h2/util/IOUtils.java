/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;

/**
 * This utility class contains input/output functions.
 */
public class IOUtils {

    private static final int BUFFER_BLOCK_SIZE = 4 * 1024;

    private IOUtils() {
        // utility class
    }

    /**
     * Close an output stream without throwing an exception.
     *
     * @param out the output stream or null
     */
    public static void closeSilently(OutputStream out) {
        if (out != null) {
            try {
                trace("closeSilently", null, out);
                out.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Skip a number of bytes in an input stream.
     *
     * @param in the input stream
     * @param skip the number of bytes to skip
     * @throws EOFException if the end of file has been reached before all bytes
     *             could be skipped
     * @throws IOException if an IO exception occurred while skipping
     */
    public static void skipFully(InputStream in, long skip) throws IOException {
        while (skip > 0) {
            long skipped = in.skip(skip);
            if (skipped <= 0) {
                throw new EOFException();
            }
            skip -= skipped;
        }
    }

    /**
     * Skip a number of characters in a reader.
     *
     * @param reader the reader
     * @param skip the number of characters to skip
     * @throws EOFException if the end of file has been reached before all
     *             characters could be skipped
     * @throws IOException if an IO exception occurred while skipping
     */
    public static void skipFully(Reader reader, long skip) throws IOException {
        while (skip > 0) {
            long skipped = reader.skip(skip);
            if (skipped <= 0) {
                throw new EOFException();
            }
            skip -= skipped;
        }
    }

    /**
     * Copy all data from the input stream to the output stream and close both
     * streams. Exceptions while closing are ignored.
     *
     * @param in the input stream
     * @param out the output stream
     * @return the number of bytes copied
     */
    public static long copyAndClose(InputStream in, OutputStream out) throws IOException {
        try {
            long len = copyAndCloseInput(in, out);
            out.close();
            return len;
        } finally {
            closeSilently(out);
        }
    }

    /**
     * Copy all data from the input stream to the output stream and close the
     * input stream. Exceptions while closing are ignored.
     *
     * @param in the input stream
     * @param out the output stream
     * @return the number of bytes copied
     */
    public static long copyAndCloseInput(InputStream in, OutputStream out) throws IOException {
        try {
            return copy(in, out);
        } finally {
            closeSilently(in);
        }
    }

    /**
     * Copy all data from the input stream to the output stream. Both streams
     * are kept open.
     *
     * @param in the input stream
     * @param out the output stream
     * @return the number of bytes copied
     */
    public static long copy(InputStream in, OutputStream out) throws IOException {
        long written = 0;
        byte[] buffer = new byte[4 * 1024];
        while (true) {
            int len = in.read(buffer);
            if (len < 0) {
                break;
            }
            out.write(buffer, 0, len);
            written += len;
        }
        return written;
    }

    /**
     * Copy all data from the reader to the writer and close the reader.
     * Exceptions while closing are ignored.
     *
     * @param in the reader
     * @param out the writer
     * @return the number of characters copied
     */
    public static long copyAndCloseInput(Reader in, Writer out) throws IOException {
        long written = 0;
        try {
            char[] buffer = new char[4 * 1024];
            while (true) {
                int len = in.read(buffer);
                if (len < 0) {
                    break;
                }
                out.write(buffer, 0, len);
                written += len;
            }
        } finally {
            in.close();
        }
        return written;
    }

    /**
     * Close an input stream without throwing an exception.
     *
     * @param in the input stream or null
     */
    public static void closeSilently(InputStream in) {
        if (in != null) {
            try {
                trace("closeSilently", null, in);
                in.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Close a reader without throwing an exception.
     *
     * @param reader the reader or null
     */
    public static void closeSilently(Reader reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Close a writer without throwing an exception.
     *
     * @param writer the writer or null
     */
    public static void closeSilently(Writer writer) {
        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Read a number of bytes from an input stream and close the stream.
     *
     * @param in the input stream
     * @param length the maximum number of bytes to read, or -1 to read until
     *            the end of file
     * @return the bytes read
     */
    public static byte[] readBytesAndClose(InputStream in, int length) throws IOException {
        try {
            if (length <= 0) {
                length = Integer.MAX_VALUE;
            }
            int block = Math.min(BUFFER_BLOCK_SIZE, length);
            ByteArrayOutputStream out = new ByteArrayOutputStream(block);
            byte[] buff = new byte[block];
            while (length > 0) {
                int len = Math.min(block, length);
                len = in.read(buff, 0, len);
                if (len < 0) {
                    break;
                }
                out.write(buff, 0, len);
                length -= len;
            }
            return out.toByteArray();
        } finally {
            in.close();
        }
    }

    /**
     * Read a number of characters from a reader and close it.
     *
     * @param in the reader
     * @param length the maximum number of characters to read, or -1 to read
     *            until the end of file
     * @return the string read
     */
    public static String readStringAndClose(Reader in, int length) throws IOException {
        try {
            if (length <= 0) {
                length = Integer.MAX_VALUE;
            }
            int block = Math.min(BUFFER_BLOCK_SIZE, length);
            StringWriter out = new StringWriter(length == Integer.MAX_VALUE ? block : length);
            char[] buff = new char[block];
            while (length > 0) {
                int len = Math.min(block, length);
                len = in.read(buff, 0, len);
                if (len < 0) {
                    break;
                }
                out.write(buff, 0, len);
                length -= len;
            }
            return out.toString();
        } finally {
            in.close();
        }
    }

    /**
     * Try to read the given number of bytes to the buffer. This method reads
     * until the maximum number of bytes have been read or until the end of
     * file.
     *
     * @param in the input stream
     * @param buffer the output buffer
     * @param off the offset in the buffer
     * @param max the number of bytes to read at most
     * @return the number of bytes read
     */
    public static int readFully(InputStream in, byte[] buffer, int off, int max) throws IOException {
        int len = Math.min(max, buffer.length);
        int result = 0;
        while (len > 0) {
            int l = in.read(buffer, off, len);
            if (l < 0) {
                break;
            }
            result += l;
            off += l;
            len -= l;
        }
        return result;
    }

    /**
     * Try to read the given number of characters to the buffer. This method
     * reads until the maximum number of characters have been read or until the
     * end of file.
     *
     * @param in the reader
     * @param buffer the output buffer
     * @param max the number of characters to read at most
     * @return the number of characters read
     */
    public static int readFully(Reader in, char[] buffer, int max) throws IOException {
        int off = 0, len = Math.min(max, buffer.length);
        if (len == 0) {
            return 0;
        }
        while (true) {
            int l = len - off;
            if (l <= 0) {
                break;
            }
            l = in.read(buffer, off, l);
            if (l < 0) {
                break;
            }
            off += l;
        }
        return off <= 0 ? -1 : off;
    }

    /**
     * Create a reader to read from an input stream using the UTF-8 format. If
     * the input stream is null, this method returns null.
     *
     * @param in the input stream or null
     * @return the reader
     */
    public static Reader getReader(InputStream in) {
        try {
            // InputStreamReader may read some more bytes
            return in == null ? null : new BufferedReader(new InputStreamReader(in, Constants.UTF8));
        } catch (UnsupportedEncodingException e) {
            throw Message.convertToInternal(e);
        }
    }

    /**
     * Create a buffered writer to write to an output stream using the UTF-8
     * format. If the output stream is null, this method returns null.
     *
     * @param out the output stream or null
     * @return the writer
     */
    public static Writer getWriter(OutputStream out) {
        try {
            return out == null ? null : new BufferedWriter(new OutputStreamWriter(out, Constants.UTF8));
        } catch (UnsupportedEncodingException e) {
            throw Message.convertToInternal(e);
        }
    }

    /**
     * Create an input stream to read from a string. The string is converted to
     * a byte array using UTF-8 encoding.
     * If the string is null, this method returns null.
     *
     * @param s the string
     * @return the input stream
     */
    public static InputStream getInputStream(String s) {
        if (s == null) {
            return null;
        }
        return new ByteArrayInputStream(StringUtils.utf8Encode(s));
    }

    /**
     * Create a reader to read from a string.
     * If the string is null, this method returns null.
     *
     * @param s the string or null
     * @return the reader
     */
    public static Reader getReader(String s) {
        return s == null ? null : new StringReader(s);
    }

    /**
     * Wrap an input stream in a reader. The bytes are converted to characters
     * using the US-ASCII character set.
     *
     * @param in the input stream
     * @return the reader
     */
    public static Reader getAsciiReader(InputStream in) {
        try {
            return in == null ? null : new InputStreamReader(in, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            throw Message.convertToInternal(e);
        }
    }

    private static void trace(String method, String fileName, Object o) {
        if (SysProperties.TRACE_IO) {
            System.out.println("IOUtils." + method + " " + fileName + " " + o);
        }
    }

}
