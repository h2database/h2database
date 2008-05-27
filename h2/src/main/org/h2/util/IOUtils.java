/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.SQLException;

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

    public static void skipFully(InputStream in, long skip) throws IOException {
        while (skip > 0) {
            long skipped = in.skip(skip);
            if (skipped <= 0) {
                throw new EOFException();
            }
            skip -= skipped;
        }
    }

    public static void skipFully(Reader reader, long skip) throws IOException {
        while (skip > 0) {
            long skipped = reader.skip(skip);
            if (skipped <= 0) {
                throw new EOFException();
            }
            skip -= skipped;
        }
    }

    public static long copyAndClose(InputStream in, OutputStream out) throws IOException {
        try {
            long len = copyAndCloseInput(in, out);
            out.close();
            return len;
        } finally {
            closeSilently(out);
        }
    }

    public static long copyAndCloseInput(InputStream in, OutputStream out) throws IOException {
        try {
            return copy(in, out);
        } finally {
            closeSilently(in);
        }
    }

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

    public static void closeSilently(Reader reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

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
     * Read the given number of bytes to the buffer.
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

    public static Reader getReader(InputStream in) throws SQLException {
        try {
            // InputStreamReader may read some more bytes
            return in == null ? null : new BufferedReader(new InputStreamReader(in, Constants.UTF8));
        } catch (UnsupportedEncodingException e) {
            throw Message.convert(e);
        }
    }

    public static InputStream getInputStream(String s) throws SQLException {
        if (s == null) {
            return null;
        }
        return new ByteArrayInputStream(StringUtils.utf8Encode(s));
    }

    public static Reader getReader(String s) {
        return s == null ? null : new StringReader(s);
    }

    public static Reader getAsciiReader(InputStream x) throws SQLException {
        try {
            return x == null ? null : new InputStreamReader(x, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            throw Message.convert(e);
        }
    }

    private static void trace(String method, String fileName, Object o) {
        if (SysProperties.TRACE_IO) {
            System.out.println("IOUtils." + method + " " + fileName + " " + o);
        }
    }

}
