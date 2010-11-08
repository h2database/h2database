/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.h2.util.IOUtils;

/**
 * This class is backed by an input stream and supports reading values and
 * variable size data.
 */
public class DataReader {

    private static final EOFException EOF = new EOFException();
    private InputStream in;

    /**
     * Create a new data reader.
     *
     * @param in the input stream
     */
    public DataReader(InputStream in) {
        this.in = in;
    }

    /**
     * Read a byte.
     *
     * @return the byte
     */
    public byte read() throws IOException {
        int x = in.read();
        if (x < 0) {
            throw EOF;
        }
        return (byte) x;
    }

    /**
     * Read a variable size integer.
     *
     * @return the value
     */
    public int readVarInt() throws IOException {
        int b = read();
        if (b >= 0) {
            return b;
        }
        int x = b & 0x7f;
        b = read();
        if (b >= 0) {
            return x | (b << 7);
        }
        x |= (b & 0x7f) << 7;
        b = read();
        if (b >= 0) {
            return x | (b << 14);
        }
        x |= (b & 0x7f) << 14;
        b = read();
        if (b >= 0) {
            return x | b << 21;
        }
        return x | ((b & 0x7f) << 21) | (read() << 28);
    }

    /**
     * Read a variable size long.
     *
     * @return the value
     */
    public long readVarLong() throws IOException {
        long x = read();
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7;; s += 7) {
            long b = read();
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                return x;
            }
        }
    }

    /**
     * Read an integer.
     *
     * @return the value
     */
    // public int readInt() throws IOException {
    //     return (read() << 24) + ((read() & 0xff) << 16) +
    //             ((read() & 0xff) << 8) + (read() & 0xff);
    //}

    /**
     * Read a long.
     *
     * @return the value
     */
    // public long readLong() throws IOException {
    //    return ((long) (readInt()) << 32) + (readInt() & 0xffffffffL);
    // }

    /**
     * Read a number of bytes.
     *
     * @param buff the target buffer
     * @param offset the offset within the target buffer
     * @param len the number of bytes to read
     */
    public void readFully(byte[] buff, int offset, int len) throws IOException {
        int got = IOUtils.readFully(in, buff, offset, len);
        if (got < len) {
            throw EOF;
        }
    }

    /**
     * Read a string from the stream.
     *
     * @return the string
     */
    public String readString() throws IOException {
        int len = readVarInt();
        return readString(len);
    }

    private String readString(int len) throws IOException {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            int x = read() & 0xff;
            if (x < 0x80) {
                chars[i] = (char) x;
            } else if (x >= 0xe0) {
                chars[i] = (char) (((x & 0xf) << 12) + ((read() & 0x3f) << 6) + (read() & 0x3f));
            } else {
                chars[i] = (char) (((x & 0x1f) << 6) + (read() & 0x3f));
            }
        }
        return new String(chars);
    }

}
