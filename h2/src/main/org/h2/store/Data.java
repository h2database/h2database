/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 *
 * The variable size number format code is a port from SQLite,
 * but stored in reverse order (least significant bits in the first byte).
 */
package org.h2.store;

import static org.h2.util.Bits.INT_VH_BE;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;

import org.h2.engine.Constants;
import org.h2.util.MathUtils;
import org.h2.util.Utils;

/**
 * This class represents a byte buffer that contains persistent data of a page.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class Data {

    /**
     * The data itself.
     */
    private byte[] data;

    /**
     * The current write or read position.
     */
    private int pos;

    private Data(byte[] data) {
        this.data = data;
    }

    /**
     * Write an integer at the current position.
     * The current position is incremented.
     *
     * @param x the value
     */
    public void writeInt(int x) {
        INT_VH_BE.set(data, pos, x);
        pos += 4;
    }

    /**
     * Read an integer at the current position.
     * The current position is incremented.
     *
     * @return the value
     */
    public int readInt() {
        int x = (int) INT_VH_BE.get(data, pos);
        pos += 4;
        return x;
    }

    private void writeStringWithoutLength(char[] chars, int len) {
        int p = pos;
        byte[] buff = data;
        for (int i = 0; i < len; i++) {
            int c = chars[i];
            if (c < 0x80) {
                buff[p++] = (byte) c;
            } else if (c >= 0x800) {
                buff[p++] = (byte) (0xe0 | (c >> 12));
                buff[p++] = (byte) ((c >> 6) & 0x3f);
                buff[p++] = (byte) (c & 0x3f);
            } else {
                buff[p++] = (byte) (0xc0 | (c >> 6));
                buff[p++] = (byte) (c & 0x3f);
            }
        }
        pos = p;
    }

    /**
     * Create a new buffer.
     *
     * @param capacity the initial capacity of the buffer
     * @return the buffer
     */
    public static Data create(int capacity) {
        return new Data(new byte[capacity]);
    }

    /**
     * Get the current write position of this buffer, which is the current
     * length.
     *
     * @return the length
     */
    public int length() {
        return pos;
    }

    /**
     * Get the byte array used for this page.
     *
     * @return the byte array
     */
    public byte[] getBytes() {
        return data;
    }

    /**
     * Set the position to 0.
     */
    public void reset() {
        pos = 0;
    }

    /**
     * Append a number of bytes to this buffer.
     *
     * @param buff the data
     * @param off the offset in the data
     * @param len the length in bytes
     */
    public void write(byte[] buff, int off, int len) {
        System.arraycopy(buff, off, data, pos, len);
        pos += len;
    }

    /**
     * Copy a number of bytes to the given buffer from the current position. The
     * current position is incremented accordingly.
     *
     * @param buff the output buffer
     * @param off the offset in the output buffer
     * @param len the number of bytes to copy
     */
    public void read(byte[] buff, int off, int len) {
        System.arraycopy(data, pos, buff, off, len);
        pos += len;
    }

    /**
     * Set the current read / write position.
     *
     * @param pos the new position
     */
    public void setPos(int pos) {
        this.pos = pos;
    }

    /**
     * Read one single byte.
     *
     * @return the value
     */
    public byte readByte() {
        return data[pos++];
    }

    /**
     * Check if there is still enough capacity in the buffer.
     * This method extends the buffer if required.
     *
     * @param plus the number of additional bytes required
     */
    public void checkCapacity(int plus) {
        if (pos + plus >= data.length) {
            // a separate method to simplify inlining
            expand(plus);
        }
    }

    private void expand(int plus) {
        // must copy everything, because pos could be 0 and data may be
        // still required
        data = Utils.copyBytes(data, (data.length + plus) * 2);
    }

    /**
     * Fill up the buffer with empty space and an (initially empty) checksum
     * until the size is a multiple of Constants.FILE_BLOCK_SIZE.
     */
    public void fillAligned() {
        // 0..6 > 8, 7..14 > 16, 15..22 > 24, ...
        int len = MathUtils.roundUpInt(pos + 2, Constants.FILE_BLOCK_SIZE);
        pos = len;
        if (data.length < len) {
            checkCapacity(len - data.length);
        }
    }

    /**
     * Copy a String from a reader to an output stream.
     *
     * @param source the reader
     * @param target the output stream
     * @throws IOException on failure
     */
    public static void copyString(Reader source, OutputStream target)
            throws IOException {
        char[] buff = new char[Constants.IO_BUFFER_SIZE];
        Data d = new Data(new byte[3 * Constants.IO_BUFFER_SIZE]);
        while (true) {
            int l = source.read(buff);
            if (l < 0) {
                break;
            }
            d.writeStringWithoutLength(buff, l);
            target.write(d.data, 0, d.pos);
            d.reset();
        }
    }

}
