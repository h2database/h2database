/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.index.Page;
import org.h2.message.Message;

/**
 * A data page of a stream. The format is:
 * <ul>
 * <li>0-3: the trunk page id</li>
 * <li>4-4: page type</li>
 * <li>5-8: the number of bytes used</li>
 * <li>9-remainder: data</li>
 * </ul>
 */
public class PageStreamData extends Record {

    private static final int LENGTH_START = 5;
    private static final int DATA_START = 9;

    private final PageStore store;
    private final int trunk;
    private DataPage data;
    private int remaining;
    private int length;

    PageStreamData(PageStore store, int pageId, int trunk) {
        setPos(pageId);
        this.store = store;
        this.trunk = trunk;
    }

    /**
     * Read the page from the disk.
     */
    void read() throws SQLException {
        data = store.createDataPage();
        store.readPage(getPos(), data);
        data.setPos(4);
        int t = data.readByte();
        if (t != Page.TYPE_STREAM_DATA) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "pos:" + getPos() + " type:" + t +
                    " expected type:" + Page.TYPE_STREAM_DATA);
        }
        length = data.readInt();
    }

    public int getByteCount(DataPage dummy) {
        return store.getPageSize();
    }

    /**
     * Write the header data.
     */
    void initWrite() {
        data = store.createDataPage();
        data.writeInt(trunk);
        data.writeByte((byte) Page.TYPE_STREAM_DATA);
        data.writeInt(0);
        remaining = store.getPageSize() - data.length();
    }

    /**
     * Write the data to the buffer.
     *
     * @param buff the source data
     * @param off the offset in the source buffer
     * @param len the number of bytes to write
     * @return the number of bytes written
     */
    int write(byte[] buff, int offset, int len) {
        int max = Math.min(remaining, len);
        data.write(buff, offset, max);
        length += max;
        remaining -= max;
        return max;
    }

    public void write(DataPage buff) throws SQLException {
        data.setInt(LENGTH_START, length);
        store.writePage(getPos(), data);
    }

    /**
     * Get the number of bytes that fit in a page.
     *
     * @param pageSize the page size
     * @return the number of bytes
     */
    static int getCapacity(int pageSize) {
        return pageSize - DATA_START;
    }

    int getLength() {
        return length;
    }

    /**
     * Read the next bytes from the buffer.
     *
     * @param buff the target buffer
     * @param off the offset in the target buffer
     * @param len the number of bytes to read
     */
    void read(byte[] buff, int off, int len) {
        data.read(buff, off, len);
    }

}