/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;
import org.h2.index.Page;
import org.h2.util.BitField;

/**
 * Transaction log mechanism.
 * The log is split in pages, the format is:
 * <ul><li>0-3: parent page id
 * </li><li>4-4: page type (LOG)
 * </li><li>5-8: the next page (0 for end)
 * </li><li>9-: data
 * </li></ul>
 * The data format is:
 * <ul><li>0-0: type (0: end, 1: undo)
 * </li><li>1-4: page id
 * </li><li>5-: data
 * </li></ul>
 */
public class PageLog {

    private PageStore store;
    private BitField undo = new BitField();
    private int bufferPos;
    private int firstPage;
    private int nextPage;
    private DataPageBinary data;
    private DataPageBinary output;

    PageLog(PageStore store, int firstPage) {
        this.store = store;
        this.firstPage = firstPage;
        data = store.createDataPage();
        output = store.createDataPage();
    }

    void open() throws SQLException {
        if (firstPage == 0) {
            return;
        }
        undo();
        prepareOutput();
    }

    private void prepareOutput() {
        output.reset();
        output.writeInt(0);
        output.writeByte((byte) Page.TYPE_LOG);
        output.writeInt(store.allocatePage());
    }

    private void undo() throws SQLException {
        int next = firstPage;
        while (next != 0) {
            data = store.readPage(firstPage);
            data.setPos(4);
        }
    }

    void addUndo(int pageId) throws SQLException {
        if (undo.get(pageId)) {
            return;
        }
        undo.set(pageId);
        data.reset();
        data.writeByte((byte) 1);
        data.writeInt(pageId);
        DataPageBinary p = store.readPage(pageId);
        data.write(p.getBytes(), 0, store.getPageSize());
        write(data.getBytes(), 0, data.length());
    }

    private void write(byte[] data, int offset, int length) {
        if (bufferPos + length > store.getPageSize()) {
            while (length > 0) {
                int len = Math.min(length, store.getPageSize() - bufferPos);
                write(data, offset, len);
                offset += len;
                length -= len;
            }
            return;
        }
        System.arraycopy(data, offset, output.getBytes(), bufferPos, length);
        bufferPos += length;
//        if (bufferPos != BUFFER_SIZE) {
//            return;
//        }

    }

}
