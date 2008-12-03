/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import org.h2.util.BitField;

/**
 * Transaction log mechanism.
 */
public class PageLog {

    private static final int BUFFER_SIZE = 32 * 1024;

    private PageStore store;
    private BitField undo = new BitField();
    private byte[] ringBuffer = new byte[BUFFER_SIZE];
    private int bufferPos;

    PageLog(PageStore store) {
        this.store = store;
    }

    void addUndo(int pageId) {

    }

    private void write(byte[] data, int offset, int length) {
        if (bufferPos + length > BUFFER_SIZE) {
            while (length > 0) {
                int len = Math.min(length, BUFFER_SIZE - bufferPos);
                write(data, offset, len);
                offset += len;
                length -= len;
            }
            return;
        }
        System.arraycopy(data, offset, ringBuffer, bufferPos, length);
        bufferPos += length;
        if (bufferPos == BUFFER_SIZE) {

        }
    }

}
