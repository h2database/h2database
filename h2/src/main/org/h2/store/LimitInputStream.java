/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An filter input stream that limits the number of bytes that can be read.
 */
public class LimitInputStream extends FilterInputStream {

    private long remaining;

    public LimitInputStream(InputStream in, long maxLength) {
        super(in);
        this.remaining = maxLength;
    }

    @Override
    public int available() throws IOException {
        return (int) Math.min(remaining, in.available());
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public int read() throws IOException {
        if (remaining == 0) {
            return -1;
        }
        int result = in.read();
        if (result >= 0) {
            remaining--;
        }
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (remaining == 0) {
            return -1;
        }
        len = (int) Math.min(len, remaining);
        int result = in.read(b, off, len);
        if (result >= 0) {
            remaining -= result;
        }
        return result;
    }

}
