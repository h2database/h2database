package org.h2.store;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public final class RangeInputStream extends FilterInputStream {
    private long limit;

    public RangeInputStream(InputStream in, long offset, long limit) throws IOException {
        super(in);
        this.limit = limit;
        while (offset > 0) {
            long skip = in.skip(offset);
            if (skip <= 0) {
                int b = read();
                if (b < 0)
                    throw new EOFException();
                offset--;
            } else {
                offset -= skip;
            }
        }
    }

    @Override
    public int read() throws IOException {
        if (limit < 1) {
            return -1;
        }
        int b = in.read();
        if (b >= 0) {
            limit--;
        }
        return b;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (len > limit) {
            len = (int) limit;
        }
        int cnt = in.read(b, off, len);
        if (cnt > 0) {
            limit -= cnt;
        }
        return cnt;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n > limit) {
            n = (int) limit;
        }
        n = in.skip(n);
        limit -= n;
        return n;
    }

    @Override
    public int available() throws IOException {
        int cnt = in.available();
        if (cnt > limit) {
            return (int) limit;
        }
        return cnt;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public void mark(int readlimit) {
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }
}
