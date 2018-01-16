package org.h2.store;

import java.io.IOException;
import java.io.Reader;

public final class RangeReader extends Reader {
    private final Reader r;

    private long offset, limit;

    public RangeReader(Reader r, long offset, long limit) {
        this.r = r;
        this.offset = offset;
        this.limit = limit;
    }

    private void before() throws IOException {
        while (offset > 0) {
            offset -= r.skip(offset);
        }
    }

    @Override
    public int read() throws IOException {
        before();
        if (limit < 1) {
            return -1;
        }
        int c = r.read();
        if (c >= 0) {
            limit--;
        }
        return c;
    }

    @Override
    public int read(char cbuf[], int off, int len) throws IOException {
        before();
        if (len > limit) {
            len = (int) limit;
        }
        int cnt = r.read(cbuf, off, len);
        if (cnt > 0) {
            limit -= cnt;
        }
        return cnt;
    }

    @Override
    public long skip(long n) throws IOException {
        before();
        if (n > limit) {
            n = (int) limit;
        }
        n = r.skip(n);
        limit -= n;
        return n;
    }

    @Override
    public boolean ready() throws IOException {
        before();
        if (limit > 0) {
            return r.ready();
        }
        return false;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readAheadLimit) throws IOException {
        throw new IOException("mark() not supported");
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("reset() not supported");
    }

    @Override
    public void close() throws IOException {
        r.close();
    }
}
