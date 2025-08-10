/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

package org.h2.tools;

import java.io.IOException;
import java.io.InputStream;

/**
 * A wrapper InputStream that works around the issue where some compressed streams
 * (like Kanzi CompressedInputStream) may return 0 bytes instead of -1 for EOF,
 * causing StreamDecoder to throw "Underlying input stream returned zero bytes".
 */
public class ZeroBytesEOFInputStream extends InputStream {

    private final InputStream wrapped;
    private int consecutiveZeroReads = 0;
    private static final int MAX_ZERO_READS = 10;
    private boolean eofReached = false;

    public ZeroBytesEOFInputStream(InputStream wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public int read() throws IOException {
        if (eofReached) {
            return -1;
        }

        while (consecutiveZeroReads < MAX_ZERO_READS) {
            int result = wrapped.read();

            if (result == -1) {
                eofReached = true;
                return -1;
            } else if (result >= 0) {
                consecutiveZeroReads = 0;
                return result;
            }
            // This shouldn't happen with read(), but just in case
            consecutiveZeroReads++;
        }

        eofReached = true;
        return -1;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (eofReached) {
            return -1;
        }

        while (consecutiveZeroReads < MAX_ZERO_READS) {
            int result = wrapped.read(b, off, len);

            if (result == -1) {
                eofReached = true;
                return -1;
            } else if (result > 0) {
                consecutiveZeroReads = 0;
                return result;
            } else {
                consecutiveZeroReads++;
                // Small delay to allow compressed stream to process more data
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while reading compressed stream", e);
                }
            }
        }

        // If we've hit the maximum zero reads, treat it as EOF
        eofReached = true;
        return -1;
    }

    @Override
    public long skip(long n) throws IOException {
        return wrapped.skip(n);
    }

    @Override
    public int available() throws IOException {
        return wrapped.available();
    }

    @Override
    public void close() throws IOException {
        wrapped.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        wrapped.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        wrapped.reset();
        consecutiveZeroReads = 0;
        eofReached = false;
    }

    @Override
    public boolean markSupported() {
        return wrapped.markSupported();
    }
}