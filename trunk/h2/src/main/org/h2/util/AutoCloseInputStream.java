package org.h2.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * This input stream wrapper closes the base input stream when fully read.
 */
public class AutoCloseInputStream extends InputStream {

    private final InputStream in;
    private boolean closed;

    /**
     * Create a new input stream.
     *
     * @param in the input stream
     */
    public AutoCloseInputStream(InputStream in) {
        this.in = in;
    }

    private int autoClose(int x) throws IOException {
        if (x < 0) {
            close();
        }
        return x;
    }

    public void close() throws IOException {
        if (!closed) {
            in.close();
            closed = true;
        }
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return closed ? -1 : autoClose(in.read(b, off, len));
    }

    public int read(byte[] b) throws IOException {
        return closed ? -1 : autoClose(in.read(b));
    }

    public int read() throws IOException {
        return closed ? -1 : autoClose(in.read());
    }

}
