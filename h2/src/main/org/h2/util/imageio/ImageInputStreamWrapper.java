/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.imageio;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wrap an ImageInputStream in order to be used as a simple InputStream.
 * @author Nicolas Fortin
 */
public class ImageInputStreamWrapper extends InputStream {
    private ImageInputStream imageInputStream;

    public ImageInputStreamWrapper(ImageInputStream imageInputStream) {
        this.imageInputStream = imageInputStream;
    }

    @Override
    public int read() throws IOException {
        return imageInputStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return imageInputStream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return imageInputStream.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return imageInputStream.skipBytes(n);
    }

    @Override
    public int available() throws IOException {
        long available = imageInputStream.length() - imageInputStream
                .getStreamPosition();
        return available > 0 ? (int)available : 0;
    }

    @Override
    public void close() throws IOException {
        imageInputStream.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        imageInputStream.mark();
    }

    @Override
    public synchronized void reset() throws IOException {
        imageInputStream.reset();
    }

    @Override
    public boolean markSupported() {
        return true;
    }
}
