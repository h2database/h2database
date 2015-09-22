package org.h2.util;

import javax.imageio.stream.ImageOutputStreamImpl;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Nicolas Fortin
 */
public class ImageOutputStreamWrapper extends ImageOutputStreamImpl {
    private OutputStream outputStream;

    public ImageOutputStreamWrapper(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        outputStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }
}
