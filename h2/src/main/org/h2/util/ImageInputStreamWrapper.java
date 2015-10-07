/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.engine.Session;
import org.h2.value.Value;

import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageInputStreamImpl;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * Wrap a input stream provider using InputStream simulate a ImageInputStream.
 * @author Nicolas Fortin
 */
public class ImageInputStreamWrapper extends ImageInputStreamImpl {
    private InputStreamProvider value;
    private InputStream inputStream;
    private long internalPos = 0;
    private long lastPositionChecked = 0;
    private long readBytes = 0;
    private static final long CHECK_CANCEL_POSITION_SHIFT = 100000;
    private Session session;

    /**
     * Wrap a input stream provider using InputStream simulate a
     * ImageInputStream. (Random access through skip)
     * @param value
     * @param session
     * @throws IOException
     */
    public ImageInputStreamWrapper(InputStreamProvider value, Session session)
            throws
            IOException{
        this.value = value;
        this.inputStream = value.getInputStream();
        this.session = session;
    }

    /**
     * Helper method
     * @param value H2 Value
     * @return ImageInputStream
     * @throws IOException
     */
    public static ImageInputStream create(Value value) throws IOException {
        return new ImageInputStreamWrapper(new ValueStreamProvider(value), null);
    }

    /**
     * Helper method
     * @param value H2 Value
     * @return ImageInputStream
     * @throws IOException
     */
    public static ImageInputStream create(Value value, Session session) throws IOException {
        return new ImageInputStreamWrapper(new ValueStreamProvider(value), session);
    }

    /**
     * Helper method
     * @param blob H2 JDBC Value
     * @return ImageInputStream
     * @throws IOException
     */
    public static ImageInputStream create(Blob blob) throws IOException  {
        return new ImageInputStreamWrapper(new BlobStreamProvider(blob), null);
    }

    /**
     * Helper method
     * @param blob H2 JDBC Value
     * @return ImageInputStream
     * @throws IOException
     */
    public static ImageInputStream create(Blob blob, Session session) throws IOException  {
        return new ImageInputStreamWrapper(new BlobStreamProvider(blob), session);
    }

    @Override
    public int read() throws IOException {
        checkClosed();
        synchronize();
        streamPos += 1;
        internalPos += 1;
        return inputStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkClosed();
        synchronize();
        int readOffset = inputStream.read(b, off, len);
        if(readOffset != -1) {
            streamPos += readOffset;
            internalPos += readOffset;
        }
        readBytes += len;
        // Check for cancel
        if(session != null && lastPositionChecked +
                CHECK_CANCEL_POSITION_SHIFT < readBytes) {
            session.checkCanceled();
            lastPositionChecked = readBytes;
        }
        return readOffset;
    }

    @Override
    public void close() throws IOException {
        checkClosed();
        inputStream.close();
        value.close();
        super.close();
    }

    private void synchronize() throws IOException {
        if(streamPos != internalPos) {
            if (internalPos < streamPos) {
                long skip = streamPos - internalPos;
                IOUtils.skipFully(inputStream, skip);
            } else {
                // If position is before stream, reopen, then skip data to pos.
                inputStream.close();
                inputStream = value.getInputStream();
                IOUtils.skipFully(inputStream, streamPos);
            }
            internalPos = streamPos;
        }
    }

    public interface InputStreamProvider extends Closeable {
        InputStream getInputStream() throws IOException;
    }

    public static class ValueStreamProvider implements InputStreamProvider {
        private Value value;

        public ValueStreamProvider(Value value) {
            this.value = value;
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return value.getInputStream();
        }

        @Override
        public void close() throws IOException {

        }
    }

    public static class BlobStreamProvider implements InputStreamProvider {
        private Blob blob;

        public BlobStreamProvider(Blob blob) {
            this.blob = blob;
        }

        @Override
        public InputStream getInputStream() throws IOException {
            try {
                return blob.getBinaryStream();
            } catch (SQLException ex) {
                throw new IOException(ex);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                blob.free();
            } catch (SQLException ex) {
                throw new IOException(ex);
            }
        }
    }
}
