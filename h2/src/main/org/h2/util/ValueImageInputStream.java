/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.value.Value;

import javax.imageio.stream.ImageInputStreamImpl;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wrap a Value using InputStream simulate a ImageInputStream.
 * @author Nicolas Fortin
 */
public class ValueImageInputStream extends ImageInputStreamImpl {
    private Value value;
    private InputStream inputStream;
    private long internalPos = 0;

    public ValueImageInputStream(Value value) {
        this.value = value;
        this.inputStream = value.getInputStream();
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
        return readOffset;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        super.close();
    }

    private void synchronize() throws IOException {
        if(streamPos != internalPos) {
            if (internalPos < streamPos) {
                long skip = streamPos - internalPos;
                if(inputStream.skip(skip) != skip) {
                    throw new EOFException();
                }
            } else {
                // If position is before stream, reopen, then skip data to pos.
                inputStream.close();
                inputStream = value.getInputStream();
                inputStream.skip(streamPos);
            }
            internalPos = streamPos;
        }
    }
}
