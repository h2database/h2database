/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.zip;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.h2.store.fs.FakeFileChannel;
import org.h2.store.fs.FileBase;
import org.h2.util.IOUtils;

/**
 * The file is read from a stream. When reading from start to end, the same
 * input stream is re-used, however when reading from end to start, a new input
 * stream is opened for each request.
 */
class FileZip extends FileBase {

    private static final byte[] SKIP_BUFFER = new byte[1024];

    private final ZipFile file;
    private final ZipEntry entry;
    private long pos;
    private InputStream in;
    private long inPos;
    private final long length;
    private boolean skipUsingRead;

    FileZip(ZipFile file, ZipEntry entry) {
        this.file = file;
        this.entry = entry;
        length = entry.getSize();
    }

    @Override
    public long position() {
        return pos;
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        seek();
        int len = in.read(dst.array(), dst.arrayOffset() + dst.position(),
                dst.remaining());
        if (len > 0) {
            dst.position(dst.position() + len);
            pos += len;
            inPos += len;
        }
        return len;
    }

    private void seek() throws IOException {
        if (inPos > pos) {
            if (in != null) {
                in.close();
            }
            in = null;
        }
        if (in == null) {
            in = file.getInputStream(entry);
            inPos = 0;
        }
        if (inPos < pos) {
            long skip = pos - inPos;
            if (!skipUsingRead) {
                try {
                    IOUtils.skipFully(in, skip);
                } catch (NullPointerException e) {
                    // workaround for Android
                    skipUsingRead = true;
                }
            }
            if (skipUsingRead) {
                while (skip > 0) {
                    int s = (int) Math.min(SKIP_BUFFER.length, skip);
                    s = in.read(SKIP_BUFFER, 0, s);
                    skip -= s;
                }
            }
            inPos = pos;
        }
    }

    @Override
    public FileChannel position(long newPos) {
        this.pos = newPos;
        return this;
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        throw new IOException("File is read-only");
    }

    @Override
    public void force(boolean metaData) throws IOException {
        // nothing to do
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new NonWritableChannelException();
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        if (shared) {
            return new FileLock(FakeFileChannel.INSTANCE, position, size, shared) {

                @Override
                public boolean isValid() {
                    return true;
                }

                @Override
                public void release() throws IOException {
                    // ignore
                }};
        }
        return null;
    }

    @Override
    protected void implCloseChannel() throws IOException {
        if (in != null) {
            in.close();
            in = null;
        }
        file.close();
    }

}