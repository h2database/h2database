/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.rec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Arrays;
import org.h2.store.fs.FileBase;
import org.h2.store.fs.Recorder;

/**
 * A file object that records all write operations and can re-play them.
 */
class FileRec extends FileBase {

    private final FilePathRec rec;
    private final FileChannel channel;
    private final String name;

    FileRec(FilePathRec rec, FileChannel file, String fileName) {
        this.rec = rec;
        this.channel = file;
        this.name = fileName;
    }

    @Override
    public void implCloseChannel() throws IOException {
        channel.close();
    }

    @Override
    public long position() throws IOException {
        return channel.position();
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return channel.read(dst, position);
    }

    @Override
    public FileChannel position(long pos) throws IOException {
        channel.position(pos);
        return this;
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        rec.log(Recorder.TRUNCATE, name, null, newLength);
        channel.truncate(newLength);
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        channel.force(metaData);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        byte[] buff = src.array();
        int len = src.remaining();
        if (src.position() != 0 || len != buff.length) {
            int offset = src.arrayOffset() + src.position();
            buff = Arrays.copyOfRange(buff, offset, offset + len);
        }
        int result = channel.write(src);
        rec.log(Recorder.WRITE, name, buff, channel.position());
        return result;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        byte[] buff = src.array();
        int len = src.remaining();
        if (src.position() != 0 || len != buff.length) {
            int offset = src.arrayOffset() + src.position();
            buff = Arrays.copyOfRange(buff, offset, offset + len);
        }
        int result = channel.write(src, position);
        rec.log(Recorder.WRITE, name, buff, position);
        return result;
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        return channel.tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return name;
    }

}