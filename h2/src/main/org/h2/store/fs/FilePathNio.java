/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import org.h2.engine.Constants;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;

/**
 * This file system stores files on disk and uses java.nio to access the files.
 * This class uses FileChannel.
 */
public class FilePathNio extends FilePathWrapper {

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileNio(name.substring(getScheme().length() + 1), mode);
    }

    @Override
    public String getScheme() {
        return "nio";
    }

}

/**
 * File which uses NIO FileChannel.
 */
class FileNio extends FileBase {

    private final String name;
    private final FileChannel channel;

    FileNio(String fileName, String mode) throws IOException {
        this.name = fileName;
        channel = new RandomAccessFile(fileName, mode).getChannel();
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
    public FileChannel position(long pos) throws IOException {
        channel.position(pos);
        return this;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return channel.read(dst, position);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        if(src.isDirect()){
            return channel.write(src, position);
        }

        // Write page by page(or in the unit of a few pages) for avoiding to allocate direct buffer poorly
        // in "sun.nio.ch.FileChannelImpl" of java.nio.channels.FileChannel and "sun.nio.ch.IOUtil".
        // Please see the issue "https://github.com/h2database/h2database/issues/1502".
        // @since 2018-10-07 little-pan
        final int unitSize = Constants.DEFAULT_PAGE_SIZE;
        final FileChannel file = channel;
        final long pos = position;
        int off = 0;
        for(; src.hasRemaining(); ) {
            final ByteBuffer unitBuffer;
            final int rem = src.remaining();
            if(rem > unitSize){
                final int lim = src.limit();
                try {
                    src.limit(src.position() + unitSize);
                    unitBuffer = src.slice();
                }finally {
                    src.limit(lim);
                }
            }else{
                unitBuffer = src.slice();
            }
            for(;unitBuffer.hasRemaining();){
                final int len = file.write(unitBuffer, pos + off);
                off += len;
                src.position(src.position() + len);
            }
        }
        return off;
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        long size = channel.size();
        if (newLength < size) {
            long pos = channel.position();
            channel.truncate(newLength);
            long newPos = channel.position();
            if (pos < newLength) {
                // position should stay
                // in theory, this should not be needed
                if (newPos != pos) {
                    channel.position(pos);
                }
            } else if (newPos > newLength) {
                // looks like a bug in this FileChannel implementation, as
                // the documentation says the position needs to be changed
                channel.position(newLength);
            }
        }
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        channel.force(metaData);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        try {
            return channel.write(src);
        } catch (NonWritableChannelException e) {
            throw new IOException("read only");
        }
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        return channel.tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return "nio:" + name;
    }

}
