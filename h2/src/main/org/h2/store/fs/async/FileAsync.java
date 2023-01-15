/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.h2.store.fs.FileBaseDefault;
import org.h2.store.fs.FileUtils;

/**
 * File which uses NIO2 AsynchronousFileChannel.
 */
class FileAsync extends FileBaseDefault {

    private final String name;
    private final AsynchronousFileChannel channel;

    private static <T> T complete(Future<T> future) throws IOException {
        boolean interrupted = false;
        for (;;) {
            try {
                T result = future.get();
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                return result;
            } catch (InterruptedException e) {
                interrupted = true;
            } catch (ExecutionException e) {
                throw new IOException(e.getCause());
            }
        }
    }

    FileAsync(String fileName, String mode) throws IOException {
        this.name = fileName;
        channel = AsynchronousFileChannel.open(Paths.get(fileName), FileUtils.modeToOptions(mode), null,
                FileUtils.NO_ATTRIBUTES);
    }

    @Override
    public void implCloseChannel() throws IOException {
        channel.close();
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return complete(channel.read(dst, position));
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        return complete(channel.write(src, position));
    }

    @Override
    protected void implTruncate(long newLength) throws IOException {
        channel.truncate(newLength);
    }

    @Override
    public void force(boolean metaData) throws IOException {
        channel.force(metaData);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return channel.tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return "async:" + name;
    }

}