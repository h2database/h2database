package org.h2.store.fs.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.h2.store.fs.FileBase;
import org.h2.store.fs.FileUtils;

/**
 * File which uses NIO2 AsynchronousFileChannel.
 */
class FileAsync extends FileBase {

    private final String name;

    private final AsynchronousFileChannel channel;

    private long position;

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
    public long position() throws IOException {
        return position;
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int read = complete(channel.read(dst, position));
        if (read > 0) {
            position += read;
        }
        return read;
    }

    @Override
    public FileChannel position(long pos) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException();
        }
        position = pos;
        return this;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return complete(channel.read(dst, position));
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        try {
            return complete(channel.write(src, position));
        } catch (NonWritableChannelException e) {
            throw new IOException("read only");
        }
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        channel.truncate(newLength);
        if (newLength < position) {
            position = newLength;
        }
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        channel.force(metaData);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int written = complete(channel.write(src, position));
        position += written;
        return written;
    }

    @Override
    public synchronized FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return channel.tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return "async:" + name;
    }

}