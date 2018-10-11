/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * This file system stores files on disk and uses java.nio to access the files as {@link FilePathNio}.
 * This class uses {@link java.nio.channels.FileChannel}. But it's different that this uses two thread
 * pools to handle read/write operations for two intents:
 * </p>
 * <p>
 * 1. Protects {@link java.nio.channels.FileChannel} from closed by application thread interrupted.Please see
 * the <a href="https://github.com/h2database/h2database/issues/227" target="_blank">issue 227</a>.<br />
 * 2. Solves the JDK poor allocation of direct buffer in {@link java.nio.channels.FileChannel}. Please see 
 * the <a href="https://github.com/h2database/h2database/issues/1502" target="_blank">issue 1502</a>.<br/>
 * </p>
 * 
 * @since 2018-10-10
 * @author little-pan
 */
public class FilePathNioAsync extends FilePathWrapper {
    
    static final String SCHEME = "nioAsync";

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileNioAsync(name.substring(getScheme().length() + 1), mode);
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

}

/**
 * File which uses NIO FileChannel and two read/write thread pools.
 */
class FileNioAsync extends FileBase {
    
    // The life-cycle of read/write pool is the same as FileStore, for the motivation:
    // In some environments such as Apache tomcat, applications can be unloaded or reloaded.
    // And most of the time, the read/write pool is long-running.
    // @since 2018-10-11 little-pan
    final ExecutorService readPool;
    final ExecutorService writePool;
    
    {
        final int processors = Math.min(9, Runtime.getRuntime().availableProcessors()<<1);
        final String pfx = "H2 " + FilePathNioAsync.SCHEME + "-";
        readPool = Executors.newFixedThreadPool(processors, new AsyncThreadFactory(pfx + "r"));
        boolean failed = true;
        try {
            // Only one write thread per H2 MVStore, controlled by storeLock.
            writePool = Executors.newSingleThreadExecutor(new AsyncThreadFactory(pfx + "w"));
            failed = false;
        } finally {
            if(failed){
                readPool.shutdownNow();
            }
        }
    }

    private final RandomAccessFile file;
    private final String name;
    private final FileChannel channel;
    
    FileNioAsync(String fileName, String mode) throws IOException {
        boolean failed = true;
        try {
            this.name = fileName;
            this.file = new RandomAccessFile(fileName, mode);
            this.channel = file.getChannel();
            failed = false;
        } finally {
            if(failed){
                readPool.shutdown();
                writePool.shutdown();
                if(this.file != null){
                    this.file.close();
                }
            }
        }
    }

    @Override
    public void implCloseChannel() throws IOException {
        channel.close();
        file.close();
        readPool.shutdown();
        writePool.shutdown();
    }

    @Override
    public long position() throws IOException {
        return submit(readPool, new Callable<Long>(){
             @Override
            public Long call() throws Exception {
                return channel.position();
            }
        });
    }

    @Override
    public long size() throws IOException {
        return submit(readPool, new Callable<Long>(){
            @Override
           public Long call() throws Exception {
                return channel.size();
           }
        });
    }

    @Override
    public int read(final ByteBuffer dst) throws IOException {
        return submit(readPool, new Callable<Integer>(){
            @Override
           public Integer call() throws Exception {
                return channel.read(dst);
           }
        });
    }

    @Override
    public FileChannel position(final long pos) throws IOException {
        submit(writePool, new Callable<Void>(){
            @Override
           public Void call() throws Exception {
                channel.position(pos);
                return null;
           }
        });
        return this;
    }

    @Override
    public int read(final ByteBuffer dst, final long position) throws IOException {
        return submit(readPool, new Callable<Integer>(){
            @Override
           public Integer call() throws Exception {
                return channel.read(dst, position);
           }
        });
    }

    @Override
    public int write(final ByteBuffer src, final long position) throws IOException {
        return submit(writePool, new Callable<Integer>(){
            @Override
           public Integer call() throws Exception {
                return channel.write(src, position);
           }
        });
    }

    @Override
    public FileChannel truncate(final long newLength) throws IOException {
        submit(writePool, new Callable<Void>(){
            @Override
           public Void call() throws Exception {
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
                return null;
           }
        });
        return this;
    }

    @Override
    public void force(final boolean metaData) throws IOException {
        submit(writePool, new Callable<Void>(){
            @Override
           public Void call() throws Exception {
                channel.force(metaData);
                return null;
           }
        });
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
        return submit(writePool, new Callable<Integer>(){
            @Override
           public Integer call() throws Exception {
                try {
                    return channel.write(src);
                } catch (NonWritableChannelException e) {
                    throw new IOException("read only");
                }
           }
        });
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        return channel.tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return FilePathNioAsync.SCHEME + ":" + name;
    }
    
    /**
     * <p>
     * Submit the callable task to executor.
     * </p>
     * 
     * @param executor
     * @param call
     * @return the executor result
     * @throws IOException if IO error
     */
    static <V> V submit(ExecutorService executor, Callable<V> call) throws IOException {
        final Future<V> f = executor.submit(call);
        for(boolean interrupted = false;;){
            try {
                final V result = f.get();
                // I-1. Keep the interrupted status.
                // @since 2018-10-11 little-pan
                if(interrupted){
                    Thread.currentThread().interrupt();
                }
                return result;
            } catch (InterruptedException e) {
                interrupted = true;
                continue;
            } catch (ExecutionException e) {
                // I-2
                if(interrupted){
                    Thread.currentThread().interrupt();
                }
                final Throwable cause = e.getCause();
                if(cause instanceof IOException){
                    throw (IOException)cause;
                }
                if(cause instanceof Error){
                    throw (Error)cause;
                }
                throw (RuntimeException)cause;
            }
        }
    }
    
}

class AsyncThreadFactory implements ThreadFactory {
    
    final AtomicInteger counter = new AtomicInteger(0);
    final String name;
    
    public AsyncThreadFactory(final String name){
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        final Thread t = new Thread(r);
        t.setName(name + counter.getAndIncrement());
        t.setDaemon(true);
        return t;
    }
    
}
