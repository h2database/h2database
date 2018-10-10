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
 * 1. Protects {@link java.nio.channels.FileChannel} from closed by application thread interrupted.<br />
 * 2. Solves the JDK poor allocation of direct buffer in {@link java.nio.channels.FileChannel}.<br />
 * Please see the <a href="https://github.com/h2database/h2database/issues/1502" target="_blank">issue 1502</a> .
 * </p>
 * 
 * @since 2018-10-10
 * @author little-pan
 */
public class FilePathNioAsync extends FilePathWrapper {
    
    final static String SCHEME = "nioAsync";

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
    
    final static ExecutorService readPool;
    final static ExecutorService writePool;
    
    static {
        final int nThreads = Runtime.getRuntime().availableProcessors();
        final String pfx = FilePathNioAsync.SCHEME + "-";
        readPool = Executors.newFixedThreadPool(nThreads, new AioThreadFactory(pfx + "r"));
        boolean failed = true;
        try {
            writePool = Executors.newFixedThreadPool(nThreads, new AioThreadFactory(pfx + "w"));
            failed = false;
        }finally{
            if(failed){
                readPool.shutdownNow();
            }
        }
    }

    private final RandomAccessFile file;
    private final String name;
    private final FileChannel channel;
    
    FileNioAsync(String fileName, String mode) throws IOException {
        this.name = fileName;
        this.file = new RandomAccessFile(fileName, mode);
        this.channel = file.getChannel();
    }

    @Override
    public void implCloseChannel() throws IOException {
        channel.close();
        file.close();
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
        for(;;){
            try {
                return f.get();
            } catch (InterruptedException e) {
                continue;
            } catch (ExecutionException e) {
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

class AioThreadFactory implements ThreadFactory {
    
    final AtomicInteger counter = new AtomicInteger(0);
    final String name;
    
    public AioThreadFactory(final String name){
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
