/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import org.h2.store.fs.FileBase;
import org.h2.store.fs.FilePathWrapper;

/**
 * A file with a read cache.
 */
public class FilePathCache extends FilePathWrapper {

    public static FileChannel wrap(FileChannel f) {
        return new FileCache(f);
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileCache(getBase().open(mode));
    }

    @Override
    public String getScheme() {
        return "cache";
    }

    /**
     * A file with a read cache.
     */
    public static class FileCache extends FileBase {

        private static final int CACHE_BLOCK_SIZE = 4 * 1024;
        private final FileChannel base;
        // 1 MB (256 * 4 * 1024)
        private final CacheLongKeyLIRS<ByteBuffer> cache =
                new CacheLongKeyLIRS<ByteBuffer>(256);

        FileCache(FileChannel base) {
            this.base = base;
        }

        @Override
        protected void implCloseChannel() throws IOException {
            base.close();
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            base.position(newPosition);
            return this;
        }

        @Override
        public long position() throws IOException {
            return base.position();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return base.read(dst);
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            long cachePos = getCachePos(position);
            int off = (int) (position - cachePos);
            int len = CACHE_BLOCK_SIZE - off;
            ByteBuffer buff = cache.get(cachePos);
            if (buff == null) {
                buff = ByteBuffer.allocate(CACHE_BLOCK_SIZE);
                int read = base.read(buff, cachePos);
                if (read == CACHE_BLOCK_SIZE) {
                    cache.put(cachePos, buff);
                } else {
                    if (read < 0) {
                        return -1;
                    }
                    len = Math.min(len, read);
                }
            }
            len = Math.min(len, dst.remaining());
            System.arraycopy(buff.array(), off, dst.array(), dst.position(), len);
            dst.position(dst.position() + len);
            return len;
        }

        private static long getCachePos(long pos) {
            return (pos / CACHE_BLOCK_SIZE) * CACHE_BLOCK_SIZE;
        }

        @Override
        public long size() throws IOException {
            return base.size();
        }

        @Override
        public FileChannel truncate(long newSize) throws IOException {
            cache.clear();
            base.truncate(newSize);
            return this;
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            if (cache.size() > 0) {
                int len = src.remaining();
                long p = getCachePos(position);
                while (len > 0) {
                    cache.remove(p);
                    p += CACHE_BLOCK_SIZE;
                    len -= CACHE_BLOCK_SIZE;
                }
            }
            return base.write(src, position);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            throw new UnsupportedOperationException();
            // return base.write(src);
        }

        @Override
        public void force(boolean metaData) throws IOException {
            base.force(metaData);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return base.tryLock(position, size, shared);
        }

        @Override
        public String toString() {
            return "cache:" + base.toString();
        }

    }

}
