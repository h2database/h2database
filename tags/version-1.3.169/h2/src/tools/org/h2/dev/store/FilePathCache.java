/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Map;
import org.h2.store.fs.FileBase;
import org.h2.store.fs.FilePathWrapper;
import org.h2.util.SmallLRUCache;

/**
 * A file with a read cache.
 */
public class FilePathCache extends FilePathWrapper {

    public static FileChannel wrap(FileChannel f) throws IOException {
        return new FileCache(f);
    }

    public FileChannel open(String mode) throws IOException {
        return new FileCache(getBase().open(mode));
    }

    public String getScheme() {
        return "cache";
    }

    /**
     * A file with a read cache.
     */
    public static class FileCache extends FileBase {

        private static final int CACHE_BLOCK_SIZE = 4 * 1024;
        private final FileChannel base;
        private long pos, posBase, size;
        private final Map<Long, ByteBuffer> cache = SmallLRUCache.newInstance(16);

        FileCache(FileChannel base) throws IOException {
            this.base = base;
            this.size = base.size();
        }

        protected void implCloseChannel() throws IOException {
            base.close();
        }

        public FileChannel position(long newPosition) throws IOException {
            this.pos = newPosition;
            return this;
        }

        public long position() throws IOException {
            return pos;
        }

        private void positionBase(long pos) throws IOException {
            if (posBase != pos) {
                base.position(pos);
                posBase = pos;
            }
        }

        public int read(ByteBuffer dst) throws IOException {
            long cachePos = getCachePos(pos);
            int off = (int) (pos - cachePos);
            int len = CACHE_BLOCK_SIZE - off;
            ByteBuffer buff = cache.get(cachePos);
            if (buff == null) {
                buff = ByteBuffer.allocate(CACHE_BLOCK_SIZE);
                positionBase(cachePos);
                int read = base.read(buff);
                posBase += read;
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
            pos += len;
            return len;
        }

        private static long getCachePos(long pos) {
            return (pos / CACHE_BLOCK_SIZE) * CACHE_BLOCK_SIZE;
        }

        public long size() throws IOException {
            return size;
        }

        public FileChannel truncate(long newSize) throws IOException {
            cache.clear();
            base.truncate(newSize);
            size = Math.min(size, newSize);
            pos = Math.min(pos, newSize);
            posBase = pos;
            return this;
        }

        public int write(ByteBuffer src) throws IOException {
            if (cache.size() > 0) {
                for (long p = getCachePos(pos), len = src.remaining(); len > 0; p += CACHE_BLOCK_SIZE, len -= CACHE_BLOCK_SIZE) {
                    cache.remove(p);
                }
            }
            positionBase(pos);
            int len = base.write(src);
            posBase += len;
            pos += len;
            size = Math.max(size, pos);
            return len;
        }

        public void force(boolean metaData) throws IOException {
            base.force(metaData);
        }

        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return base.tryLock(position, size, shared);
        }

        public String toString() {
            return "cache:" + base.toString();
        }

    }

}
