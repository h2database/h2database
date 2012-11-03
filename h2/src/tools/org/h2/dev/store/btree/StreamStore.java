/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.h2.util.IOUtils;

/**
 * A facility to store streams in a map. Streams are split into blocks, which
 * are stored in a map. Very small streams are inlined in the stream id.
 * <p>
 * The key of the map is a long (incremented for each stored block). The default
 * initial value is 0. Before storing blocks into the map, the stream store
 * checks if there is already a block with the next key, and if necessary
 * searches the next free entry using a binary search (0 to Long.MAX_VALUE).
 * <p>
 * The format of the binary id is: An empty id represents 0 bytes of data.
 * In-place data is encoded as the size (a variable size int), then the data. A
 * stored block is encoded as 0, the length of the block (a variable size long),
 * the length of the key (a variable size int), then the key. If the key large,
 * it is stored itself. This is encoded as -1 (a variable size int), the total
 * length (a variable size long), the length of the key (a variable size int),
 * and the key that points to the id. Multiple ids can be concatenated to
 * concatenate the data.
 */
public class StreamStore {

    private final Map<Long, byte[]> map;
    private int minBlockSize = 256;
    private int maxBlockSize = 256 * 1024;
    private final AtomicLong nextKey = new AtomicLong();

    /**
     * Create a stream store instance.
     *
     * @param map the map to store blocks of data
     */
    public StreamStore(Map<Long, byte[]> map) {
        this.map = map;
    }

    public Map<Long, byte[]> getMap() {
        return map;
    }

    public void setNextKey(long nextKey) {
        this.nextKey.set(nextKey);
    }

    public long getNextKey() {
        return nextKey.get();
    }

    public void setMinBlockSize(int minBlockSize) {
        this.minBlockSize = minBlockSize;
    }

    public int getMinBlockSize() {
        return minBlockSize;
    }

    public void setMaxBlockSize(int maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
    }

    public long getMaxBlockSize() {
        return maxBlockSize;
    }

    /**
     * Store the stream, and return the id.
     *
     * @param in the stream
     * @return the id (potentially an empty array)
     */
    public byte[] put(InputStream in) throws IOException {
        ByteArrayOutputStream idStream = new ByteArrayOutputStream();
        long length = 0;
        while (true) {
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            int len = (int) IOUtils.copy(in, outBuffer, maxBlockSize);
            if (len == 0) {
                break;
            }
            boolean eof = len < maxBlockSize;
            byte[] data = outBuffer.toByteArray();
            ByteArrayOutputStream idBlock = new ByteArrayOutputStream();
            if (len < minBlockSize) {
                DataUtils.writeVarInt(idBlock, len);
                idBlock.write(data);
            } else {
                long key = writeBlock(data);
                DataUtils.writeVarInt(idBlock, 0);
                DataUtils.writeVarLong(idBlock, len);
                int keyLen = DataUtils.getVarLongLen(key);
                DataUtils.writeVarInt(idBlock, keyLen);
                DataUtils.writeVarLong(idBlock, key);
            }
            if (idStream.size() > 0) {
                int idSize = idStream.size() + idBlock.size();
                if (idSize > maxBlockSize || (eof && idSize > minBlockSize)) {
                    data = idStream.toByteArray();
                    idStream.reset();
                    long key = writeBlock(data);
                    DataUtils.writeVarInt(idStream, -1);
                    DataUtils.writeVarLong(idStream, length);
                    int keyLen = DataUtils.getVarLongLen(key);
                    DataUtils.writeVarInt(idStream, keyLen);
                    DataUtils.writeVarLong(idStream, key);
                }
            }
            length += len;
            idBlock.writeTo(idStream);
            if (eof) {
                break;
            }
        }
        return idStream.toByteArray();
    }

    private long writeBlock(byte[] data) {
        long key = getAndIncrementNextKey();
        map.put(key, data);
        return key;
    }

    private long getAndIncrementNextKey() {
        long key = nextKey.getAndIncrement();
        if (!map.containsKey(key)) {
            return key;
        }
        // search the next free id using binary search
        synchronized (this) {
            long low = key, high = Long.MAX_VALUE;
            while (low < high) {
                long x = (low + high) >>> 1;
                if (map.containsKey(x)) {
                    low = x + 1;
                } else {
                    high = x;
                }
            }
            key = low;
            nextKey.set(key + 1);
            return key;
        }
    }

    /**
     * Remove all stored blocks for the given id.
     *
     * @param id the id
     */
    public void remove(byte[] id) {
        ByteBuffer idBuffer = ByteBuffer.wrap(id);
        while (idBuffer.hasRemaining()) {
            removeBlock(idBuffer);
        }
    }

    private void removeBlock(ByteBuffer idBuffer) {
        int lenInPlace = DataUtils.readVarInt(idBuffer);
        if (lenInPlace > 0) {
            idBuffer.position(idBuffer.position() + lenInPlace);
            return;
        }
        DataUtils.readVarLong(idBuffer);
        int lenId = DataUtils.readVarInt(idBuffer);
        byte[] key = new byte[lenId];
        idBuffer.get(key);
        if (lenInPlace < 0) {
            // recurse
            remove(readBlock(key));
        }
        removeBlock(key);
    }

    private void removeBlock(byte[] key) {
        map.remove(getKey(key));
    }

    /**
     * Calculate the number of data bytes for the given id. As the length is
     * encoded in the id, this operation does not cause any reads in the map.
     *
     * @param id the id
     * @return the length
     */
    public long length(byte[] id) {
        ByteBuffer idBuffer = ByteBuffer.wrap(id);
        long length = 0;
        while (idBuffer.hasRemaining()) {
            length += readLength(idBuffer);
        }
        return length;
    }

    private static long readLength(ByteBuffer idBuffer) {
        int lenInPlace = DataUtils.readVarInt(idBuffer);
        if (lenInPlace > 0) {
            idBuffer.position(idBuffer.position() + lenInPlace);
            return lenInPlace;
        }
        long len = DataUtils.readVarLong(idBuffer);
        int lenId = DataUtils.readVarInt(idBuffer);
        idBuffer.position(idBuffer.position() + lenId);
        return len;
    }

    /**
     * Check whether the id itself contains all the data. This operation does
     * not cause any reads in the map.
     *
     * @param id the id
     * @return if the id contains the data
     */
    public boolean isInPlace(byte[] id) {
        ByteBuffer idBuffer = ByteBuffer.wrap(id);
        while (idBuffer.hasRemaining()) {
            if (!isInPlace(idBuffer)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isInPlace(ByteBuffer idBuffer) {
        int lenInPlace = DataUtils.readVarInt(idBuffer);
        if (lenInPlace > 0) {
            idBuffer.position(idBuffer.position() + lenInPlace);
            return true;
        }
        return false;
    }

    byte[] readBlock(byte[] key) {
        return map.get(getKey(key));
    }

    private static Long getKey(byte[] key) {
        int todoRemove;
        ByteBuffer buff = ByteBuffer.wrap(key);
        return DataUtils.readVarLong(buff);
    }

    /**
     * Open an input stream to read data.
     *
     * @param id the id
     * @return the stream
     */
    public InputStream get(byte[] id) {
        return new Stream(this, id);
    }

    /**
     * A stream backed by a map.
     */
    static class Stream extends InputStream {

        private StreamStore store;
        private byte[] oneByteBuffer;
        private ByteBuffer idBuffer;
        private ByteArrayInputStream buffer;
        private long skip;
        private long length;
        private long pos;

        Stream(StreamStore store, byte[] id) {
            this.store = store;
            this.length = store.length(id);
            this.idBuffer = ByteBuffer.wrap(id);
        }

        @Override
        public int read() {
            byte[] buffer = oneByteBuffer;
            if (buffer == null) {
                buffer = oneByteBuffer = new byte[1];
            }
            int len = read(buffer, 0, 1);
            return len == -1 ? -1 : (buffer[0] & 255);
        }

        @Override
        public long skip(long n) {
            n = Math.min(length - pos, n);
            if (n == 0) {
                return 0;
            }
            if (buffer != null) {
                long s = buffer.skip(n);
                if (s > 0) {
                    n = s;
                } else {
                    buffer = null;
                    skip += n;
                }
            } else {
                skip += n;
            }
            pos += n;
            return n;
        }

        @Override
        public void close() {
            buffer = null;
            idBuffer.position(idBuffer.limit());
            pos = length;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            while (true) {
                if (buffer == null) {
                    buffer = nextBuffer();
                    if (buffer == null) {
                        return -1;
                    }
                }
                int result = buffer.read(b, off, len);
                if (result > 0) {
                    pos += result;
                    return result;
                }
                buffer = null;
            }
        }

        private ByteArrayInputStream nextBuffer() {
            while (idBuffer.hasRemaining()) {
                int lenInPlace = DataUtils.readVarInt(idBuffer);
                if (lenInPlace > 0) {
                    if (skip >= lenInPlace) {
                        skip -= lenInPlace;
                        idBuffer.position(idBuffer.position() + lenInPlace);
                        continue;
                    }
                    int p = (int) (idBuffer.position() + skip);
                    int l = (int) (lenInPlace - skip);
                    idBuffer.position(p + l);
                    return new ByteArrayInputStream(idBuffer.array(), p, l);
                }
                long length = DataUtils.readVarLong(idBuffer);
                int lenId = DataUtils.readVarInt(idBuffer);
                if (skip >= length) {
                    skip -= length;
                    idBuffer.position(idBuffer.position() + lenId);
                    continue;
                }
                byte[] key = new byte[lenId];
                idBuffer.get(key);
                if (lenInPlace < 0) {
                    byte[] k = store.readBlock(key);
                    ByteBuffer newBuffer = ByteBuffer.allocate(k.length + idBuffer.limit() - idBuffer.position());
                    newBuffer.put(k);
                    newBuffer.put(idBuffer);
                    newBuffer.flip();
                    idBuffer = newBuffer;
                    return nextBuffer();
                }
                byte[] data = store.readBlock(key);
                int s = (int) skip;
                skip = 0;
                return new ByteArrayInputStream(data, s, data.length - s);
            }
            return null;
        }

    }

}
