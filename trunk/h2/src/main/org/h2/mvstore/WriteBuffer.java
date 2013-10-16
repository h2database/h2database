/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;

/**
 * An auto-resize buffer to write data into a ByteBuffer.
 */
public class WriteBuffer {

    private static final int MAX_REUSE_LIMIT = 4 * 1024 * 1024;
    
    /**
     * The maximum byte to grow a buffer at a time.
     */
    private static final int MAX_GROW = 4 * 1024 * 1024;

    
    private ByteBuffer reuse = ByteBuffer.allocate(512 * 1024);

    private ByteBuffer buff = reuse;
    
    public void writeVarInt(int x) {
        DataUtils.writeVarInt(ensureCapacity(5), x);
    }
    
    public void writeVarLong(long x) {
        DataUtils.writeVarLong(ensureCapacity(10), x);
    }

    public void writeStringData(String s, int len) {
        ByteBuffer b = ensureCapacity(3 * len);
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (c < 0x80) {
                b.put((byte) c);
            } else if (c >= 0x800) {
                b.put((byte) (0xe0 | (c >> 12)));
                b.put((byte) (((c >> 6) & 0x3f)));
                b.put((byte) (c & 0x3f));
            } else {
                b.put((byte) (0xc0 | (c >> 6)));
                b.put((byte) (c & 0x3f));
            }
        }
    }

    public void put(byte x) {
        ensureCapacity(1).put(x);
    }
    
    public void putChar(char x) {
        ensureCapacity(2).putChar(x);
    }
    
    public void putShort(short x) {
        ensureCapacity(2).putShort(x);
    }
    
    public void putInt(int x) {
        ensureCapacity(4).putInt(x);
    }
    
    public void putLong(long x) {
        ensureCapacity(8).putLong(x);
    }
    
    public void putFloat(float x) {
        ensureCapacity(4).putFloat(x);
    }
    
    public void putDouble(double x) {
        ensureCapacity(8).putDouble(x);
    }
    
    public void put(byte[] bytes) {
        ensureCapacity(bytes.length).put(bytes);
    }
    
    public void put(byte[] bytes, int offset, int length) {
        ensureCapacity(length).put(bytes, offset, length);
    }

    public void position(int newPosition) {
        buff.position(newPosition);
    }

    public int position() {
        return buff.position();
    }
    
    public void get(byte[] dst) {
        buff.get(dst);
    }
    
    public void putInt(int index, int value) {
        buff.putInt(index, value);
    }
    
    public void putShort(int index, short value) {
        buff.putShort(index, value);
    }
    
    public void put(ByteBuffer src) {
        ensureCapacity(buff.remaining()).put(src);
    }

    public void limit(int newLimit) {
        ensureCapacity(newLimit - buff.position()).limit(newLimit);
    }
    
    public int limit() {
        return buff.limit();
    }
    
    public int capacity() {
        return buff.capacity();
    }
    
    public ByteBuffer getBuffer() {
        return buff;
    }

    /**
     * Clear the buffer after use.
     */
    void clear() {
        if (buff.limit() > MAX_REUSE_LIMIT) {
            buff = reuse;
        }
        buff.clear();
    }
    
    private ByteBuffer ensureCapacity(int len) {
        if (buff.remaining() < len) {
            grow(len);
        }
        return buff;
    }
    
    private void grow(int len) {
        ByteBuffer temp = buff;
        len = temp.remaining() + len;
        int capacity = temp.capacity();
        len = Math.max(len, Math.min(capacity + MAX_GROW, capacity * 2));
        buff = ByteBuffer.allocate(len);
        temp.flip();
        buff.put(temp);
        if (len <= MAX_REUSE_LIMIT) {
            reuse = buff;
        }
    }

}
