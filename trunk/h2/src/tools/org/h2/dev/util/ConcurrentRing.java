/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.util.Iterator;

import org.h2.mvstore.DataUtils;

/**
 * A ring buffer that supports concurrent access.
 * 
 * @param <K> the key type
 */
public class ConcurrentRing<K> {

    K[] buffer;
    volatile int readPos;
    volatile int writePos;

    @SuppressWarnings("unchecked")
    public ConcurrentRing() {
        buffer = (K[]) new Object[4];
    }
    
    public K peekFirst() {
        return buffer[getIndex(readPos)];
    }

    public K peekLast() {
        return buffer[getIndex(writePos - 1)];
    }

    public void add(K obj) {
        buffer[getIndex(writePos)] = obj;
        writePos++;
        if (writePos - readPos >= buffer.length) {
            // double the capacity
            @SuppressWarnings("unchecked")
            K[] b2 = (K[]) new Object[buffer.length * 2];
            for (int i = readPos; i < writePos; i++) {
                K x = buffer[getIndex(i)];
                int i2 = i & b2.length - 1;
                b2[i2] = x;
            }
            buffer = b2;
        }
    }
    
    public boolean removeFirst(K obj) {
        int p = readPos;
        int idx = getIndex(p);
        if (buffer[idx] != obj) {
            return false;
        }
        buffer[idx] = null;
        readPos = p + 1;
        return true;
    }

    public boolean removeLast(K obj) {
        int p = writePos;
        int idx = getIndex(p - 1);
        if (buffer[idx] != obj) {
            return false;
        }
        buffer[idx] = null;
        writePos = p - 1;
        return true;
    }
    
    int getIndex(int pos) {
        return pos & (buffer.length - 1);
    }

    public Iterator<K> iterator() {
        return new Iterator<K>() {

            int offset;

            @Override
            public boolean hasNext() {
                return readPos + offset < writePos;
            }

            @Override
            public K next() {
                if (buffer[getIndex(readPos + offset)] == null) {
                    System.out.println("" + readPos);
                    System.out.println("" + getIndex(readPos + offset));
                    System.out.println("null?");
                }
                return buffer[getIndex(readPos + offset++)];
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("remove");
            }

        };
    }
    
}
