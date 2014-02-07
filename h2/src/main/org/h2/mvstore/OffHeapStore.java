/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * A storage mechanism that "persists" data in the off-heap area of the main
 * memory.
 */
public class OffHeapStore extends FileStore {

    private final TreeMap<Long, ByteBuffer> memory = new TreeMap<Long, ByteBuffer>();

    @Override
    public void open(String fileName, boolean readOnly, char[] encryptionKey) {
        // nothing to do
    }

    @Override
    public String toString() {
        return memory.toString();
    }

    @Override
    public ByteBuffer readFully(long pos, int len) {
        Entry<Long, ByteBuffer> memEntry = memory.floorEntry(pos);
        if (memEntry == null) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_READING_FAILED,
                    "Could not read from position {0}", pos);
        }
        readCount++;
        ByteBuffer buff = memEntry.getValue();
        ByteBuffer read = buff.duplicate();
        int offset = (int) (pos - memEntry.getKey());
        read.position(offset);
        read.limit(len + offset);
        return read.slice();
    }

    @Override
    public void free(long pos, int length) {
        freeSpace.free(pos, length);
        ByteBuffer buff = memory.remove(pos);
        if (buff == null) {
            // nothing was written (just allocated)
        } else if (buff.remaining() != length) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_READING_FAILED,
                    "Partial remove is not supported at position {0}", pos);
        }
    }

    @Override
    public void writeFully(long pos, ByteBuffer src) {
        fileSize = Math.max(fileSize, pos + src.remaining());
        Entry<Long, ByteBuffer> mem = memory.floorEntry(pos);
        if (mem == null) {
            // not found: create a new entry
            writeNewEntry(pos, src);
            return;
        }
        long prevPos = mem.getKey();
        ByteBuffer buff = mem.getValue();
        int prevLength = buff.capacity();
        int length = src.remaining();
        if (prevPos == pos) {
            if (prevLength != length) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_READING_FAILED,
                        "Could not write to position {0}; partial overwrite is not supported", pos);
            }
            writeCount++;
            buff.rewind();
            buff.put(src);
            return;
        }
        if (prevPos + prevLength > pos) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_READING_FAILED,
                    "Could not write to position {0}; partial overwrite is not supported", pos);
        }
        writeNewEntry(pos, src);
    }

    private void writeNewEntry(long pos, ByteBuffer src) {
        writeCount++;
        int length = src.remaining();
        ByteBuffer buff = ByteBuffer.allocateDirect(length);
        buff.put(src);
        buff.rewind();
        memory.put(pos, buff);
    }

    @Override
    public void truncate(long size) {
        writeCount++;
        if (size == 0) {
            fileSize = 0;
            memory.clear();
            return;
        }
        fileSize = size;
        for (Iterator<Long> it = memory.keySet().iterator(); it.hasNext();) {
            long pos = it.next();
            if (pos < size) {
                break;
            }
            ByteBuffer buff = memory.get(pos);
            if (buff.capacity() > size) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_READING_FAILED,
                        "Could not truncate to {0}; partial truncate is not supported", pos);
            }
            it.remove();
        }
    }

    @Override
    public void close() {
        // do nothing (keep the data until it is garbage collected)
    }

    @Override
    public void sync() {
        // nothing to do
    }

    @Override
    public int getDefaultRetentionTime() {
        return 0;
    }

}
