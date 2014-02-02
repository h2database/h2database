/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * A chunk of data, containing one or multiple pages.
 * <p>
 * Chunks are page aligned (each page is usually 4096 bytes).
 * There are at most 67 million (2^26) chunks,
 * each chunk is at most 2 GB large.
 * Chunk format:
 * 1 byte: 'c'
 * 4 bytes: length
 * 4 bytes: chunk id (an incrementing number)
 * 4 bytes: pageCount
 * 8 bytes: metaRootPos
 * 8 bytes: maxLengthLive
 * [ Page ] *
 */
public class Chunk {

    /**
     * The maximum length of a chunk header, in bytes.
     */
    static final int MAX_HEADER_LENGTH = 1024;

    /**
     * The chunk id.
     */
    public final int id;

    /**
     * The start position within the file.
     */
    public long start;

    /**
     * The length in bytes.
     */
    public int length;

    /**
     * The total number of pages in this chunk.
     */
    public int pageCount;

    /**
     * The number of pages still alive.
     */
    public int pageCountLive;

    /**
     * The sum of the max length of all pages.
     */
    public long maxLength;

    /**
     * The sum of the max length of all pages that are in use.
     */
    public long maxLengthLive;

    /**
     * The garbage collection priority.
     */
    public int collectPriority;

    /**
     * The position of the meta root.
     */
    public long metaRootPos;

    /**
     * The version stored in this chunk.
     */
    public long version;

    /**
     * When this chunk was created, in milliseconds after the store was created.
     */
    public long time;

    Chunk(int id) {
        this.id = id;
    }

    /**
     * Read the header from the byte buffer.
     *
     * @param buff the source buffer
     * @param start the start of the chunk in the file
     * @return the chunk
     */
    static Chunk fromHeader(ByteBuffer buff, long start) {
        int pos = buff.position();
        if (buff.get() != '{') {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupt reading chunk at position {0}", start);
        }
        byte[] data = new byte[Math.min(buff.remaining(), MAX_HEADER_LENGTH)];
        // set the position to the start of the first page
        buff.get(data);
        for (int i = 0; i < data.length; i++) {
            if (data[i] == '\n') {
                buff.position(pos + i + 2);
                break;
            }
        }
        String s = new String(data, 0, data.length, DataUtils.UTF8);
        return fromString(s);
    }

    /**
     * Write the chunk header.
     *
     * @param buff the target buffer
     */
    void writeHeader(WriteBuffer buff) {
        buff.put((byte) '{');
        buff.put(asString().getBytes(DataUtils.UTF8));
        buff.put((byte) '}');
        buff.put((byte) ' ');
    }

    /**
     * Build a block from the given string.
     *
     * @param s the string
     * @return the block
     */
    public static Chunk fromString(String s) {
        HashMap<String, String> map = DataUtils.parseMap(s);
        int id = Integer.parseInt(map.get("chunk"));
        Chunk c = new Chunk(id);
        c.start = Long.parseLong(map.get("start"));
        c.length = Integer.parseInt(map.get("length"));
        c.pageCount = Integer.parseInt(map.get("pageCount"));
        c.pageCountLive = Integer.parseInt(map.get("pageCountLive"));
        c.maxLength = Long.parseLong(map.get("maxLength"));
        c.maxLengthLive = Long.parseLong(map.get("maxLengthLive"));
        c.metaRootPos = Long.parseLong(map.get("metaRoot"));
        c.time = Long.parseLong(map.get("time"));
        c.version = Long.parseLong(map.get("version"));
        return c;
    }

    public int getFillRate() {
        return (int) (maxLength == 0 ? 0 : 100 * maxLengthLive / maxLength);
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Chunk && ((Chunk) o).id == id;
    }

    /**
     * Get the chunk data as a string.
     *
     * @return the string
     */
    public String asString() {
        return
                "chunk:" + id + "," +
                "length:" + length + "," +
                "maxLength:" + maxLength + "," +
                "maxLengthLive:" + maxLengthLive + "," +
                "metaRoot:" + metaRootPos + "," +
                "pageCount:" + pageCount + "," +
                "pageCountLive:" + pageCountLive + "," +
                "start:" + start + "," +
                "time:" + time + "," +
                "version:" + version;
    }

    @Override
    public String toString() {
        return asString();
    }

}

