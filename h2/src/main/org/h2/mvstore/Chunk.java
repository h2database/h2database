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
     * The start block number within the file.
     */
    public long block;

    /**
     * The length in number of blocks.
     */
    public int blocks;

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
    void writeHeader(WriteBuffer buff, int minLength) {
        long pos = buff.position();
        buff.put((byte) '{');
        buff.put(asString().getBytes(DataUtils.UTF8));
        buff.put((byte) '}');
        while (buff.position() - pos < minLength - 1) {
            buff.put((byte) ' ');
        }
        buff.put((byte) '\n');
    }
    
    static String getMetaKey(int chunkId) {
        return "chunk." + Integer.toHexString(chunkId);
    }

    /**
     * Build a block from the given string.
     *
     * @param s the string
     * @return the block
     */
    public static Chunk fromString(String s) {
        HashMap<String, String> map = DataUtils.parseMap(s);
        int id = Integer.parseInt(map.get("chunk"), 16);
        Chunk c = new Chunk(id);
        c.block = Long.parseLong(map.get("block"), 16);
        c.blocks = Integer.parseInt(map.get("blocks"), 16);
        c.pageCount = Integer.parseInt(map.get("pages"), 16);
        c.pageCountLive = DataUtils.parseHexInt(map.get("livePages"), c.pageCount);
        c.maxLength = Long.parseLong(map.get("max"), 16);
        c.maxLengthLive = DataUtils.parseHexLong(map.get("liveMax"), c.maxLength);
        c.metaRootPos = Long.parseLong(map.get("root"), 16);
        c.time = Long.parseLong(map.get("time"), 16);
        c.version = Long.parseLong(map.get("version"), 16);
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
        StringBuilder buff = new StringBuilder();
        buff.append("chunk:").append(Integer.toHexString(id)).
            append(",block:").append(Long.toHexString(block)).
            append(",blocks:").append(Integer.toHexString(blocks));
        if (maxLength != maxLengthLive) {
            buff.append(",liveMax:").append(Long.toHexString(maxLengthLive));
        }
        if (pageCount != pageCountLive) {
            buff.append(",livePages:").append(Integer.toHexString(pageCountLive));
        }
        buff.append(",max:").append(Long.toHexString(maxLength)).
            append(",pages:").append(Integer.toHexString(pageCount)).
            append(",root:").append(Long.toHexString(metaRootPos)).
            append(",time:").append(Long.toHexString(time)).
            append(",version:").append(Long.toHexString(version));
        return buff.toString();
    }

    @Override
    public String toString() {
        return asString();
    }

}

