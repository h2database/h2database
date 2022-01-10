/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Map;

import org.h2.util.StringUtils;

/**
 * A chunk of data, containing one or multiple pages.
 * <p>
 * Minimum chunk size is usually 4096 bytes, and it grows in those fixed increments (blocks).
 * Chunk's length and it's position in the underlying filestore
 * are multiples of that increment (block size),
 * therefore they both are measured in blocks, instead of bytes.
 * There are at most 67 million (2^26) chunks,
 * and each chunk is at most 2 GB large.
 */
public final class Chunk {

    /**
     * The maximum chunk id.
     */
    public static final int MAX_ID = (1 << 26) - 1;

    /**
     * The maximum length of a chunk header, in bytes.
     */
    static final int MAX_HEADER_LENGTH = 1024;

    /**
     * The length of the chunk footer. The longest footer is:
     * chunk:ffffffff,block:ffffffffffffffff,
     * version:ffffffffffffffff,fletcher:ffffffff
     */
    static final int FOOTER_LENGTH = 128;

    private static final String ATTR_CHUNK = "chunk";
    private static final String ATTR_BLOCK = "block";
    private static final String ATTR_LEN = "len";
    private static final String ATTR_MAP = "map";
    private static final String ATTR_MAX = "max";
    private static final String ATTR_NEXT = "next";
    private static final String ATTR_PAGES = "pages";
    private static final String ATTR_ROOT = "root";
    private static final String ATTR_TIME = "time";
    private static final String ATTR_VERSION = "version";
    private static final String ATTR_LIVE_MAX = "liveMax";
    private static final String ATTR_LIVE_PAGES = "livePages";
    private static final String ATTR_UNUSED = "unused";
    private static final String ATTR_UNUSED_AT_VERSION = "unusedAtVersion";
    private static final String ATTR_PIN_COUNT = "pinCount";
    private static final String ATTR_TOC = "toc";
    private static final String ATTR_OCCUPANCY = "occupancy";
    private static final String ATTR_FLETCHER = "fletcher";

    /**
     * The chunk id.
     */
    public final int id;

    /**
     * The start block number within the file.
     */
    public volatile long block;

    /**
     * The length in number of blocks.
     */
    public int len;

    /**
     * The total number of pages in this chunk.
     */
    int pageCount;

    /**
     * The number of pages that are still alive in the latest version of the store.
     */
    int pageCountLive;

    /**
     * Offset (from the beginning of the chunk) for the table of content.
     * Table of content is holding a value of type "long" for each page in the chunk.
     * This value consists of map id, page offset, page length and page type.
     * Format is the same as page's position id, but with map id replacing chunk id.
     *
     * @see DataUtils#getTocElement(int, int, int, int) for field format details
     */
    int tocPos;

    /**
     * Collection of "deleted" flags for all pages in the chunk.
     */
    BitSet occupancy;

    /**
     * The sum of the max length of all pages.
     */
    public long maxLen;

    /**
     * The sum of the length of all pages that are still alive.
     */
    public long maxLenLive;

    /**
     * The garbage collection priority. Priority 0 means it needs to be
     * collected, a high value means low priority.
     */
    int collectPriority;

    /**
     * The position of the root of layout map.
     */
    long layoutRootPos;

    /**
     * The version stored in this chunk.
     */
    public long version;

    /**
     * When this chunk was created, in milliseconds after the store was created.
     */
    public long time;

    /**
     * When this chunk was no longer needed, in milliseconds after the store was
     * created. After this, the chunk is kept alive a bit longer (in case it is
     * referenced in older versions).
     */
    public long unused;

    /**
     * Version of the store at which chunk become unused and therefore can be
     * considered "dead" and collected after this version is no longer in use.
     */
    long unusedAtVersion;

    /**
     * The last used map id.
     */
    public int mapId;

    /**
     * The predicted position of the next chunk.
     */
    public long next;

    /**
     * Number of live pinned pages.
     */
    private int pinCount;


    private Chunk(String s) {
        this(DataUtils.parseMap(s), true);
    }

    Chunk(Map<String, String> map) {
        this(map, false);
    }

    private Chunk(Map<String, String> map, boolean full) {
        this(DataUtils.readHexInt(map, ATTR_CHUNK, 0));
        block = DataUtils.readHexLong(map, ATTR_BLOCK, 0);
        version = DataUtils.readHexLong(map, ATTR_VERSION, id);
        if (full) {
            len = DataUtils.readHexInt(map, ATTR_LEN, 0);
            pageCount = DataUtils.readHexInt(map, ATTR_PAGES, 0);
            pageCountLive = DataUtils.readHexInt(map, ATTR_LIVE_PAGES, pageCount);
            mapId = DataUtils.readHexInt(map, ATTR_MAP, 0);
            maxLen = DataUtils.readHexLong(map, ATTR_MAX, 0);
            maxLenLive = DataUtils.readHexLong(map, ATTR_LIVE_MAX, maxLen);
            layoutRootPos = DataUtils.readHexLong(map, ATTR_ROOT, 0);
            time = DataUtils.readHexLong(map, ATTR_TIME, 0);
            unused = DataUtils.readHexLong(map, ATTR_UNUSED, 0);
            unusedAtVersion = DataUtils.readHexLong(map, ATTR_UNUSED_AT_VERSION, 0);
            next = DataUtils.readHexLong(map, ATTR_NEXT, 0);
            pinCount = DataUtils.readHexInt(map, ATTR_PIN_COUNT, 0);
            tocPos = DataUtils.readHexInt(map, ATTR_TOC, 0);
            byte[] bytes = DataUtils.parseHexBytes(map, ATTR_OCCUPANCY);
            if (bytes == null) {
                occupancy = new BitSet();
            } else {
                occupancy = BitSet.valueOf(bytes);
                if (pageCount - pageCountLive != occupancy.cardinality()) {
                    throw DataUtils.newMVStoreException(
                            DataUtils.ERROR_FILE_CORRUPT, "Inconsistent occupancy info {0} - {1} != {2} {3}",
                            pageCount, pageCountLive, occupancy.cardinality(), this);
                }
            }
        }
    }

    Chunk(int id) {
        this.id = id;
        if (id <=  0) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_FILE_CORRUPT, "Invalid chunk id {0}", id);
        }
    }

    /**
     * Read the header from the byte buffer.
     *
     * @param buff the source buffer
     * @param start the start of the chunk in the file
     * @return the chunk
     */
    static Chunk readChunkHeader(ByteBuffer buff, long start) {
        int pos = buff.position();
        byte[] data = new byte[Math.min(buff.remaining(), MAX_HEADER_LENGTH)];
        buff.get(data);
        try {
            for (int i = 0; i < data.length; i++) {
                if (data[i] == '\n') {
                    // set the position to the start of the first page
                    buff.position(pos + i + 1);
                    String s = new String(data, 0, i, StandardCharsets.ISO_8859_1).trim();
                    return fromString(s);
                }
            }
        } catch (Exception e) {
            // there could be various reasons
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupt reading chunk at position {0}", start, e);
        }
        throw DataUtils.newMVStoreException(
                DataUtils.ERROR_FILE_CORRUPT,
                "File corrupt reading chunk at position {0}", start);
    }

    /**
     * Write the chunk header.
     *
     * @param buff the target buffer
     * @param minLength the minimum length
     */
    void writeChunkHeader(WriteBuffer buff, int minLength) {
        long delimiterPosition = buff.position() + minLength - 1;
        buff.put(asString().getBytes(StandardCharsets.ISO_8859_1));
        while (buff.position() < delimiterPosition) {
            buff.put((byte) ' ');
        }
        if (minLength != 0 && buff.position() > delimiterPosition) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_INTERNAL,
                    "Chunk metadata too long");
        }
        buff.put((byte) '\n');
    }

    /**
     * Get the metadata key for the given chunk id.
     *
     * @param chunkId the chunk id
     * @return the metadata key
     */
    static String getMetaKey(int chunkId) {
        return ATTR_CHUNK + "." + Integer.toHexString(chunkId);
    }

    /**
     * Build a block from the given string.
     *
     * @param s the string
     * @return the block
     */
    public static Chunk fromString(String s) {
        return new Chunk(s);
    }

    /**
     * Calculate the fill rate in %. 0 means empty, 100 means full.
     *
     * @return the fill rate
     */
    int getFillRate() {
        assert maxLenLive <= maxLen : maxLenLive + " > " + maxLen;
        if (maxLenLive <= 0) {
            return 0;
        } else if (maxLenLive == maxLen) {
            return 100;
        }
        return 1 + (int) (98 * maxLenLive / maxLen);
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
        StringBuilder buff = new StringBuilder(240);
        DataUtils.appendMap(buff, ATTR_CHUNK, id);
        DataUtils.appendMap(buff, ATTR_BLOCK, block);
        DataUtils.appendMap(buff, ATTR_LEN, len);
        if (maxLen != maxLenLive) {
            DataUtils.appendMap(buff, ATTR_LIVE_MAX, maxLenLive);
        }
        if (pageCount != pageCountLive) {
            DataUtils.appendMap(buff, ATTR_LIVE_PAGES, pageCountLive);
        }
        DataUtils.appendMap(buff, ATTR_MAP, mapId);
        DataUtils.appendMap(buff, ATTR_MAX, maxLen);
        if (next != 0) {
            DataUtils.appendMap(buff, ATTR_NEXT, next);
        }
        DataUtils.appendMap(buff, ATTR_PAGES, pageCount);
        DataUtils.appendMap(buff, ATTR_ROOT, layoutRootPos);
        DataUtils.appendMap(buff, ATTR_TIME, time);
        if (unused != 0) {
            DataUtils.appendMap(buff, ATTR_UNUSED, unused);
        }
        if (unusedAtVersion != 0) {
            DataUtils.appendMap(buff, ATTR_UNUSED_AT_VERSION, unusedAtVersion);
        }
        DataUtils.appendMap(buff, ATTR_VERSION, version);
        if (pinCount > 0) {
            DataUtils.appendMap(buff, ATTR_PIN_COUNT, pinCount);
        }
        if (tocPos > 0) {
            DataUtils.appendMap(buff, ATTR_TOC, tocPos);
        }
        if (!occupancy.isEmpty()) {
            DataUtils.appendMap(buff, ATTR_OCCUPANCY,
                    StringUtils.convertBytesToHex(occupancy.toByteArray()));
        }
        return buff.toString();
    }

    byte[] getFooterBytes() {
        StringBuilder buff = new StringBuilder(FOOTER_LENGTH);
        DataUtils.appendMap(buff, ATTR_CHUNK, id);
        DataUtils.appendMap(buff, ATTR_BLOCK, block);
        DataUtils.appendMap(buff, ATTR_VERSION, version);
        byte[] bytes = buff.toString().getBytes(StandardCharsets.ISO_8859_1);
        int checksum = DataUtils.getFletcher32(bytes, 0, bytes.length);
        DataUtils.appendMap(buff, ATTR_FLETCHER, checksum);
        while (buff.length() < FOOTER_LENGTH - 1) {
            buff.append(' ');
        }
        buff.append('\n');
        return buff.toString().getBytes(StandardCharsets.ISO_8859_1);
    }

    boolean isSaved() {
        return block != Long.MAX_VALUE;
    }

    boolean isLive() {
        return pageCountLive > 0;
    }

    boolean isRewritable() {
        return isSaved()
                && isLive()
                && pageCountLive < pageCount    // not fully occupied
                && isEvacuatable();
    }

    private boolean isEvacuatable() {
        return pinCount == 0;
    }

    /**
     * Read a page of data into a ByteBuffer.
     *
     * @param fileStore to use
     * @param offset of the page data
     * @param pos page pos
     * @return ByteBuffer containing page data.
     */
    ByteBuffer readBufferForPage(FileStore fileStore, int offset, long pos) {
        assert isSaved() : this;
        while (true) {
            long originalBlock = block;
            try {
                long filePos = originalBlock * MVStore.BLOCK_SIZE;
                long maxPos = filePos + (long) len * MVStore.BLOCK_SIZE;
                filePos += offset;
                if (filePos < 0) {
                    throw DataUtils.newMVStoreException(
                            DataUtils.ERROR_FILE_CORRUPT,
                            "Negative position {0}; p={1}, c={2}", filePos, pos, toString());
                }

                int length = DataUtils.getPageMaxLength(pos);
                if (length == DataUtils.PAGE_LARGE) {
                    // read the first bytes to figure out actual length
                    length = fileStore.readFully(filePos, 128).getInt();
                    // pageNo is deliberately not included into length to preserve compatibility
                    // TODO: remove this adjustment when page on disk format is re-organized
                    length += 4;
                }
                length = (int) Math.min(maxPos - filePos, length);
                if (length < 0) {
                    throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT,
                            "Illegal page length {0} reading at {1}; max pos {2} ", length, filePos, maxPos);
                }

                ByteBuffer buff = fileStore.readFully(filePos, length);

                if (originalBlock == block) {
                    return buff;
                }
            } catch (MVStoreException ex) {
                if (originalBlock == block) {
                    throw ex;
                }
            }
        }
    }

    long[] readToC(FileStore fileStore) {
        assert isSaved() : this;
        assert tocPos > 0;
        while (true) {
            long originalBlock = block;
            try {
                long filePos = originalBlock * MVStore.BLOCK_SIZE + tocPos;
                int length = pageCount * 8;
                long[] toc = new long[pageCount];
                fileStore.readFully(filePos, length).asLongBuffer().get(toc);
                if (originalBlock == block) {
                    return toc;
                }
            } catch (MVStoreException ex) {
                if (originalBlock == block) {
                    throw ex;
                }
            }
        }
    }

    /**
     * Modifies internal state to reflect the fact that one more page is stored
     * within this chunk.
     *  @param pageLengthOnDisk
     *            size of the page
     * @param singleWriter
     *            indicates whether page belongs to append mode capable map
     *            (single writer map). Such pages are "pinned" to the chunk,
     *            they can't be evacuated (moved to a different chunk) while
     */
    void accountForWrittenPage(int pageLengthOnDisk, boolean singleWriter) {
        maxLen += pageLengthOnDisk;
        pageCount++;
        maxLenLive += pageLengthOnDisk;
        pageCountLive++;
        if (singleWriter) {
            pinCount++;
        }
        assert pageCount - pageCountLive == occupancy.cardinality()
                : pageCount + " - " + pageCountLive + " <> " + occupancy.cardinality() + " : " + occupancy;
    }

    /**
     * Modifies internal state to reflect the fact that one the pages within
     * this chunk was removed from the map.
     *
     * @param pageNo
     *            sequential page number within the chunk
     * @param pageLength
     *            on disk of the removed page
     * @param pinned
     *            whether removed page was pinned
     * @param now
     *            is a moment in time (since creation of the store), when
     *            removal is recorded, and retention period starts
     * @param version
     *            at which page was removed
     * @return true if all of the pages, this chunk contains, were already
     *         removed, and false otherwise
     */
    boolean accountForRemovedPage(int pageNo, int pageLength, boolean pinned, long now, long version) {
        assert isSaved() : this;
        // legacy chunks do not have a table of content,
        // therefore pageNo is not valid, skip
        if (tocPos > 0) {
            assert pageNo >= 0 && pageNo < pageCount : pageNo + " // " +  pageCount;
            assert !occupancy.get(pageNo) : pageNo + " " + this + " " + occupancy;
            assert pageCount - pageCountLive == occupancy.cardinality()
                    : pageCount + " - " + pageCountLive + " <> " + occupancy.cardinality() + " : " + occupancy;
            occupancy.set(pageNo);
        }

        maxLenLive -= pageLength;
        pageCountLive--;
        if (pinned) {
            pinCount--;
        }

        if (unusedAtVersion < version) {
            unusedAtVersion = version;
        }

        assert pinCount >= 0 : this;
        assert pageCountLive >= 0 : this;
        assert pinCount <= pageCountLive : this;
        assert maxLenLive >= 0 : this;
        assert (pageCountLive == 0) == (maxLenLive == 0) : this;

        if (!isLive()) {
            unused = now;
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return asString();
    }


    public static final class PositionComparator implements Comparator<Chunk> {
        public static final Comparator<Chunk> INSTANCE = new PositionComparator();

        private PositionComparator() {}

        @Override
        public int compare(Chunk one, Chunk two) {
            return Long.compare(one.block, two.block);
        }
    }
}

