/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
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
public abstract class Chunk<C extends Chunk<C>> {

    /**
     * The maximum chunk id.
     */
    public static final int MAX_ID = (1 << 26) - 1;

    /**
     * The maximum length of a chunk header, in bytes.
     * chunk:ffffffff,len:ffffffff,pages:ffffffff,pinCount:ffffffff,max:ffffffffffffffff,map:ffffffff,
     * root:ffffffffffffffff,time:ffffffffffffffff,version:ffffffffffffffff,next:ffffffffffffffff,toc:ffffffff
     */
    static final int MAX_HEADER_LENGTH = 1024;  // 199 really

    /**
     * The length of the chunk footer. The longest footer is:
     * chunk:ffffffff,len:ffffffff,version:ffffffffffffffff,fletcher:ffffffff
     */
    static final int FOOTER_LENGTH = 128; // it's really 70 now

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
     * Byte offset (from the beginning of the chunk) for the table of content (ToC).
     * Table of content is holding a value of type "long" for each page in the chunk.
     * This value consists of map id, page offset, page length and page type.
     * Format is the same as page's position id, but with map id replacing chunk id.
     *
     * @see DataUtils#composeTocElement(int, int, int, int) for field format details
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
     * When this chunk was created, in milliseconds since the store was created.
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

    /**
     * ByteBuffer holding this Chunk's serialized content before it gets saved to file store.
     * This allows to release pages of this Chunk earlier, allowing them to be garbage collected.
     */
    public volatile ByteBuffer buffer;

    Chunk(String s) {
        this(DataUtils.parseMap(s), true);
    }

    Chunk(Map<String, String> map, boolean full) {
        this(DataUtils.readHexInt(map, ATTR_CHUNK, -1));
        block = DataUtils.readHexLong(map, ATTR_BLOCK, 0);
        len = DataUtils.readHexInt(map, ATTR_LEN, 0);
        version = DataUtils.readHexLong(map, ATTR_VERSION, id);
        if (full) {
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
                assert pageCountLive == pageCount;
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
        if (id < 0 || id > MAX_ID) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_FILE_CORRUPT, "Invalid chunk id {0}", id);
        }
    }

    protected abstract ByteBuffer readFully(FileStore<C> fileStore, long filePos, int length);

    /**
     * Read the header from the byte buffer.
     *
     * @param buff the source buffer
     * @return the chunk
     * @throws MVStoreException if {@code buff} does not contain a chunk header
     */
    static String readChunkHeader(ByteBuffer buff) {
        int pos = buff.position();
        byte[] data = new byte[Math.min(buff.remaining(), MAX_HEADER_LENGTH)];
        buff.get(data);
        for (int i = 0; i < data.length; i++) {
            if (data[i] == '\n') {
                // set the position to the start of the first page
                buff.position(pos + i + 1);
                String s = new String(data, 0, i, StandardCharsets.ISO_8859_1).trim();
                return s;
            }
        }
        throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT, "Not a valid chunk header");
    }

    /**
     * Write the chunk header.
     *
     * @return estimated size of the header
     */
    int estimateHeaderSize() {
        byte[] headerBytes = getHeaderBytes();
        int headerLength = headerBytes.length;
        // Initial chunk will look like (length-wise) something in between those two lines:
        // chunk:0,len:0,pages:0,max:0,map:0,root:0,time:0,version:0                                      // 57
        // chunk:ffffffff,len:0,pages:0,max:0,map:0,root:0,time:ffffffffffffffff,version:ffffffffffffffff // 94
        assert 57 <= headerLength && headerLength <= 94 : headerLength + " " + getHeader();
        // When header is fully formed, it will grow and here are fields,
        // which do not exist in initial header or may grow from their initial values:
        // len:0[fffffff],pages:0[fffffff][,pinCount:ffffffff],max:0[fffffffffffffff],map:0[fffffff],
        // root:0[fffffffffffffff,next:ffffffffffffffff,toc:fffffffff]                       // 104 extra chars
        return headerLength + 104 + 1; // extra one for the terminator
    }

    /**
     * Write the chunk header.
     *
     * @param buff the target buffer
     * @param maxLength length of the area reserved for the header
     */
    void writeChunkHeader(WriteBuffer buff, int maxLength) {
        int terminatorPosition = buff.position() + maxLength - 1;
        byte[] headerBytes = getHeaderBytes();
        buff.put(headerBytes);
        while (buff.position() < terminatorPosition) {
            buff.put((byte) ' ');
        }
        if (maxLength != 0 && buff.position() > terminatorPosition) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_INTERNAL,
                    "Chunk metadata too long {0} {1} {2}", terminatorPosition, buff.position(),
                    getHeader());
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

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object o) {
        return o instanceof Chunk && ((Chunk<C>) o).id == id;
    }

    /**
     * Get the chunk metadata as a string to be stored in a layout map.
     *
     * @return the string
     */
    public final String asString() {
        StringBuilder buff = new StringBuilder(240);
        dump(buff);
        return buff.toString();
    }

    protected void dump(StringBuilder buff) {
        DataUtils.appendMap(buff, ATTR_CHUNK, id);
        DataUtils.appendMap(buff, ATTR_BLOCK, block);
        DataUtils.appendMap(buff, ATTR_LEN, len);
        DataUtils.appendMap(buff, ATTR_PAGES, pageCount);
        if (pageCount != pageCountLive) {
            DataUtils.appendMap(buff, ATTR_LIVE_PAGES, pageCountLive);
        }
        DataUtils.appendMap(buff, ATTR_MAX, maxLen);
        if (maxLen != maxLenLive) {
            DataUtils.appendMap(buff, ATTR_LIVE_MAX, maxLenLive);
        }
        DataUtils.appendMap(buff, ATTR_MAP, mapId);
        if (next != 0) {
            DataUtils.appendMap(buff, ATTR_NEXT, next);
        }
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
        if (occupancy != null && !occupancy.isEmpty()) {
            DataUtils.appendMap(buff, ATTR_OCCUPANCY,
                    StringUtils.convertBytesToHex(occupancy.toByteArray()));
        }
    }

    public String getHeader() {
        return new String(getHeaderBytes(), StandardCharsets.ISO_8859_1);
    }

    private byte[] getHeaderBytes() {
        StringBuilder buff = new StringBuilder(240);
        DataUtils.appendMap(buff, ATTR_CHUNK, id);
        DataUtils.appendMap(buff, ATTR_LEN, len);
        DataUtils.appendMap(buff, ATTR_PAGES, pageCount);
        if (pinCount > 0) {
            DataUtils.appendMap(buff, ATTR_PIN_COUNT, pinCount);
        }
        DataUtils.appendMap(buff, ATTR_MAX, maxLen);
        DataUtils.appendMap(buff, ATTR_MAP, mapId);
        DataUtils.appendMap(buff, ATTR_ROOT, layoutRootPos);
        DataUtils.appendMap(buff, ATTR_TIME, time);
        DataUtils.appendMap(buff, ATTR_VERSION, version);
        if (next != 0) {
            DataUtils.appendMap(buff, ATTR_NEXT, next);
        }
        if (tocPos > 0) {
            DataUtils.appendMap(buff, ATTR_TOC, tocPos);
        }
        return buff.toString().getBytes(StandardCharsets.ISO_8859_1);
    }

    byte[] getFooterBytes() {
        StringBuilder buff = new StringBuilder(FOOTER_LENGTH);
        DataUtils.appendMap(buff, ATTR_CHUNK, id);
        DataUtils.appendMap(buff, ATTR_LEN, len);
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

    boolean isAllocated() {
        return block != 0;
    }

    boolean isSaved() {
        return isAllocated() && buffer == null;
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
    ByteBuffer readBufferForPage(FileStore<C> fileStore, int offset, long pos) {
        assert isSaved() : this;
        while (true) {
            long originalBlock = block;
            try {
                long filePos = originalBlock * FileStore.BLOCK_SIZE;
                long maxPos = filePos + (long) len * FileStore.BLOCK_SIZE;
                filePos += offset;
                if (filePos < 0) {
                    throw DataUtils.newMVStoreException(
                            DataUtils.ERROR_FILE_CORRUPT,
                            "Negative position {0}; p={1}, c={2}", filePos, pos, toString());
                }

                int length = DataUtils.getPageMaxLength(pos);
                if (length == DataUtils.PAGE_LARGE) {
                    // read the first bytes to figure out actual length
                    length = readFully(fileStore, filePos, 128).getInt();
                    // pageNo is deliberately not included into length to preserve compatibility
                    // TODO: remove this adjustment when page on disk format is re-organized
                    length += 4;
                }
                length = (int) Math.min(maxPos - filePos, length);
                if (length < 0) {
                    throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT,
                            "Illegal page length {0} reading at {1}; max pos {2} ", length, filePos, maxPos);
                }

                ByteBuffer buff = buffer;
                if (buff == null) {
                    buff = readFully(fileStore, filePos, length);
                } else {
                    buff = buff.duplicate();
                    buff.position(offset);
                    buff = buff.slice();
                    buff.limit(length);
                }

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

    long[] readToC(FileStore<C> fileStore) {
        assert buffer != null || isAllocated() : this;
        assert tocPos > 0;
        long[] toc = new long[pageCount];
        while (true) {
            long originalBlock = block;
            try {
                ByteBuffer buff = buffer;
                if (buff == null) {
                    int length = pageCount * 8;
                    long filePos = originalBlock * FileStore.BLOCK_SIZE + tocPos;
                    buff = readFully(fileStore, filePos, length);
                } else {
                    buff = buff.duplicate();
                    buff.position(tocPos);
                    buff = buff.slice();
                }
                buff.asLongBuffer().get(toc);
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
        assert buffer != null || isAllocated() : this;
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
        return asString() + (buffer == null ? "" : ", buf");
    }


    public static final class PositionComparator<C extends Chunk<C>> implements Comparator<C>
    {
        public static final Comparator<? extends Chunk<?>> INSTANCE = new PositionComparator<>();

        @SuppressWarnings("unchecked")
        public static <C extends Chunk<C>> Comparator<C> instance() {
            return (Comparator<C>)INSTANCE;
        }

        private PositionComparator() {}

        @Override
        public int compare(C one, C two) {
            return Long.compare(one.block, two.block);
        }
    }
}

