/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages serialization of pages into a chunk's write buffer.
 * <p>
 * Tracks per-chunk state: the target {@link WriteBuffer}, the table of
 * contents (ToC), and sequential page numbering. Position encoding
 * and checksum patching are self-contained; interactions with
 * {@link FileStore} (caching, accounting, statistics) are delegated
 * to a {@link Callback} supplied at construction time.
 * <p>
 * This class is not thread-safe. For parallel serialization of
 * independent map trees, create one instance per worker and merge
 * the results afterwards.
 */
public final class PageSerializationManager {

    /**
     * Callback interface for side-effects that belong to the
     * enclosing {@link FileStore} rather than to the serialization
     * context itself.
     */
    public interface Callback {

        /**
         * Put a page into the read cache.
         *
         * @param page the page to cache
         */
        void cachePage(Page<?,?> page);

        /**
         * Record that a page has been removed.
         *
         * @param pos     the page position
         * @param version the version at which the removal takes effect
         * @param pinned  whether the page was pinned
         * @param pageNo  the sequential page number within its chunk
         */
        void accountForRemovedPage(long pos, long version, boolean pinned, int pageNo);

        /**
         * Record that a page has been written into the chunk.
         *
         * @param diskSpaceUsed bytes consumed on disk
         * @param isPinned      whether the page belongs to a single-writer map
         */
        void accountForWrittenPage(int diskSpaceUsed, boolean isPinned);

        /**
         * Cache a chunk's table of contents.
         *
         * @param chunkId  the chunk id
         * @param tocArray the ToC array
         */
        void cacheToC(int chunkId, long[] tocArray);

        /**
         * Increment page-type statistics.
         *
         * @param isLeaf true for leaf pages, false for internal nodes
         */
        void countNewPage(boolean isLeaf);
    }

    /**
     * Record of a page that has been serialized into the buffer.
     * <p>
     * Captures the buffer-level layout information and the {@link Page}
     * reference. This data is sufficient for {@code rebasePositions()}
     * (Step 5b) to adjust all positions when a local buffer is merged
     * into a global buffer at a different base offset.
     */
    public static final class SerializedPageRecord {
        /** The serialized page. Set by {@link #onPageSerialized}. */
        Page<?,?> page;
        /** The owning map's id. */
        final int mapId;
        /** Byte offset of the page image within the buffer. */
        final int bufferOffset;
        /** Serialized page length in bytes. */
        final int pageLength;
        /** Page type ({@code PAGE_TYPE_LEAF} or {@code PAGE_TYPE_NODE}). */
        final int type;
        /** Composed page position (chunkId + tocElement). */
        final long pagePos;
        /** The ToC element encoding (mapId, offset, pageLength, type). */
        final long tocElement;

        SerializedPageRecord(int mapId, int bufferOffset, int pageLength,
                             int type, long pagePos, long tocElement) {
            this.mapId = mapId;
            this.bufferOffset = bufferOffset;
            this.pageLength = pageLength;
            this.type = type;
            this.pagePos = pagePos;
            this.tocElement = tocElement;
        }
    }


    private final int chunkId;
    private final long chunkVersion;
    private final WriteBuffer buff;
    private final List<Long> toc = new ArrayList<>();
    private final Callback callback;
    private final List<SerializedPageRecord> serializedPages = new ArrayList<>();

    /**
     * Create a new serialization manager.
     *
     * @param chunkId      id of the chunk being built
     * @param chunkVersion version stored in the chunk
     * @param buff         target write buffer (positioned past the chunk header)
     * @param callback     receiver for cache / accounting side-effects
     */
    public PageSerializationManager(int chunkId, long chunkVersion,
                                    WriteBuffer buff, Callback callback) {
        this.chunkId = chunkId;
        this.chunkVersion = chunkVersion;
        this.buff = buff;
        this.callback = callback;
    }

    /**
     * @return the target write buffer
     */
    public WriteBuffer getBuffer() {
        return buff;
    }

    /**
     * @return the chunk id
     */
    public int getChunkId() {
        return chunkId;
    }

    /**
     * @return the next 0-based page number (equal to the number of pages already recorded)
     */
    public int getPageNo() {
        return toc.size();
    }

    /**
     * Compute and record the position for a page that has just been
     * serialized into the buffer. This also patches the page-length
     * and check fields at the start of the page image.
     *
     * @param mapId      the owning map's id
     * @param offset     byte offset of the page within the buffer
     * @param pageLength serialized page length in bytes
     * @param type       page type (leaf / node)
     * @return the composed page position suitable for storage in
     *         {@link Page#pos} and in parent-node child references
     */
    public long getPagePosition(int mapId, int offset, int pageLength, int type) {
        long tocElement = DataUtils.composeTocElement(mapId, offset, pageLength, type);
        toc.add(tocElement);
        long pagePos = DataUtils.composePagePos(chunkId, tocElement);
        int check = DataUtils.getCheckValue(chunkId)
                    ^ DataUtils.getCheckValue(offset)
                    ^ DataUtils.getCheckValue(pageLength);
        buff.putInt(offset, pageLength)
            .putShort(offset + 4, (short) check);
        serializedPages.add(new SerializedPageRecord(
                mapId, offset, pageLength, type, pagePos, tocElement));
        return pagePos;
    }

    /**
     * Notify that a page has been fully serialized.
     * Delegates caching and bookkeeping to the {@link Callback}.
     *
     * @param page          the serialized page
     * @param isDeleted     true if the page was concurrently removed
     * @param diskSpaceUsed decoded on-disk size
     * @param isPinned      whether the page belongs to a single-writer map
     */
    public void onPageSerialized(Page<?,?> page, boolean isDeleted,
                                 int diskSpaceUsed, boolean isPinned) {
        // Associate the Page reference with the record created in getPagePosition().
        // getPagePosition() is always called immediately before onPageSerialized()
        // for the same page, so the last record is the correct one.
        serializedPages.get(serializedPages.size() - 1).page = page;

        callback.cachePage(page);
        if (!page.isLeaf()) {
            // cache again — keeps nodes in cache longer
            callback.cachePage(page);
        }
        callback.accountForWrittenPage(diskSpaceUsed, isPinned);
        if (isDeleted) {
            callback.accountForRemovedPage(
                    page.getPos(), chunkVersion + 1, isPinned, page.pageNo);
        }
    }

    /**
     * Adjust all page positions, ToC entries, buffer checksums, and
     * NonLeaf child pointers by the given base offset. Called when a
     * local serialization buffer is merged into a global chunk buffer
     * at a position other than offset 0.
     * <p>
     * After this call:
     * <ul>
     *   <li>Every serialized page's {@code Page.pos} encodes the
     *       rebased offset</li>
     *   <li>The {@link #toc} list entries encode the rebased offset</li>
     *   <li>The check values in the buffer reflect the rebased offset</li>
     *   <li>NonLeaf child pointer longs in the buffer that reference
     *       pages within this buffer are updated to their rebased
     *       positions</li>
     * </ul>
     * <p>
     * Note: {@link SerializedPageRecord} fields are <em>not</em>
     * updated — they retain the original local values.
     *
     * @param baseOffset the byte offset to add to every page position;
     *                   passing 0 is a no-op
     */
    public void rebasePositions(int baseOffset) {
        if (baseOffset == 0) {
            return;
        }
        ByteBuffer buf = buff.getBuffer();
        int pageCount = serializedPages.size();

        // Map old pagePos → new pagePos for child-pointer fixup
        Map<Long, Long> positionMap = new HashMap<>(pageCount * 2);

        // ---- Pass 1: Rebase page positions, checksums, and ToC ----
        for (int i = 0; i < pageCount; i++) {
            SerializedPageRecord rec = serializedPages.get(i);
            int newOffset = rec.bufferOffset + baseOffset;

            // Recompose ToC element and page position with rebased offset
            long newTocElement = DataUtils.composeTocElement(
                    rec.mapId, newOffset, rec.pageLength, rec.type);
            long newPagePos = DataUtils.composePagePos(chunkId, newTocElement);

            // Update ToC list (used later by serializeToC)
            toc.set(i, newTocElement);

            // Patch check value in buffer — offset is part of the check
            int newCheck = DataUtils.getCheckValue(chunkId)
                           ^ DataUtils.getCheckValue(newOffset)
                           ^ DataUtils.getCheckValue(rec.pageLength);
            buf.putShort(rec.bufferOffset + 4, (short) newCheck);

            // CAS-update Page.pos from local to rebased position
            rec.page.rebasePos(rec.pagePos, newPagePos);

            positionMap.put(rec.pagePos, newPagePos);
        }

        // ---- Pass 2: Rebase child pointers in NonLeaf pages ----
        for (SerializedPageRecord rec : serializedPages) {
            if (rec.type != DataUtils.PAGE_TYPE_LEAF) {
                rebaseChildPointers(buf, rec, positionMap);
            }
        }
    }

    /**
     * Rewrite child-pointer longs in a NonLeaf page's buffer region
     * that reference pages within this buffer (i.e. pages whose old
     * position appears in the {@code positionMap}).
     * <p>
     * Child pointers referencing pages from previous chunks are left
     * untouched — they already have correct absolute positions.
     */
    private void rebaseChildPointers(ByteBuffer buf,
                                     SerializedPageRecord rec,
                                     Map<Long, Long> positionMap) {
        // Parse the page header to locate the child-pointer region.
        // Layout: [int pageLength][short check][varInt pageNo]
        //         [varInt mapId][varInt keyCount][byte type]
        //         [long childPos * (keyCount+1)] ...
        ByteBuffer dup = buf.duplicate();
        dup.order(buf.order());
        // Skip pageLength (4) + check (2) = 6 bytes from page start
        dup.position(rec.bufferOffset + 6);
        DataUtils.readVarInt(dup);              // pageNo — skip
        DataUtils.readVarInt(dup);              // mapId — skip
        int keyCount = DataUtils.readVarInt(dup); // keyCount
        dup.get();                              // type byte — skip
        int childrenPos = dup.position();
        int childCount = keyCount + 1;

        for (int i = 0; i < childCount; i++) {
            int ptrOffset = childrenPos + i * 8;
            long childPagePos = buf.getLong(ptrOffset);
            Long rebased = positionMap.get(childPagePos);
            if (rebased != null) {
                buf.putLong(ptrOffset, rebased);
            }
        }
    }

    /**
     * Write the table of contents to the buffer and update caches.
     * Must be called after all pages have been serialized.
     */
    public void serializeToC() {
        long[] tocArray = new long[toc.size()];
        int index = 0;
        for (long tocElement : toc) {
            tocArray[index++] = tocElement;
            buff.putLong(tocElement);
            callback.countNewPage(DataUtils.isLeafPosition(tocElement));
        }
        callback.cacheToC(chunkId, tocArray);
    }

    /**
     * @return an unmodifiable snapshot of the ToC entries accumulated so far
     */
    public List<Long> getToc() {
        return List.copyOf(toc);
    }

    /**
     * @return the list of serialized page records, in serialization order
     */
    public List<SerializedPageRecord> getSerializedPages() {
        return serializedPages;
    }
}