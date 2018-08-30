/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import static org.h2.engine.Constants.MEMORY_ARRAY;
import static org.h2.engine.Constants.MEMORY_OBJECT;
import static org.h2.engine.Constants.MEMORY_POINTER;
import static org.h2.mvstore.DataUtils.PAGE_TYPE_LEAF;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import org.h2.compress.Compressor;
import org.h2.mvstore.type.DataType;
import org.h2.util.Utils;

/**
 * A page (a node or a leaf).
 * <p>
 * For b-tree nodes, the key at a given index is larger than the largest key of
 * the child at the same index.
 * <p>
 * File format:
 * page length (including length): int
 * check value: short
 * map id: varInt
 * number of keys: varInt
 * type: byte (0: leaf, 1: node; +2: compressed)
 * compressed: bytes saved (varInt)
 * keys
 * leaf: values (one for each key)
 * node: children (1 more than keys)
 */
public abstract class Page implements Cloneable
{
    /**
     * Map this page belongs to
     */
    public final MVMap<?, ?> map;

    /**
     * Position of this page's saved image within a Chunk or 0 if this page has not been saved yet.
     */
    private long pos;

    /**
     * The last result of a find operation is cached.
     */
    private int cachedCompare;

    /**
     * The estimated memory used in persistent case, IN_MEMORY marker value otherwise.
     */
    private int memory;

    /**
     * Amount of used disk space by this page only in persistent case.
     */
    private int diskSpaceUsed;

    /**
     * The keys.
     */
    private Object[] keys;

    /**
     * Whether the page is an in-memory (not stored, or not yet stored) page,
     * and it is removed. This is to keep track of pages that concurrently
     * changed while they are being stored, in which case the live bookkeeping
     * needs to be aware of such cases.
     */
    private volatile boolean removedInMemory;

    /**
     * The estimated number of bytes used per child entry.
     */
    static final int PAGE_MEMORY_CHILD = MEMORY_POINTER + 16; //  16 = two longs

    /**
     * The estimated number of bytes used per base page.
     */
    private static final int PAGE_MEMORY =
            MEMORY_OBJECT +           // this
            2 * MEMORY_POINTER +      // map, keys
            MEMORY_ARRAY +            // Object[] keys
            17;                       // pos, cachedCompare, memory, removedInMemory
    /**
     * The estimated number of bytes used per empty internal page object.
     */
    static final int PAGE_NODE_MEMORY =
            PAGE_MEMORY +             // super
            MEMORY_POINTER +          // children
            MEMORY_ARRAY +            // Object[] children
            8;                        // totalCount

    /**
     * The estimated number of bytes used per empty leaf page.
     */
    static final int PAGE_LEAF_MEMORY =
            PAGE_MEMORY +             // super
            MEMORY_POINTER +          // values
            MEMORY_ARRAY;             // Object[] values

    /**
     * An empty object array.
     */
    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    /**
     * Marker value for memory field, meaning that memory accounting is replaced by key count.
     */
    private static final int IN_MEMORY = Integer.MIN_VALUE;

    private static final PageReference[] SINGLE_EMPTY = { PageReference.EMPTY };


    Page(MVMap<?, ?> map) {
        this.map = map;
    }

    Page(MVMap<?, ?> map, Page source) {
        this(map, source.keys);
        memory = source.memory;
    }

    Page(MVMap<?, ?> map, Object keys[]) {
        this.map = map;
        this.keys = keys;
    }

    /**
     * Create a new, empty page.
     *
     * @param map the map
     * @return the new page
     */
    static Page createEmptyLeaf(MVMap<?, ?> map) {
        Page page = new Leaf(map, EMPTY_OBJECT_ARRAY, EMPTY_OBJECT_ARRAY);
        page.initMemoryAccount(PAGE_LEAF_MEMORY);
        return page;
    }

    public static Page createEmptyNode(MVMap<?, ?> map) {
        Page page = new NonLeaf(map, EMPTY_OBJECT_ARRAY, SINGLE_EMPTY, 0);
        page.initMemoryAccount(PAGE_NODE_MEMORY +
                                MEMORY_POINTER + PAGE_MEMORY_CHILD); // there is always one child
        return page;
    }

    /**
     * Create a new page. The arrays are not cloned.
     *
     * @param map the map
     * @param keys the keys
     * @param values the values
     * @param children the child page positions
     * @param totalCount the total number of keys
     * @param memory the memory used in bytes
     * @return the page
     */
    public static Page create(MVMap<?, ?> map,
            Object[] keys, Object[] values, PageReference[] children,
            long totalCount, int memory) {
        assert keys != null;
        Page p = children == null ? new Leaf(map, keys, values) :
                                    new NonLeaf(map, keys, children, totalCount);
        p.initMemoryAccount(memory);
        return p;
    }

    private void initMemoryAccount(int memoryCount) {
        if(map.store.getFileStore() == null) {
            memory = IN_MEMORY;
        } else if (memoryCount == 0) {
            recalculateMemory();
        } else {
            addMemory(memoryCount);
            assert memoryCount == getMemory();
        }
    }

    /**
     * Get the value for the given key, or null if not found.
     * Search is done in the tree rooted at given page.
     *
     * @param key the key
     * @param p the root page
     * @return the value, or null if not found
     */
    static Object get(Page p, Object key) {
        while (true) {
            int index = p.binarySearch(key);
            if (p.isLeaf()) {
                return index >= 0 ? p.getValue(index) : null;
            } else if (index++ < 0) {
                index = -index;
            }
            p = p.getChildPage(index);
        }
    }

    /**
     * Read a page.
     *
     * @param fileStore the file store
     * @param pos the position
     * @param map the map
     * @param filePos the position in the file
     * @param maxPos the maximum position (the end of the chunk)
     * @return the page
     */
    static Page read(FileStore fileStore, long pos, MVMap<?, ?> map,
            long filePos, long maxPos) {
        ByteBuffer buff;
        int maxLength = DataUtils.getPageMaxLength(pos);
        if (maxLength == DataUtils.PAGE_LARGE) {
            buff = fileStore.readFully(filePos, 128);
            maxLength = buff.getInt();
            // read the first bytes again
        }
        maxLength = (int) Math.min(maxPos - filePos, maxLength);
        int length = maxLength;
        if (length < 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1}; max pos {2} ",
                    length, filePos, maxPos);
        }
        buff = fileStore.readFully(filePos, length);
        boolean leaf = (DataUtils.getPageType(pos) & 1) == PAGE_TYPE_LEAF;
        Page p = leaf ? new Leaf(map) : new NonLeaf(map);
        p.pos = pos;
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, maxLength);
        return p;
    }

    /**
     * Read an inner node page from the buffer, but ignore the keys and
     * values.
     *
     * @param fileStore the file store
     * @param pos the position
     * @param filePos the position in the file
     * @param maxPos the maximum position (the end of the chunk)
     * @param collector to report child pages positions to
     */
    static List<Future<?>> readChildrenPositions(FileStore fileStore, long pos,
                                        long filePos, long maxPos,
                                        final MVStore.ChunkIdsCollector collector,
                                        ExecutorService executorService) {
        ByteBuffer buff;
        int maxLength = DataUtils.getPageMaxLength(pos);
        if (maxLength == DataUtils.PAGE_LARGE) {
            buff = fileStore.readFully(filePos, 128);
            maxLength = buff.getInt();
            // read the first bytes again
        }
        maxLength = (int) Math.min(maxPos - filePos, maxLength);
        int length = maxLength;
        if (length < 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1}; max pos {2} ",
                    length, filePos, maxPos);
        }
        buff = fileStore.readFully(filePos, length);
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        int start = buff.position();
        int pageLength = buff.getInt();
        if (pageLength > maxLength) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length =< {1}, got {2}",
                    chunkId, maxLength, pageLength);
        }
        buff.limit(start + pageLength);
        short check = buff.getShort();
        int m = DataUtils.readVarInt(buff);
        int mapId = collector.getMapId();
        if (m != mapId) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected map id {1}, got {2}",
                    chunkId, mapId, m);
        }
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}",
                    chunkId, checkTest, check);
        }
        int len = DataUtils.readVarInt(buff);
        int type = buff.get();
        if ((type & 1) != DataUtils.PAGE_TYPE_NODE) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Position {0} expected to be a non-leaf", pos);
        }
        final List<Future<?>> futures = new ArrayList<>(len);
        for (int i = 0; i <= len; i++) {
            final long childPagePos = buff.getLong();
            Future<?> f = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    collector.visit(childPagePos);
                }
            });
            futures.add(f);
        }
        return futures;
    }

    /**
     * Get the id of the page's owner map
     * @return id
     */
    public final int getMapId() {
        return map.getId();
    }

    /**
     * Create a copy of this page with potentially different owning map.
     * This is used exclusively during bulk map copying.
     * Child page references for nodes are cleared (re-pointed to an empty page)
     * to be filled-in later to copying procedure. This way it can be saved
     * mid-process without tree integrity violation
     *
     * @param map new map to own resulting page
     * @return the page
     */
    abstract Page copy(MVMap<?, ?> map);

    /**
     * Get the key at the given index.
     *
     * @param index the index
     * @return the key
     */
    public Object getKey(int index) {
        return keys[index];
    }

    /**
     * Get the child page at the given index.
     *
     * @param index the index
     * @return the child page
     */
    public abstract Page getChildPage(int index);

    /**
     * Get the child page at the given index only if is
     * already loaded. Does not make any attempt to load
     * the page or retrieve it from the cache.
     *
     * @param index the index
     * @return the child page, null if it is not loaded
     */
    public abstract Page getChildPageIfLoaded(int index);

    /**
     * Get the position of the child.
     *
     * @param index the index
     * @return the position
     */
    public abstract long getChildPagePos(int index);

    /**
     * Get the value at the given index.
     *
     * @param index the index
     * @return the value
     */
    public abstract Object getValue(int index);

    /**
     * Get the number of keys in this page.
     *
     * @return the number of keys
     */
    public final int getKeyCount() {
        return keys.length;
    }

    /**
     * Check whether this is a leaf page.
     *
     * @return true if it is a leaf
     */
    public final boolean isLeaf() {
        return getNodeType() == PAGE_TYPE_LEAF;
    }

    public abstract int getNodeType();

    /**
     * Get the position of the page
     *
     * @return the position
     */
    public final long getPos() {
        return pos;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        dump(buff);
        return buff.toString();
    }

    protected void dump(StringBuilder buff) {
        buff.append("id: ").append(System.identityHashCode(this)).append('\n');
        buff.append("pos: ").append(Long.toHexString(pos)).append('\n');
        if (isSaved()) {
            int chunkId = DataUtils.getPageChunkId(pos);
            buff.append("chunk: ").append(Long.toHexString(chunkId)).append('\n');
        }
    }

    /**
     * Create a copy of this page.
     *
     * @return a mutable copy of this page
     */
    public final Page copy() {
        return copy(false);
    }

    public final Page copy(boolean countRemoval) {
        Page newPage = clone();
        newPage.pos = 0;
        // mark the old as deleted
        if(countRemoval) {
            removePage();
            if(isPersistent()) {
                map.store.registerUnsavedPage(newPage.getMemory());
            }
        }
        return newPage;
    }

    @Override
    protected final Page clone() {
        Page clone;
        try {
            clone = (Page) super.clone();
        } catch (CloneNotSupportedException impossible) {
            throw new RuntimeException(impossible);
        }
        return clone;
    }

    /**
     * Search the key in this page using a binary search. Instead of always
     * starting the search in the middle, the last found index is cached.
     * <p>
     * If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page. See also Arrays.binarySearch.
     *
     * @param key the key
     * @return the value or null
     */
    public int binarySearch(Object key) {
        int low = 0, high = keys.length - 1;
        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = cachedCompare - 1;
        if (x < 0 || x > high) {
            x = high >>> 1;
        }
        Object[] k = keys;
        while (low <= high) {
            int compare = map.compare(key, k[x]);
            if (compare > 0) {
                low = x + 1;
            } else if (compare < 0) {
                high = x - 1;
            } else {
                cachedCompare = x + 1;
                return x;
            }
            x = (low + high) >>> 1;
        }
        cachedCompare = low;
        return -(low + 1);
    }

    /**
     * Split the page. This modifies the current page.
     *
     * @param at the split index
     * @return the page with the entries after the split index
     */
    abstract Page split(int at);

    final Object[] splitKeys(int aCount, int bCount) {
        assert aCount + bCount <= getKeyCount();
        Object aKeys[] = createKeyStorage(aCount);
        Object bKeys[] = createKeyStorage(bCount);
        System.arraycopy(keys, 0, aKeys, 0, aCount);
        System.arraycopy(keys, getKeyCount() - bCount, bKeys, 0, bCount);
        keys = aKeys;
        return bKeys;
    }

    /**
     * Get the total number of key-value pairs, including child pages.
     *
     * @return the number of key-value pairs
     */
    public abstract long getTotalCount();

    /**
     * Get the descendant counts for the given child.
     *
     * @param index the child index
     * @return the descendant count
     */
    abstract long getCounts(int index);

    /**
     * Replace the child page.
     *
     * @param index the index
     * @param c the new child page
     */
    public abstract void setChild(int index, Page c);

    /**
     * Replace the key at an index in this page.
     *
     * @param index the index
     * @param key the new key
     */
    public final void setKey(int index, Object key) {
        keys = keys.clone();
        if(isPersistent()) {
            Object old = keys[index];
            DataType keyType = map.getKeyType();
            int mem = keyType.getMemory(key);
            if (old != null) {
                mem -= keyType.getMemory(old);
            }
            addMemory(mem);
        }
        keys[index] = key;
    }

    /**
     * Replace the value at an index in this page.
     *
     * @param index the index
     * @param value the new value
     * @return the old value
     */
    public abstract Object setValue(int index, Object value);

    /**
     * Insert a key-value pair into this leaf.
     *
     * @param index the index
     * @param key the key
     * @param value the value
     */
    public abstract void insertLeaf(int index, Object key, Object value);

    /**
     * Insert a child page into this node.
     *
     * @param index the index
     * @param key the key
     * @param childPage the child page
     */
    public abstract void insertNode(int index, Object key, Page childPage);

    final void insertKey(int index, Object key) {
        int keyCount = getKeyCount();
        assert index <= keyCount : index + " > " + keyCount;
        Object[] newKeys = new Object[keyCount + 1];
        DataUtils.copyWithGap(keys, newKeys, keyCount, index);
        keys = newKeys;

        keys[index] = key;

        if (isPersistent()) {
            addMemory(MEMORY_POINTER + map.getKeyType().getMemory(key));
        }
    }

    /**
     * Remove the key and value (or child) at the given index.
     *
     * @param index the index
     */
    public void remove(int index) {
        int keyCount = getKeyCount();
        DataType keyType = map.getKeyType();
        if (index == keyCount) {
            --index;
        }
        if(isPersistent()) {
            Object old = getKey(index);
            addMemory(-MEMORY_POINTER - keyType.getMemory(old));
        }
        Object newKeys[] = new Object[keyCount - 1];
        DataUtils.copyExcept(keys, newKeys, keyCount, index);
        keys = newKeys;
    }

    /**
     * Read the page from the buffer.
     *
     * @param buff the buffer
     * @param chunkId the chunk id
     * @param offset the offset within the chunk
     * @param maxLength the maximum length
     */
    private void read(ByteBuffer buff, int chunkId, int offset, int maxLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        if (pageLength > maxLength || pageLength < 4) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}",
                    chunkId, maxLength, pageLength);
        }
        buff.limit(start + pageLength);
        short check = buff.getShort();
        int mapId = DataUtils.readVarInt(buff);
        if (mapId != map.getId()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected map id {1}, got {2}",
                    chunkId, map.getId(), mapId);
        }
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}",
                    chunkId, checkTest, check);
        }
        int len = DataUtils.readVarInt(buff);
        keys = new Object[len];
        int type = buff.get();
        if(isLeaf() != ((type & 1) == PAGE_TYPE_LEAF)) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected node type {1}, got {2}",
                    chunkId, isLeaf() ? "0" : "1" , type);
        }
        if (!isLeaf()) {
            readPayLoad(buff);
        }
        boolean compressed = (type & DataUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor;
            if ((type & DataUtils.PAGE_COMPRESSED_HIGH) ==
                    DataUtils.PAGE_COMPRESSED_HIGH) {
                compressor = map.getStore().getCompressorHigh();
            } else {
                compressor = map.getStore().getCompressorFast();
            }
            int lenAdd = DataUtils.readVarInt(buff);
            int compLen = pageLength + start - buff.position();
            byte[] comp = Utils.newBytes(compLen);
            buff.get(comp);
            int l = compLen + lenAdd;
            buff = ByteBuffer.allocate(l);
            compressor.expand(comp, 0, compLen, buff.array(),
                    buff.arrayOffset(), l);
        }
        map.getKeyType().read(buff, keys, len, true);
        if (isLeaf()) {
            readPayLoad(buff);
        }
        diskSpaceUsed = maxLength;
        recalculateMemory();
    }

    protected abstract void readPayLoad(ByteBuffer buff);

    public final boolean isSaved() {
        return DataUtils.isPageSaved(pos);
    }

    /**
     * Store the page and update the position.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     * @return the position of the buffer just after the type
     */
    protected final int write(Chunk chunk, WriteBuffer buff) {
        int start = buff.position();
        int len = getKeyCount();
        int type = isLeaf() ? PAGE_TYPE_LEAF : DataUtils.PAGE_TYPE_NODE;
        buff.putInt(0).
            putShort((byte) 0).
            putVarInt(map.getId()).
            putVarInt(len);
        int typePos = buff.position();
        buff.put((byte) type);
        writeChildren(buff, true);
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, getKeyCount(), true);
        writeValues(buff);
        MVStore store = map.getStore();
        int expLen = buff.position() - compressStart;
        if (expLen > 16) {
            int compressionLevel = store.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = map.getStore().getCompressorFast();
                    compressType = DataUtils.PAGE_COMPRESSED;
                } else {
                    compressor = map.getStore().getCompressorHigh();
                    compressType = DataUtils.PAGE_COMPRESSED_HIGH;
                }
                byte[] exp = new byte[expLen];
                buff.position(compressStart).get(exp);
                byte[] comp = new byte[expLen * 2];
                int compLen = compressor.compress(exp, expLen, comp, 0);
                int plus = DataUtils.getVarIntLen(compLen - expLen);
                if (compLen + plus < expLen) {
                    buff.position(typePos).
                        put((byte) (type + compressType));
                    buff.position(compressStart).
                        putVarInt(expLen - compLen).
                        put(comp, 0, compLen);
                }
            }
        }
        int pageLength = buff.position() - start;
        int chunkId = chunk.id;
        int check = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        buff.putInt(start, pageLength).
            putShort(start + 4, (short) check);
        if (isSaved()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        pos = DataUtils.getPagePos(chunkId, start, pageLength, type);
        store.cachePage(this);
        if (type == DataUtils.PAGE_TYPE_NODE) {
            // cache again - this will make sure nodes stays in the cache
            // for a longer time
            store.cachePage(this);
        }
        int max = DataUtils.getPageMaxLength(pos);
        chunk.maxLen += max;
        chunk.maxLenLive += max;
        chunk.pageCount++;
        chunk.pageCountLive++;
        if (removedInMemory) {
            // if the page was removed _before_ the position was assigned, we
            // need to mark it removed here, so the fields are updated
            // when the next chunk is stored
            map.removePage(pos, memory);
        }
        diskSpaceUsed = max != DataUtils.PAGE_LARGE ? max : pageLength;
        return typePos + 1;
    }

    protected abstract void writeValues(WriteBuffer buff);

    protected abstract void writeChildren(WriteBuffer buff, boolean withCounts);

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     */
    abstract void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff);

    /**
     * Unlink the children recursively after all data is written.
     */
    abstract void writeEnd();

    public abstract int getRawChildPageCount();

    @Override
    public final boolean equals(Object other) {
        return other == this || other instanceof Page && isSaved() && ((Page) other).pos == pos;
    }

    @Override
    public final int hashCode() {
        return isSaved() ? (int) (pos | (pos >>> 32)) : super.hashCode();
    }

    protected final boolean isPersistent() {
        return memory != IN_MEMORY;
    }

    public final int getMemory() {
        if (isPersistent()) {
//            assert memory == calculateMemory() :
//                    "Memory calculation error " + memory + " != " + calculateMemory();
            return memory;
        }
        return 0;
    }

    /**
     * Amount of used disk space in persistent case including child pages.
     *
     * @return amount of used disk space in persistent case
     */
    public long getDiskSpaceUsed() {
        long r = 0;
        if (isPersistent()) {
            r += diskSpaceUsed;
        }
        if (!isLeaf()) {
            for (int i = 0; i < getRawChildPageCount(); i++) {
                long pos = getChildPagePos(i);
                if (pos != 0) {
                    r += getChildPage(i).getDiskSpaceUsed();
                }
            }
        }
        return r;
    }

    final void addMemory(int mem) {
        memory += mem;
    }

    protected final void recalculateMemory() {
        assert isPersistent();
        memory = calculateMemory();
    }

    protected int calculateMemory() {
        int mem = keys.length * MEMORY_POINTER;
        DataType keyType = map.getKeyType();
        for (Object key : keys) {
            mem += keyType.getMemory(key);
        }
        return mem;
    }

    /**
     * Remove the page.
     */
    public final void removePage() {
        if(isPersistent()) {
            long p = pos;
            if (p == 0) {
                removedInMemory = true;
            }
            map.removePage(p, memory);
        }
    }

    public abstract CursorPos getAppendCursorPos(CursorPos cursorPos);

    public abstract void removeAllRecursive();

    private Object[] createKeyStorage(int size)
    {
        return new Object[size];
    }

    final Object[] createValueStorage(int size)
    {
        return new Object[size];
    }

    /**
     * A pointer to a page, either in-memory or using a page position.
     */
    public static final class PageReference {

        public static final PageReference EMPTY = new PageReference(null, 0, 0);

        /**
         * The position, if known, or 0.
         */
        private long pos;

        /**
         * The page, if in memory, or null.
         */
        private Page page;

        /**
         * The descendant count for this child page.
         */
        final long count;

        public PageReference(Page page) {
            this(page, page.getPos(), page.getTotalCount());
        }

        PageReference(long pos, long count) {
            this(null, pos, count);
            assert DataUtils.isPageSaved(pos);
        }

        private PageReference(Page page, long pos, long count) {
            this.page = page;
            this.pos = pos;
            this.count = count;
        }

        public Page getPage() {
            return page;
        }

        void clearPageReference() {
            if (page != null) {
                if (!page.isSaved()) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_INTERNAL, "Page not written");
                }
                page.writeEnd();
                assert pos == page.getPos();
                assert count == page.getTotalCount();
                page = null;
            }
        }

        long getPos() {
            return pos;
        }

        void resetPos() {
            Page p = page;
            if (p != null) {
                pos = p.getPos();
                assert count == p.getTotalCount();
            }
        }

        @Override
        public String toString() {
            return "Cnt:" + count + ", pos:" + DataUtils.getPageChunkId(pos) +
                    "-" + DataUtils.getPageOffset(pos) + ":" + DataUtils.getPageMaxLength(pos) +
                    (DataUtils.getPageType(pos) == 0 ? " leaf" : " node") + ", " + page;
        }
    }


    private static final class NonLeaf extends Page
    {
        /**
         * The child page references.
         */
        private PageReference[] children;

        /**
        * The total entry count of this page and all children.
        */
        private long totalCount;

        NonLeaf(MVMap<?, ?> map) {
            super(map);
        }

        private NonLeaf(MVMap<?, ?> map, NonLeaf source, PageReference children[], long totalCount) {
            super(map, source);
            this.children = children;
            this.totalCount = totalCount;
        }

        NonLeaf(MVMap<?, ?> map, Object keys[], PageReference children[], long totalCount) {
            super(map, keys);
            this.children = children;
            this.totalCount = totalCount;
        }

        @Override
        public int getNodeType() {
            return DataUtils.PAGE_TYPE_NODE;
        }

        @Override
        public Page copy(MVMap<?, ?> map) {
            // replace child pages with empty pages
            PageReference[] children = new PageReference[this.children.length];
            Arrays.fill(children, PageReference.EMPTY);
            return new NonLeaf(map, this, children, 0);
        }

        @Override
        public Page getChildPage(int index) {
            PageReference ref = children[index];
            Page page = ref.getPage();
            if(page == null) {
                page = map.readPage(ref.getPos());
                assert ref.getPos() == page.getPos();
                assert ref.count == page.getTotalCount();
            }
            return page;
        }

        @Override
        public Page getChildPageIfLoaded(int index) {
            return children[index].getPage();
        }

        @Override
        public long getChildPagePos(int index) {
            return children[index].getPos();
        }

        @Override
        public Object getValue(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        @SuppressWarnings("SuspiciousSystemArraycopy")
        public Page split(int at) {
            assert !isSaved();
            int b = getKeyCount() - at;
            Object bKeys[] = splitKeys(at, b - 1);
            PageReference[] aChildren = new PageReference[at + 1];
            PageReference[] bChildren = new PageReference[b];
            System.arraycopy(children, 0, aChildren, 0, at + 1);
            System.arraycopy(children, at + 1, bChildren, 0, b);
            children = aChildren;

            long t = 0;
            for (PageReference x : aChildren) {
                t += x.count;
            }
            totalCount = t;
            t = 0;
            for (PageReference x : bChildren) {
                t += x.count;
            }
            Page newPage = create(map, bKeys, null, bChildren, t, 0);
            if(isPersistent()) {
                recalculateMemory();
            }
            return newPage;
        }

        @Override
        public long getTotalCount() {
            assert totalCount == calculateTotalCount() :
                        "Total count: " + totalCount + " != " + calculateTotalCount();
            return totalCount;
        }

        private long calculateTotalCount() {
            long check = 0;
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                check += children[i].count;
            }
            return check;
        }

        @Override
        long getCounts(int index) {
            return children[index].count;
        }

        @Override
        public void setChild(int index, Page c) {
            assert c != null;
            PageReference child = children[index];
            if (c != child.getPage() || c.getPos() != child.getPos()) {
                totalCount += c.getTotalCount() - child.count;
                children = children.clone();
                children[index] = new PageReference(c);
            }
        }

        @Override
        public Object setValue(int index, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertLeaf(int index, Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertNode(int index, Object key, Page childPage) {
            int childCount = getRawChildPageCount();
            insertKey(index, key);

            PageReference newChildren[] = new PageReference[childCount + 1];
            DataUtils.copyWithGap(children, newChildren, childCount, index);
            children = newChildren;
            children[index] = new PageReference(childPage);

            totalCount += childPage.getTotalCount();
            if (isPersistent()) {
                addMemory(MEMORY_POINTER + PAGE_MEMORY_CHILD);
            }
        }

        @Override
        public void remove(int index) {
            int childCount = getRawChildPageCount();
            super.remove(index);
            if(isPersistent()) {
                addMemory(-MEMORY_POINTER - PAGE_MEMORY_CHILD);
            }
            totalCount -= children[index].count;
            PageReference newChildren[] = new PageReference[childCount - 1];
            DataUtils.copyExcept(children, newChildren, childCount, index);
            children = newChildren;
        }

        @Override
        public void removeAllRecursive() {
            if (isPersistent()) {
                for (int i = 0, size = map.getChildPageCount(this); i < size; i++) {
                    PageReference ref = children[i];
                    Page page = ref.getPage();
                    if (page != null) {
                        page.removeAllRecursive();
                    } else {
                        long c = ref.getPos();
                        int type = DataUtils.getPageType(c);
                        if (type == PAGE_TYPE_LEAF) {
                            int mem = DataUtils.getPageMaxLength(c);
                            map.removePage(c, mem);
                        } else {
                            map.readPage(c).removeAllRecursive();
                        }
                    }
                }
            }
            removePage();
        }

        @Override
        public CursorPos getAppendCursorPos(CursorPos cursorPos) {
            int keyCount = getKeyCount();
            Page childPage = getChildPage(keyCount);
            return childPage.getAppendCursorPos(new CursorPos(this, keyCount, cursorPos));
        }

        @Override
        protected void readPayLoad(ByteBuffer buff) {
            int keyCount = getKeyCount();
            children = new PageReference[keyCount + 1];
            long p[] = new long[keyCount + 1];
            for (int i = 0; i <= keyCount; i++) {
                p[i] = buff.getLong();
            }
            long total = 0;
            for (int i = 0; i <= keyCount; i++) {
                long s = DataUtils.readVarLong(buff);
                long position = p[i];
                assert position == 0 ? s == 0 : s >= 0;
                total += s;
                children[i] = position == 0 ? PageReference.EMPTY : new PageReference(position, s);
            }
            totalCount = total;
        }

        @Override
        protected void writeValues(WriteBuffer buff) {}

        @Override
        protected void writeChildren(WriteBuffer buff, boolean withCounts) {
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                buff.putLong(children[i].getPos());
            }
            if(withCounts) {
                for (int i = 0; i <= keyCount; i++) {
                    buff.putVarLong(children[i].count);
                }
            }
        }

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff) {
            if (!isSaved()) {
                int patch = write(chunk, buff);
                int len = getRawChildPageCount();
                for (int i = 0; i < len; i++) {
                    PageReference ref = children[i];
                    Page p = ref.getPage();
                    if (p != null) {
                        p.writeUnsavedRecursive(chunk, buff);
                        ref.resetPos();
                    }
                }
                int old = buff.position();
                buff.position(patch);
                writeChildren(buff, false);
                buff.position(old);
            }
        }

        @Override
        void writeEnd() {
            int len = getRawChildPageCount();
            for (int i = 0; i < len; i++) {
                children[i].clearPageReference();
            }
        }

        @Override
        public int getRawChildPageCount() {
            return getKeyCount() + 1;
        }

        @Override
        protected int calculateMemory() {
            return super.calculateMemory() + PAGE_NODE_MEMORY +
                        getRawChildPageCount() * (MEMORY_POINTER + PAGE_MEMORY_CHILD);
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append("[").append(Long.toHexString(children[i].getPos())).append("]");
                if(i < keyCount) {
                    buff.append(" ").append(getKey(i));
                }
            }
        }
    }


    private static class Leaf extends Page
    {
        /**
         * The storage for values.
         */
        private Object values[];

        Leaf(MVMap<?, ?> map) {
            super(map);
        }

        private Leaf(MVMap<?, ?> map, Leaf source) {
            super(map, source);
            this.values = source.values;
        }

        Leaf(MVMap<?, ?> map, Object keys[], Object values[]) {
            super(map, keys);
            this.values = values;
        }

        @Override
        public int getNodeType() {
            return PAGE_TYPE_LEAF;
        }

        @Override
        public Page copy(MVMap<?, ?> map) {
            return new Leaf(map, this);
        }

        @Override
        public Page getChildPage(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page getChildPageIfLoaded(int index) { throw new UnsupportedOperationException(); }

        @Override
        public long getChildPagePos(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        @SuppressWarnings("SuspiciousSystemArraycopy")
        public Page split(int at) {
            assert !isSaved();
            int b = getKeyCount() - at;
            Object bKeys[] = splitKeys(at, b);
            Object bValues[] = createValueStorage(b);
            if(values != null) {
                Object aValues[] = createValueStorage(at);
                System.arraycopy(values, 0, aValues, 0, at);
                System.arraycopy(values, at, bValues, 0, b);
                values = aValues;
            }
            Page newPage = create(map, bKeys, bValues, null, b, 0);
            if(isPersistent()) {
                recalculateMemory();
            }
            return newPage;
        }

        @Override
        public long getTotalCount() {
            return getKeyCount();
        }

        @Override
        long getCounts(int index) {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setChild(int index, Page c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object setValue(int index, Object value) {
            DataType valueType = map.getValueType();
            values = values.clone();
            Object old = setValueInternal(index, value);
            if(isPersistent()) {
                addMemory(valueType.getMemory(value) -
                            valueType.getMemory(old));
            }
            return old;
        }

        private Object setValueInternal(int index, Object value) {
            Object old = values[index];
            values[index] = value;
            return old;
        }

        @Override
        public void insertLeaf(int index, Object key, Object value) {
            int keyCount = getKeyCount();
            insertKey(index, key);

            if(values != null) {
                Object newValues[] = createValueStorage(keyCount + 1);
                DataUtils.copyWithGap(values, newValues, keyCount, index);
                values = newValues;
                setValueInternal(index, value);
                if (isPersistent()) {
                    addMemory(MEMORY_POINTER + map.getValueType().getMemory(value));
                }
            }
        }
        @Override
        public void insertNode(int index, Object key, Page childPage) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove(int index) {
            int keyCount = getKeyCount();
            super.remove(index);
            if (values != null) {
                if(isPersistent()) {
                    Object old = getValue(index);
                    addMemory(-MEMORY_POINTER - map.getValueType().getMemory(old));
                }
                Object newValues[] = createValueStorage(keyCount - 1);
                DataUtils.copyExcept(values, newValues, keyCount, index);
                values = newValues;
            }
        }

        @Override
        public void removeAllRecursive() {
            removePage();
        }

        @Override
        public CursorPos getAppendCursorPos(CursorPos cursorPos) {
            int keyCount = getKeyCount();
            return new CursorPos(this, -keyCount - 1, cursorPos);
        }

        @Override
        protected void readPayLoad(ByteBuffer buff) {
            int keyCount = getKeyCount();
            values = createValueStorage(keyCount);
            map.getValueType().read(buff, values, getKeyCount(), false);
        }

        @Override
        protected void writeValues(WriteBuffer buff) {
            map.getValueType().write(buff, values, getKeyCount(), false);
        }

        @Override
        protected void writeChildren(WriteBuffer buff, boolean withCounts) {}

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff) {
            if (!isSaved()) {
                write(chunk, buff);
            }
        }

        @Override
        void writeEnd() {}

        @Override
        public int getRawChildPageCount() {
            return 0;
        }

        @Override
        protected int calculateMemory() {
            int mem = super.calculateMemory() + PAGE_LEAF_MEMORY +
                                        values.length * MEMORY_POINTER;
            DataType valueType = map.getValueType();
            for (Object value : values) {
                mem += valueType.getMemory(value);
            }
            return mem;
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            int keyCount = getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append(getKey(i));
                if (values != null) {
                    buff.append(':');
                    buff.append(getValue(i));
                }
            }
        }
    }
}
