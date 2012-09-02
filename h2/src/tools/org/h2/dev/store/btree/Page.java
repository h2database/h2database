/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import org.h2.compress.Compressor;

/**
 * A page (a node or a leaf).
 * <p>
 * For b-tree nodes, the key at a given index is larger than the largest key of the
 * child at the same index.
 * <p>
 * File format: page length (including length): int check value: short map id:
 * varInt number of keys: varInt type: byte (0: leaf, 1: node; +2: compressed)
 * compressed: bytes saved (varInt) keys leaf: values (one for each key) node:
 * children (1 more than keys)
 */
public class Page {

    private static final int SHARED_KEYS = 1, SHARED_VALUES = 2, SHARED_CHILDREN = 4, SHARED_COUNTS = 8;

    private final MVMap<?, ?> map;
    private final long version;
    private long pos;
    private long totalCount;
    private int keyCount;

    /**
     * The last result of a find operation is cached.
     */
    private int cachedCompare;

    /**
     * Which arrays are shared with another version of this page.
     */
    private int sharedFlags;

    private Object[] keys;
    private Object[] values;
    private long[] children;
    private Page[] childrenPages;
    private long[] counts;

    private Page(MVMap<?, ?> map, long version) {
        this.map = map;
        this.version = version;
    }

    /**
     * Create a new page. The arrays are not cloned.
     *
     * @param map the map
     * @param version the version
     * @param keys the keys
     * @param values the values
     * @param children the children
     * @return the page
     */
    public static Page create(MVMap<?, ?> map, long version,
            int keyCount, Object[] keys,
            Object[] values, long[] children, Page[] childrenPages, long[] counts,
            long totalCount, int sharedFlags) {
        Page p = new Page(map, version);
        // the position is 0
        p.keys = keys;
        p.keyCount = keyCount;
        p.values = values;
        p.children = children;
        p.childrenPages = childrenPages;
        p.counts = counts;
        p.totalCount = totalCount;
        p.sharedFlags = sharedFlags;
        return p;
    }

    /**
     * Read a page.
     *
     * @param map the map
     * @param pos the page position
     * @param buff the source buffer
     * @return the page
     */
    static Page read(FileChannel file, MVMap<?, ?> map,
            long filePos, long pos) {
        int maxLength = DataUtils.getPageMaxLength(pos), length = maxLength;
        ByteBuffer buff;
        try {
            file.position(filePos);
            if (maxLength == Integer.MAX_VALUE) {
                buff = ByteBuffer.wrap(new byte[128]);
                DataUtils.readFully(file, buff);
                maxLength = buff.getInt();
                file.position(filePos);
            }
            buff = ByteBuffer.wrap(new byte[length]);
            DataUtils.readFully(file, buff);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Page p = new Page(map, 0);
        p.pos = pos;
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, maxLength);
        return p;
    }

    public Object getKey(int index) {
        return keys[index];
    }

    public Page getChildPage(int index) {
        Page p = childrenPages[index];
        return p != null ? p : map.readPage(children[index]);
    }

    long getChildPagePos(int index) {
        return children[index];
    }

    public Object getValue(int x) {
        return values[x];
    }

    public int getKeyCount() {
        return keyCount;
    }

    public boolean isLeaf() {
        return children == null;
    }

    /**
     * Get the position of the page
     *
     * @return the position
     */
    public long getPos() {
        return pos;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("pos: ").append(pos).append("\n");
        for (int i = 0; i <= keyCount; i++) {
            if (i > 0) {
                buff.append(" ");
            }
            if (children != null) {
                buff.append("[" + children[i] + "] ");
            }
            if (i < keyCount) {
                buff.append(keys[i]);
                if (values != null) {
                    buff.append(':');
                    buff.append(values[i]);
                }
            }
        }
        return buff.toString();
    }

    public Page copyOnWrite(long writeVersion) {
        if (version == writeVersion) {
            return this;
        }
        map.getStore().removePage(pos);
        Page newPage = create(map, writeVersion,
                keyCount, keys, values, children, childrenPages,
                counts, totalCount,
                SHARED_KEYS | SHARED_VALUES | SHARED_CHILDREN | SHARED_COUNTS);
        newPage.cachedCompare = cachedCompare;
        return newPage;
    }

    /**
     * Search the key in this page using a binary search. Instead of always
     * starting the search in the middle, the last found index is cached. If the
     * key was found, the returned value is the index in the key array. If not
     * found, the returned value is negative, where -1 means the provided key is
     * smaller than any keys in this page. See also Arrays.binarySearch.
     *
     * @param key the key
     * @return the value or null
     */
    public int binarySearch(Object key) {
        int low = 0, high = keyCount - 1;
        int x = cachedCompare - 1;
        if (x < 0 || x > high) {
            x = (low + high) >>> 1;
        }
        while (low <= high) {
            int compare = map.compare(key, keys[x]);
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

        // regular binary search (without caching)
        // int low = 0, high = keyCount - 1;
        // while (low <= high) {
        //     int x = (low + high) >>> 1;
        //     int compare = map.compare(key, keys[x]);
        //     if (compare > 0) {
        //         low = x + 1;
        //     } else if (compare < 0) {
        //         high = x - 1;
        //     } else {
        //         return x;
        //     }
        // }
        // return -(low + 1);
    }

    public Page split(int at) {
        return isLeaf() ? splitLeaf(at) : splitNode(at);
    }

    private Page splitLeaf(int at) {
        int a = at, b = keyCount - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a, bKeys, 0, b);
        keys = aKeys;
        keyCount = a;
        Object[] aValues = new Object[a];
        Object[] bValues = new Object[b];
        bValues = new Object[b];
        System.arraycopy(values, 0, aValues, 0, a);
        System.arraycopy(values, a, bValues, 0, b);
        values = aValues;
        sharedFlags &= ~(SHARED_KEYS | SHARED_VALUES);
        totalCount = a;
        Page newPage = create(map, version, b,
                bKeys, bValues, null, null, null,
                bKeys.length, 0);
        return newPage;
    }

    private Page splitNode(int at) {
        int a = at, b = keyCount - a;

        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b - 1];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a + 1, bKeys, 0, b - 1);
        keys = aKeys;
        keyCount = a;

        long[] aChildren = new long[a + 1];
        long[] bChildren = new long[b];
        System.arraycopy(children, 0, aChildren, 0, a + 1);
        System.arraycopy(children, a + 1, bChildren, 0, b);
        children = aChildren;

        Page[] aChildrenPages = new Page[a + 1];
        Page[] bChildrenPages = new Page[b];
        System.arraycopy(childrenPages, 0, aChildrenPages, 0, a + 1);
        System.arraycopy(childrenPages, a + 1, bChildrenPages, 0, b);
        childrenPages = aChildrenPages;

        long[] aCounts = new long[a + 1];
        long[] bCounts = new long[b];
        System.arraycopy(counts, 0, aCounts, 0, a + 1);
        System.arraycopy(counts, a + 1, bCounts, 0, b);
        counts = aCounts;

        sharedFlags &= ~(SHARED_KEYS | SHARED_CHILDREN | SHARED_COUNTS);
        long t = 0;
        for (long x : aCounts) {
            t += x;
        }
        totalCount = t;
        t = 0;
        for (long x : bCounts) {
            t += x;
        }
        Page newPage = create(map, version, b - 1,
                bKeys, null, bChildren, bChildrenPages,
                bCounts, t, 0);
        return newPage;
    }

    public long getTotalCount() {
        if (MVStore.ASSERT) {
            long check = 0;
            if (isLeaf()) {
                check = keyCount;
            } else {
                for (long x : counts) {
                    check += x;
                }
            }
            if (check != totalCount) {
                throw new AssertionError("Expected: " + check + " got: "
                        + totalCount);
            }
        }
        return totalCount;
    }

    public void setChild(int index, Page c) {
        if (c != childrenPages[index] || c.getPos() != children[index]) {
            if ((sharedFlags & SHARED_CHILDREN) != 0) {
                children = Arrays.copyOf(children, children.length);
                childrenPages = Arrays.copyOf(childrenPages, childrenPages.length);
                sharedFlags &= ~SHARED_CHILDREN;
            }
            children[index] = c.getPos();
            childrenPages[index] = c;
        }
        if (c.getTotalCount() != counts[index]) {
            if ((sharedFlags & SHARED_COUNTS) != 0) {
                counts = Arrays.copyOf(counts, counts.length);
                sharedFlags &= ~SHARED_COUNTS;
            }
            long oldCount = counts[index];
            counts[index] = c.getTotalCount();
            totalCount += counts[index] - oldCount;
        }
    }

    public void setKey(int index, Object key) {
        if ((sharedFlags & SHARED_KEYS) != 0) {
            keys = Arrays.copyOf(keys, keys.length);
            sharedFlags &= ~SHARED_KEYS;
        }
        keys[index] = key;
    }

    public Object setValue(int index, Object value) {
        Object old = values[index];
        if ((sharedFlags & SHARED_VALUES) != 0) {
            values = Arrays.copyOf(values, values.length);
            sharedFlags &= ~SHARED_VALUES;
        }
        values[index] = value;
        return old;
    }

    /**
     * Remove this page and all child pages.
     */
    void removeAllRecursive() {
        if (children != null) {
            for (int i = 0, size = children.length; i < size; i++) {
                Page p = childrenPages[i];
                if (p != null) {
                    p.removeAllRecursive();
                } else {
                    long c = children[i];
                    int type = DataUtils.getPageType(c);
                    if (type == DataUtils.PAGE_TYPE_LEAF) {
                        map.getStore().removePage(c);
                    } else {
                        map.readPage(c).removeAllRecursive();
                    }
                }
            }
        }
        map.getStore().removePage(pos);
    }

    public void insertLeaf(int index, Object key, Object value) {
        if (((sharedFlags & SHARED_KEYS) == 0) && keys.length > keyCount + 1) {
            if (index < keyCount) {
                System.arraycopy(keys, index, keys, index + 1, keyCount - index);
                System.arraycopy(values, index, values, index + 1, keyCount - index);
            }
        } else {
            int len = keyCount + 6;
            Object[] newKeys = new Object[len];
            DataUtils.copyWithGap(keys, newKeys, keyCount, index);
            keys = newKeys;
            Object[] newValues = new Object[len];
            DataUtils.copyWithGap(values, newValues, keyCount, index);
            values = newValues;
        }
        keys[index] = key;
        values[index] = value;
        keyCount++;
        sharedFlags &= ~(SHARED_KEYS | SHARED_VALUES);
        totalCount++;
    }

    public void insertNode(int index, Object key, Page childPage) {

        Object[] newKeys = new Object[keyCount + 1];
        DataUtils.copyWithGap(keys, newKeys, keyCount, index);
        newKeys[index] = key;
        keys = newKeys;
        keyCount++;

        long[] newChildren = new long[children.length + 1];
        DataUtils.copyWithGap(children, newChildren, children.length, index);
        newChildren[index] = childPage.getPos();
        children = newChildren;

        Page[] newChildrenPages = new Page[childrenPages.length + 1];
        DataUtils.copyWithGap(childrenPages, newChildrenPages, childrenPages.length, index);
        newChildrenPages[index] = childPage;
        childrenPages = newChildrenPages;

        long[] newCounts = new long[counts.length + 1];
        DataUtils.copyWithGap(counts, newCounts, counts.length, index);
        newCounts[index] = childPage.getTotalCount();
        counts = newCounts;

        sharedFlags &= ~(SHARED_KEYS | SHARED_CHILDREN | SHARED_COUNTS);
        totalCount += childPage.getTotalCount();
    }

    public void remove(int index) {
        int keyIndex = index >= keyCount ? index - 1 : index;
        if ((sharedFlags & SHARED_KEYS) == 0 && keys.length > keyCount - 4) {
            if (keyIndex < keyCount - 1) {
                System.arraycopy(keys, keyIndex + 1, keys, keyIndex, keyCount - keyIndex - 1);
            }
            keys[keyCount - 1] = null;
        } else {
            Object[] newKeys = new Object[keyCount - 1];
            DataUtils.copyExcept(keys, newKeys, keyCount, keyIndex);
            keys = newKeys;
            sharedFlags &= ~SHARED_KEYS;
        }

        if (values != null) {
            if ((sharedFlags & SHARED_VALUES) == 0 && values.length > keyCount - 4) {
                if (index < keyCount - 1) {
                    System.arraycopy(values, index + 1, values, index, keyCount - index - 1);
                }
                values[keyCount - 1] = null;
            } else {
                Object[] newValues = new Object[keyCount - 1];
                DataUtils.copyExcept(values, newValues, keyCount, index);
                values = newValues;
                sharedFlags &= ~SHARED_VALUES;
            }
            totalCount--;
        }
        keyCount--;
        if (children != null) {
            long countOffset = counts[index];

            long[] newChildren = new long[children.length - 1];
            DataUtils.copyExcept(children, newChildren, children.length, index);
            children = newChildren;

            Page[] newChildrenPages = new Page[childrenPages.length - 1];
            DataUtils.copyExcept(childrenPages, newChildrenPages,
                    childrenPages.length, index);
            childrenPages = newChildrenPages;

            long[] newCounts = new long[counts.length - 1];
            DataUtils.copyExcept(counts, newCounts, counts.length, index);
            counts = newCounts;

            sharedFlags &= ~(SHARED_CHILDREN | SHARED_COUNTS);
            totalCount -= countOffset;
        }
    }

//    public void remove(int index) {
//        Object[] newKeys = new Object[keyCount - 1];
//        int keyIndex = index >= keyCount ? index - 1 : index;
//        DataUtils.copyExcept(keys, newKeys, keyCount, keyIndex);
//        keys = newKeys;
//        sharedFlags &= ~SHARED_KEYS;
//        if (values != null) {
//            Object[] newValues = new Object[keyCount - 1];
//            DataUtils.copyExcept(values, newValues, keyCount, index);
//            values = newValues;
//            sharedFlags &= ~SHARED_VALUES;
//            totalCount--;
//        }
//        keyCount--;
//        if (children != null) {
//            long countOffset = counts[index];
//
//            long[] newChildren = new long[children.length - 1];
//            DataUtils.copyExcept(children, newChildren, children.length, index);
//            children = newChildren;
//
//            Page[] newChildrenPages = new Page[childrenPages.length - 1];
//            DataUtils.copyExcept(childrenPages, newChildrenPages, childrenPages.length, index);
//            childrenPages = newChildrenPages;
//
//            long[] newCounts = new long[counts.length - 1];
//            DataUtils.copyExcept(counts, newCounts,
//                    counts.length, index);
//            counts = newCounts;
//
//            sharedFlags &= ~(SHARED_CHILDREN | SHARED_COUNTS);
//            totalCount -= countOffset;
//        }
//    }

    private void read(ByteBuffer buff, int chunkId, int offset, int maxLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        if (pageLength > maxLength) {
            throw new RuntimeException("Length too large, expected =< "
                    + maxLength + " got " + pageLength);
        }
        short check = buff.getShort();
        int mapId = DataUtils.readVarInt(buff);
        if (mapId != map.getId()) {
            throw new RuntimeException("Error reading page, expected map "
                    + map.getId() + " got " + mapId);
        }
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw new RuntimeException("Error in check value, expected "
                    + checkTest + " got " + check);
        }
        int len = DataUtils.readVarInt(buff);
        keys = new Object[len];
        keyCount = len;
        int type = buff.get();
        boolean node = (type & 1) == DataUtils.PAGE_TYPE_NODE;
        boolean compressed = (type & DataUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor = map.getStore().getCompressor();
            int lenAdd = DataUtils.readVarInt(buff);
            int compLen = pageLength + start - buff.position();
            byte[] comp = new byte[compLen];
            buff.get(comp);
            byte[] exp = new byte[compLen + lenAdd];
            compressor.expand(comp, 0, compLen, exp, 0, exp.length);
            buff = ByteBuffer.wrap(exp);
        }
        for (int i = 0; i < len; i++) {
            keys[i] = map.getKeyType().read(buff);
        }
        if (node) {
            children = new long[len + 1];
            for (int i = 0; i <= len; i++) {
                children[i] = buff.getLong();
            }
            childrenPages = new Page[len + 1];
            counts = new long[len + 1];
            long total = 0;
            for (int i = 0; i <= len; i++) {
                long s = DataUtils.readVarLong(buff);
                total += s;
                counts[i] = s;
            }
            totalCount = total;
        } else {
            values = new Object[len];
            for (int i = 0; i < len; i++) {
                values[i] = map.getValueType().read(buff);
            }
            totalCount = len;
        }
    }

    /**
     * Store the page and update the position.
     *
     * @param buff the target buffer
     * @param chunkId the chunk id
     */
    private void write(ByteBuffer buff, int chunkId) {
        int start = buff.position();
        buff.putInt(0);
        buff.putShort((byte) 0);
        DataUtils.writeVarInt(buff, map.getId());
        int len = keyCount;
        DataUtils.writeVarInt(buff, len);
        Compressor compressor = map.getStore().getCompressor();
        int type = children != null ? DataUtils.PAGE_TYPE_NODE
                : DataUtils.PAGE_TYPE_LEAF;
        buff.put((byte) type);
        int compressStart = buff.position();
        for (int i = 0; i < len; i++) {
            map.getKeyType().write(buff, keys[i]);
        }
        if (type == DataUtils.PAGE_TYPE_NODE) {
            for (int i = 0; i <= len; i++) {
                buff.putLong(children[i]);
            }
            for (int i = 0; i <= len; i++) {
                DataUtils.writeVarLong(buff, counts[i]);
            }
        } else {
            for (int i = 0; i < len; i++) {
                map.getValueType().write(buff, values[i]);
            }
        }
        if (compressor != null) {
            int expLen = buff.position() - compressStart;
            byte[] exp = new byte[expLen];
            buff.position(compressStart);
            buff.get(exp);
            byte[] comp = new byte[exp.length * 2];
            int compLen = compressor.compress(exp, exp.length, comp, 0);
            if (compLen + DataUtils.getVarIntLen(compLen - expLen) < expLen) {
                buff.position(compressStart - 1);
                buff.put((byte) (type + DataUtils.PAGE_COMPRESSED));
                DataUtils.writeVarInt(buff, expLen - compLen);
                buff.put(comp, 0, compLen);
            }
        }
        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int check = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        buff.putShort(start + 4, (short) check);
        this.pos = DataUtils.getPagePos(chunkId, start, pageLength, type);
    }

    /**
     * Get the maximum length in bytes to store temporary records, recursively.
     *
     * @return the next page id
     */
    int getMaxLengthTempRecursive() {
        // length, check, map id, key length, type
        int maxLength = 4 + 2 + DataUtils.MAX_VAR_INT_LEN
                + DataUtils.MAX_VAR_INT_LEN + 1;
        int len = keyCount;
        for (int i = 0; i < len; i++) {
            maxLength += map.getKeyType().getMaxLength(keys[i]);
        }
        if (isLeaf()) {
            for (int i = 0; i < len; i++) {
                maxLength += map.getValueType().getMaxLength(values[i]);
            }
        } else {
            maxLength += 8 * len;
            maxLength += DataUtils.MAX_VAR_LONG_LEN * len;
            for (int i = 0; i <= len; i++) {
                Page p = childrenPages[i];
                if (p != null) {
                    maxLength += p.getMaxLengthTempRecursive();
                }
            }
        }
        return maxLength;
    }

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     *
     * @param buff the target buffer
     * @param posOffset the offset of the id
     * @return the page id
     */
    long writeTempRecursive(ByteBuffer buff, int chunkId) {
        if (!isLeaf()) {
            int len = children.length;
            for (int i = 0; i < len; i++) {
                Page p = childrenPages[i];
                if (p != null) {
                    children[i] = p.writeTempRecursive(buff, chunkId);
                    childrenPages[i] = null;
                }
            }
        }
        write(buff, chunkId);
        return pos;
    }

    /**
     * Count the temporary pages recursively.
     *
     * @return the number of temporary pages
     */
    int countTempRecursive() {
        int count = 1;
        if (!isLeaf()) {
            for (Page p : childrenPages) {
                if (p != null) {
                    count += p.countTempRecursive();
                }
            }
        }
        return count;
    }

    public long getCounts(int index) {
        return counts[index];
    }

    long getVersion() {
        return version;
    }

    int getChildPageCount() {
        return children.length;
    }

}
