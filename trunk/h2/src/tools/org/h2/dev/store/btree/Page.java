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

    private final BtreeMap<?, ?> map;
    private final long version;
    private long pos;
    private Object[] keys;
    private Object[] values;
    private long[] children;
    private long[] counts;
    private int cachedCompare;
    private long totalCount;

    private Page(BtreeMap<?, ?> map, long version) {
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
    public static Page create(BtreeMap<?, ?> map, long version, Object[] keys,
            Object[] values, long[] children, long[] counts,
            long totalCount) {
        Page p = new Page(map, version);
        p.pos = map.getStore().registerTempPage(p);
        p.keys = keys;
        p.values = values;
        p.children = children;
        p.counts = counts;
        p.totalCount = totalCount;
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
    static Page read(FileChannel file, BtreeMap<?, ?> map, long filePos,
            long pos) {
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
        return map.readPage(children[index]);
    }

    public Object getValue(int x) {
        return values[x];
    }

    public int getKeyCount() {
        return keys.length;
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
        for (int i = 0; i <= keys.length; i++) {
            if (i > 0) {
                buff.append(" ");
            }
            if (children != null) {
                buff.append("[" + children[i] + "] ");
            }
            if (i < keys.length) {
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
        Page newPage = create(map, writeVersion, keys, values, children,
                counts, totalCount);
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
        int low = 0, high = keys.length - 1;
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
        // int low = 0, high = keys.length - 1;
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
        int a = at, b = keys.length - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a, bKeys, 0, b);
        keys = aKeys;
        Object[] aValues = new Object[a];
        Object[] bValues = new Object[b];
        bValues = new Object[b];
        System.arraycopy(values, 0, aValues, 0, a);
        System.arraycopy(values, a, bValues, 0, b);
        values = aValues;
        totalCount = keys.length;
        Page newPage = create(map, version, bKeys, bValues, null, null,
                bKeys.length);
        return newPage;
    }

    private Page splitNode(int at) {
        int a = at, b = keys.length - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b - 1];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a + 1, bKeys, 0, b - 1);
        keys = aKeys;
        long[] aChildren = new long[a + 1];
        long[] bChildren = new long[b];
        System.arraycopy(children, 0, aChildren, 0, a + 1);
        System.arraycopy(children, a + 1, bChildren, 0, b);
        children = aChildren;
        long[] aCounts = new long[a + 1];
        long[] bCounts = new long[b];
        System.arraycopy(counts, 0, aCounts, 0, a + 1);
        System.arraycopy(counts, a + 1, bCounts, 0, b);
        counts = aCounts;
        long t = 0;
        for (long x : aCounts) {
            t += x;
        }
        totalCount = t;
        t = 0;
        for (long x : bCounts) {
            t += x;
        }
        Page newPage = create(map, version, bKeys, null, bChildren,
                bCounts, t);
        return newPage;
    }

    public long getTotalSize() {
        if (BtreeMapStore.ASSERT) {
            long check = 0;
            if (isLeaf()) {
                check = keys.length;
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
        if (c.getPos() != children[index]) {
            long[] newChildren = new long[children.length];
            System.arraycopy(children, 0, newChildren, 0, newChildren.length);
            newChildren[index] = c.getPos();
            children = newChildren;
        }
        if (c.getTotalSize() != counts[index]) {
            long[] newCounts = new long[counts.length];
            System.arraycopy(counts, 0, newCounts, 0,
                    newCounts.length);
            newCounts[index] = c.getTotalSize();
            totalCount += newCounts[index] - counts[index];
            counts = newCounts;
        }
    }

    public void setKey(int index, Object key) {
        // create a copy - not required if already cloned once in this version,
        // but avoid unnecessary cloning would require a "modified" flag
        Object[] newKeys = new Object[keys.length];
        System.arraycopy(keys, 0, newKeys, 0, newKeys.length);
        newKeys[index] = key;
        keys = newKeys;
    }

    public void setValue(int index, Object value) {
        // create a copy - not required if already cloned once in this version,
        // but avoid unnecessary cloning would require a "modified" flag
        Object[] newValues = new Object[values.length];
        System.arraycopy(values, 0, newValues, 0, newValues.length);
        newValues[index] = value;
        values = newValues;
    }

    /**
     * Remove this page and all child pages.
     */
    void removeAllRecursive() {
        if (children != null) {
            for (long c : children) {
                int type = DataUtils.getPageType(c);
                if (type == DataUtils.PAGE_TYPE_LEAF) {
                    map.getStore().removePage(c);
                } else {
                    map.readPage(c).removeAllRecursive();
                }
            }
        }
        map.getStore().removePage(pos);
    }

    public void insert(int index, Object key, Object value, long child,
            long count) {
        Object[] newKeys = new Object[keys.length + 1];
        DataUtils.copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = key;
        keys = newKeys;
        if (values != null) {
            Object[] newValues = new Object[values.length + 1];
            DataUtils.copyWithGap(values, newValues, values.length, index);
            newValues[index] = value;
            values = newValues;
            totalCount++;
        }
        if (children != null) {
            long[] newChildren = new long[children.length + 1];
            DataUtils.copyWithGap(children, newChildren, children.length, index);
            newChildren[index] = child;
            children = newChildren;
            long[] newCounts = new long[counts.length + 1];
            DataUtils.copyWithGap(counts, newCounts, counts.length, index);
            newCounts[index] = count;
            counts = newCounts;
            totalCount += count;
        }
    }

    public void remove(int index) {
        Object[] newKeys = new Object[keys.length - 1];
        int keyIndex = index >= keys.length ? index - 1 : index;
        DataUtils.copyExcept(keys, newKeys, keys.length, keyIndex);
        keys = newKeys;
        if (values != null) {
            Object[] newValues = new Object[values.length - 1];
            DataUtils.copyExcept(values, newValues, values.length, index);
            values = newValues;
            totalCount--;
        }
        if (children != null) {
            long countOffset = counts[index];
            long[] newChildren = new long[children.length - 1];
            DataUtils.copyExcept(children, newChildren, children.length, index);
            children = newChildren;
            long[] newCounts = new long[counts.length - 1];
            DataUtils.copyExcept(counts, newCounts,
                    counts.length, index);
            counts = newCounts;
            totalCount -= countOffset;
        }
    }

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
        int len = keys.length;
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
        int len = keys.length;
        for (int i = 0; i < len; i++) {
            maxLength += map.getKeyType().getMaxLength(keys[i]);
        }
        if (children != null) {
            maxLength += 8 * len;
            maxLength += DataUtils.MAX_VAR_LONG_LEN * len;
            for (int i = 0; i <= len; i++) {
                long c = children[i];
                if (c < 0) {
                    maxLength += map.readPage(c).getMaxLengthTempRecursive();
                }
            }
        } else {
            for (int i = 0; i < len; i++) {
                maxLength += map.getValueType().getMaxLength(values[i]);
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
        if (children != null) {
            int len = children.length;
            for (int i = 0; i < len; i++) {
                long c = children[i];
                if (c < 0) {
                    children[i] = map.readPage(c).writeTempRecursive(buff,
                            chunkId);
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
        if (children != null) {
            int size = children.length;
            for (int i = 0; i < size; i++) {
                long c = children[i];
                if (c < 0) {
                    count += map.readPage(c).countTempRecursive();
                }
            }
        }
        return count;
    }

    public long getCounts(int index) {
        return counts[index];
    }

}
