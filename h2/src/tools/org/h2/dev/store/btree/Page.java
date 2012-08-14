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
import java.util.ArrayList;
import org.h2.compress.Compressor;

/**
 * A btree page (a node or a leaf).
 * <p>
 * For nodes, the key at a given index is larger than the largest key of the
 * child at the same index.
 * <p>
 * File format: page length (including length): int check value: short map id:
 * varInt number of keys: varInt type: byte (0: leaf, 1: node; +2: compressed)
 * compressed: bytes saved (varInt) keys leaf: values (one for each key) node:
 * children (1 more than keys)
 */
public class Page {

    private static final IllegalArgumentException KEY_NOT_FOUND = new IllegalArgumentException(
            "Key not found");
    private static final IllegalArgumentException KEY_ALREADY_EXISTS = new IllegalArgumentException(
            "Key already exists");

    private final BtreeMap<?, ?> map;
    private final long version;
    private long pos;
    private Object[] keys;
    private Object[] values;
    private long[] children;
    private long[] childrenSize;
    private int cachedCompare;
    private long totalSize;

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
    static Page create(BtreeMap<?, ?> map, long version, Object[] keys,
            Object[] values, long[] children, long[] childrenSize,
            long totalSize) {
        Page p = new Page(map, version);
        p.pos = map.getStore().registerTempPage(p);
        p.keys = keys;
        p.values = values;
        p.children = children;
        p.childrenSize = childrenSize;
        p.totalSize = totalSize;
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

    private Page copyOnWrite(long writeVersion) {
        if (version == writeVersion) {
            return this;
        }
        getStore().removePage(pos);
        Page newPage = create(map, writeVersion, keys, values, children,
                childrenSize, totalSize);
        newPage.cachedCompare = cachedCompare;
        return newPage;
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

    /**
     * Get the position of the page
     *
     * @return the position
     */
    long getPos() {
        return pos;
    }

    /**
     * Get the value for the given key, or null if not found.
     *
     * @param key the key
     * @return the value or null
     */
    Object find(Object key) {
        int x = findKey(key);
        if (children != null) {
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            Page p = map.readPage(children[x]);
            return p.find(key);
        }
        if (x >= 0) {
            return values[x];
        }
        return null;
    }

    /**
     * Get the value for the given key, or null if not found.
     *
     * @param key the key
     * @return the page or null
     */
    Page findPage(Object key) {
        int x = findKey(key);
        if (children != null) {
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            Page p = map.readPage(children[x]);
            return p.findPage(key);
        }
        if (x >= 0) {
            return this;
        }
        return null;
    }

    private int findKey(Object key) {
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

    /**
     * Go to the first element for the given key.
     *
     * @param p the current page
     * @param parents the stack of parent page positions
     * @param key the key
     */
    static void min(Page p, ArrayList<CursorPos> parents, Object key) {
        while (p != null) {
            if (p.children != null) {
                int x = key == null ? -1 : p.findKey(key);
                if (x < 0) {
                    x = -x - 1;
                } else {
                    x++;
                }
                CursorPos c = new CursorPos();
                c.page = p;
                c.index = x;
                parents.add(c);
                p = p.map.readPage(p.children[x]);
            } else {
                int x = key == null ? 0 : p.findKey(key);
                if (x < 0) {
                    x = -x - 1;
                }
                CursorPos c = new CursorPos();
                c.page = p;
                c.index = x;
                parents.add(c);
                return;
            }
        }
    }

    /**
     * Get the next key.
     *
     * @param parents the stack of parent page positions
     * @return the next key
     */
    static Object nextKey(ArrayList<CursorPos> parents) {
        if (parents.size() == 0) {
            return null;
        }
        while (true) {
            // TODO performance: avoid remove/add pairs if possible
            CursorPos p = parents.remove(parents.size() - 1);
            int index = p.index++;
            if (index < p.page.keys.length) {
                parents.add(p);
                return p.page.keys[index];
            }
            while (true) {
                if (parents.size() == 0) {
                    return null;
                }
                p = parents.remove(parents.size() - 1);
                index = ++p.index;
                if (index < p.page.children.length) {
                    parents.add(p);
                    Page x = p.page;
                    x = x.map.readPage(x.children[index]);
                    min(x, parents, null);
                    break;
                }
            }
        }
    }

    private int keyCount() {
        return keys.length;
    }

    private boolean isLeaf() {
        return children == null;
    }

    private Page split(int at) {
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
        totalSize = keys.length;
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
        long[] aChildrenSize = new long[a + 1];
        long[] bChildrenSize = new long[b];
        System.arraycopy(childrenSize, 0, aChildrenSize, 0, a + 1);
        System.arraycopy(childrenSize, a + 1, bChildrenSize, 0, b);
        childrenSize = aChildrenSize;
        long t = 0;
        for (long x : aChildrenSize) {
            t += x;
        }
        totalSize = t;
        t = 0;
        for (long x : bChildrenSize) {
            t += x;
        }
        Page newPage = create(map, version, bKeys, null, bChildren,
                bChildrenSize, t);
        return newPage;
    }

    /**
     * Update a value for an existing key.
     *
     * @param map the map
     * @param p the page (may not be null)
     * @param writeVersion the write version
     * @param key the key
     * @param value the value
     * @return the root page
     * @throws InvalidArgumentException if this key does not exist (without
     *         stack trace)
     */
    static Page set(BtreeMap<?, ?> map, Page p, long writeVersion, Object key,
            Object value) {
        if (p == null) {
            throw KEY_NOT_FOUND;
        }
        int index = p.findKey(key);
        if (p.isLeaf()) {
            if (index < 0) {
                throw KEY_NOT_FOUND;
            }
            p = p.copyOnWrite(writeVersion);
            p.setValue(index, value);
            return p;
        }
        // it is a node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        Page c = map.readPage(p.children[index]);
        Page c2 = set(map, c, writeVersion, key, value);
        if (c != c2) {
            p = p.copyOnWrite(writeVersion);
            p.setChild(index, c2.getPos(), c2.getPos());
        }
        return p;
    }

    /**
     * Add a new key-value pair.
     *
     * @param map the map
     * @param p the page (may be null)
     * @param writeVersion the write version
     * @param key the key
     * @param value the value
     * @return the root page
     * @throws InvalidArgumentException if this key already exists (without
     *         stack trace)
     */
    static Page add(BtreeMap<?, ?> map, Page p, long writeVersion, Object key,
            Object value) {
        if (p == null) {
            Object[] keys = { key };
            Object[] values = { value };
            p = create(map, writeVersion, keys, values, null, null, 1);
            return p;
        }
        if (p.keyCount() >= map.getStore().getMaxPageSize()) {
            // only possible if this is the root,
            // otherwise we would have split earlier
            p = p.copyOnWrite(writeVersion);
            int at = p.keyCount() / 2;
            long totalSize = p.getTotalSize();
            Object k = p.keys[at];
            Page split = p.split(at);
            Object[] keys = { k };
            long[] children = { p.getPos(), split.getPos() };
            long[] childrenSize = { p.getTotalSize(), split.getTotalSize() };
            p = create(map, writeVersion, keys, null, children, childrenSize,
                    totalSize);
            // now p is a node; insert continues
        } else if (p.isLeaf()) {
            int index = p.findKey(key);
            if (index >= 0) {
                throw KEY_ALREADY_EXISTS;
            }
            index = -index - 1;
            p = p.copyOnWrite(writeVersion);
            p.insert(index, key, value, 0, 0);
            return p;
        }
        // p is a node
        int index = p.findKey(key);
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        Page c = map.readPage(p.children[index]);
        if (c.keyCount() >= map.getStore().getMaxPageSize()) {
            // split on the way down
            c = c.copyOnWrite(writeVersion);
            int at = c.keyCount() / 2;
            Object k = c.keys[at];
            Page split = c.split(at);
            p = p.copyOnWrite(writeVersion);
            p.setChild(index, c.getPos(), c.getTotalSize());
            p.insert(index, k, null, split.getPos(), split.getTotalSize());
            // now we are not sure where to add
            return add(map, p, writeVersion, key, value);
        }
        Page c2 = add(map, c, writeVersion, key, value);
        p = p.copyOnWrite(writeVersion);
        // the child might be the same, but not the size
        p.setChild(index, c2.getPos(), c2.getTotalSize());
        return p;
    }

    long getTotalSize() {
        if (BtreeMapStore.ASSERT) {
            long check = 0;
            if (isLeaf()) {
                check = keys.length;
            } else {
                for (long x : childrenSize) {
                    check += x;
                }
            }
            if (check != totalSize) {
                throw new AssertionError("Expected: " + check + " got: "
                        + totalSize);
            }
        }
        return totalSize;
    }

    private void setChild(int index, long pos, long childSize) {
        if (pos != children[index]) {
            long[] newChildren = new long[children.length];
            System.arraycopy(children, 0, newChildren, 0, newChildren.length);
            newChildren[index] = pos;
            children = newChildren;
        }
        if (childSize != childrenSize[index]) {
            long[] newChildrenSize = new long[childrenSize.length];
            System.arraycopy(childrenSize, 0, newChildrenSize, 0,
                    newChildrenSize.length);
            newChildrenSize[index] = childSize;
            totalSize += newChildrenSize[index] - childrenSize[index];
            childrenSize = newChildrenSize;
        }
    }

    private void setValue(int index, Object value) {
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
                    getStore().removePage(c);
                } else {
                    map.readPage(c).removeAllRecursive();
                }
            }
        }
        getStore().removePage(pos);
    }

    /**
     * Remove an existing key-value pair.
     *
     * @param p the page (may not be null)
     * @param writeVersion the write version
     * @param key the key
     * @return the new root page (null if empty)
     * @throws InvalidArgumentException if not found (without stack trace)
     */
    static Page removeExisting(Page p, long writeVersion, Object key) {
        if (p == null) {
            throw KEY_NOT_FOUND;
        }
        int index = p.findKey(key);
        if (p.isLeaf()) {
            if (index >= 0) {
                if (p.keyCount() == 1) {
                    p.getStore().removePage(p.pos);
                    return null;
                }
                p = p.copyOnWrite(writeVersion);
                p.remove(index);
            } else {
                throw KEY_NOT_FOUND;
            }
            return p;
        }
        // node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        Page c = p.map.readPage(p.children[index]);
        Page c2 = removeExisting(c, writeVersion, key);
        p = p.copyOnWrite(writeVersion);
        if (c2 == null) {
            // this child was deleted
            p.remove(index);
            if (p.keyCount() == 0) {
                p.getStore().removePage(p.pos);
                p = p.map.readPage(p.children[0]);
            }
        } else {
            p.setChild(index, c2.getPos(), c2.getTotalSize());
        }
        return p;
    }

    private void insert(int index, Object key, Object value, long child,
            long childSize) {
        Object[] newKeys = new Object[keys.length + 1];
        DataUtils.copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = key;
        keys = newKeys;
        if (values != null) {
            Object[] newValues = new Object[values.length + 1];
            DataUtils.copyWithGap(values, newValues, values.length, index);
            newValues[index] = value;
            values = newValues;
            totalSize++;
        }
        if (children != null) {
            long[] newChildren = new long[children.length + 1];
            DataUtils.copyWithGap(children, newChildren, children.length,
                    index + 1);
            newChildren[index + 1] = child;
            children = newChildren;
            long[] newChildrenSize = new long[childrenSize.length + 1];
            DataUtils.copyWithGap(childrenSize, newChildrenSize,
                    childrenSize.length, index + 1);
            newChildrenSize[index + 1] = childSize;
            childrenSize = newChildrenSize;
            totalSize += childSize;
        }
    }

    private void remove(int index) {
        Object[] newKeys = new Object[keys.length - 1];
        int keyIndex = index >= keys.length ? index - 1 : index;
        DataUtils.copyExcept(keys, newKeys, keys.length, keyIndex);
        keys = newKeys;
        if (values != null) {
            Object[] newValues = new Object[values.length - 1];
            DataUtils.copyExcept(values, newValues, values.length, index);
            values = newValues;
            totalSize--;
        }
        if (children != null) {
            long sizeOffset = childrenSize[index];
            long[] newChildren = new long[children.length - 1];
            DataUtils.copyExcept(children, newChildren, children.length, index);
            children = newChildren;
            long[] newChildrenSize = new long[childrenSize.length - 1];
            DataUtils.copyExcept(childrenSize, newChildrenSize,
                    childrenSize.length, index);
            childrenSize = newChildrenSize;
            totalSize -= sizeOffset;
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
        int len = DataUtils.readVarInt(buff);
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(map.getId())
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength)
                ^ DataUtils.getCheckValue(len);
        if (check != (short) checkTest) {
            throw new RuntimeException("Error in check value, expected "
                    + checkTest + " got " + check);
        }
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
            childrenSize = new long[len + 1];
            long total = 0;
            for (int i = 0; i <= len; i++) {
                long s = DataUtils.readVarLong(buff);
                total += s;
                childrenSize[i] = s;
            }
            totalSize = total;
        } else {
            values = new Object[len];
            for (int i = 0; i < len; i++) {
                values[i] = map.getValueType().read(buff);
            }
            totalSize = len;
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
                DataUtils.writeVarLong(buff, childrenSize[i]);
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
                ^ DataUtils.getCheckValue(map.getId())
                ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength)
                ^ DataUtils.getCheckValue(len);
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

    BtreeMapStore getStore() {
        return map.getStore();
    }

    long getVersion() {
        return version;
    }

}
