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
 * File format:
 * page length (including length): int
 * check value: short
 * number of keys: varInt
 * type: byte (0: leaf, 1: node; +2: compressed)
 * compressed: bytes saved (varInt)
 * keys
 * leaf: values (one for each key)
 * node: children (1 more than keys)
 */
public class Page {

    private final BtreeMap<?, ?> map;
    private final long version;
    private long pos;
    private Object[] keys;
    private Object[] values;
    private long[] children;
    private int cachedCompare;

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
    static Page create(BtreeMap<?, ?> map, long version, Object[] keys, Object[] values, long[] children) {
        Page p = new Page(map, version);
        p.keys = keys;
        p.values = values;
        p.children = children;
        p.pos = map.getStore().registerTempPage(p);
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
    static Page read(FileChannel file, BtreeMap<?, ?> map, long filePos, long pos) {
        int maxLength = DataUtils.getMaxLength(pos), length = maxLength;
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
        int chunkId = DataUtils.getChunkId(pos);
        int offset = DataUtils.getOffset(pos);
        p.read(buff, chunkId, offset, maxLength);
        return p;
    }

    private Page copyOnWrite(long writeVersion) {
        if (version == writeVersion) {
            return this;
        }
        getStore().removePage(pos);
        Page newPage = create(map, writeVersion, keys, values, children);
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
        Page newPage = create(map, version, bKeys, bValues, null);
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
        Page newPage = create(map, version, bKeys, null, bChildren);
        return newPage;
    }

    /**
     * Add or replace the key-value pair.
     *
     * @param map the map
     * @param p the page
     * @param writeVersion the write version
     * @param key the key
     * @param value the value
     * @return the root page
     */
    static Page put(BtreeMap<?, ?> map, Page p, long writeVersion, Object key, Object value) {
        if (p == null) {
            Object[] keys = { key };
            Object[] values = { value };
            p = create(map, writeVersion, keys, values, null);
            return p;
        }
        p = p.copyOnWrite(writeVersion);
        Page top = p;
        Page parent = null;
        int parentIndex = 0;
        while (true) {
            if (parent != null) {
                parent.setChild(parentIndex, p.pos);
            }
            if (!p.isLeaf()) {
                if (p.keyCount() >= map.getStore().getMaxPageSize()) {
                    // TODO almost duplicate code
                    int pos = p.keyCount() / 2;
                    Object k = p.keys[pos];
                    Page split = p.splitNode(pos);
                    if (parent == null) {
                        Object[] keys = { k };
                        long[] children = { p.getPos(), split.getPos() };
                        top = create(map, writeVersion, keys, null, children);
                        p = top;
                    } else {
                        parent.insert(parentIndex, k, null, split.getPos());
                        p = parent;
                    }
                }
            }
            int index = p.findKey(key);
            if (p.isLeaf()) {
                if (index >= 0) {
                    p.setValue(index, value);
                    break;
                }
                index = -index - 1;
                p.insert(index, key, value, 0);
                if (p.keyCount() >= map.getStore().getMaxPageSize()) {
                    int pos = p.keyCount() / 2;
                    Object k = p.keys[pos];
                    Page split = p.splitLeaf(pos);
                    if (parent == null) {
                        Object[] keys = { k };
                        long[] children = { p.getPos(), split.getPos() };
                        top = create(map, writeVersion, keys, null, children);
                    } else {
                        parent.insert(parentIndex, k, null, split.getPos());
                    }
                }
                break;
            }
            if (index < 0) {
                index = -index - 1;
            } else {
                index++;
            }
            parent = p;
            parentIndex = index;
            p = map.readPage(p.children[index]);
            p = p.copyOnWrite(writeVersion);
        }
        return top;
    }

    private void setChild(int index, long value) {
        long[] newChildren = new long[children.length];
        System.arraycopy(children, 0, newChildren, 0, newChildren.length);
        newChildren[index] = value;
        children = newChildren;
    }

    private void setValue(int index, Object value) {
        // create a copy - not always required, but avoid unnecessary cloning
        // would require a "modified" flag
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
                map.readPage(c).removeAllRecursive();
            }
        }
        getStore().removePage(pos);
    }

    /**
     * Remove a key-value pair.
     *
     * @param p the root page
     * @param writeVersion the write version
     * @param key the key
     * @return the new root page
     */
    static Page remove(Page p, long writeVersion, Object key) {
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
                // not found
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
        Page c2 = remove(c, writeVersion, key);
        if (c2 == c) {
            // not found
        } else if (c2 == null) {
            // child was deleted
            p = p.copyOnWrite(writeVersion);
            p.remove(index);
            if (p.keyCount() == 0) {
                p.getStore().removePage(p.pos);
                p = p.map.readPage(p.children[0]);
            }
        } else {
            p = p.copyOnWrite(writeVersion);
            p.setChild(index, c2.pos);
        }
        return p;
    }

    private void insert(int index, Object key, Object value, long child) {
        Object[] newKeys = new Object[keys.length + 1];
        DataUtils.copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = key;
        keys = newKeys;
        if (values != null) {
            Object[] newValues = new Object[values.length + 1];
            DataUtils.copyWithGap(values, newValues, values.length, index);
            newValues[index] = value;
            values = newValues;
        }
        if (children != null) {
            long[] newChildren = new long[children.length + 1];
            DataUtils.copyWithGap(children, newChildren, children.length, index + 1);
            newChildren[index + 1] = child;
            children = newChildren;
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
        }
        if (children != null) {
            long[] newChildren = new long[children.length - 1];
            DataUtils.copyExcept(children, newChildren, children.length, index);
            children = newChildren;
        }
    }

    private void read(ByteBuffer buff, int chunkId, int offset, int maxLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        if (pageLength > maxLength) {
            throw new RuntimeException("Length too large, expected =< " + maxLength + " got " + pageLength);
        }
        short check = buff.getShort();
        int len = DataUtils.readVarInt(buff);
        int checkTest = DataUtils.getCheckValue(chunkId) ^
                DataUtils.getCheckValue(map.getId()) ^
                DataUtils.getCheckValue(offset) ^
                DataUtils.getCheckValue(pageLength) ^
                DataUtils.getCheckValue(len);
        if (check != (short) checkTest) {
            throw new RuntimeException("Error in check value, expected " + checkTest + " got " + check);
        }
        keys = new Object[len];
        int type = buff.get();
        boolean node = (type & 1) != 0;
        boolean compressed = (type & 2) != 0;
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
        } else {
            values = new Object[len];
            for (int i = 0; i < len; i++) {
                values[i] = map.getValueType().read(buff);
            }
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
        int len = keys.length;
        DataUtils.writeVarInt(buff, len);
        Compressor compressor = map.getStore().getCompressor();
        int type = children != null ? 1 : 0;
        buff.put((byte) type);
        int compressStart = buff.position();
        for (int i = 0; i < len; i++) {
            map.getKeyType().write(buff, keys[i]);
        }
        if (type == 1) {
            for (int i = 0; i < len + 1; i++) {
                buff.putLong(children[i]);
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
                buff.put((byte) (type + 2));
                DataUtils.writeVarInt(buff, expLen - compLen);
                buff.put(comp,  0, compLen);
            }
        }
        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int check =
                DataUtils.getCheckValue(chunkId) ^
                DataUtils.getCheckValue(map.getId()) ^
                DataUtils.getCheckValue(start) ^
                DataUtils.getCheckValue(pageLength) ^
                DataUtils.getCheckValue(len);
        buff.putShort(start + 4, (short) check);
        this.pos = DataUtils.getPos(chunkId, start, pageLength);
    }

    /**
     * Get the maximum length in bytes to store temporary records, recursively.
     *
     * @return the next page id
     */
    int getMaxLengthTempRecursive() {
        // length, check, key length, type
        int maxLength = 4 + 2 + DataUtils.MAX_VAR_INT_LEN + 1;
        int len = keys.length;
        for (int i = 0; i < len; i++) {
            maxLength += map.getKeyType().getMaxLength(keys[i]);
        }
        if (children != null) {
            maxLength += 8 * len;
            for (int i = 0; i < len + 1; i++) {
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
                    children[i] = map.readPage(c).writeTempRecursive(buff, chunkId);
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
