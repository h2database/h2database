/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * A btree page (a node or a leaf).
 * <p>
 * For nodes, the key at a given index is larger than the largest key of the
 * child at the same index.
 */
public class Page {

    private final BtreeMap<?, ?> map;
    private long id;
    private long transaction;
    private Object[] keys;
    private Object[] values;
    private long[] children;
    private int cachedCompare;

    private Page(BtreeMap<?, ?> map) {
        this.map = map;
    }

    /**
     * Create a new page.
     *
     * @param map the map
     * @param keys the keys
     * @param values the values
     * @param children the children
     * @return the page
     */
    static Page create(BtreeMap<?, ?> map, Object[] keys, Object[] values, long[] children) {
        Page p = new Page(map);
        p.keys = keys;
        p.values = values;
        p.children = children;
        p.transaction = map.getTransaction();
        p.id = map.registerTempPage(p);
        return p;
    }

    /**
     * Read a page.
     *
     * @param map the map
     * @param id the page id
     * @param buff the source buffer
     * @return the page
     */
    static Page read(BtreeMap<?, ?> map, long id, ByteBuffer buff) {
        Page p = new Page(map);
        p.id = id;
        p.read(buff);
        return p;
    }

    private Page copyOnWrite() {
        // TODO avoid creating objects (arrays) that are then not used
        // possibly add shortcut for copy with add / copy with remove
        long t = map.getTransaction();
        if (transaction == t) {
            return this;
        }
        map.removePage(id);
        Page p2 = create(map, keys, values, children);
        p2.transaction = t;
        p2.cachedCompare = cachedCompare;
        return p2;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("nodeId: ").append(id).append("\n");
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
     * Get the page id.
     *
     * @return the page id
     */
    long getId() {
        return id;
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
            // TODO avoid remove/add pairs if possible
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
        Page newPage = create(map, bKeys, bValues, null);
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
        Page newPage = create(map, bKeys, null, bChildren);
        return newPage;
    }

    /**
     * Add or replace the key-value pair.
     *
     * @param map the map
     * @param p the page
     * @param key the key
     * @param value the value
     * @return the root page
     */
    static Page put(BtreeMap<?, ?> map, Page p, Object key, Object value) {
        if (p == null) {
            Object[] keys = { key };
            Object[] values = { value };
            p = create(map, keys, values, null);
            return p;
        }
        p = p.copyOnWrite();
        Page top = p;
        Page parent = null;
        int parentIndex = 0;
        while (true) {
            if (parent != null) {
                parent.children[parentIndex] = p.id;
            }
            if (!p.isLeaf()) {
                if (p.keyCount() >= map.getMaxPageSize()) {
                    // TODO almost duplicate code
                    int pos = p.keyCount() / 2;
                    Object k = p.keys[pos];
                    Page split = p.splitNode(pos);
                    if (parent == null) {
                        Object[] keys = { k };
                        long[] children = { p.getId(), split.getId() };
                        top = create(map, keys, null, children);
                        p = top;
                    } else {
                        parent.insert(parentIndex, k, null, split.getId());
                        p = parent;
                    }
                }
            }
            int index = p.findKey(key);
            if (p.isLeaf()) {
                if (index >= 0) {
                    // create a copy
                    // TODO might not be required, but needs a "modified" flag
                    Object[] v2 = new Object[p.values.length];
                    System.arraycopy(p.values, 0, v2, 0, v2.length);
                    p.values = v2;
                    p.values[index] = value;
                    break;
                }
                index = -index - 1;
                p.insert(index, key, value, 0);
                if (p.keyCount() >= map.getMaxPageSize()) {
                    int pos = p.keyCount() / 2;
                    Object k = p.keys[pos];
                    Page split = p.splitLeaf(pos);
                    if (parent == null) {
                        Object[] keys = { k };
                        long[] children = { p.getId(), split.getId() };
                        top = create(map, keys, null, children);
                    } else {
                        parent.insert(parentIndex, k, null, split.getId());
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
            p = p.copyOnWrite();
        }
        return top;
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
        map.removePage(id);
    }

    /**
     * Remove a key-value pair.
     *
     * @param p the root node
     * @param key the key
     * @return the new root node
     */
    static Page remove(Page p, Object key) {
        int index = p.findKey(key);
        if (p.isLeaf()) {
            if (index >= 0) {
                if (p.keyCount() == 1) {
                    p.map.removePage(p.id);
                    return null;
                }
                p = p.copyOnWrite();
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
        Page c2 = remove(c, key);
        if (c2 == c) {
            // not found
        } else if (c2 == null) {
            // child was deleted
            p = p.copyOnWrite();
            p.remove(index);
            if (p.keyCount() == 0) {
                p.map.removePage(p.id);
                p = p.map.readPage(p.children[0]);
            }
        } else {
            p = p.copyOnWrite();
            p.children[index] = c2.id;
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

    private void read(ByteBuffer buff) {
        // len
        buff.getInt();
        long id = buff.getLong();
        if (id != map.getId()) {
            throw new RuntimeException("Page map id missmatch, expected " + map.getId() + " got " + id);
        }
        boolean node = buff.get() == 1;
        if (node) {
            int len = DataUtils.readVarInt(buff);
            children = new long[len];
            keys = new Object[len - 1];
            for (int i = 0; i < len; i++) {
                children[i] = buff.getLong();
            }
            for (int i = 0; i < len - 1; i++) {
                keys[i] = map.getKeyType().read(buff);
            }
        } else {
            int len = DataUtils.readVarInt(buff);
            keys = new Object[len];
            values = new Object[len];
            for (int i = 0; i < len; i++) {
                keys[i] = map.getKeyType().read(buff);
                values[i] = map.getValueType().read(buff);
            }
        }
    }

    /**
     * Store the page.
     *
     * @param buff the target buffer
     */
    private void write(ByteBuffer buff) {
        int pos = buff.position();
        buff.putInt(0);
        buff.putLong(map.getId());
        if (children != null) {
            buff.put((byte) 1);
            int len = children.length;
            DataUtils.writeVarInt(buff, len);
            for (int i = 0; i < len; i++) {
                buff.putLong(children[i]);
            }
            for (int i = 0; i < len - 1; i++) {
                map.getKeyType().write(buff, keys[i]);
            }
        } else {
            buff.put((byte) 0);
            int len = keys.length;
            DataUtils.writeVarInt(buff, len);
            for (int i = 0; i < len; i++) {
                map.getKeyType().write(buff, keys[i]);
                map.getValueType().write(buff, values[i]);
            }
        }
        int len = buff.position() - pos;
        buff.putInt(pos, len);
    }

    /**
     * Get the maximum length in bytes to store temporary records, recursively.
     *
     * @param pageId the new page id
     * @return the next page id
     */
    int getMaxLengthTempRecursive() {
        int maxLength = 4 + 8 + 1;
        if (children != null) {
            int len = children.length;
            maxLength += DataUtils.MAX_VAR_INT_LEN;
            maxLength += 8 * len;
            for (int i = 0; i < len - 1; i++) {
                maxLength += map.getKeyType().getMaxLength(keys[i]);
            }
            for (int i = 0; i < len; i++) {
                long c = children[i];
                if (c < 0) {
                    maxLength += map.readPage(c).getMaxLengthTempRecursive();
                }
            }
        } else {
            int len = keys.length;
            maxLength += DataUtils.MAX_VAR_INT_LEN;
            for (int i = 0; i < len; i++) {
                maxLength += map.getKeyType().getMaxLength(keys[i]);
                maxLength += map.getValueType().getMaxLength(values[i]);
            }
        }
        return maxLength;
    }

    /**
     * Store this page and all children that are changed,
     * in reverse order, and update the id and child ids.
     *
     * @param buff the target buffer
     * @param idOffset the offset of the id
     * @return the page id
     */
    long writeTempRecursive(ByteBuffer buff, long idOffset) {
        if (children != null) {
            int len = children.length;
            for (int i = 0; i < len; i++) {
                long c = children[i];
                if (c < 0) {
                    children[i] = map.readPage(c).writeTempRecursive(buff, idOffset);
                }
            }
        }
        this.id = idOffset + buff.position();
        write(buff);
        return id;
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

}
