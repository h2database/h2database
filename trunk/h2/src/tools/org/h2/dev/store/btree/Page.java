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
 * A btree page implementation.
 */
class Page {

    private static final int MAX_SIZE = 20;

    private final BtreeMap<?, ?> map;
    private long id;
    private long storedId;
    private long transaction;
    private Object[] keys;
    private Object[] values;
    private long[] children;

    private Page(BtreeMap<?, ?> map) {
        this.map = map;
    }

    /**
     * Create a new page.
     *
     * @param map the map
     * @param key the keys
     * @param values the values
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
        p.id = p.storedId = id;
        p.read(buff);
        return p;
    }

    private Page copyOnWrite() {
        long t = map.getTransaction();
        if (transaction == t) {
            return this;
        }
        map.removePage(id);
        Page p2 = create(map, keys, values, children);
        p2.transaction = t;
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
     * Set the page id.
     *
     * @param id the new id
     */
    void setId(long id) {
        this.id = id;
    }

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

    private int findKey(Object key) {
        int low = 0, high = keys.length - 1;
        while (low <= high) {
            int x = (low + high) >>> 1;
            int compare = map.compare(key, keys[x]);
            if (compare > 0) {
                low = x + 1;
            } else if (compare < 0) {
                high = x - 1;
            } else {
                return x;
            }
        }
        return -(low + 1);
    }

    /**
     * A position in a cursor
     */
    static class CursorPos {
        Page page;
        int index;
    }

    static void min(Page p, ArrayList<CursorPos> parents, Object key) {
        int todo;
        while (p != null) {
            int x = key == null ? 0 : p.findKey(key);
            if (p.children != null) {
                if (x < 0) {
                    x = -x - 1;
                } else {
                    x++;
                }
                p = p.map.readPage(p.children[x]);
                CursorPos c = new CursorPos();
                c.page = p;
                c.index = x;
                parents.add(c);
            } else {
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

    public static Object nextKey(ArrayList<CursorPos> parents) {
        int todoTest;
        if (parents.size() == 0) {
            return null;
        }
        while (true) {
            // TODO avoid remove/add pairs is possible
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
                index = p.index++;
                if (index < p.page.children.length) {
                    parents.add(p);
                    min(p.page, parents, null);
                    break;
                }
            }
        }
    }

    private int size() {
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
     * @param data the value
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
                if (p.size() >= MAX_SIZE) {
                    // TODO almost duplicate code
                    int pos = p.size() / 2;
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
                    p.values[index] = value;
                    break;
                }
                index = -index - 1;
                p.insert(index, key, value, 0);
                if (p.size() >= MAX_SIZE) {
                    int pos = p.size() / 2;
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
            }
            parent = p;
            parentIndex = index;
            p = map.readPage(p.children[index]);
            p = p.copyOnWrite();
        }
        return top;
    }

    /**
     * Remove a key-value pair.
     *
     * @param p the root node
     * @param key the key
     * @return the new root node
     */
    static Page remove(Page p, Object key) {
        // TODO avoid separate lookup
        if (p.find(key) == null) {
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
            int index = p.findKey(key);
            if (p.isLeaf()) {
                if (index >= 0) {
                    p.remove(index);
                } else {
                    // not found?
                    throw new RuntimeException("Not found: " + key);
                }
                if (p.size() == 0) {
                    if (parent != null) {
                        parent.remove(parentIndex);
                        // TODO recursive, or on the way down
                    }
                }
                break;
            }
            if (index < 0) {
                index = -index - 1;
            }
            parent = p;
            parentIndex = index;
            p = p.map.readPage(p.children[index]);
            p = p.copyOnWrite();
        }
        return top;
    }

    /**
     * Remove a key.
     *
     * @param key the key
     * @return the new page or null if the page is now empty
     */
    private Page remove(Object key) {
        int index = findKey(key);
        if (isLeaf()) {
            if (index < 0) {
                // not found
                return this;
            }
        }

        Page p = copyOnWrite();
        p = p.remove(key);
        return p;
    }

    void insert(int index, Object key, Object value, long child) {
        Object[] newKeys = new Object[keys.length + 1];
        copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = key;
        keys = newKeys;
        if (values != null) {
            Object[] newValues = new Object[values.length + 1];
            copyWithGap(values, newValues, values.length, index);
            newValues[index] = value;
            values = newValues;
        }
        if (children != null) {
            long[] newChildren = new long[children.length + 1];
            copyWithGap(children, newChildren, children.length, index + 1);
            newChildren[index + 1] = child;
            children = newChildren;
        }
    }

    void remove(int index) {
        Object[] newKeys = new Object[keys.length - 1];
        copyExcept(keys, newKeys, keys.length, index);
        keys = newKeys;
        if (values != null) {
            Object[] newValues = new Object[values.length - 1];
            copyExcept(values, newValues, values.length, index);
            values = newValues;
        }
        if (children != null) {
            long[] newChildren = new long[children.length - 1];
            copyExcept(children, newChildren, children.length, index);
            children = newChildren;
        }
    }

//        int x = findKey(key);
//        if (x >= 0) {
//            return values[x];
//        }
//        x = -x - 1;
//        Page p = map.readPage(children[x]);
//        return p.find(key);
//        return null;
//    }

    private void read(ByteBuffer buff) {
        boolean node = buff.get() == 1;
        if (node) {
            int len = BtreeMap.readVarInt(buff);
            children = new long[len];
            keys = new Object[len - 1];
            for (int i = 0; i < len; i++) {
                children[i] = buff.getLong();
                if (i < keys.length) {
                    keys[i] = map.getKeyType().read(buff);
                }
            }
        } else {
            int len = BtreeMap.readVarInt(buff);
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
    void write(ByteBuffer buff) {
        if (children != null) {
            buff.put((byte) 1);
            int size = children.length;
            BtreeMap.writeVarInt(buff, size);
            for (int i = 0; i < size; i++) {
                long c = map.readPage(children[i]).storedId;
                buff.putLong(c);
                if (i < keys.length) {
                    map.getKeyType().write(buff, keys[i]);
                }
            }
        } else {
            buff.put((byte) 0);
            int size = keys.length;
            BtreeMap.writeVarInt(buff, size);
            for (int i = 0; i < size; i++) {
                map.getKeyType().write(buff, keys[i]);
                map.getValueType().write(buff, values[i]);
            }
        }
    }

    /**
     * Get the length in bytes, including temporary children.
     *
     * @return the length
     */
    int lengthIncludingTempChildren() {
        int len = length();
if (len > 1024) {
    int test;
    System.out.println("??");
}
        if (children != null) {
            int size = children.length;
            for (int i = 0; i < size; i++) {
                long c = children[i];
                if (c < 0) {
                    len += map.readPage(c).lengthIncludingTempChildren();
                }
            }
        }
        return len;
    }

    long updatePageIds(long pageId) {
        this.storedId = pageId;
        pageId += length();
        if (children != null) {
            int size = children.length;
            for (int i = 0; i < size; i++) {
                long c = children[i];
                if (c < 0) {
                    pageId = map.readPage(c).updatePageIds(pageId);
                }
            }
        }
        return pageId;
    }

    long storeTemp(ByteBuffer buff) {
        write(buff);
        if (children != null) {
            int size = children.length;
            for (int i = 0; i < size; i++) {
                long c = children[i];
                if (c < 0) {
                    children[i] = map.readPage(c).storeTemp(buff);
                }
            }
        }
        this.id = storedId;
        return id;
    }

    int countTemp() {
        int count = 1;
        if (children != null) {
            int size = children.length;
            for (int i = 0; i < size; i++) {
                long c = children[i];
                if (c < 0) {
                    count += map.readPage(c).countTemp();
                }
            }
        }
        return count;
    }

    /**
     * Get the length in bytes.
     *
     * @return the length
     */
    int length() {
        int len = 1;
        if (children != null) {
            int size = children.length;
            len += BtreeMap.getVarIntLen(size);
            for (int i = 0; i < size; i++) {
                len += 8;
                if (i < keys.length) {
                    len += map.getKeyType().length(keys[i]);
                }
            }
        } else {
            int size = keys.length;
            len += BtreeMap.getVarIntLen(size);
            for (int i = 0; i < size; i++) {
                len += map.getKeyType().length(keys[i]);
                len += map.getValueType().length(values[i]);
            }
        }
        return len;
    }

    private static void copyWithGap(Object src, Object dst, int oldSize, int gapIndex) {
        if (gapIndex > 0) {
            System.arraycopy(src, 0, dst, 0, gapIndex);
        }
        if (gapIndex < oldSize) {
            System.arraycopy(src, gapIndex, dst, gapIndex + 1, oldSize - gapIndex);
        }
    }

    private static void copyExcept(Object src, Object dst, int oldSize, int removeIndex) {
        if (removeIndex > 0 && oldSize > 0) {
            System.arraycopy(src, 0, dst, 0, removeIndex);
        }
        if (removeIndex < oldSize) {
            System.arraycopy(src, removeIndex + 1, dst, removeIndex, oldSize - removeIndex - 1);
        }
    }

    public Object getKey(int index) {
        return keys[index];
    }

}
