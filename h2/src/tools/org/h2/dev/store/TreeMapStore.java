/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.TreeSet;
import org.h2.store.fs.FilePath;
import org.h2.util.SmallLRUCache;

/*

file format:

header
header
[ transaction log | data ] *

header:
# H3 store #
pageSize=4096
r=1

data:
'd' [length] root ...

transaction log:
't' [length] ...

todo:

- garbage collection

- use page checksums

- compress chunks

- encode length in pos (1=32, 2=128, 3=512,...)

- don't use any 't' blocks

- floating header (avoid duplicate header)
  for each chunk, store chunk (a counter)
  for each page, store chunk id and offset to root
  for each chunk, store position of expected next chunks

 */

/**
 * A persistent tree map.
 */
public class TreeMapStore {

    private final KeyType keyType;
    private final ValueType valueType;
    private final String fileName;
    private FileChannel file;
    private int pageSize = 4 * 1024;
    private long rootPos;
    private HashMap<Long, Node> cache = SmallLRUCache.newInstance(50000);
    private TreeSet<Block> blocks = new TreeSet<Block>();

    // TODO use an int instead? (with rollover to 0)
    private long transaction;

    private int tempNodeId;
    private long storePos;

    private Node root;

    private int loadCount;

    private TreeMapStore(String fileName, Class<?> keyClass, Class<?> valueClass) {
        this.fileName = fileName;
        if (keyClass == Integer.class) {
            keyType = new IntegerType();
        } else if (keyClass == String.class) {
            keyType = new StringType();
        } else {
            throw new RuntimeException("Unsupported key class " + keyClass.toString());
        }
        if (valueClass == Integer.class) {
            valueType = new IntegerType();
        } else if (valueClass == String.class) {
            valueType = new StringType();
        } else {
            throw new RuntimeException("Unsupported value class " + keyClass.toString());
        }
    }

    static TreeMapStore open(String fileName, Class<?> keyClass, Class<?> valueClass) {
        TreeMapStore s = new TreeMapStore(fileName, keyClass, valueClass);
        s.open();
        return s;
    }

    void open() {
        new File(fileName).getParentFile().mkdirs();
        try {
            file = FilePathCache.wrap(FilePath.get(fileName).open("rw"));
            if (file.size() == 0) {
                writeHeader();
                storePos = pageSize * 2;
            } else {
                readHeader();
                if (rootPos > 0) {
                    root = loadNode(rootPos);
                }
            }
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private void writeHeader() {
        try {
            ByteBuffer header = ByteBuffer.wrap((
                "# H2 1.5\n" +
                "read-version: 1\n" +
                "write-version: 1\n" +
                "root: " + rootPos + "\n" +
                "transaction: " + transaction + "\n" +
                "storePos: " + storePos + "\n").getBytes());
            file.position(0);
            file.write(header);
            file.position(pageSize);
            file.write(header);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private void readHeader() {
        try {
            file.position(0);
            byte[] header = new byte[pageSize];
            // TODO read fully; read both headers
            file.read(ByteBuffer.wrap(header));
            Properties prop = new Properties();
            prop.load(new ByteArrayInputStream(header));
            rootPos = Long.parseLong(prop.get("root").toString());
            storePos = Long.parseLong(prop.get("storePos").toString());
            transaction = Long.parseLong(prop.get("transaction").toString());
        } catch (Exception e) {
            throw convert(e);
        }
    }

    public String toString() {
        return "cache size: " + cache.size() + " loadCount: " + this.loadCount + " " + blocks;
    }

    private static RuntimeException convert(Exception e) {
        throw new RuntimeException("Exception: " + e, e);
    }

    public void close() {
        if (root != null && root.getId() < 0) {
            store();
        }
        if (file != null) {
            try {
                file.close();
            } catch (Exception e) {
                file = null;
                throw convert(e);
            }
        }
    }

    private long updateId(Node n, long offset) {
        n.setId(offset);
        cache.put(offset, n);
        offset += n.length();
        if (n.getLeftId() < 0) {
            offset = updateId(n.getLeft(), offset);
        }
        if (n.getRightId() < 0) {
            offset = updateId(n.getRight(), offset);
        }
        return offset;
    }

    private int store(ByteBuffer buff, Node n) {
        Node left = n.getLeftId() < 0 ? n.getLeft() : null;
        if (left != null) {
            n.setLeftId(left.getId());
        }
        Node right = n.getRightId() < 0 ? n.getRight() : null;
        if (right != null) {
            n.setRightId(right.getId());
        }
        int count = 1;
        n.store(buff);
        if (left != null) {
            count += store(buff, left);
        }
        if (right != null) {
            count += store(buff, right);
        }
        return count;
    }

    void store() {
        if (root == null || root.getId() >= 0) {
            // TODO truncate file if empty
            return;
        }
        commit();
        Block b = new Block(storePos);
        b.transaction = transaction;
        long end = updateId(root, storePos + 1);
        ByteBuffer buff = ByteBuffer.allocate((int) (end - storePos));
        buff.put((byte) 'd');
        b.entryCount = store(buff, root);
        b.liveCount = b.entryCount;
        b.length = buff.limit();
        blocks.add(b);
        if (buff.hasRemaining()) {
            throw new RuntimeException("remaining: " + buff.remaining());
        }
        buff.rewind();
        try {
            file.position(storePos);
            file.write(buff);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        storePos = end;
        rootPos = root.getId();
        writeHeader();
        tempNodeId = 0;
    }

    public long getTransaction() {
        return transaction;
    }

    public long nextTempNodeId() {
        return -(++tempNodeId);
    }

    public long commit() {
        return ++transaction;
    }

    public void add(Object key, Object data) {
        root = Node.add(this, root, key, data);
    }

    public void remove(Object key) {
        root = Node.remove(root, key);
    }

    public Object find(Object key) {
        Node n = Node.findNode(root, key);
        return n == null ? null : n.getData();
    }

    public Object getRoot() {
        return root;
    }

    public Node loadNode(long id) {
        Node n = cache.get(id);
        if (n == null) {
            try {
                file.position(id);
                ByteBuffer buff = ByteBuffer.wrap(new byte[1024]);
                // TODO read fully; read only required bytes
                do {
                    int len = file.read(buff);
                    if (len < 0) {
                        break;
                    }
                } while (buff.remaining() > 0);
                buff.rewind();
                n = Node.load(this, id, buff);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            cache.put(id, n);
        }
        return n;
    }

    static int getVarIntLen(int x) {
        if ((x & (-1 << 7)) == 0) {
            return 1;
        } else if ((x & (-1 << 14)) == 0) {
            return 2;
        } else if ((x & (-1 << 21)) == 0) {
            return 3;
        } else if ((x & (-1 << 28)) == 0) {
            return 4;
        }
        return 5;
    }

    static void writeVarInt(ByteBuffer buff, int x) {
        while ((x & ~0x7f) != 0) {
            buff.put((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        buff.put((byte) x);
    }

    static int readVarInt(ByteBuffer buff) {
        int b = buff.get();
        if (b >= 0) {
            return b;
        }
        // a separate function so that this one can be inlined
        return readVarIntRest(buff, b);
    }

    static int readVarIntRest(ByteBuffer buff, int b) {
        int x = b & 0x7f;
        b = buff.get();
        if (b >= 0) {
            return x | (b << 7);
        }
        x |= (b & 0x7f) << 7;
        b = buff.get();
        if (b >= 0) {
            return x | (b << 14);
        }
        x |= (b & 0x7f) << 14;
        b = buff.get();
        if (b >= 0) {
            return x | b << 21;
        }
        x |= ((b & 0x7f) << 21) | (buff.get() << 28);
        return x;
    }

    int compare(Object a, Object b) {
        return keyType.compare(a, b);
    }

    /**
     * A value type.
     */
    static interface ValueType {
        int length(Object obj);
        void write(ByteBuffer buff, Object x);
        Object read(ByteBuffer buff);
    }

    /**
     * A key type.
     */
    static interface KeyType extends ValueType {
        int compare(Object a, Object b);
    }

    /**
     * An integer type.
     */
    static class IntegerType implements KeyType {

        public int compare(Object a, Object b) {
            return ((Integer) a).compareTo((Integer) b);
        }

        public int length(Object obj) {
            return getVarIntLen((Integer) obj);
        }

        public Integer read(ByteBuffer buff) {
            return readVarInt(buff);
        }

        public void write(ByteBuffer buff, Object x) {
            writeVarInt(buff, (Integer) x);
        }

    }

    /**
     * A string type.
     */
    static class StringType implements KeyType {

        public int compare(Object a, Object b) {
            return a.toString().compareTo(b.toString());
        }

        public int length(Object obj) {
            try {
                byte[] bytes = obj.toString().getBytes("UTF-8");
                return getVarIntLen(bytes.length) + bytes.length;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String read(ByteBuffer buff) {
            int len = readVarInt(buff);
            byte[] bytes = new byte[len];
            buff.get(bytes);
            try {
                return new String(bytes, "UTF-8");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void write(ByteBuffer buff, Object x) {
            try {
                byte[] bytes = x.toString().getBytes("UTF-8");
                writeVarInt(buff, bytes.length);
                buff.put(bytes);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    KeyType getKeyType() {
        return keyType;
    }

    ValueType getValueType() {
        return valueType;
    }

    void removeNode(long id) {
        if (id > 0) {
            getBlock(id).liveCount--;
        }
    }

    private Block getBlock(long pos) {
        return blocks.lower(new Block(pos));
    }

    public Cursor cursor() {
        return new Cursor(root);
    }

    /**
     * A cursor to iterate over all elements.
     */
    public static class Cursor {
        Node current;
        ArrayList<Node> parents = new ArrayList<Node>();

        Cursor(Node root) {
            min(root);
        }

        void min(Node n) {
            while (true) {
                Node x = n.getLeft();
                if (x == null) {
                    break;
                }
                parents.add(n);
                n = x;
            }
            current = n;
        }

        Object next() {
            Node c = current;
            if (c != null) {
                fetchNext();
            }
            return c == null ? null : c.getKey();
        }

        private void fetchNext() {
            Node r = current.getRight();
            if (r != null) {
                min(r);
                return;
            }
            if (parents.size() == 0) {
                current = null;
                return;
            }
            current = parents.remove(parents.size() - 1);
        }
    }

    /**
     * A cursor to iterate beginning from the root
     * (not in ascending order).
     */
    public static class RootCursor {
        Node current;
        ArrayList<Node> parents = new ArrayList<Node>();
        RootCursor(Node root) {
            current = root;
        }
        Object next() {
            Node c = current;
            if (c != null) {
                fetchNext();
            }
            return c == null ? null : c.getKey();
        }
        private void fetchNext() {
            Node l = current.getLeft();
            if (l != null) {
                parents.add(current);
                current = l;
                return;
            }
            while (true) {
                Node r = current.getRight();
                if (r != null) {
                    current = r;
                    return;
                }
                if (parents.size() == 0) {
                    current = null;
                    return;
                }
                current = parents.remove(parents.size() - 1);
            }
        }
    }

    /**
     * A block of data.
     */
    static class Block implements Comparable<Block> {
        long transaction;
        long start;
        long length;
        int entryCount;
        int liveCount;
        int referencesToOthers;

        Block(long start) {
            this.start = start;
        }

        public int compareTo(Block o) {
            return start == o.start ? 0 : start < o.start ? -1 : 1;
        }

        public String toString() {
            return "[" + start + "-" + (start + length - 1) + " c:" + entryCount + " l:"
                    + liveCount + " " + (100 * liveCount / entryCount) + "%]";
        }

    }

}
