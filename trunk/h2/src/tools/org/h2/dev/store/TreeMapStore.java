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
import java.util.HashMap;
import java.util.Properties;
import java.util.TreeSet;
import org.h2.store.fs.FilePath;
import org.h2.util.New;
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
 * A persistent storage for tree maps.
 */
public class TreeMapStore {

    private final String fileName;
    private FileChannel file;
    private int pageSize = 4 * 1024;
    private long rootPos;
    private HashMap<Long, Node> cache = SmallLRUCache.newInstance(50000);
    private TreeSet<Block> blocks = new TreeSet<Block>();
    private StoredMap<String, String> meta;
    private HashMap<String, StoredMap<?, ?>> maps = New.hashMap();
    private HashMap<String, StoredMap<?, ?>> mapsChanged = New.hashMap();

    // TODO use an int instead? (with rollover to 0)
    private long transaction;

    private int tempNodeId;
    private long storePos;

    private int loadCount;

    private TreeMapStore(String fileName) {
        this.fileName = fileName;
    }

    public static TreeMapStore open(String fileName) {
        TreeMapStore s = new TreeMapStore(fileName);
        s.open();
        return s;
    }

    public <K, V> StoredMap<K, V> openMap(String name, Class<K> keyClass, Class<V> valueClass) {
        @SuppressWarnings("unchecked")
        StoredMap<K, V> m = (StoredMap<K, V>) maps.get(name);
        if (m == null) {
            String root = meta.get("map." + name);
            m = StoredMap.open(this, name, keyClass, valueClass);
            maps.put(name, m);
            if (root != null) {
                m.setRoot(Long.parseLong(root));
            }
        }
        return m;
    }

    void changed(String name, StoredMap<?, ?> map) {
        if (map != meta) {
            mapsChanged.put(name, map);
        }
    }

    void open() {
        meta = StoredMap.open(this, "meta", String.class, String.class);

        new File(fileName).getParentFile().mkdirs();
        try {
            file = FilePathCache.wrap(FilePath.get(fileName).open("rw"));
            if (file.size() == 0) {
                writeHeader();
                storePos = pageSize * 2;
            } else {
                readHeader();
                if (rootPos > 0) {
                    meta.setRoot(rootPos);
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
        store();
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
        n.write(buff);
        if (left != null) {
            count += store(buff, left);
        }
        if (right != null) {
            count += store(buff, right);
        }
        return count;
    }

    public void store() {
        if (!meta.isChanged() && mapsChanged.size() == 0) {
            // TODO truncate file if empty
            return;
        }
        commit();
        Block b = new Block(storePos);
        b.transaction = transaction;
        long end = storePos + 1 + 8;
        for (StoredMap<?, ?> m : mapsChanged.values()) {
            meta.put("map." + m.getName(), String.valueOf(end));
            end = updateId(m.getRoot(), end);
        }
        long metaPos = end;
        end = updateId(meta.getRoot(), end);

        ByteBuffer buff = ByteBuffer.allocate((int) (end - storePos));
        buff.put((byte) 'd');
        buff.putLong(metaPos - storePos);
        int entryCount = 0;
        for (StoredMap<?, ?> m : mapsChanged.values()) {
            entryCount += store(buff, m.getRoot());
        }
        entryCount += store(buff, meta.getRoot());
        b.entryCount = entryCount;
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
        rootPos = meta.getRoot().getId();
        storePos = end;
        writeHeader();
        tempNodeId = 0;
        mapsChanged.clear();
    }

    long getTransaction() {
        return transaction;
    }

    long nextTempNodeId() {
        return -(++tempNodeId);
    }

    public long commit() {
        return ++transaction;
    }

    Node readNode(StoredMap<?, ?> map, long id) {
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
                n = Node.read(map, id, buff);
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

    void removeNode(long id) {
        if (id > 0) {
            getBlock(id).liveCount--;
        }
    }

    private Block getBlock(long pos) {
        return blocks.lower(new Block(pos));
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
