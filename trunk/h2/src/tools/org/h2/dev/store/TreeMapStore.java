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
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeSet;
import org.h2.store.fs.FilePath;
import org.h2.util.New;
import org.h2.util.SmallLRUCache;

/*

file format:

header
header
[ chunk ] *

header:
# H3 store #
pageSize=4096
r=1

chunk:
'd' [length] root ...

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
    private int nextBlockId;

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
            } else {
                readHeader();
                if (rootPos > 0) {
                    meta.setRoot(rootPos);
                }
                readMeta();
            }
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private void readMeta() {
        Iterator<String> it = meta.keyIterator("block.");
        while (it.hasNext()) {
            String s = it.next();
            if (!s.startsWith("block.")) {
                break;
            }
            Block b = Block.fromString(meta.get(s));
            nextBlockId = Math.max(b.id + 1, nextBlockId);
            blocks.add(b);
        }
    }

    private void writeHeader() {
        try {
            ByteBuffer header = ByteBuffer.wrap((
                "# H2 1.5\n" +
                "read-version: 1\n" +
                "write-version: 1\n" +
                "root: " + rootPos + "\n" +
                "transaction: " + transaction + "\n").getBytes());
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
            transaction = Long.parseLong(prop.get("transaction").toString());
        } catch (Exception e) {
            throw convert(e);
        }
    }

    public String toString() {
        return "cache size: " + cache.size() + " loadCount: " + loadCount + "\n" + blocks.toString().replace('\n', ' ');
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

    private int length(Node n) {
        int len = 0;
        if (n != null) {
            len += n.length();
            if (n.getLeftId() < 0) {
                len += length(n.getLeft());
            }
            if (n.getRightId() < 0) {
                len += length(n.getRight());
            }
        }
        return len;
    }

    private long updateId(Node n, long offset) {
        if (n != null) {
            n.setId(offset);
            cache.put(offset, n);
            offset += n.length();
            if (n.getLeftId() < 0) {
                offset = updateId(n.getLeft(), offset);
            }
            if (n.getRightId() < 0) {
                offset = updateId(n.getRight(), offset);
            }
        }
        return offset;
    }

    private void store(ByteBuffer buff, Node n) {
        if (n == null) {
            return;
        }
        Node left = n.getLeftId() < 0 ? n.getLeft() : null;
        if (left != null) {
            n.setLeftId(left.getId());
        }
        Node right = n.getRightId() < 0 ? n.getRight() : null;
        if (right != null) {
            n.setRightId(right.getId());
        }
        n.write(buff);
        if (left != null) {
            store(buff, left);
        }
        if (right != null) {
            store(buff, right);
        }
    }

    public void store() {
        if (!meta.isChanged() && mapsChanged.size() == 0) {
            // TODO truncate file if empty
            return;
        }
        commit();

        // the length estimate is not correct,
        // as we don't know the exact positions and entry counts
        int lenEstimate = 1 + 8;
        for (StoredMap<?, ?> m : mapsChanged.values()) {
            meta.put("map." + m.getName(), String.valueOf(Long.MAX_VALUE));
            lenEstimate += length(m.getRoot());
        }
        Block b = new Block(nextBlockId++);
        b.start = Long.MAX_VALUE;
        b.entryCount = Integer.MAX_VALUE;
        b.liveCount = Integer.MAX_VALUE;
        blocks.add(b);
        for (Block x : blocks) {
            if (x.liveCount == 0) {
                meta.remove("block." + x.id);
            } else {
                meta.put("block." + x.id, x.toString());
            }
        }
        lenEstimate += length(meta.getRoot());
        b.length = lenEstimate;

        long storePos = allocate(lenEstimate);

        int test;
        // System.out.println("use " + storePos + ".." + (storePos + lenEstimate));

        long end = storePos + 1 + 8;
        for (StoredMap<?, ?> m : mapsChanged.values()) {
            meta.put("map." + m.getName(), String.valueOf(end));
            end = updateId(m.getRoot(), end);
        }
        long metaPos = end;

        b.start = storePos;
        b.entryCount = tempNodeId;
        b.liveCount = b.entryCount;
        meta.put("block." + b.id, b.toString());

        end = updateId(meta.getRoot(), end);

        ByteBuffer buff = ByteBuffer.allocate((int) (end - storePos));
        buff.put((byte) 'd');
        buff.putLong(metaPos - storePos);
        for (StoredMap<?, ?> m : mapsChanged.values()) {
            store(buff, m.getRoot());
        }
        store(buff, meta.getRoot());
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
        writeHeader();
        tempNodeId = 0;
        mapsChanged.clear();
    }

    private long allocate(long length) {
        BitSet set = new BitSet();
        set.set(0);
        set.set(1);
        for (Block b : blocks) {
            if (b.start == Long.MAX_VALUE) {
                continue;
            }
            int first = (int) (b.start / pageSize);
            int last = (int) ((b.start + b.length) / pageSize);
            set.set(first, last +1);
        }
        int required = (int) (length / pageSize) + 1;
        for (int i = 0; i < set.size(); i++) {
            if (!set.get(i)) {
                boolean ok = true;
                for (int j = 1; j <= required; j++) {
                    if (set.get(i + j)) {
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    return i * pageSize;
                }
            }
        }
        return set.size() * pageSize;
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
        Block b = new Block(0);
        b.start = pos;
        return blocks.headSet(b).last();
    }
    /**
     * A block of data.
     */
    static class Block implements Comparable<Block> {
        int id;
        long start;
        long length;
        int entryCount;
        int liveCount;
//        int outRevCount;

        Block(int id) {
            this.id = id;
        }

        public static Block fromString(String s) {
            Block b = new Block(0);
            Properties prop = new Properties();
            try {
                prop.load(new ByteArrayInputStream(s.getBytes("UTF-8")));
                b.id = Integer.parseInt(prop.get("id").toString());
                b.start = Long.parseLong(prop.get("start").toString());
                b.length = Long.parseLong(prop.get("length").toString());
                b.entryCount = Integer.parseInt(prop.get("entryCount").toString());
                b.liveCount = Integer.parseInt(prop.get("liveCount").toString());
                return b;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public int compareTo(Block o) {
            return start == o.start ? 0 : start < o.start ? -1 : 1;
        }

        public String toString() {
            return
                "id:" + id + "\n" +
                "start:" + start + "\n" +
                "length:" + length + "\n" +
                "entryCount:" + entryCount + "\n" +
                "liveCount:" + liveCount + "\n";
        }

    }

}
