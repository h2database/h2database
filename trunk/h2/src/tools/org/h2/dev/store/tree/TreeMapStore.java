/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.tree;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;
import org.h2.dev.store.FilePathCache;
import org.h2.store.fs.FilePath;
import org.h2.util.New;
import org.h2.util.SmallLRUCache;
import org.h2.util.StringUtils;

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
'd' [id] [metaRootPos] data ...

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
    private long rootBlockStart;
    private HashMap<Long, Node> cache = SmallLRUCache.newInstance(50000);
    private TreeMap<Integer, Block> blocks = new TreeMap<Integer, Block>();
    private StoredMap<String, String> meta;
    private HashMap<String, StoredMap<?, ?>> maps = New.hashMap();
    private HashMap<String, StoredMap<?, ?>> mapsChanged = New.hashMap();

    // TODO use an int instead? (with rollover to 0)
    private long transaction;

    private int tempNodeId;
    private int lastBlockId;

    private int loadCount;

    private TreeMapStore(String fileName) {
        this.fileName = fileName;
    }

    /**
     * Open a tree store.
     *
     * @param fileName the file name
     * @return the store
     */
    public static TreeMapStore open(String fileName) {
        TreeMapStore s = new TreeMapStore(fileName);
        s.open();
        return s;
    }

    /**
     * Open a map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @param keyClass the key class
     * @param valueClass the value class
     * @return the map
     */
    public <K, V> StoredMap<K, V> openMap(String name, Class<K> keyClass, Class<V> valueClass) {
        @SuppressWarnings("unchecked")
        StoredMap<K, V> m = (StoredMap<K, V>) maps.get(name);
        if (m == null) {
            String root = meta.get("map." + name);
            m = StoredMap.open(this, name, keyClass, valueClass);
            maps.put(name, m);
            if (root != null) {
                root = StringUtils.arraySplit(root, ',', false)[0];
                if (!root.equals("0")) {
                    m.setRoot(Long.parseLong(root));
                }
            }
        }
        return m;
    }

    /**
     * Mark a map as changed.
     *
     * @param name the map name
     * @param map the map
     */
    void markChanged(String name, StoredMap<?, ?> map) {
        if (map != meta) {
            mapsChanged.put(name, map);
        }
    }

    private void open() {
        meta = StoredMap.open(this, "meta", String.class, String.class);
        new File(fileName).getParentFile().mkdirs();
        try {
            log("file open");
            file = FilePathCache.wrap(FilePath.get(fileName).open("rw"));
            if (file.size() == 0) {
                writeHeader();
            } else {
                readHeader();
                readMeta();
            }
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private void readMeta() {
        long rootId = readMetaRootId(rootBlockStart);
        lastBlockId = getBlockId(rootId);
        Block b = new Block(lastBlockId);
        b.start = rootBlockStart;
        blocks.put(b.id, b);
        meta.setRoot(rootId);
        Iterator<String> it = meta.keyIterator("block.");
        while (it.hasNext()) {
            String s = it.next();
            if (!s.startsWith("block.")) {
                break;
            }
            b = Block.fromString(meta.get(s));
            lastBlockId = Math.max(b.id, lastBlockId);
            blocks.put(b.id, b);
        }
    }

    private void writeHeader() {
        try {
            ByteBuffer header = ByteBuffer.wrap((
                "# H2 1.5\n" +
                "read-version: 1\n" +
                "write-version: 1\n" +
                "rootBlock: " + rootBlockStart + "\n" +
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
            rootBlockStart = Long.parseLong(prop.get("rootBlock").toString());
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

    /**
     * Close the file.
     */
    public void close() {
        store();
        if (file != null) {
            try {
                log("file close");
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

    private int count(Node n) {
        if (n == null) {
            return 0;
        }
        int count = 1;
        if (n.getLeftId() < 0) {
            count += count(n.getLeft());
        }
        if (n.getRightId() < 0) {
            count += count(n.getRight());
        }
        return count;
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

    private long getPosition(long nodeId) {
        Block b = getBlock(nodeId);
        if (b == null) {
            throw new RuntimeException("Block " + getBlockId(nodeId) + " not found");
        }
        long pos = b.start;
        pos += (int) (nodeId & Integer.MAX_VALUE);
        return pos;
    }

    private long getId(int blockId, int offset) {
        return ((long) blockId << 32) | offset;
    }

    /**
     * Persist all changes to disk.
     */
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
            meta.put("map." + m.getName(), String.valueOf(Long.MAX_VALUE) +
                    "," + m.getKeyType().getName() + "," + m.getValueType().getName());
            lenEstimate += length(m.getRoot());
        }
        int blockId = ++lastBlockId;
        Block b = new Block(blockId);
        b.start = Long.MAX_VALUE;
        b.entryCount = Integer.MAX_VALUE;
        b.liveCount = Integer.MAX_VALUE;
        blocks.put(b.id, b);
        for (Block x : blocks.values()) {
            if (x.liveCount == 0) {
                meta.remove("block." + x.id);
            } else {
                meta.put("block." + x.id, "temp " + x.toString());
            }
        }
        // modifying the meta can itself affect the metadata
        // TODO solve this in a better way
        for (Block x : new ArrayList<Block>(blocks.values())) {
            if (x.liveCount == 0) {
                meta.remove("block." + x.id);
                blocks.remove(x.id);
            } else {
                meta.put("block." + x.id, x.toString());
            }
        }
        lenEstimate += length(meta.getRoot());
        b.length = lenEstimate;

        long storePos = allocateBlock(lenEstimate);

        long nodeId = getId(blockId, 1 + 8);
        for (StoredMap<?, ?> m : mapsChanged.values()) {
            Node r = m.getRoot();
            long p = r == null ? 0 : nodeId;
            meta.put("map." + m.getName(), String.valueOf(p) + "," + m.getKeyType().getName() + "," + m.getValueType().getName());
            nodeId = updateId(r, nodeId);
        }
        int metaNodeOffset = (int) (nodeId - getId(blockId, 0));

        // add a dummy entry so the count is correct
        meta.put("block." + b.id, b.toString());
        int count = 0;
        for (StoredMap<?, ?> m : mapsChanged.values()) {
            count += count(m.getRoot());
        }
        count += count(meta.getRoot());

        b.start = storePos;
        b.entryCount = count;
        b.liveCount = b.entryCount;

        meta.put("block." + b.id, b.toString());

        nodeId = updateId(meta.getRoot(), nodeId);
        int len = (int) (nodeId - getId(blockId, 0));

        ByteBuffer buff = ByteBuffer.allocate(len);
        buff.put((byte) 'd');
        buff.putInt(b.id);
        buff.putInt(metaNodeOffset);
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
        rootBlockStart = storePos;
        writeHeader();
        tempNodeId = 0;
        mapsChanged.clear();
    }

    private long allocateBlock(long length) {
        BitSet set = new BitSet();
        set.set(0);
        set.set(1);
        for (Block b : blocks.values()) {
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

    /**
     * Get the current transaction number.
     *
     * @return the transaction number
     */
    long getTransaction() {
        return transaction;
    }

    /**
     * Get the next temporary node id.
     *
     * @return the node id
     */
    long nextTempNodeId() {
        return -(++tempNodeId);
    }

    /**
     * Commit the current transaction.
     *
     * @return the transaction id
     */
    public long commit() {
        return ++transaction;
    }

    private long readMetaRootId(long blockStart) {
        try {
            file.position(blockStart);
            ByteBuffer buff = ByteBuffer.wrap(new byte[16]);
            file.read(buff);
            buff.rewind();
            if (buff.get() != 'd') {
                throw new RuntimeException("File corrupt");
            }
            int blockId = buff.getInt();
            int offset = buff.getInt();
            return getId(blockId, offset);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Try to reduce the file size. Blocks with a low number of live items will
     * be re-written.
     */
    public void compact() {
        if (blocks.size() <= 1) {
            return;
        }
        long liveCountTotal = 0, entryCountTotal = 0;
        for (Block b : blocks.values()) {
            entryCountTotal += b.entryCount;
            liveCountTotal += b.liveCount;
        }
        int averageEntryCount = (int) (entryCountTotal / blocks.size());
        if (entryCountTotal == 0) {
            return;
        }
        int percentTotal = (int) (100 * liveCountTotal / entryCountTotal);
        if (percentTotal > 80) {
            return;
        }
        ArrayList<Block> old = New.arrayList();
        for (Block b : blocks.values()) {
            int age = lastBlockId - b.id + 1;
            b.collectPriority = b.getFillRate() / age;
            old.add(b);
        }
        Collections.sort(old, new Comparator<Block>() {
            public int compare(Block o1, Block o2) {
                return new Integer(o1.collectPriority).compareTo(o2.collectPriority);
            }
        });
        int moveCount = 0;
        Block move = null;
        for (Block b : old) {
            if (moveCount + b.liveCount > averageEntryCount) {
                break;
            }
            log(" block " + b.id + " " + b.getFillRate() + "% full; prio=" + b.collectPriority);
            moveCount += b.liveCount;
            move = b;
        }
        boolean remove = false;
        for (Iterator<Block> it = old.iterator(); it.hasNext();) {
            Block b = it.next();
            if (move == b) {
                remove = true;
            } else if (remove) {
                it.remove();
            }
        }
        long oldMetaRootId = readMetaRootId(move.start);
        long offset = getPosition(oldMetaRootId);
        log("  meta:" + move.id + "/" + offset);
        StoredMap<String, String> oldMeta = StoredMap.open(this, "old-meta", String.class, String.class);
        oldMeta.setRoot(oldMetaRootId);
        Iterator<String> it = oldMeta.keyIterator(null);
        ArrayList<Integer> oldBlocks = New.arrayList();
        while (it.hasNext()) {
            String k = it.next();
            String v = oldMeta.get(k);
            log("    " + k + " " + v.replace('\n', ' '));
            if (k.startsWith("block.")) {
                String s = oldMeta.get(k);
                Block b = Block.fromString(s);
                if (!blocks.containsKey(b.id)) {
                    oldBlocks.add(b.id);
                    blocks.put(b.id, b);
                }
                continue;
            }
            if (!k.startsWith("map.")) {
                continue;
            }
            k = k.substring("map.".length());
            if (!maps.containsKey(k)) {
                continue;
            }
            String[] d = StringUtils.arraySplit(v, ',', false);
            Class<?> kt = StoredMap.getClass(d[1]);
            Class<?> vt = StoredMap.getClass(d[2]);
            StoredMap<?, ?> oldData = StoredMap.open(this, "old-" + k, kt, vt);
            long oldDataRoot = Long.parseLong(d[0]);
            oldData.setRoot(oldDataRoot);
            @SuppressWarnings("unchecked")
            StoredMap<Object, Object> data = (StoredMap<Object, Object>) maps.get(k);
            Iterator<?> dataIt = oldData.keyIterator(null);
            while (dataIt.hasNext()) {
                Object o = dataIt.next();
                Node n = data.getNode(o);
                if (n == null) {
                    // was removed later - ignore
                } else if (n.getId() < 0) {
                    // temporarily changed - ok
                } else {
                    Block b = getBlock(n.getId());
                    if (old.contains(b)) {
                        log("       move key:" + o + " block:" + b.id);
                        data.remove(o);
                        data.put(o, n.getData());
                    }
                }
            }
        }
        for (int o : oldBlocks) {
            blocks.remove(o);
        }
    }

    /**
     * Read a node.
     *
     * @param map the map
     * @param id the node id
     * @return the node
     */
    Node readNode(StoredMap<?, ?> map, long id) {
        Node n = cache.get(id);
        if (n == null) {
            try {
                long pos = getPosition(id);
                file.position(pos);
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

    /**
     * Remove a node.
     *
     * @param id the node id
     */
    void removeNode(long id) {
        if (id > 0) {
            if (getBlock(id).liveCount == 0) {
                throw new RuntimeException("Negative live count: " + id);
            }
            getBlock(id).liveCount--;
        }
    }

    private int getBlockId(long nodeId) {
        return (int) (nodeId >>> 32);
    }

    private Block getBlock(long nodeId) {
        return blocks.get(getBlockId(nodeId));
    }

    /**
     * A block of data.
     */
    static class Block implements Comparable<Block> {
        public int collectPriority;
        int id;
        long start;
        long length;
        int entryCount;
        int liveCount;

        Block(int id) {
            this.id = id;
        }

        /**
         * Build a block from the given string.
         *
         * @param s the string
         * @return the block
         */
        static Block fromString(String s) {
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

        public int getFillRate() {
            return entryCount == 0 ? 0 : 100 * liveCount / entryCount;
        }

        public int compareTo(Block o) {
            return start == o.start ? 0 : start < o.start ? -1 : 1;
        }

        public int hashCode() {
            return id;
        }

        public boolean equals(Object o) {
            return o instanceof Block && ((Block) o).id == id;
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

    /**
     * Log the string, if logging is enabled.
     *
     * @param string the string to log
     */
    public void log(String string) {
        // System.out.println(string);
    }

}
