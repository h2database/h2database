/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.io.ByteArrayInputStream;
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
import org.h2.store.fs.FileUtils;
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
public class BtreeMapStore {

    private final String fileName;
    private FileChannel file;
    private int pageSize = 4 * 1024;
    private long rootBlockStart;
    private HashMap<Long, Page> cache = SmallLRUCache.newInstance(50000);
    private ArrayList<Page> temp = New.arrayList();
    private TreeMap<Integer, Block> blocks = new TreeMap<Integer, Block>();
    private BtreeMap<String, String> meta;
    private HashMap<String, BtreeMap<?, ?>> maps = New.hashMap();
    private HashMap<String, BtreeMap<?, ?>> mapsChanged = New.hashMap();

    // TODO use an int instead? (with rollover to 0)
    private long transaction;

    private int lastBlockId;

    // TODO support quota (per map, per storage)
    // TODO support r-tree
    // TODO support triggers and events (possibly on a different layer)

    private BtreeMapStore(String fileName) {
        this.fileName = fileName;
    }

    /**
     * Open a tree store.
     *
     * @param fileName the file name
     * @return the store
     */
    public static BtreeMapStore open(String fileName) {
        BtreeMapStore s = new BtreeMapStore(fileName);
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
    public <K, V> BtreeMap<K, V> openMap(String name, Class<K> keyClass, Class<V> valueClass) {
        @SuppressWarnings("unchecked")
        BtreeMap<K, V> m = (BtreeMap<K, V>) maps.get(name);
        if (m == null) {
            String root = meta.get("map." + name);
            m = BtreeMap.open(this, name, keyClass, valueClass);
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
    void markChanged(String name, BtreeMap<?, ?> map) {
        if (map != meta) {
            mapsChanged.put(name, map);
        }
    }

    private void open() {
        meta = BtreeMap.open(this, "meta", String.class, String.class);
        FileUtils.createDirectories(FileUtils.getParent(fileName));
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

    private static RuntimeException convert(Exception e) {
        throw new RuntimeException("Exception: " + e, e);
    }

    /**
     * Close the file. Uncommitted changes are ignored.
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

    private long getPosition(long pageId) {
        Block b = getBlock(pageId);
        if (b == null) {
            throw new RuntimeException("Block " + getBlockId(pageId) + " not found");
        }
        long pos = b.start;
        pos += (int) (pageId & Integer.MAX_VALUE);
        return pos;
    }

    private static long getId(int blockId, int offset) {
        return ((long) blockId << 32) | offset;
    }

    /**
     * Commit all changes and persist them to disk.
     *
     * @return the transaction id
     */
    public long store() {
        if (!meta.isChanged() && mapsChanged.size() == 0) {
            // TODO truncate file if empty
            return transaction;
        }
        long trans = commit();

        // the length estimate is not correct,
        // as we don't know the exact positions and entry counts
        int lenEstimate = 1 + 8;
        for (BtreeMap<?, ?> m : mapsChanged.values()) {
            meta.put("map." + m.getName(), String.valueOf(Long.MAX_VALUE) +
                    "," + m.getKeyType().getName() + "," + m.getValueType().getName());
            Page p = m.getRoot();
            if (p != null) {
                lenEstimate += p.lengthIncludingTempChildren();
            }
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
                meta.put("block." + x.id, "temp-" + x.toString());
            }
        }
        // modifying the meta can itself affect the metadata
        // TODO solve this in a better way
        ArrayList<Integer> removedBlocks = New.arrayList();
        for (Block x : new ArrayList<Block>(blocks.values())) {
            if (x.liveCount == 0) {
                meta.remove("block." + x.id);
                removedBlocks.add(x.id);
            } else {
                meta.put("block." + x.id, x.toString());
            }
        }
        lenEstimate += meta.getRoot().lengthIncludingTempChildren();
        b.length = lenEstimate;

        blocks.remove(b.id);
        long storePos = allocateBlock(lenEstimate);
        blocks.put(b.id, b);

        for (int id : removedBlocks) {
            blocks.remove(id);
        }

        long pageId = getId(blockId, 1 + 8);
        for (BtreeMap<?, ?> m : mapsChanged.values()) {
            Page r = m.getRoot();
            long p = r == null ? 0 : pageId;
            meta.put("map." + m.getName(), String.valueOf(p) + "," + m.getKeyType().getName() + "," + m.getValueType().getName());
            if (r != null) {
                pageId = r.updatePageIds(pageId);
            }
        }
        int metaRootOffset = (int) (pageId - getId(blockId, 0));
        // add a dummy entry so the count is correct
        meta.put("block." + b.id, b.toString());
        int count = 0;
        for (BtreeMap<?, ?> m : mapsChanged.values()) {
            Page p = m.getRoot();
            if (p != null) {
                count += p.countTemp();
            }
        }
        count += meta.getRoot().countTemp();

        b.start = storePos;
        b.entryCount = count;
        b.liveCount = b.entryCount;

        meta.put("block." + b.id, b.toString());

        pageId = meta.getRoot().updatePageIds(pageId);
        int len = (int) (pageId - getId(blockId, 0));

        ByteBuffer buff = ByteBuffer.allocate(len);
        buff.put((byte) 'd');
        buff.putInt(b.id);
        buff.putInt(metaRootOffset);
        for (BtreeMap<?, ?> m : mapsChanged.values()) {
            Page p = m.getRoot();
            if (p != null) {
                p.storeTemp(buff);
            }
        }
        meta.getRoot().storeTemp(buff);
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
        mapsChanged.clear();
        temp.clear();
        return trans;
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
     * Register a page and get the next temporary page id.
     *
     * @param p the new page
     * @return the page id
     */
    long registerTempPage(Page p) {
        temp.add(p);
        return -temp.size();
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
        log("  meta:" + move.id + "/" + offset + " start: " + move.start);
        BtreeMap<String, String> oldMeta = BtreeMap.open(this, "old-meta", String.class, String.class);
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
            Class<?> kt = BtreeMap.getClass(d[1]);
            Class<?> vt = BtreeMap.getClass(d[2]);
            BtreeMap<?, ?> oldData = BtreeMap.open(this, "old-" + k, kt, vt);
            long oldDataRoot = Long.parseLong(d[0]);
            if (oldDataRoot == 0) {
                // no rows
            } else {
                oldData.setRoot(oldDataRoot);
                @SuppressWarnings("unchecked")
                BtreeMap<Object, Object> data = (BtreeMap<Object, Object>) maps.get(k);
                Iterator<?> dataIt = oldData.keyIterator(null);
                while (dataIt.hasNext()) {
                    Object o = dataIt.next();
                    Page p = data.getPage(o);
                    if (p == null) {
                        // was removed later - ignore
                    } else if (p.getId() < 0) {
                        // temporarily changed - ok
                        // TODO move old data if changed temporarily?
                    } else {
                        Block b = getBlock(p.getId());
                        if (old.contains(b)) {
                            log("       move key:" + o + " block:" + b.id);
                            Object value = data.get(o);
                            data.remove(o);
                            data.put(o, value);
                        }
                    }
                }
            }
        }
        for (int o : oldBlocks) {
            blocks.remove(o);
        }
    }

    /**
     * Read a page.
     *
     * @param map the map
     * @param id the page id
     * @return the page
     */
    Page readPage(BtreeMap<?, ?> map, long id) {
        if (id < 0) {
            return temp.get((int) (-id - 1));
        }
        Page p = cache.get(id);
        if (p == null) {
            try {
                long pos = getPosition(id);
                file.position(pos);
                ByteBuffer buff = ByteBuffer.wrap(new byte[8 * 1024]);
                // TODO read fully; read only required bytes
                do {
                    int len = file.read(buff);
                    if (len < 0) {
                        break;
                    }
                } while (buff.remaining() > 0);
                buff.rewind();
                p = Page.read(map, id, buff);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            cache.put(id, p);
        }
        return p;
    }

    /**
     * Remove a page.
     *
     * @param id the page id
     */
    void removePage(long id) {
        if (id > 0) {
            if (getBlock(id).liveCount == 0) {
                throw new RuntimeException("Negative live count: " + id);
            }
            getBlock(id).liveCount--;
        }
    }

    private static int getBlockId(long pageId) {
        return (int) (pageId >>> 32);
    }

    private Block getBlock(long pageId) {
        return blocks.get(getBlockId(pageId));
    }

    /**
     * A block of data.
     */
    static class Block {

        /**
         * The block id.
         */
        int id;

        /**
         * The start position within the file.
         */
        long start;

        /**
         * The length in bytes.
         */
        long length;

        /**
         * The entry count.
         */
        int entryCount;

        /**
         * The number of life (non-garbage) objects.
         */
        int liveCount;

        /**
         * The garbage collection priority.
         */
        int collectPriority;

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
    void log(String string) {
        // TODO logging
        // System.out.println(string);
    }

}
