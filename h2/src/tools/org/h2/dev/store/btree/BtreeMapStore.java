/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.h2.dev.store.FilePathCache;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FileUtils;
import org.h2.util.New;
import org.h2.util.StringUtils;

/*

file format:

header
header
[ chunk ] *

header:
# H3 store #
blockSize=4096

chunk:
1 byte: 'c'
4 bytes: length
4 bytes: chunk id (an incrementing number)
8 bytes: metaRootPos
data ...

Limits: there are at most 67 million chunks (each chunk is at most 2 GB large).

TODO:

- use partial page checksums
- compress chunks
- rollback feature
- support range deletes
- keep page type (leaf/node)  in pos to speed up large deletes

- floating header (avoid duplicate header)
    for each chunk, store chunk (a counter)
    for each page, store chunk id and offset to root
    for each chunk, store position of expected next chunks

- support reading metadata to copy all data,
- support quota (per map, per storage)

- support r-tree, kd-tree

- map ids should be per chunk, to ensure uniqueness

*/

/**
 * A persistent storage for tree maps.
 */
public class BtreeMapStore {

    private static final StringType STRING_TYPE = new StringType();

    private final String fileName;
    private final DataTypeFactory typeFactory;

    private int readCacheSize = 2 * 1024 * 1024;

    private int maxPageSize = 30;

    private FileChannel file;
    private int blockSize = 4 * 1024;
    private long rootChunkStart;

    private int tempPageId;
    private Map<Long, Page> cache = CacheLIRS.newInstance(readCacheSize, 2048);
    private HashMap<Long, Page> temp = New.hashMap();

    private int lastChunkId;
    private HashMap<Integer, Chunk> chunks = New.hashMap();

    // TODO use bit set, and integer instead of long
    private BtreeMap<String, String> meta;
    private HashMap<String, BtreeMap<?, ?>> maps = New.hashMap();
    private HashMap<String, BtreeMap<?, ?>> mapsChanged = New.hashMap();
    private int mapIdMin;
    private BitSet mapIds = new BitSet();

    // TODO use an int instead? (with rollover to 0)
    private long transaction;

    private BtreeMapStore(String fileName, DataTypeFactory typeFactory) {
        this.fileName = fileName;
        this.typeFactory = typeFactory;
    }

    /**
     * Open a tree store.
     *
     * @param fileName the file name
     * @return the store
     */
    public static BtreeMapStore open(String fileName) {
        return open(fileName, null);
    }

    /**
     * Open a tree store.
     *
     * @param fileName the file name
     * @param typeFactory the type factory
     * @return the store
     */
    public static BtreeMapStore open(String fileName, DataTypeFactory typeFactory) {
        BtreeMapStore s = new BtreeMapStore(fileName, typeFactory);
        s.open();
        return s;
    }

    /**
     * Open a map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @param keyType the key type
     * @param valueType the value type
     * @return the map
     */
    public <K, V> BtreeMap<K, V> openMap(String name, DataType keyType, DataType valueType) {
        @SuppressWarnings("unchecked")
        BtreeMap<K, V> m = (BtreeMap<K, V>) maps.get(name);
        if (m == null) {
            String identifier = meta.get("map." + name);
            int id;
            String root;
            if (identifier == null) {
                id = nextMapId();
                String types = id + "/" + keyType.asString() + "/" + valueType.asString();
                meta.put("map." + name, types);
                root = null;
            } else {
                String types = meta.get("map." + name);
                String[] idTypeList = StringUtils.arraySplit(types, '/', false);
                id = Integer.parseInt(idTypeList[0]);
                keyType = getDataType(idTypeList[1]);
                valueType = getDataType(idTypeList[2]);
                root = meta.get("root." + id);
            }
            m = BtreeMap.open(this, id, name, keyType, valueType);
            maps.put(name, m);
            if (root != null && !"0".equals(root)) {
                m.setRootPos(Long.parseLong(root));
            }
        }
        return m;
    }

    private int nextMapId() {
        int result;
        while (true) {
            result = mapIds.nextClearBit(mapIdMin);
            mapIds.set(result);
            // TODO need to check in oldest
            if (meta.get("root." + result) == null) {
                break;
            }
        }
        mapIdMin = result;
        return result;
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
        DataType keyType = getDataType(keyClass);
        DataType valueType = getDataType(valueClass);
        return openMap(name, keyType, valueType);
    }

    void removeMap(int id) {
        mapIds.clear(id);
        mapIdMin = Math.min(id,  mapIdMin);
    }

    private DataType getDataType(Class<?> clazz) {
        if (clazz == String.class) {
            return STRING_TYPE;
        }
        return getTypeFactory().getDataType(clazz);
    }

    private DataType getDataType(String s) {
        if (s.equals("")) {
            return STRING_TYPE;
        }
        return getTypeFactory().fromString(s);
    }

    private DataTypeFactory getTypeFactory() {
        if (typeFactory == null) {
            throw new RuntimeException("No data type factory set");
        }
        return typeFactory;
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
        meta = BtreeMap.open(this, 0, "meta", STRING_TYPE, STRING_TYPE);
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
        Chunk header = readChunkHeader(rootChunkStart);
        lastChunkId = header.id;
        chunks.put(header.id, header);
        meta.setRootPos(header.metaRootPos);
        Iterator<String> it = meta.keyIterator("chunk.");
        while (it.hasNext()) {
            String s = it.next();
            if (!s.startsWith("chunk.")) {
                break;
            }
            Chunk c = Chunk.fromString(meta.get(s));
            if (c.id == header.id) {
                c.start = header.start;
                c.length = header.length;
                c.metaRootPos = header.metaRootPos;
            }
            lastChunkId = Math.max(c.id, lastChunkId);
            chunks.put(c.id, c);
        }
    }

    private void writeHeader() {
        try {
            ByteBuffer header = ByteBuffer.wrap((
                "# H2 1.5\n" +
                "versionRead:1\n" +
                "versionWrite:1\n" +
                "blockSize:" + blockSize + "\n" +
                "rootChunk:" + rootChunkStart + "\n" +
                "lastMapId:" + mapIdMin + "\n" +
                "transaction:" + transaction + "\n").getBytes("UTF-8"));
            file.position(0);
            file.write(header);
            file.position(blockSize);
            file.write(header);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private void readHeader() {
        try {
            file.position(0);
            byte[] header = new byte[blockSize];
            // TODO read fully; read both headers
            file.read(ByteBuffer.wrap(header));
            Properties prop = new Properties();
            prop.load(new StringReader(new String(header, "UTF-8")));
            rootChunkStart = Long.parseLong(prop.get("rootChunk").toString());
            transaction = Long.parseLong(prop.get("transaction").toString());
            mapIdMin = Integer.parseInt(prop.get("lastMapId").toString());
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

    private Chunk getChunk(long pos) {
        return chunks.get(Page.getChunkId(pos));
    }

    private long getFilePosition(long pos) {
        Chunk c = getChunk(pos);
        if (c == null) {
            throw new RuntimeException("Chunk " + Page.getChunkId(pos) + " not found");
        }
        long filePos = c.start;
        filePos += Page.getOffset(pos);
        return filePos;
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

        int chunkId = ++lastChunkId;

        Chunk c = new Chunk(chunkId);
        c.entryCount = Integer.MAX_VALUE;
        c.liveCount = Integer.MAX_VALUE;
        c.start = Long.MAX_VALUE;
        c.length = Long.MAX_VALUE;
        chunks.put(c.id, c);
        meta.put("chunk." + c.id, c.toString());
        ArrayList<Integer> removedChunks = New.arrayList();
        for (Chunk x : chunks.values()) {
            if (x.liveCount == 0) {
                meta.remove("chunk." + x.id);
                removedChunks.add(x.id);
            } else {
                meta.put("chunk." + x.id, x.toString());
            }
        }
        for (int x : removedChunks) {
            chunks.remove(x);
        }
        int count = 0;
        int maxLength = 1 + 4 + 4 + 8;
        for (BtreeMap<?, ?> m : mapsChanged.values()) {
            Page p = m.getRoot();
            if (p != null) {
                maxLength += p.getMaxLengthTempRecursive();
                count += p.countTempRecursive();
                meta.put("root." + m.getId(), String.valueOf(Long.MAX_VALUE));
            } else {
                meta.put("root." + m.getId(), "0");
            }
        }
        maxLength += meta.getRoot().getMaxLengthTempRecursive();
        count += meta.getRoot().countTempRecursive();

        ByteBuffer buff = ByteBuffer.allocate(maxLength);
        // need to patch the header later
        buff.put((byte) 'c');
        buff.putInt(0);
        buff.putInt(0);
        buff.putLong(0);
        for (BtreeMap<?, ?> m : mapsChanged.values()) {
            Page p = m.getRoot();
            if (p != null) {
                long root = p.writeTempRecursive(buff, chunkId);
                meta.put("root." + m.getId(), "" + root);
            }
        }

        // fix metadata
        c.entryCount = count;
        c.liveCount = count;
        meta.put("chunk." + c.id, c.toString());

        meta.getRoot().writeTempRecursive(buff, chunkId);

        buff.flip();
        int length = buff.limit();
        long filePos = allocateChunk(length);

        buff.rewind();
        buff.put((byte) 'c');
        buff.putInt(length);
        buff.putInt(chunkId);
        buff.putLong(meta.getRoot().getPos());
        buff.rewind();
        try {
            file.position(filePos);
            file.write(buff);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        rootChunkStart = filePos;
        writeHeader();
        mapsChanged.clear();
        temp.clear();
        tempPageId = 0;

        // update the start position and length
        c.start = filePos;
        c.length = length;
        meta.put("chunk." + c.id, c.toString());

        return trans;
    }

    private long allocateChunk(long length) {
        BitSet set = new BitSet();
        set.set(0);
        set.set(1);
        for (Chunk c : chunks.values()) {
            if (c.start == Long.MAX_VALUE) {
                continue;
            }
            int first = (int) (c.start / blockSize);
            int last = (int) ((c.start + c.length) / blockSize);
            set.set(first, last +1);
        }
        int required = (int) (length / blockSize) + 1;
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
                    return i * blockSize;
                }
            }
        }
        return set.size() * blockSize;
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
        long id = --tempPageId;
        temp.put(id, p);
        return id;
    }

    /**
     * Commit the current transaction.
     *
     * @return the transaction id
     */
    public long commit() {
        return ++transaction;
    }

    private Chunk readChunkHeader(long start) {
        try {
            file.position(start);
            ByteBuffer buff = ByteBuffer.wrap(new byte[32]);
            DataUtils.readFully(file, buff);
            buff.rewind();
            if (buff.get() != 'c') {
                throw new RuntimeException("File corrupt");
            }
            int length = buff.getInt();
            int chunkId = buff.getInt();
            long metaRootPos = buff.getLong();
            Chunk c = new Chunk(chunkId);
            c.start = start;
            c.length = length;
            c.metaRootPos = metaRootPos;
            return c;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Try to reduce the file size. Chunks with a low number of live items will
     * be re-written.
     */
    public void compact() {
        if (chunks.size() <= 1) {
            return;
        }
        long liveCountTotal = 0, entryCountTotal = 0;
        for (Chunk c : chunks.values()) {
            entryCountTotal += c.entryCount;
            liveCountTotal += c.liveCount;
        }
        int averageEntryCount = (int) (entryCountTotal / chunks.size());
        if (entryCountTotal == 0) {
            return;
        }
        int percentTotal = (int) (100 * liveCountTotal / entryCountTotal);
        if (percentTotal > 80) {
            return;
        }
        ArrayList<Chunk> old = New.arrayList();
        for (Chunk c : chunks.values()) {
            int age = lastChunkId - c.id + 1;
            c.collectPriority = c.getFillRate() / age;
            old.add(c);
        }
        Collections.sort(old, new Comparator<Chunk>() {
            public int compare(Chunk o1, Chunk o2) {
                return new Integer(o1.collectPriority).compareTo(o2.collectPriority);
            }
        });
        int moveCount = 0;
        Chunk move = null;
        for (Chunk c : old) {
            if (moveCount + c.liveCount > averageEntryCount) {
                break;
            }
            log(" chunk " + c.id + " " + c.getFillRate() + "% full; prio=" + c.collectPriority);
            moveCount += c.liveCount;
            move = c;
        }
        boolean remove = false;
        for (Iterator<Chunk> it = old.iterator(); it.hasNext();) {
            Chunk c = it.next();
            if (move == c) {
                remove = true;
            } else if (remove) {
                it.remove();
            }
        }
        Chunk header = readChunkHeader(move.start);
        log("  meta:" + move.id + "/" + header.metaRootPos + " start: " + move.start);
        BtreeMap<String, String> oldMeta = BtreeMap.open(this, 0, "old-meta", STRING_TYPE, STRING_TYPE);
        oldMeta.setRootPos(header.metaRootPos);
        Iterator<String> it = oldMeta.keyIterator(null);
        ArrayList<Integer> oldChunks = New.arrayList();
        while (it.hasNext()) {
            String k = it.next();
            String s = oldMeta.get(k);
            log("    " + k + " " + s.replace('\n', ' '));
            if (k.startsWith("chunk.")) {
                Chunk c = Chunk.fromString(s);
                if (!chunks.containsKey(c.id)) {
                    oldChunks.add(c.id);
                    chunks.put(c.id, c);
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
            String[] idTypesList = StringUtils.arraySplit(s, '/', false);
            int id = Integer.parseInt(idTypesList[0]);
            DataType kt = getDataType(idTypesList[1]);
            DataType vt = getDataType(idTypesList[2]);
            long oldDataRoot = Long.parseLong(oldMeta.get("root." + id));
            BtreeMap<?, ?> oldData = BtreeMap.open(this, id, "old-" + k, kt, vt);
            if (oldDataRoot == 0) {
                // no rows
            } else {
                oldData.setRootPos(oldDataRoot);
                @SuppressWarnings("unchecked")
                BtreeMap<Object, Object> data = (BtreeMap<Object, Object>) maps.get(k);
                Iterator<?> dataIt = oldData.keyIterator(null);
                while (dataIt.hasNext()) {
                    Object o = dataIt.next();
                    Page p = data.getPage(o);
                    if (p == null) {
                        // was removed later - ignore
                    } else if (p.getPos() < 0) {
                        // temporarily changed - ok
                        // TODO move old data if there is an uncommitted change?
                    } else {
                        Chunk c = getChunk(p.getPos());
                        if (old.contains(c)) {
                            log("       move key:" + o + " chunk:" + c.id);
                            Object value = data.get(o);
                            data.remove(o);
                            data.put(o, value);
                        }
                    }
                }
            }
        }
        for (int o : oldChunks) {
            chunks.remove(o);
        }
    }

    /**
     * Read a page.
     *
     * @param map the map
     * @param pos the page position
     * @return the page
     */
    Page readPage(BtreeMap<?, ?> map, long pos) {
        if (pos < 0) {
            return temp.get(pos);
        }
        Page p = cache.get(pos);
        if (p == null) {
            long filePos = getFilePosition(pos);
            p = Page.read(file, map, filePos, pos);
            cache.put(pos, p);
        }
        return p;
    }

    /**
     * Remove a page.
     *
     * @param pos the position of the page
     */
    void removePage(long pos) {
        if (pos > 0) {
            cache.remove(pos);
            if (getChunk(pos).liveCount == 0) {
                throw new RuntimeException("Negative live count: " + pos);
            }
            getChunk(pos).liveCount--;
        } else {
            temp.remove(pos);
            if (temp.size() == 0) {
                tempPageId = 0;
            }
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

    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }

    int getMaxPageSize() {
        return maxPageSize;
    }

}
