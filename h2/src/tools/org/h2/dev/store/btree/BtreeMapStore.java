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
import org.h2.compress.CompressLZF;
import org.h2.compress.Compressor;
import org.h2.dev.store.FilePathCache;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FileUtils;
import org.h2.util.New;
import org.h2.util.StringUtils;

/*

File format:
header: 4096 bytes
header: 4096 bytes
[ chunk ] *
(there are two headers for security)
header:
# H3 store #
blockSize=4096

TODO:
- support fast range deletes
- support custom pager for r-tree, kd-tree
- need an 'end of chunk' marker to verify all data is written
- truncate the file if it is empty
- ability to diff / merge versions
- check if range reads would be faster
- map.getVersion and opening old maps read-only
- limited support for writing to old versions (branches)
- Serializer instead of DataType, (serialize, deserialize)
- implement Map interface
- maybe rename to MVStore, MVMap, TestMVStore
- implement Map interface
- atomic operations (test-and-set)
- support back ground writes (store old version)
- re-use map ids that were not used for a very long time
- file header could be a regular chunk, end of file the second
- possibly split chunk data into immutable and mutable
- reduce minimum chunk size, speed up very small transactions
- defragment: use total max length instead of page count (liveCount)

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

    /**
     * The map of temporarily freed entries in the chunks. The key is the
     * unsaved version, the value is the map of chunks. The maps of chunks
     * contains the number of freed entries per chunk.
     */
    private HashMap<Long, HashMap<Integer, Chunk>> freedChunks = New.hashMap();

    private BtreeMap<String, String> meta;
    private HashMap<String, BtreeMap<?, ?>> maps = New.hashMap();

    /**
     * The set of maps with potentially unsaved changes.
     */
    private HashMap<Integer, BtreeMap<?, ?>> mapsChanged = New.hashMap();
    private int lastMapId;

    private boolean reuseSpace = true;
    private int retainChunk = -1;

    private Compressor compressor = new CompressLZF();

    private long currentVersion;
    private int readCount;
    private int writeCount;

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
            long root;
            long createVersion;
            if (identifier == null) {
                id = ++lastMapId;
                String types = id + "/" + currentVersion + "/" + keyType.asString() + "/" + valueType.asString();
                meta.put("map." + name, types);
                root = 0;
                createVersion = currentVersion;
            } else {
                String types = meta.get("map." + name);
                String[] idTypeList = StringUtils.arraySplit(types, '/', false);
                id = Integer.parseInt(idTypeList[0]);
                createVersion = Long.parseLong(idTypeList[1]);
                keyType = getDataType(idTypeList[2]);
                valueType = getDataType(idTypeList[3]);
                String r = meta.get("root." + id);
                root = r == null ? 0 : Long.parseLong(r);
            }
            m = new BtreeMap<K, V>(this, id, name, keyType, valueType, createVersion);
            maps.put(name, m);
            m.setRootPos(root);
        }
        return m;
    }

    /**
     * Get the metadata map. It contains the following entries:
     *
     * <pre>
     * map.{name} = {mapId}/{keyType}/{valueType}
     * root.{mapId} = {rootPos}
     * chunk.{chunkId} = {chunkData}
     * </pre>
     *
     * @return the metadata map
     */
    public BtreeMap<String, String> getMetaMap() {
        return meta;
    }

    private BtreeMap<String, String> getMetaMap(long version) {
        Chunk c = getChunkForVersion(version);
        BtreeMap<String, String> oldMeta = new BtreeMap<String, String>(this, 0, "old-meta", STRING_TYPE, STRING_TYPE, 0);
        oldMeta.setRootPos(c.metaRootPos);
        return oldMeta;
    }

    private Chunk getChunkForVersion(long version) {
        for (int chunkId = lastChunkId;; chunkId--) {
            Chunk x = chunks.get(chunkId);
            if (x == null || x.version < version) {
                return null;
            } else if (x.version == version) {
                return x;
            }
        }
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
        BtreeMap<?, ?> m = maps.remove(id);
        mapsChanged.remove(m);
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
     * Mark a map as changed (containing unsaved changes).
     *
     * @param map the map
     */
    void markChanged(BtreeMap<?, ?> map) {
        mapsChanged.put(map.getId(), map);
    }

    private void open() {
        meta = new BtreeMap<String, String>(this, 0, "meta", STRING_TYPE, STRING_TYPE, 0);
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
                "lastMapId:" + lastMapId + "\n" +
                "version:" + currentVersion + "\n").getBytes("UTF-8"));
            writeCount++;
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
            byte[] header = new byte[blockSize];
            readCount++;
            file.position(0);
            // TODO read fully; read both headers
            file.read(ByteBuffer.wrap(header));
            Properties prop = new Properties();
            prop.load(new StringReader(new String(header, "UTF-8")));
            rootChunkStart = Long.parseLong(prop.get("rootChunk").toString());
            currentVersion = Long.parseLong(prop.get("version").toString());
            lastMapId = Integer.parseInt(prop.get("lastMapId").toString());
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private static RuntimeException convert(Exception e) {
        throw new RuntimeException("Exception: " + e, e);
    }

    /**
     * Close the file. Uncommitted changes are ignored, and all open maps are closed.
     */
    public void close() {
        if (file != null) {
            try {
                log("file close");
                file.close();
                for (BtreeMap<?, ?> m : New.arrayList(maps.values())) {
                    m.close();
                }
                temp.clear();
                meta = null;
                compressor = null;
                chunks.clear();
                cache.clear();
                maps.clear();
                mapsChanged.clear();
            } catch (Exception e) {
                file = null;
                throw convert(e);
            }
        }
    }

    private Chunk getChunk(long pos) {
        return chunks.get(DataUtils.getPageChunkId(pos));
    }

    private long getFilePosition(long pos) {
        Chunk c = getChunk(pos);
        if (c == null) {
            throw new RuntimeException("Chunk " + DataUtils.getPageChunkId(pos) + " not found");
        }
        long filePos = c.start;
        filePos += DataUtils.getPageOffset(pos);
        return filePos;
    }

    /**
     * Commit all changes and persist them to disk. This method does nothing if
     * there are no unsaved changes.
     *
     * @return the new version
     */
    public long store() {
        if (!hasUnsavedChanges()) {
            return currentVersion;
        }
        long newVersion = commit();

        int chunkId = ++lastChunkId;

        Chunk c = new Chunk(chunkId);
        c.entryCount = Integer.MAX_VALUE;
        c.liveCount = Integer.MAX_VALUE;
        c.start = Long.MAX_VALUE;
        c.length = Long.MAX_VALUE;
        c.version = currentVersion;
        chunks.put(c.id, c);
        meta.put("chunk." + c.id, c.toString());
        applyFreedChunks();
        ArrayList<Integer> removedChunks = New.arrayList();
        for (Chunk x : chunks.values()) {
            if (x.liveCount == 0 && (retainChunk == -1 || x.id < retainChunk)) {
                meta.remove("chunk." + x.id);
                removedChunks.add(x.id);
            } else {
                meta.put("chunk." + x.id, x.toString());
            }
            applyFreedChunks();
        }
        for (int x : removedChunks) {
            chunks.remove(x);
        }
        int count = 0;
        int maxLength = 1 + 4 + 4 + 8;
        for (BtreeMap<?, ?> m : mapsChanged.values()) {
            if (m == meta || !m.hasUnsavedChanges()) {
                continue;
            }
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
            if (m == meta || !m.hasUnsavedChanges()) {
                continue;
            }
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
            writeCount++;
            file.position(filePos);
            file.write(buff);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        rootChunkStart = filePos;
        writeHeader();
        revertTemp();
        // update the start position and length
        c.start = filePos;
        c.length = length;
        meta.put("chunk." + c.id, c.toString());

        return newVersion;
    }

    private void applyFreedChunks() {
        // apply liveCount changes
        for (HashMap<Integer, Chunk> freed : freedChunks.values()) {
            for (Chunk f : freed.values()) {
                Chunk c = chunks.get(f.id);
                c.liveCount += f.liveCount;
            }
        }
        freedChunks.clear();
    }

    private long allocateChunk(long length) {
        if (!reuseSpace) {
            int min = 0;
            for (Chunk c : chunks.values()) {
                if (c.start == Long.MAX_VALUE) {
                    continue;
                }
                int last = (int) ((c.start + c.length) / blockSize);
                min = Math.max(min,  last + 1);
            }
            return min * blockSize;
        }
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
     * Commit the changes, incrementing the current version.
     *
     * @return the new version
     */
    public long commit() {
        return ++currentVersion;
    }

    /**
     * Check whether there are any unsaved changes.
     *
     * @return if there are any changes
     */
    public boolean hasUnsavedChanges() {
        if (mapsChanged.size() == 0) {
            return false;
        }
        for (BtreeMap<?, ?> m : mapsChanged.values()) {
            if (m.hasUnsavedChanges()) {
                return true;
            }
        }
        return false;
    }

    private Chunk readChunkHeader(long start) {
        try {
            readCount++;
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
        // TODO not needed - we already have the chunk object
        Chunk header = readChunkHeader(move.start);
        log("  meta:" + move.id + "/" + header.metaRootPos + " start: " + move.start);
        BtreeMap<String, String> oldMeta = new BtreeMap<String, String>(this, 0, "old-meta", STRING_TYPE, STRING_TYPE, 0);
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
            DataType kt = getDataType(idTypesList[2]);
            DataType vt = getDataType(idTypesList[3]);
            long oldDataRoot = Long.parseLong(oldMeta.get("root." + id));
            BtreeMap<?, ?> oldData = new BtreeMap<Object, Object>(this, id, "old-" + k, kt, vt, 0);
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
            readCount++;
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
        // we need to keep temporary pages,
        // to support reading old versions and rollback
        if (pos > 0) {
            // this could result in a cache miss
            // if the operation is rolled back,
            // but we don't optimize for rollback
            cache.remove(pos);
            Chunk c = getChunk(pos);
            if (c.liveCount == 0) {
                throw new RuntimeException("Negative live count: " + pos);
            }
            HashMap<Integer, Chunk>freed = freedChunks.get(currentVersion);
            if (freed == null) {
                freed = New.hashMap();
                freedChunks.put(currentVersion, freed);
            }
            Chunk f = freed.get(c.id);
            if (f == null) {
                f = new Chunk(c.id);
                freed.put(c.id, f);
            }
            f.liveCount--;
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

    /**
     * The maximum number of key-value pairs in a page.
     *
     * @return the maximum number of entries
     */
    int getMaxPageSize() {
        return maxPageSize;
    }

    public Compressor getCompressor() {
        return compressor;
    }

    public void setCompressor(Compressor compressor) {
        this.compressor = compressor;
    }

    public boolean getReuseSpace() {
        return reuseSpace;
    }

    public void setReuseSpace(boolean reuseSpace) {
        this.reuseSpace = reuseSpace;
    }

    public int getRetainChunk() {
        return retainChunk;
    }

    /**
     * Which chunk to retain. If not set, old chunks are re-used as soon as
     * possible, which may make it impossible to roll back beyond a save
     * operation, or read a older version before.
     * <p>
     * This setting is not persisted.
     *
     * @param retainChunk the earliest chunk to retain (0 to retain all chunks,
     *        -1 to re-use space as early as possible)
     */
    public void setRetainChunk(int retainChunk) {
        this.retainChunk = retainChunk;
    }

    public boolean isKnownVersion(long version) {
        if (version > currentVersion || version < 0) {
            return false;
        }
        if (chunks.size() == 0) {
            return true;
        }
        Chunk c = getChunkForVersion(version);
        if (c == null) {
            return false;
        }
        BtreeMap<String, String> oldMeta = getMetaMap(version);
        if (oldMeta == null) {
            return false;
        }
        for (Iterator<String> it = oldMeta.keyIterator("chunk."); it.hasNext();) {
            String chunkKey = it.next();
            if (!chunkKey.startsWith("chunk.")) {
                break;
            }
            if (!meta.containsKey(chunkKey)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Revert to the given version. All later changes (stored or not) are
     * forgotten. All maps that were created later are closed. A rollback to
     * a version before the last stored version is immediately persisted.
     *
     * @param version the version to keep
     */
    public void rollbackTo(long version) {
        if (!isKnownVersion(version)) {
            throw new IllegalArgumentException("Unknown version: " + version);
        }
        // TODO could remove newer temporary pages on rollback
        for (BtreeMap<?, ?> m : mapsChanged.values()) {
            m.rollbackTo(version);
        }
        for (long v = currentVersion; v >= version; v--) {
            if (freedChunks.size() == 0) {
                break;
            }
            freedChunks.remove(v);
        }
        meta.rollbackTo(version);
        boolean loadFromFile = false;
        Chunk last = chunks.get(lastChunkId);
        if (last != null) {
            if (last.version >= version) {
                revertTemp();
            }
            if (last.version > version) {
                loadFromFile = true;
                while (last != null && last.version > version) {
                    chunks.remove(lastChunkId);
                    lastChunkId--;
                    last = chunks.get(lastChunkId);
                }
                rootChunkStart = last.start;
                writeHeader();
                readHeader();
                readMeta();
            }
        }
        for (BtreeMap<?, ?> m : maps.values()) {
            if (m.getCreatedVersion() > version) {
                m.close();
                removeMap(m.getId());
            } else {
                if (loadFromFile) {
                    String r = meta.get("root." + m.getId());
                    long root = r == null ? 0 : Long.parseLong(r);
                    m.setRootPos(root);
                }
            }
        }
        this.currentVersion = version;
    }

    private void revertTemp() {
        freedChunks.clear();
        mapsChanged.clear();
        temp.clear();
        tempPageId = 0;
    }

    /**
     * Get the current version of the store. When a new store is created, the
     * version is 0. For each commit, it is incremented by one if there was a
     * change.
     *
     * @return the version
     */
    public long getCurrentVersion() {
        return currentVersion;
    }

    /**
     * Get the number of write operations since this store was opened.
     *
     * @return the number of write operations
     */
    public int getWriteCount() {
        return writeCount;
    }

    /**
     * Get the number of read operations since this store was opened.
     *
     * @return the number of read operations
     */
    public int getReadCount() {
        return readCount;
    }

}
