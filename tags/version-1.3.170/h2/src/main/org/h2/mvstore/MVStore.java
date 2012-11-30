/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.h2.compress.CompressLZF;
import org.h2.compress.Compressor;
import org.h2.mvstore.cache.CacheLongKeyLIRS;
import org.h2.mvstore.cache.FilePathCache;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.DataTypeFactory;
import org.h2.mvstore.type.ObjectTypeFactory;
import org.h2.mvstore.type.StringType;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FileUtils;
import org.h2.util.New;

/*

File format:
header: (blockSize) bytes
header: (blockSize) bytes
[ chunk ] *
(there are two headers for security)
header:
H:3,...

TODO:
- store creation in file header, and seconds since creation
-- in chunk header (plus a counter) - ensure time never goes backwards
- recovery: keep some old chunks; don't overwritten
-- for 5 minutes (configurable)
- allocate memory with Utils.newBytes and so on
- unified exception handling
- concurrent map; avoid locking during IO (pre-load pages)
- maybe split database into multiple files, to speed up compact
- automated 'kill process' and 'power failure' test
- implement table engine for H2
- auto-compact from time to time and on close
- test and possibly improve compact operation (for large dbs)
- support background writes (concurrent modification & store)
- limited support for writing to old versions (branches)
- support concurrent operations (including file I/O)
- on insert, if the child page is already full, don't load and modify it
-- split directly (for leaves with 1 entry)
- performance test with encrypting file system
- possibly split chunk data into immutable and mutable
- compact: avoid processing pages using a counting bloom filter
- defragment (re-creating maps, specially those with small pages)
- write using ByteArrayOutputStream; remove DataType.getMaxLength
- file header: check formatRead and format (is formatRead
-- needed if equal to format?)
- chunk header: store changed chunk data as row; maybe after the root
- chunk checksum (header, last page, 2 bytes per page?)
- allow renaming maps
- file locking: solve problem that locks are shared for a VM
- online backup
- MapFactory is the wrong name (StorePlugin?) or is too flexible: remove?
- store file "header" at the end of each chunk; at the end of the file
- is there a better name for the file header,
-- if it's no longer always at the beginning of a file?
- maybe let a chunk point to possible next chunks
-- (so no fixed location header is needed)
- support stores that span multiple files (chunks stored in other files)
- triggers (can be implemented with a custom map)
- store write operations per page (maybe defragment
-- if much different than count)
- r-tree: nearest neighbor search
- use FileChannel by default (nio file system), but:
-- an interrupt close the FileChannel
- auto-save temporary data if it uses too much memory,
-- but revert it on startup if needed.
- map and chunk metadata: do not store default values
- support maps without values (non-unique indexes),
- and maps without keys (counted b-tree)
- use a small object cache (StringCache)
- dump values
- tool to import / manipulate CSV files (maybe concurrently)
- map split / merge (fast if no overlap)

*/

/**
 * A persistent storage for maps.
 */
public class MVStore {

    /**
     * Whether assertions are enabled.
     */
    public static final boolean ASSERT = false;

    /**
     * The block size (physical sector size) of the disk. The file header is
     * written twice, one copy in each block, to ensure it survives a crash.
     */
    static final int BLOCK_SIZE = 4 * 1024;

    private final HashMap<String, Object> config;

    private final String fileName;
    private final DataTypeFactory dataTypeFactory;

    private int pageSize = 6 * 1024;

    private FileChannel file;
    private FileLock fileLock;
    private long fileSize;
    private long rootChunkStart;

    /**
     * The cache. The default size is 16 MB, and the average size is 2 KB. It is
     * split in 16 segments. The stack move distance is 2% of the expected
     * number of entries.
     */
    private final CacheLongKeyLIRS<Page> cache;

    private int lastChunkId;
    private final HashMap<Integer, Chunk> chunks = New.hashMap();

    /**
     * The map of temporarily freed entries in the chunks. The key is the
     * unsaved version, the value is the map of chunks. The maps of chunks
     * contains the number of freed entries per chunk.
     */
    private final HashMap<Long, HashMap<Integer, Chunk>> freedChunks = New.hashMap();

    private MVMap<String, String> meta;
    private final HashMap<String, MVMap<?, ?>> maps = New.hashMap();

    /**
     * The set of maps with potentially unsaved changes.
     */
    private final HashMap<Integer, MVMap<?, ?>> mapsChanged = New.hashMap();

    private HashMap<String, String> fileHeader = New.hashMap();

    private final boolean readOnly;

    private int lastMapId;

    private volatile boolean reuseSpace = true;
    private long retainVersion = -1;
    private int retainChunk = -1;

    private Compressor compressor;

    private long currentVersion;
    private int fileReadCount;
    private int fileWriteCount;
    private int unsavedPageCount;

    MVStore(HashMap<String, Object> config) {
        this.config = config;
        this.fileName = (String) config.get("fileName");
        DataTypeFactory parent = new ObjectTypeFactory();
        DataTypeFactory f = (DataTypeFactory) config.get("dataTypeFactory");
        if (f == null) {
            f = parent;
        } else {
            f.setParent(parent);
        }
        this.dataTypeFactory = f;
        this.readOnly = "r".equals(config.get("openMode"));
        this.compressor = "0".equals(config.get("compression")) ? null : new CompressLZF();
        if (fileName != null) {
            Object s = config.get("cacheSize");
            int mb = s == null ? 16 : Integer.parseInt(s.toString());
            cache = CacheLongKeyLIRS.newInstance(
                    mb * 1024 * 1024, 2048, 16, mb * 1024 * 1024 / 2048 * 2 / 100);
        } else {
            cache = null;
        }
    }

    /**
     * Open a store in exclusive mode.
     *
     * @param fileName the file name (null for in-memory)
     * @return the store
     */
    public static MVStore open(String fileName) {
        HashMap<String, Object> config = New.hashMap();
        config.put("fileName", fileName);
        MVStore s = new MVStore(config);
        s.open();
        return s;
    }

    /**
     * Open an old, stored version of a map.
     *
     * @param version the version
     * @param name the map name
     * @param template the template map
     * @return the read-only map
     */
    @SuppressWarnings("unchecked")
    <T extends MVMap<?, ?>> T  openMapVersion(long version, String name, MVMap<?, ?> template) {
        MVMap<String, String> oldMeta = getMetaMap(version);
        String r = oldMeta.get("root." + template.getId());
        long rootPos = r == null ? 0 : Long.parseLong(r);
        MVMap<?, ?> m = template.openReadOnly();
        m.setRootPos(rootPos, version);
        return (T) m;
    }

    /**
     * Open a map with the previous key and value type (if the map already
     * exists), or Object if not.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @return the map
     */
    @SuppressWarnings("unchecked")
    public <K, V> MVMap<K, V> openMap(String name) {
        return (MVMap<K, V>) openMap(name, Object.class, Object.class);
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
    public <K, V> MVMap<K, V> openMap(String name, Class<K> keyClass, Class<V> valueClass) {
        DataType keyType = getDataType(keyClass);
        DataType valueType = getDataType(valueClass);
        MVMap<K, V> m = new MVMap<K, V>(keyType, valueType);
        return openMap(name, m);
    }

    /**
     * Open a map using the given template. The returned map is of the same type
     * as the template, and contains the same key and value types. If a map with
     * this name is already open, this map is returned. If it is not open,
     * the template object is opened with the applicable configuration.
     *
     * @param <T> the map type
     * @param name the name of the map
     * @param template the template map
     * @return the opened map
     */
    @SuppressWarnings("unchecked")
    public <T extends MVMap<K, V>, K, V> T openMap(String name, T template) {
        MVMap<K, V> m = (MVMap<K, V>) maps.get(name);
        if (m != null) {
            return (T) m;
        }
        m = template;
        String config = meta.get("map." + name);
        long root;
        HashMap<String, String> c;
        if (config == null) {
            c = New.hashMap();
            c.put("id", Integer.toString(++lastMapId));
            c.put("name", name);
            c.put("createVersion", Long.toString(currentVersion));
            m.open(this, c);
            meta.put("map." + name, m.asString());
            root = 0;
        } else {
            c = DataUtils.parseMap(config);
            String r = meta.get("root." + c.get("id"));
            root = r == null ? 0 : Long.parseLong(r);
        }
        m.open(this, c);
        m.setRootPos(root, -1);
        maps.put(name, m);
        return (T) m;
    }

    /**
     * Get the metadata map. This data is for informational purposes only. The
     * data is subject to change in future versions. The data should not be
     * modified (doing so may corrupt the store).
     * <p>
     * It contains the following entries:
     *
     * <pre>
     * map.{name} = {map metadata}
     * root.{mapId} = {root position}
     * chunk.{chunkId} = {chunk metadata}
     * </pre>
     *
     * @return the metadata map
     */
    public MVMap<String, String> getMetaMap() {
        return meta;
    }

    private MVMap<String, String> getMetaMap(long version) {
        Chunk c = getChunkForVersion(version);
        if (c == null) {
            throw new IllegalArgumentException("Unknown version: " + version);
        }
        c = readChunkHeader(c.start);
        MVMap<String, String> oldMeta = meta.openReadOnly();
        oldMeta.setRootPos(c.metaRootPos, version);
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
     * Remove a map.
     *
     * @param name the map name
     */
    void removeMap(String name) {
        MVMap<?, ?> map = maps.remove(name);
        mapsChanged.remove(map.getId());
    }

    private DataType getDataType(Class<?> clazz) {
        if (clazz == String.class) {
            return StringType.INSTANCE;
        }
        if (dataTypeFactory == null) {
            throw new RuntimeException("No data type factory set " +
                    "and don't know how to serialize " + clazz);
        }
        String s = dataTypeFactory.getDataType(clazz);
        return dataTypeFactory.buildDataType(s);
    }

    /**
     * Mark a map as changed (containing unsaved changes).
     *
     * @param map the map
     */
    void markChanged(MVMap<?, ?> map) {
        mapsChanged.put(map.getId(), map);
    }

    /**
     * Open the store.
     */
    void open() {
        meta = new MVMap<String, String>(StringType.INSTANCE, StringType.INSTANCE);
        HashMap<String, String> c = New.hashMap();
        c.put("id", "0");
        c.put("name", "meta");
        c.put("createVersion", Long.toString(currentVersion));
        meta.open(this, c);
        if (fileName == null) {
            return;
        }
        FileUtils.createDirectories(FileUtils.getParent(fileName));
        try {
            log("file open");
            file = FilePathCache.wrap(FilePath.get(fileName).open("rw"));
            if (readOnly) {
                fileLock = file.tryLock(0, Long.MAX_VALUE, true);
                if (fileLock == null) {
                    throw new RuntimeException("The file is locked: " + fileName);
                }
            } else {
                fileLock = file.tryLock();
                if (fileLock == null) {
                    throw new RuntimeException("The file is locked: " + fileName);
                }
            }
            fileSize = file.size();
            if (fileSize == 0) {
                fileHeader.put("H", "3");
                fileHeader.put("blockSize", "" + BLOCK_SIZE);
                fileHeader.put("format", "1");
                fileHeader.put("formatRead", "1");
                writeFileHeader();
            } else {
                readFileHeader();
                if (rootChunkStart > 0) {
                    readMeta();
                }
            }
        } catch (Exception e) {
            close();
            throw convert(e);
        }
    }

    private void readMeta() {
        Chunk header = readChunkHeader(rootChunkStart);
        lastChunkId = header.id;
        chunks.put(header.id, header);
        meta.setRootPos(header.metaRootPos, -1);
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
                c.maxLengthLive = header.maxLengthLive;
                c.pageCount = header.pageCount;
                c.maxLength = header.maxLength;
            }
            lastChunkId = Math.max(c.id, lastChunkId);
            chunks.put(c.id, c);
        }
    }

    private void readFileHeader() {
        try {
            byte[] headers = new byte[2 * BLOCK_SIZE];
            fileReadCount++;
            DataUtils.readFully(file, 0, ByteBuffer.wrap(headers));
            for (int i = 0; i <= BLOCK_SIZE; i += BLOCK_SIZE) {
                String s = new String(headers, i, BLOCK_SIZE, "UTF-8").trim();
                fileHeader = DataUtils.parseMap(s);
                rootChunkStart = Long.parseLong(fileHeader.get("rootChunk"));
                currentVersion = Long.parseLong(fileHeader.get("version"));
                lastMapId = Integer.parseInt(fileHeader.get("lastMapId"));
                int check = (int) Long.parseLong(fileHeader.get("fletcher"), 16);
                s = s.substring(0, s.lastIndexOf("fletcher") - 1) + " ";
                byte[] bytes = s.getBytes("UTF-8");
                int checksum = DataUtils.getFletcher32(bytes,
                        bytes.length / 2 * 2);
                if (check == checksum) {
                    return;
                }
            }
            throw new RuntimeException("File header is corrupt");
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private void writeFileHeader() {
        try {
            StringBuilder buff = new StringBuilder();
            fileHeader.put("lastMapId", "" + lastMapId);
            fileHeader.put("rootChunk", "" + rootChunkStart);
            fileHeader.put("version", "" + currentVersion);
            DataUtils.appendMap(buff, fileHeader);
            byte[] bytes = (buff.toString() + " ").getBytes("UTF-8");
            int checksum = DataUtils.getFletcher32(bytes, bytes.length / 2 * 2);
            DataUtils.appendMap(buff, "fletcher", Integer.toHexString(checksum));
            bytes = buff.toString().getBytes("UTF-8");
            if (bytes.length > BLOCK_SIZE) {
                throw new IllegalArgumentException("File header too large: " + buff);
            }
            ByteBuffer header = ByteBuffer.allocate(2 * BLOCK_SIZE);
            header.put(bytes);
            header.position(BLOCK_SIZE);
            header.put(bytes);
            header.rewind();
            fileWriteCount++;
            DataUtils.writeFully(file,  0, header);
            fileSize = Math.max(fileSize, 2 * BLOCK_SIZE);
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
                shrinkFileIfPossible(0);
                log("file close");
                if (fileLock != null) {
                    fileLock.release();
                    fileLock = null;
                }
                file.close();
                for (MVMap<?, ?> m : New.arrayList(maps.values())) {
                    m.close();
                }
                meta = null;
                compressor = null;
                chunks.clear();
                cache.clear();
                maps.clear();
                mapsChanged.clear();
            } catch (Exception e) {
                throw convert(e);
            } finally {
                file = null;
            }
        }
    }

    /**
     * Get the chunk for the given position.
     *
     * @param pos the position
     * @return the chunk
     */
    Chunk getChunk(long pos) {
        return chunks.get(DataUtils.getPageChunkId(pos));
    }

    /**
     * Increment the current version.
     *
     * @return the new version
     */
    public long incrementVersion() {
        return ++currentVersion;
    }

    /**
     * Commit all changes and persist them to disk. This method does nothing if
     * there are no unsaved changes, otherwise it stores the data and increments
     * the current version.
     *
     * @return the new version (incremented if there were changes)
     */
    public long store() {
        if (!hasUnsavedChanges()) {
            return currentVersion;
        }

        int currentUnsavedPageCount = unsavedPageCount;
        long storeVersion = currentVersion;
        long version = incrementVersion();

        // the last chunk was not completely correct in the last store()
        // this needs to be updated now (it's better not to update right after
        // storing, because that would modify the meta map again)
        Chunk c = chunks.get(lastChunkId);
        if (c != null) {
            meta.put("chunk." + c.id, c.asString());
        }
        c = new Chunk(++lastChunkId);
        c.maxLength = Long.MAX_VALUE;
        c.maxLengthLive = Long.MAX_VALUE;
        c.start = Long.MAX_VALUE;
        c.length = Integer.MAX_VALUE;
        c.version = version;
        chunks.put(c.id, c);
        meta.put("chunk." + c.id, c.asString());

        int maxLength = 1 + 4 + 4 + 8;
        for (MVMap<?, ?> m : mapsChanged.values()) {
            if (m == meta || !m.hasUnsavedChanges()) {
                continue;
            }
            Page p = m.openVersion(storeVersion).getRoot();
            if (p.getTotalCount() == 0) {
                meta.put("root." + m.getId(), "0");
            } else {
                maxLength += p.getMaxLengthTempRecursive();
                meta.put("root." + m.getId(), String.valueOf(Long.MAX_VALUE));
            }
        }
        applyFreedChunks();
        ArrayList<Integer> removedChunks = New.arrayList();
        do {
            for (Chunk x : chunks.values()) {
                if (x.maxLengthLive == 0 && (retainChunk == -1 || x.id < retainChunk)) {
                    meta.remove("chunk." + x.id);
                    removedChunks.add(x.id);
                } else {
                    meta.put("chunk." + x.id, x.asString());
                }
                applyFreedChunks();
            }
        } while (freedChunks.size() > 0);
        maxLength += meta.getRoot().getMaxLengthTempRecursive();

        ByteBuffer buff = ByteBuffer.allocate(maxLength);
        // need to patch the header later
        c.writeHeader(buff);
        c.maxLength = 0;
        c.maxLengthLive = 0;
        for (MVMap<?, ?> m : mapsChanged.values()) {
            if (m == meta || !m.hasUnsavedChanges()) {
                continue;
            }
            Page p = m.openVersion(storeVersion).getRoot();
            if (p.getTotalCount() > 0) {
                long root = p.writeUnsavedRecursive(c, buff);
                meta.put("root." + m.getId(), "" + root);
            }
        }

        meta.put("chunk." + c.id, c.asString());

        if (ASSERT) {
            if (freedChunks.size() > 0) {
                throw new RuntimeException("Temporary freed chunks");
            }
        }

        // this will modify maxLengthLive, but
        // the correct value is written in the chunk header
        meta.getRoot().writeUnsavedRecursive(c, buff);

        buff.flip();
        int length = buff.limit();
        long filePos = allocateChunk(length);

        // need to keep old chunks
        // until they are are no longer referenced
        // by an old version
        // so empty space is not reused too early
        for (int x : removedChunks) {
            chunks.remove(x);
        }

        buff.rewind();

        c.start = filePos;
        c.length = length;
        c.metaRootPos = meta.getRoot().getPos();
        c.writeHeader(buff);
        buff.rewind();
        try {
            fileWriteCount++;
            DataUtils.writeFully(file, filePos, buff);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        fileSize = Math.max(fileSize, filePos + buff.position());
        rootChunkStart = filePos;
        revertTemp();

        // write the new version (after the commit)
        writeFileHeader();
        shrinkFileIfPossible(1);
        // some pages might have been changed in the meantime (in the newest version)
        unsavedPageCount = Math.max(0, unsavedPageCount - currentUnsavedPageCount);
        return version;
    }

    private void applyFreedChunks() {
        for (HashMap<Integer, Chunk> freed : freedChunks.values()) {
            for (Chunk f : freed.values()) {
                Chunk c = chunks.get(f.id);
                c.maxLengthLive += f.maxLengthLive;
                if (c.maxLengthLive < 0) {
                    throw new RuntimeException("Corrupt max length");
                }
            }
        }
        freedChunks.clear();
    }

    /**
     * Shrink the file if possible, and if at least a given percentage can be
     * saved.
     *
     * @param minPercent the minimum percentage to save
     */
    private void shrinkFileIfPossible(int minPercent) {
        long used = getFileLengthUsed();
        try {
            if (used >= fileSize) {
                return;
            }
            if (minPercent > 0 && fileSize - used < BLOCK_SIZE) {
                return;
            }
            int savedPercent = (int) (100 - (used * 100 / fileSize));
            if (savedPercent < minPercent) {
                return;
            }
            file.truncate(used);
            fileSize = used;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private long getFileLengthUsed() {
        int min = 2;
        for (Chunk c : chunks.values()) {
            if (c.start == Long.MAX_VALUE) {
                continue;
            }
            int last = (int) ((c.start + c.length) / BLOCK_SIZE);
            min = Math.max(min,  last + 1);
        }
        return min * BLOCK_SIZE;
    }

    private long allocateChunk(long length) {
        if (!reuseSpace) {
            return getFileLengthUsed();
        }
        BitSet set = new BitSet();
        set.set(0);
        set.set(1);
        for (Chunk c : chunks.values()) {
            if (c.start == Long.MAX_VALUE) {
                continue;
            }
            int first = (int) (c.start / BLOCK_SIZE);
            int last = (int) ((c.start + c.length) / BLOCK_SIZE);
            set.set(first, last +1);
        }
        int required = (int) (length / BLOCK_SIZE) + 1;
        for (int i = 0; i < set.size(); i++) {
            if (!set.get(i)) {
                boolean ok = true;
                for (int j = 0; j < required; j++) {
                    if (set.get(i + j)) {
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    return i * BLOCK_SIZE;
                }
            }
        }
        return set.size() * BLOCK_SIZE;
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
        for (MVMap<?, ?> m : mapsChanged.values()) {
            if (m == meta || m.hasUnsavedChanges()) {
                return true;
            }
        }
        return false;
    }

    private Chunk readChunkHeader(long start) {
        try {
            fileReadCount++;
            ByteBuffer buff = ByteBuffer.allocate(40);
            DataUtils.readFully(file, start, buff);
            buff.rewind();
            return Chunk.fromHeader(buff, start);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Try to reduce the file size. Chunks with a low number of live items will
     * be re-written. If the current fill rate is higher than the target fill
     * rate, no optimization is done.
     *
     * @param fillRate the minimum percentage of live entries
     * @return if anything was written
     */
    public boolean compact(int fillRate) {
        if (chunks.size() == 0) {
            // avoid division by 0
            return false;
        }
        long maxLengthSum = 0, maxLengthLiveSum = 0;
        for (Chunk c : chunks.values()) {
            maxLengthSum += c.maxLength;
            maxLengthLiveSum += c.maxLengthLive;
        }
        if (maxLengthSum <= 0) {
            // avoid division by 0
            maxLengthSum = 1;
        }
        int percentTotal = (int) (100 * maxLengthLiveSum / maxLengthSum);
        if (percentTotal > fillRate) {
            return false;
        }

        // calculate the average max length
        int averageMaxLength = (int) (maxLengthSum / chunks.size());

        // the 'old' list contains the chunks we want to free up
        ArrayList<Chunk> old = New.arrayList();
        for (Chunk c : chunks.values()) {
            if (retainChunk == -1 || c.id < retainChunk) {
                int age = lastChunkId - c.id + 1;
                c.collectPriority = c.getFillRate() / age;
                old.add(c);
            }
        }
        if (old.size() == 0) {
            return false;
        }

        // sort the list, so the first entry should be collected first
        Collections.sort(old, new Comparator<Chunk>() {
            public int compare(Chunk o1, Chunk o2) {
                return new Integer(o1.collectPriority).compareTo(o2.collectPriority);
            }
        });

        // find out up to were in the old list we need to move
        // try to move one (average sized) chunk
        long moved = 0;
        Chunk move = null;
        for (Chunk c : old) {
            if (move != null && moved + c.maxLengthLive > averageMaxLength) {
                break;
            }
            log(" chunk " + c.id + " " + c.getFillRate() + "% full; prio=" + c.collectPriority);
            moved += c.maxLengthLive;
            move = c;
        }

        // remove the chunks we want to keep from this list
        boolean remove = false;
        for (Iterator<Chunk> it = old.iterator(); it.hasNext();) {
            Chunk c = it.next();
            if (move == c) {
                remove = true;
            } else if (remove) {
                it.remove();
            }
        }

        // iterate over all the pages in the old pages
        for (Chunk c : old) {
            copyLive(c, old);
        }

        store();
        return true;
    }

    private void copyLive(Chunk chunk, ArrayList<Chunk> old) {
        ByteBuffer buff = ByteBuffer.allocate(chunk.length);
        try {
            DataUtils.readFully(file, chunk.start, buff);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Chunk.fromHeader(buff, chunk.start);
        int chunkLength = chunk.length;
        // mark a change, even if it doesn't look like there was a change
        // as changes in the metadata alone are not detected
        markChanged(meta);
        while (buff.position() < chunkLength) {
            int start = buff.position();
            int pageLength = buff.getInt();
            buff.getShort();
            int mapId = DataUtils.readVarInt(buff);
            @SuppressWarnings("unchecked")
            MVMap<Object, Object> map = (MVMap<Object, Object>) getMap(mapId);
            if (map == null) {
                buff.position(start + pageLength);
                continue;
            }
            buff.position(start);
            Page page = new Page(map, 0);
            page.read(buff, chunk.id, buff.position(), chunk.length);
            for (int i = 0; i < page.getKeyCount(); i++) {
                Object k = page.getKey(i);
                Page p = map.getPage(k);
                if (p == null) {
                    // was removed later - ignore
                    // or the chunk no longer exists
                } else if (p.getPos() < 0) {
                    // temporarily changed - ok
                    // TODO move old data if there is an uncommitted change?
                } else {
                    Chunk c = getChunk(p.getPos());
                    if (old.contains(c)) {
                        log("       move key:" + k + " chunk:" + c.id);
                        Object value = map.remove(k);
                        map.put(k, value);
                    }
                }
            }
        }
    }

    private MVMap<?, ?> getMap(int mapId) {
        if (mapId == 0) {
            return meta;
        }
        for (MVMap<?, ?> m : maps.values()) {
            if (m.getId() == mapId) {
                return m;
            }
        }
        return null;
    }

    /**
     * Read a page.
     *
     * @param map the map
     * @param pos the page position
     * @return the page
     */
    Page readPage(MVMap<?, ?> map, long pos) {
        Page p = cache.get(pos);
        if (p == null) {
            Chunk c = getChunk(pos);
            if (c == null) {
                throw new RuntimeException("Chunk " + DataUtils.getPageChunkId(pos) + " not found");
            }
            long filePos = c.start;
            filePos += DataUtils.getPageOffset(pos);
            fileReadCount++;
            p = Page.read(file, map, pos, filePos, fileSize);
            cache.put(pos, p, p.getMemory());
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
        if (pos == 0) {
            unsavedPageCount--;
            return;
        }
        // this could result in a cache miss
        // if the operation is rolled back,
        // but we don't optimize for rollback
        cache.remove(pos);
        Chunk c = getChunk(pos);
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
        f.maxLengthLive -= DataUtils.getPageMaxLength(pos);
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

    /**
     * Set the amount of memory a page should contain at most, in bytes. Larger
     * pages are split. The default is 6 KB. This is not a limit in the page
     * size (pages with one entry can get larger), it is just the point where
     * pages are split.
     *
     * @param pageSize the page size
     */
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Get the page size, in bytes.
     *
     * @return the page size
     */
    public int getPageSize() {
        return pageSize;
    }

    Compressor getCompressor() {
        return compressor;
    }

    public boolean getReuseSpace() {
        return reuseSpace;
    }

    /**
     * Whether empty space in the file should be re-used. If enabled, old data
     * is overwritten (default). If disabled, writes are appended at the end of
     * the file.
     * <p>
     * This setting is specially useful for online backup. To create an online
     * backup, disable this setting, then copy the file (starting at the
     * beginning of the file). In this case, concurrent backup and write
     * operations are possible (obviously the backup process needs to be faster
     * than the write operations).
     *
     * @param reuseSpace the new value
     */
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

    /**
     * Which version to retain. If not set, all versions up to the last stored
     * version are retained.
     *
     * @param retainVersion the oldest version to retain
     */
    public void setRetainVersion(long retainVersion) {
        this.retainVersion = retainVersion;
    }

    public long getRetainVersion() {
        return retainVersion;
    }

    /**
     * Check whether all data can be read from this version. This requires that
     * all chunks referenced by this version are still available (not
     * overwritten).
     *
     * @param version the version
     * @return true if all data can be read
     */
    private boolean isKnownVersion(long version) {
        if (version > currentVersion || version < 0) {
            return false;
        }
        if (version == currentVersion || chunks.size() == 0) {
            // no stored data
            return true;
        }
        // need to check if a chunk for this version exists
        Chunk c = getChunkForVersion(version);
        if (c == null) {
            return false;
        }
        // also, all chunks referenced by this version
        // need to be available in the file
        MVMap<String, String> oldMeta = getMetaMap(version);
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
     * Get the estimated number of unsaved pages. The returned value is not
     * accurate, specially after rollbacks, but can be used to estimate the
     * memory usage for unsaved data.
     *
     * @return the number of unsaved pages
     */
    public int getUnsavedPageCount() {
        return unsavedPageCount;
    }

    /**
     * Increment the number of unsaved pages.
     */
    void registerUnsavedPage() {
        unsavedPageCount++;
    }

    /**
     * Get the store version. The store version is usually used to upgrade the
     * structure of the store after upgrading the application. Initially the
     * store version is 0, until it is changed.
     *
     * @return the store version
     */
    public int getStoreVersion() {
        String x = meta.get("setting.storeVersion");
        return x == null ? 0 : Integer.parseInt(x);
    }

    /**
     * Update the store version.
     *
     * @param version the new store version
     */
    public void setStoreVersion(int version) {
        meta.put("setting.storeVersion", Integer.toString(version));
    }

    /**
     * Revert to the beginning of the given version. All later changes (stored
     * or not) are forgotten. All maps that were created later are closed. A
     * rollback to a version before the last stored version is immediately
     * persisted.
     *
     * @param version the version to revert to
     */
    public void rollbackTo(long version) {
        if (!isKnownVersion(version)) {
            throw new IllegalArgumentException("Unknown version: " + version);
        }
        // TODO could remove newer temporary pages on rollback
        for (MVMap<?, ?> m : mapsChanged.values()) {
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
                loadFromFile = true;
                do {
                    last = chunks.remove(lastChunkId);
                    lastChunkId--;
                } while (last.version > version && chunks.size() > 0);
                rootChunkStart = last.start;
                writeFileHeader();
                readFileHeader();
                readMeta();
            }
        }
        for (MVMap<?, ?> m : maps.values()) {
            if (m.getCreateVersion() >= version) {
                m.close();
                removeMap(m.getName());
            } else {
                if (loadFromFile) {
                    String r = meta.get("root." + m.getId());
                    long root = r == null ? 0 : Long.parseLong(r);
                    m.setRootPos(root, version);
                }
            }
        }
        this.currentVersion = version;
    }

    private void revertTemp() {
        freedChunks.clear();
        for (MVMap<?, ?> m : mapsChanged.values()) {
            m.removeAllOldVersions();
        }
        mapsChanged.clear();
    }

    /**
     * Get the current version of the data. When a new store is created, the
     * version is 0.
     *
     * @return the version
     */
    public long getCurrentVersion() {
        return currentVersion;
    }

    /**
     * Get the number of file write operations since this store was opened.
     *
     * @return the number of write operations
     */
    public int getFileWriteCount() {
        return fileWriteCount;
    }

    /**
     * Get the number of file read operations since this store was opened.
     *
     * @return the number of read operations
     */
    public int getFileReadCount() {
        return fileReadCount;
    }

    /**
     * Get the file name, or null for in-memory stores.
     *
     * @return the file name
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Get the file header. This data is for informational purposes only. The
     * data is subject to change in future versions. The data should not be
     * modified (doing so may corrupt the store).
     *
     * @return the file header
     */
    public Map<String, String> getFileHeader() {
        return fileHeader;
    }

    /**
     * Get the file instance in use, if a file is used. The application may read
     * from the file (for example for online backup), but not write to it or
     * truncate it.
     *
     * @return the file, or null
     */
    public FileChannel getFile() {
        return file;
    }

    public String toString() {
        return DataUtils.appendMap(new StringBuilder(), config).toString();
    }

}
