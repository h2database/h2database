/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreException;
import org.h2.mvstore.StreamStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.tx.TransactionStore;
import org.h2.mvstore.type.BasicDataType;
import org.h2.mvstore.type.ByteArrayDataType;
import org.h2.mvstore.type.LongDataType;
import org.h2.store.CountingReaderInputStream;
import org.h2.store.LobStorageFrontend;
import org.h2.store.LobStorageInterface;
import org.h2.store.RangeInputStream;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.Value;
import org.h2.value.ValueBlob;
import org.h2.value.ValueClob;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;
import org.h2.value.lob.LobData;
import org.h2.value.lob.LobDataDatabase;

/**
 * This class stores LOB objects in the database, in maps. This is the back-end
 * i.e. the server side of the LOB storage.
 */
public final class LobStorageMap implements LobStorageInterface
{
    private static final boolean TRACE = false;

    private final Database database;
    final MVStore mvStore;
    private final AtomicLong nextLobId = new AtomicLong(0);
    private final ThreadPoolExecutor cleanupExecutor;


    /**
     * The lob metadata map. It contains the mapping from the lob id
     * (which is a long) to the blob metadata, including stream store id (which is a byte array).
     */
    private final MVMap<Long, BlobMeta> lobMap;

    /**
     * The lob metadata map for temporary lobs. It contains the mapping from the lob id
     * (which is a long) to the stream store id (which is a byte array).
     *
     * Key: lobId (long)
     * Value: streamStoreId (byte[])
     */
    private final MVMap<Long, byte[]> tempLobMap;

    /**
     * The reference map. It is used to remove data from the stream store: if no
     * more entries for the given streamStoreId exist, the data is removed from
     * the stream store.
     */
    private final MVMap<BlobReference,Value> refMap;

    private final StreamStore streamStore;

    private final Queue<LobRemovalInfo> pendingLobRemovals = new ConcurrentLinkedQueue<>();

    /**
     * Open map used to store LOB metadata
     * @param txStore containing map
     * @return MVMap instance
     */
    public static MVMap<Long, LobStorageMap.BlobMeta> openLobMap(TransactionStore txStore) {
        return txStore.openMap("lobMap", LongDataType.INSTANCE, LobStorageMap.BlobMeta.Type.INSTANCE);
    }

    /**
     * Open map used to store LOB data
     * @param txStore containing map
     * @return MVMap instance
     */
    public static MVMap<Long, byte[]> openLobDataMap(TransactionStore txStore) {
        return txStore.openMap("lobData", LongDataType.INSTANCE, ByteArrayDataType.INSTANCE);
    }

    public LobStorageMap(Database database) {
        this.database = database;
        Store s = database.getStore();
        TransactionStore txStore = s.getTransactionStore();
        mvStore = s.getMvStore();
        if (mvStore.isVersioningRequired()) {
            cleanupExecutor = Utils.createSingleThreadExecutor("H2-lob-cleaner", new SynchronousQueue<>());
            mvStore.setOldestVersionTracker(oldestVersionToKeep -> {
                if (needCleanup()) {
                    try {
                        cleanupExecutor.execute(() -> {
                            try {
                                cleanup(oldestVersionToKeep);
                            } catch (MVStoreException e) {
                                mvStore.panic(e);
                            }
                        });
                    } catch (RejectedExecutionException ignore) {/**/}
                }
            });
        } else {
            cleanupExecutor = null;
        }
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        try {
            lobMap = openLobMap(txStore);
            tempLobMap = txStore.openMap("tempLobMap", LongDataType.INSTANCE, ByteArrayDataType.INSTANCE);
            refMap = txStore.openMap("lobRef", BlobReference.Type.INSTANCE, NullValueDataType.INSTANCE);
            /* The stream store data map.
             *
             * Key: stream store block id (long).
             * Value: data (byte[]).
             */
            MVMap<Long, byte[]> dataMap = openLobDataMap(txStore);
            streamStore = new StreamStore(dataMap);
            // garbage collection of the last blocks
            if (!database.isReadOnly()) {
                // don't re-use block ids, except at the very end
                Long last = dataMap.lastKey();
                if (last != null) {
                    streamStore.setNextKey(last + 1);
                }
                // find the latest lob ID
                Long id1 = lobMap.lastKey();
                Long id2 = tempLobMap.lastKey(); // just in case we had unclean shutdown
                long next = 1;
                if (id1 != null) {
                    next = id1 + 1;
                }
                if (id2 != null) {
                    next = Math.max(next, id2 + 1);
                }
                nextLobId.set( next );
            }
        } finally {
            mvStore.deregisterVersionUsage(txCounter);
        }
    }

    @Override
    public ValueBlob createBlob(InputStream in, long maxLength) {
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        try {
            if (maxLength != -1
                    && maxLength <= database.getMaxLengthInplaceLob()) {
                byte[] small = new byte[(int) maxLength];
                int len = IOUtils.readFully(in, small, (int) maxLength);
                if (len > maxLength) {
                    throw new IllegalStateException(
                            "len > blobLength, " + len + " > " + maxLength);
                }
                if (len < small.length) {
                    small = Arrays.copyOf(small, len);
                }
                return ValueBlob.createSmall(small);
            }
            if (maxLength != -1) {
                in = new RangeInputStream(in, 0L, maxLength);
            }
            return createBlob(in);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        } finally {
            mvStore.deregisterVersionUsage(txCounter);
        }
    }

    @Override
    public ValueClob createClob(Reader reader, long maxLength) {
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        try {
            // we multiple by 3 here to get the worst-case size in bytes
            if (maxLength != -1
                    && maxLength * 3 <= database.getMaxLengthInplaceLob()) {
                char[] small = new char[(int) maxLength];
                int len = IOUtils.readFully(reader, small, (int) maxLength);
                if (len > maxLength) {
                    throw new IllegalStateException(
                            "len > blobLength, " + len + " > " + maxLength);
                }
                byte[] utf8 = new String(small, 0, len)
                        .getBytes(StandardCharsets.UTF_8);
                if (utf8.length > database.getMaxLengthInplaceLob()) {
                    throw new IllegalStateException(
                            "len > maxinplace, " + utf8.length + " > "
                                    + database.getMaxLengthInplaceLob());
                }
                return ValueClob.createSmall(utf8, len);
            }
            if (maxLength < 0) {
                maxLength = Long.MAX_VALUE;
            }
            CountingReaderInputStream in = new CountingReaderInputStream(reader, maxLength);
            ValueBlob blob = createBlob(in);
            LobData lobData = blob.getLobData();
            return new ValueClob(lobData, blob.octetLength(), in.getLength());
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        } finally {
            mvStore.deregisterVersionUsage(txCounter);
        }
    }

    private ValueBlob createBlob(InputStream in) throws IOException {
        byte[] streamStoreId;
        try {
            streamStoreId = streamStore.put(in);
        } catch (Exception e) {
            throw DataUtils.convertToIOException(e);
        }
        long lobId = generateLobId();
        long length = streamStore.length(streamStoreId);
        final int tableId = LobStorageFrontend.TABLE_TEMP;
        tempLobMap.put(lobId, streamStoreId);
        BlobReference key = new BlobReference(streamStoreId, lobId);
        refMap.put(key, ValueNull.INSTANCE);
        ValueBlob lob =  new ValueBlob(new LobDataDatabase(database, tableId, lobId), length);
        if (TRACE) {
            trace("create " + tableId + "/" + lobId);
        }
        return lob;
    }

    private long generateLobId() {
        return nextLobId.getAndIncrement();
    }

    @Override
    public boolean isReadOnly() {
        return database.isReadOnly();
    }

    @Override
    public ValueLob copyLob(ValueLob old, int tableId) {
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        try {
            final LobDataDatabase lobData = (LobDataDatabase) old.getLobData();
            final int type = old.getValueType();
            final long oldLobId = lobData.getLobId();
            long octetLength = old.octetLength();
            // get source lob
            final byte[] streamStoreId;
            if (isTemporaryLob(lobData.getTableId())) {
                streamStoreId = tempLobMap.get(oldLobId);
            } else {
                BlobMeta value = lobMap.get(oldLobId);
                streamStoreId = value.streamStoreId;
            }
            // create destination lob
            final long newLobId = generateLobId();
            if (isTemporaryLob(tableId)) {
                tempLobMap.put(newLobId, streamStoreId);
            } else {
                BlobMeta value = new BlobMeta(streamStoreId, tableId,
                        type == Value.CLOB ? old.charLength() : octetLength, 0);
                lobMap.put(newLobId, value);
            }
            BlobReference refMapKey = new BlobReference(streamStoreId, newLobId);
            refMap.put(refMapKey, ValueNull.INSTANCE);
            LobDataDatabase newLobData = new LobDataDatabase(database, tableId, newLobId);
            ValueLob lob = type == Value.BLOB ? new ValueBlob(newLobData, octetLength)
                    : new ValueClob(newLobData, octetLength, old.charLength());
            if (TRACE) {
                trace("copy " + lobData.getTableId() + "/" + lobData.getLobId() +
                        " > " + tableId + "/" + newLobId);
            }
            return lob;
        } finally {
            mvStore.deregisterVersionUsage(txCounter);
        }
    }

    @Override
    public InputStream getInputStream(long lobId, long byteCount)
            throws IOException {
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        try {
            byte[] streamStoreId = tempLobMap.get(lobId);
            if (streamStoreId == null) {
                BlobMeta value = lobMap.get(lobId);
                streamStoreId = value.streamStoreId;
            }
            if (streamStoreId == null) {
                throw DbException.get(ErrorCode.LOB_CLOSED_ON_TIMEOUT_1, "" + lobId);
            }
            InputStream inputStream = streamStore.get(streamStoreId);
            return new LobInputStream(inputStream);
        } finally {
            mvStore.deregisterVersionUsage(txCounter);
        }
    }

    @Override
    public InputStream getInputStream(long lobId, int tableId, long byteCount)
            throws IOException {
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        try {
            byte[] streamStoreId;
            if (isTemporaryLob(tableId)) {
                streamStoreId = tempLobMap.get(lobId);
            } else {
                BlobMeta value = lobMap.get(lobId);
                streamStoreId = value.streamStoreId;
            }
            if (streamStoreId == null) {
                throw DbException.get(ErrorCode.LOB_CLOSED_ON_TIMEOUT_1, "" + lobId);
            }
            InputStream inputStream = streamStore.get(streamStoreId);
            return new LobInputStream(inputStream);
        } finally {
            mvStore.deregisterVersionUsage(txCounter);
        }
    }

    private final class LobInputStream extends FilterInputStream {

        public LobInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
            try {
                return super.read(b, off, len);
            } finally {
                mvStore.deregisterVersionUsage(txCounter);
            }
        }

        @Override
        public int read() throws IOException {
            MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
            try {
                return super.read();
            } finally {
                mvStore.deregisterVersionUsage(txCounter);
            }
        }
    }

    @Override
    public void removeAllForTable(int tableId) {
        if (mvStore.isClosed()) {
            return;
        }
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        try {
            if (isTemporaryLob(tableId)) {
                final Iterator<Long> iter = tempLobMap.keyIterator(0L);
                while (iter.hasNext()) {
                    long lobId = iter.next();
                    doRemoveLob(tableId, lobId);
                }
                tempLobMap.clear();
            } else {
                final ArrayList<Long> list = new ArrayList<>();
                // This might not be very efficient, but should only happen
                // on DROP TABLE.
                // To speed it up, we would need yet another map.
                for (Entry<Long, BlobMeta> e : lobMap.entrySet()) {
                    BlobMeta value = e.getValue();
                    if (value.tableId == tableId) {
                        list.add(e.getKey());
                    }
                }
                for (long lobId : list) {
                    doRemoveLob(tableId, lobId);
                }
            }
        } finally {
            mvStore.deregisterVersionUsage(txCounter);
        }
    }

    @Override
    public void removeLob(ValueLob lob) {
        LobDataDatabase lobData = (LobDataDatabase) lob.getLobData();
        int tableId = lobData.getTableId();
        long lobId = lobData.getLobId();
        requestLobRemoval(tableId, lobId);
    }

    private void requestLobRemoval(int tableId, long lobId) {
        pendingLobRemovals.offer(new LobRemovalInfo(mvStore.getCurrentVersion(), lobId, tableId));
    }

    private boolean needCleanup() {
        return !pendingLobRemovals.isEmpty();
    }

    @Override
    public void close() {
        mvStore.setOldestVersionTracker(null);
        Utils.shutdownExecutor(cleanupExecutor);
        if (!mvStore.isClosed() && mvStore.isVersioningRequired()) {
            // remove all session variables and temporary lobs
            removeAllForTable(LobStorageFrontend.TABLE_ID_SESSION_VARIABLE);
            // remove all dead LOBs, even deleted in current version, before the store closed
            cleanup(mvStore.getCurrentVersion() + 1);
        }
    }

    private void cleanup(long oldestVersionToKeep) {
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        try {
            LobRemovalInfo lobRemovalInfo;
            while ((lobRemovalInfo = pendingLobRemovals.poll()) != null
                    && lobRemovalInfo.version < oldestVersionToKeep) {
                doRemoveLob(lobRemovalInfo.mapId, lobRemovalInfo.lobId);
            }
            if (lobRemovalInfo != null) {
                pendingLobRemovals.offer(lobRemovalInfo);
            }
        } finally {
            // we can not call deregisterVersionUsage() due to a possible infinite recursion
            mvStore.decrementVersionUsageCounter(txCounter);
        }
    }

    private void doRemoveLob(int tableId, long lobId) {
        if (TRACE) {
            trace("remove " + tableId + "/" + lobId);
        }
        byte[] streamStoreId;
        if (isTemporaryLob(tableId)) {
            streamStoreId = tempLobMap.remove(lobId);
            if (streamStoreId == null) {
                // already removed
                return;
            }
        } else {
            BlobMeta value = lobMap.remove(lobId);
            if (value == null) {
                // already removed
                return;
            }
            streamStoreId = value.streamStoreId;
        }
        BlobReference key = new BlobReference(streamStoreId, lobId);
        Value existing = refMap.remove(key);
        assert existing != null;
        // check if there are more entries for this streamStoreId
        key = new BlobReference(streamStoreId, 0L);
        BlobReference value = refMap.ceilingKey(key);
        boolean hasMoreEntries = false;
        if (value != null) {
            byte[] s2 = value.streamStoreId;
            if (Arrays.equals(streamStoreId, s2)) {
                if (TRACE) {
                    trace("  stream still needed in lob " + value.lobId);
                }
                hasMoreEntries = true;
            }
        }
        if (!hasMoreEntries) {
            if (TRACE) {
                trace("  remove stream " + StringUtils.convertBytesToHex(streamStoreId));
            }
            streamStore.remove(streamStoreId);
        }
    }

    private static boolean isTemporaryLob(int tableId) {
        return tableId == LobStorageFrontend.TABLE_ID_SESSION_VARIABLE || tableId == LobStorageFrontend.TABLE_TEMP
                || tableId == LobStorageFrontend.TABLE_RESULT;
    }

    private static void trace(String op) {
        System.out.println("[" + Thread.currentThread().getName() + "] LOB " + op);
    }


    public static final class BlobReference implements Comparable<BlobReference>
    {
        public final byte[] streamStoreId;
        public final long lobId;

        public BlobReference(byte[] streamStoreId, long lobId) {
            this.streamStoreId = streamStoreId;
            this.lobId = lobId;
        }

        @Override
        public int compareTo(BlobReference other) {
            int res = Integer.compare(streamStoreId.length, other.streamStoreId.length);
            if (res == 0) {
                for (int i = 0; res == 0 && i < streamStoreId.length; i++) {
                    res = Byte.compare(streamStoreId[i], other.streamStoreId[i]);
                }
                if (res == 0) {
                    res = Long.compare(lobId, other.lobId);
                }
            }
            return res;
        }

        public static final class Type extends BasicDataType<BlobReference> {
            public static final Type INSTANCE = new Type();

            private Type() {}

            @Override
            public int getMemory(BlobReference blobReference) {
                return blobReference.streamStoreId.length + 8;
            }

            @Override
            public int compare(BlobReference one, BlobReference two) {
                return one == two ? 0 : one == null ? 1 : two == null ? -1 : one.compareTo(two);
            }

            @Override
            public void write(WriteBuffer buff, BlobReference blobReference) {
                buff.putVarInt(blobReference.streamStoreId.length);
                buff.put(blobReference.streamStoreId);
                buff.putVarLong(blobReference.lobId);
            }

            @Override
            public BlobReference read(ByteBuffer buff) {
                int len = DataUtils.readVarInt(buff);
                byte[] streamStoreId = new byte[len];
                buff.get(streamStoreId);
                long blobId = DataUtils.readVarLong(buff);
                return new BlobReference(streamStoreId, blobId);
            }

            @Override
            public BlobReference[] createStorage(int size) {
                return new BlobReference[size];
            }
        }
    }

    public static final class BlobMeta
    {
        /**
         * Stream identifier. It is used as a key in LOB data map.
         */
        public final byte[] streamStoreId;
        final int tableId;
        final long byteCount;
        final long hash;

        public BlobMeta(byte[] streamStoreId, int tableId, long byteCount, long hash) {
            this.streamStoreId = streamStoreId;
            this.tableId = tableId;
            this.byteCount = byteCount;
            this.hash = hash;
        }

        public static final class Type extends BasicDataType<BlobMeta> {
            public static final Type INSTANCE = new Type();

            private Type() {
            }

            @Override
            public int getMemory(BlobMeta blobMeta) {
                return blobMeta.streamStoreId.length + 20;
            }

            @Override
            public void write(WriteBuffer buff, BlobMeta blobMeta) {
                buff.putVarInt(blobMeta.streamStoreId.length);
                buff.put(blobMeta.streamStoreId);
                buff.putVarInt(blobMeta.tableId);
                buff.putVarLong(blobMeta.byteCount);
                buff.putLong(blobMeta.hash);
            }

            @Override
            public BlobMeta read(ByteBuffer buff) {
                int len = DataUtils.readVarInt(buff);
                byte[] streamStoreId = new byte[len];
                buff.get(streamStoreId);
                int tableId = DataUtils.readVarInt(buff);
                long byteCount = DataUtils.readVarLong(buff);
                long hash = buff.getLong();
                return new BlobMeta(streamStoreId, tableId, byteCount, hash);
            }

            @Override
            public BlobMeta[] createStorage(int size) {
                return new BlobMeta[size];
            }
        }
    }

    private static final class LobRemovalInfo
    {
        final long version;
        final long lobId;
        final int mapId;

        LobRemovalInfo(long version, long lobId, int mapId) {
            this.version = version;
            this.lobId = lobId;
            this.mapId = mapId;
        }
    }
}
