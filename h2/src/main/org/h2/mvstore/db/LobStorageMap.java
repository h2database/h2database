/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.StreamStore;
import org.h2.store.CountingReaderInputStream;
import org.h2.store.LobStorageFrontend;
import org.h2.store.LobStorageInterface;
import org.h2.store.RangeInputStream;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueLobDatabase;
import org.h2.value.ValueLobInMemory;

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

    /**
     * The lob metadata map. It contains the mapping from the lob id
     * (which is a long) to the stream store id (which is a byte array).
     *
     * Key: lobId (long)
     * Value: { streamStoreId (byte[]), tableId (int),
     *          byteCount (long), hash (long) }.
     */
    private final MVMap<Long, Object[]> lobMap;

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
     *
     * Key: { streamStoreId (byte[]), lobId (long) }.
     * Value: true (boolean).
     */
    private final MVMap<Object[], Boolean> refMap;

    private final StreamStore streamStore;


    public LobStorageMap(Database database) {
        this.database = database;
        Store s = database.getStore();
        if (s == null) {
            // in-memory database
            mvStore = MVStore.open(null);
        } else {
            mvStore = s.getMvStore();
        }
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        try {
            lobMap = mvStore.openMap("lobMap");
            tempLobMap = mvStore.openMap("tempLobMap");
            refMap = mvStore.openMap("lobRef");

            /* The stream store data map.
             *
             * Key: stream store block id (long).
             * Value: data (byte[]).
             */
            MVMap<Long, byte[]> dataMap = mvStore.openMap("lobData");
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
    public ValueLob createBlob(InputStream in, long maxLength) {
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        int type = Value.BLOB;
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
                return ValueLobInMemory.createSmallLob(type, small);
            }
            if (maxLength != -1) {
                in = new RangeInputStream(in, 0L, maxLength);
            }
            return createLob(in, type);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        } finally {
            mvStore.deregisterVersionUsage(txCounter);
        }
    }

    @Override
    public ValueLob createClob(Reader reader, long maxLength) {
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        int type = Value.CLOB;
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
                return ValueLobInMemory.createSmallLob(type, utf8);
            }
            if (maxLength < 0) {
                maxLength = Long.MAX_VALUE;
            }
            CountingReaderInputStream in = new CountingReaderInputStream(reader,
                    maxLength);
            ValueLobDatabase lob = createLob(in, type);
            // the length is not correct
            lob = ValueLobDatabase.create(type, database, lob.getTableId(),
                    lob.getLobId(), in.getLength());
            return lob;
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        } finally {
            mvStore.deregisterVersionUsage(txCounter);
        }
    }

    private ValueLobDatabase createLob(InputStream in, int type) throws IOException {
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
        Object[] key = { streamStoreId, lobId };
        refMap.put(key, Boolean.TRUE);
        ValueLobDatabase lob = ValueLobDatabase.create(
                type, database, tableId, lobId, length);
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
    public ValueLob copyLob(ValueLob old_, int tableId, long length) {
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        try {
            final ValueLobDatabase old = (ValueLobDatabase) old_;
            final int type = old.getValueType();
            final long oldLobId = old.getLobId();
            final long oldLength = old.getType().getPrecision();
            if (oldLength != length) {
                throw DbException.getInternalError("Length is different");
            }
            // get source lob
            final byte[] streamStoreId;
            if (isTemporaryLob(old.getTableId())) {
                streamStoreId = tempLobMap.get(oldLobId);
            } else {
                Object[] value = lobMap.get(oldLobId);
                streamStoreId = (byte[]) value[0];
            }
            // create destination lob
            final long newLobId = generateLobId();
            if (isTemporaryLob(tableId)) {
                tempLobMap.put(newLobId, streamStoreId);
            } else {
                Object[] value = { streamStoreId, tableId, length, 0 };
                lobMap.put(newLobId, value);
            }
            Object[] refMapKey = {streamStoreId, newLobId};
            refMap.put(refMapKey, Boolean.TRUE);
            ValueLob lob = ValueLobDatabase.create(
                    type, database, tableId, newLobId, length);
            if (TRACE) {
                trace("copy " + old.getTableId() + "/" + old.getLobId() +
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
                Object[] value = lobMap.get(lobId);
                streamStoreId = (byte[]) value[0];
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
                Object[] value = lobMap.get(lobId);
                streamStoreId = (byte[]) value[0];
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
                    removeLob(tableId, lobId);
                }
                tempLobMap.clear();
            } else {
                final ArrayList<Long> list = new ArrayList<>();
                // This might not be very efficient, but should only happen
                // on DROP TABLE.
                // To speed it up, we would need yet another map.
                for (Entry<Long, Object[]> e : lobMap.entrySet()) {
                    Object[] value = e.getValue();
                    int t = (Integer) value[1];
                    if (t == tableId) {
                        list.add(e.getKey());
                    }
                }
                for (long lobId : list) {
                    removeLob(tableId, lobId);
                }
            }
        } finally {
            mvStore.deregisterVersionUsage(txCounter);
        }
    }

    @Override
    public void removeLob(ValueLob lob_) {
        MVStore.TxCounter txCounter = mvStore.registerVersionUsage();
        try {
            ValueLobDatabase lob = (ValueLobDatabase) lob_;
            int tableId = lob.getTableId();
            long lobId = lob.getLobId();
            removeLob(tableId, lobId);
        } finally {
            mvStore.deregisterVersionUsage(txCounter);
        }
    }

    private void removeLob(int tableId, long lobId) {
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
            Object[] value = lobMap.remove(lobId);
            if (value == null) {
                // already removed
                return;
            }
            streamStoreId = (byte[]) value[0];
        }
        Object[] key = {streamStoreId, lobId };
        refMap.remove(key);
        // check if there are more entries for this streamStoreId
        key[1] = 0L;
        Object[] value = refMap.ceilingKey(key);
        boolean hasMoreEntries = false;
        if (value != null) {
            byte[] s2 = (byte[]) value[0];
            if (Arrays.equals(streamStoreId, s2)) {
                if (TRACE) {
                    trace("  stream still needed in lob " + value[1]);
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

}
