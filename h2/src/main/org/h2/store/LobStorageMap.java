/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMapConcurrent;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.StreamStore;
import org.h2.mvstore.db.MVTableEngine.Store;
import org.h2.util.IOUtils;
import org.h2.util.New;
import org.h2.value.Value;
import org.h2.value.ValueLobDb;

/**
 * This class stores LOB objects in the database, in maps. This is the back-end
 * i.e. the server side of the LOB storage.
 */
public class LobStorageMap implements LobStorageInterface {

    private static final boolean TRACE = false;

    private final Database database;

    private boolean init;

    /**
     * The lob metadata map. It contains the mapping from the lob id
     * (which is a long) to the stream store id (which is a byte array).
     *
     * Key: lobId (long)
     * Value: { streamStoreId (byte[]), tableId (int),
     * byteCount (long), hash (long) }.
     */
    private MVMap<Long, Object[]> lobMap;

    /**
     * The reference map. It is used to remove data from the stream store: if no
     * more entries for the given streamStoreId exist, the data is removed from
     * the stream store.
     *
     * Key: { streamStoreId (byte[]), lobId (long) }.
     * Value: true (boolean).
     */
    private MVMap<Object[], Boolean> refMap;

    /**
     * The stream store data map.
     *
     * Key: stream store block id (long).
     * Value: data (byte[]).
     */
    private MVMap<Long, byte[]> dataMap;

    private StreamStore streamStore;

    public LobStorageMap(Database database) {
        this.database = database;
    }

    @Override
    public void init() {
        if (init) {
            return;
        }
        init = true;
        Store s = database.getMvStore();
        MVStore mvStore;
        if (s == null) {
            // in-memory database
            mvStore = MVStore.open(null);
        } else {
            mvStore = s.getStore();
        }
        lobMap = mvStore.openMap("lobMap",
                new MVMapConcurrent.Builder<Long, Object[]>());
        refMap = mvStore.openMap("lobRef",
                new MVMapConcurrent.Builder<Object[], Boolean>());
        dataMap = mvStore.openMap("lobData",
                new MVMapConcurrent.Builder<Long, byte[]>());
        streamStore = new StreamStore(dataMap);
    }

    @Override
    public Value createBlob(InputStream in, long maxLength) {
        init();
        int type = Value.BLOB;
        if (maxLength < 0) {
            maxLength = Long.MAX_VALUE;
        }
        int max = (int) Math.min(maxLength, database.getMaxLengthInplaceLob());
        try {
            if (max != 0 && max < Integer.MAX_VALUE) {
                BufferedInputStream b = new BufferedInputStream(in, max);
                b.mark(max);
                byte[] small = new byte[max];
                int len = IOUtils.readFully(b, small, max);
                if (len < max) {
                    if (len < small.length) {
                        small = Arrays.copyOf(small, len);
                    }
                    return ValueLobDb.createSmallLob(type, small);
                }
                b.reset();
                in = b;
            }
            return createLob(in, type);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    @Override
    public Value createClob(Reader reader, long maxLength) {
        init();
        int type = Value.CLOB;
        if (maxLength < 0) {
            maxLength = Long.MAX_VALUE;
        }
        int max = (int) Math.min(maxLength, database.getMaxLengthInplaceLob());
        try {
            if (max != 0 && max < Integer.MAX_VALUE) {
                BufferedReader b = new BufferedReader(reader, max);
                b.mark(max);
                char[] small = new char[max];
                int len = IOUtils.readFully(b, small, max);
                if (len < max) {
                    if (len < small.length) {
                        small = Arrays.copyOf(small, len);
                    }
                    byte[] utf8 = new String(small, 0, len).getBytes(Constants.UTF8);
                    return ValueLobDb.createSmallLob(type, utf8);
                }
                b.reset();
                reader = b;
            }
            CountingReaderInputStream in =
                    new CountingReaderInputStream(reader, maxLength);
            ValueLobDb lob = createLob(in, type);
            // the length is not correct
            lob = ValueLobDb.create(type, database,
                    lob.getTableId(), lob.getLobId(), null, in.getLength());
            return lob;
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private ValueLobDb createLob(InputStream in, int type) throws IOException {
        byte[] streamStoreId;
        try {
            streamStoreId = streamStore.put(in);
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        }
        long lobId = generateLobId();
        long length = streamStore.length(streamStoreId);
        int tableId = LobStorageFrontend.TABLE_TEMP;
        Object[] value = new Object[] { streamStoreId, tableId, length, 0 };
        lobMap.put(lobId, value);
        Object[] key = new Object[] { streamStoreId, lobId };
        refMap.put(key, Boolean.TRUE);
        ValueLobDb lob = ValueLobDb.create(
                type, database, tableId, lobId, null, length);
        if (TRACE) {
            trace("create " + tableId + "/" + lobId);
        }
        return lob;
    }

    private long generateLobId() {
        Long id = lobMap.lastKey();
        return id == null ? 1 : id + 1;
    }

    @Override
    public ValueLobDb copyLob(ValueLobDb old, int tableId, long length) {
        init();
        int type = old.getType();
        long oldLobId = old.getLobId();
        long oldLength = old.getPrecision();
        if (oldLength != length) {
            throw DbException.throwInternalError("Length is different");
        }
        Object[] value = lobMap.get(oldLobId);
        value = Arrays.copyOf(value, value.length);
        byte[] streamStoreId = (byte[]) value[0];
        long lobId = generateLobId();
        value[1] = tableId;
        lobMap.put(lobId, value);
        Object[] key = new Object[] { streamStoreId, lobId };
        refMap.put(key, Boolean.TRUE);
        ValueLobDb lob = ValueLobDb.create(
                type, database, tableId, lobId, null, length);
        if (TRACE) {
            trace("copy " + old.getTableId() + "/" + old.getLobId() +
                    " > " + tableId + "/" + lobId);
        }
        return lob;
    }

    @Override
    public InputStream getInputStream(ValueLobDb lob, byte[] hmac, long byteCount)
            throws IOException {
        init();
        Object[] value = lobMap.get(lob.getLobId());
        byte[] streamStoreId = (byte[]) value[0];
        return streamStore.get(streamStoreId);
    }

    @Override
    public void setTable(ValueLobDb lob, int tableId) {
        init();
        long lobId = lob.getLobId();
        Object[] value = lobMap.remove(lobId);
        if (TRACE) {
            trace("move " + lob.getTableId() + "/" + lob.getLobId() +
                    " > " + tableId + "/" + lobId);
        }
        value[1] = tableId;
        lobMap.put(lobId, value);
    }

    @Override
    public void removeAllForTable(int tableId) {
        init();
        // this might not be very efficient -
        // to speed it up, we would need yet another map
        ArrayList<Long> list = New.arrayList();
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

    @Override
    public void removeLob(ValueLobDb lob) {
        init();
        int tableId = lob.getTableId();
        long lobId = lob.getLobId();
        removeLob(tableId, lobId);
    }

    private void removeLob(int tableId, long lobId) {
        if (TRACE) {
            trace("remove " + tableId + "/" + lobId);
        }
        Object[] value = lobMap.remove(lobId);
        byte[] streamStoreId = (byte[]) value[0];
        Object[] key = new Object[] {streamStoreId, lobId };
        refMap.remove(key);
        // check if there are more entries for this streamStoreId
        key = new Object[] {streamStoreId, 0 };
        value = refMap.ceilingKey(key);
        boolean hasMoreEntries = false;
        if (value != null) {
            byte[] s2 = (byte[]) value[0];
            if (Arrays.equals(streamStoreId, s2)) {
                hasMoreEntries = true;
            }
        }
        if (!hasMoreEntries) {
            streamStore.remove(streamStoreId);
        }
    }

    private static void trace(String op) {
        System.out.println("LOB " + op);
    }

}
