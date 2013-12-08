/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.util.New;

/**
 * A store that supports concurrent transactions.
 */
public class TransactionStore {

    // TODO should not be hard-coded
    private static final int MAX_UNSAVED_PAGES = 4 * 1024;

    /**
     * The store.
     */
    final MVStore store;

    /**
     * The persisted map of prepared transactions.
     * Key: transactionId, value: [ status, name ].
     */
    final MVMap<Integer, Object[]> preparedTransactions;

    /**
     * The undo log.
     * <p>
     * If the first entry for a transaction doesn't have a logId
     * of 0, then the transaction is partially committed (which means rollback
     * is not possible). Log entries are written before the data is changed
     * (write-ahead).
     * <p>
     * Key: [ transactionId, logId ], value: [ opType, mapId, key, oldValue ].
     */
    final MVMap<long[], Object[]> undoLog;
    ;
    // TODO should be <long, Object[]> (operationId, oldValue)
    // TODO probably opType is not needed

    /**
     * The lock timeout in milliseconds. 0 means timeout immediately.
     */
    long lockTimeout;

    private final DataType dataType;

    private int lastTransactionId;
    
    private int maxTransactionId = 0xffff;
    
    private HashMap<Integer, MVMap<Object, VersionedValue>> maps = New.hashMap();

    /**
     * Create a new transaction store.
     *
     * @param store the store
     */
    public TransactionStore(MVStore store) {
        this(store, new ObjectDataType());
    }

    /**
     * Create a new transaction store.
     *
     * @param store the store
     * @param dataType the data type for map keys and values
     */
    public TransactionStore(MVStore store, DataType dataType) {
        this.store = store;
        this.dataType = dataType;
        preparedTransactions = store.openMap("openTransactions",
                new MVMap.Builder<Integer, Object[]>());
        // TODO commit of larger transaction could be faster if we have one undo
        // log per transaction, or a range delete operation for maps
        VersionedValueType oldValueType = new VersionedValueType(dataType);
        ArrayType undoLogValueType = new ArrayType(new DataType[]{
                new ObjectDataType(), new ObjectDataType(), dataType,
                oldValueType
        });
        MVMap.Builder<long[], Object[]> builder =
                new MVMap.Builder<long[], Object[]>().
                valueType(undoLogValueType);
        // TODO escape other map names, to avoid conflicts
        undoLog = store.openMap("undoLog", builder);
        init();
    }
    
    /**
     * Set the maximum transaction id, after which ids are re-used. If the old
     * transaction is still in use when re-using an old id, the new transaction
     * fails.
     * 
     * @param max the maximum id
     */
    public void setMaxTransactionId(int max) {
        this.maxTransactionId = max;
    }
    
    private static long getOperationId(int transactionId, long logId) {
        DataUtils.checkArgument(transactionId >= 0 && transactionId < (1 << 24), 
                "Transaction id out of range: {0}", transactionId);
        DataUtils.checkArgument(logId >= 0 && logId < (1L << 40), 
                "Transaction log id out of range: {0}", logId);
        return ((long) transactionId << 40) | logId;
    }
    
    private static int getTransactionId(long operationId) {
        return (int) (operationId >>> 40);
    }

    private static long getLogId(long operationId) {
        return operationId & ((1L << 40) - 1);
    }

    private synchronized void init() {
        synchronized (undoLog) {
            if (undoLog.size() > 0) {
                long[] key = undoLog.firstKey();
                lastTransactionId = (int) key[0];
            }
        }
    }

    /**
     * Get the list of unclosed transactions that have pending writes.
     *
     * @return the list of transactions (sorted by id)
     */
    public List<Transaction> getOpenTransactions() {
        synchronized (undoLog) {
            ArrayList<Transaction> list = New.arrayList();
            long[] key = undoLog.firstKey();
            while (key != null) {
                int transactionId = (int) key[0];
                long[] end = { transactionId, Long.MAX_VALUE };
                key = undoLog.floorKey(end);
                long logId = key[1] + 1;
                Object[] data = preparedTransactions.get(transactionId);
                int status;
                String name;
                if (data == null) {
                    key = new long[] { key[0], 0 };
                    if (undoLog.containsKey(key)) {
                        status = Transaction.STATUS_OPEN;
                    } else {
                        status = Transaction.STATUS_COMMITTING;
                    }
                    name = null;
                } else {
                    status = (Integer) data[0];
                    name = (String) data[1];
                }
                Transaction t = new Transaction(this, transactionId, status, name, logId);
                list.add(t);
                key = undoLog.higherKey(end);
            }
            return list;
        }
    }

    /**
     * Close the transaction store.
     */
    public synchronized void close() {
        store.commit();
    }

    /**
     * Begin a new transaction.
     *
     * @return the transaction
     */
    public synchronized Transaction begin() {
        int transactionId = ++lastTransactionId;
        if (lastTransactionId >= maxTransactionId) {
            lastTransactionId = 0;
        }
        int status = Transaction.STATUS_OPEN;
        return new Transaction(this, transactionId, status, null, 0);
    }

    private void commitIfNeeded() {
        if (store.getUnsavedPageCount() > MAX_UNSAVED_PAGES) {
            store.commit();
        }
    }

    /**
     * Store a transaction.
     *
     * @param t the transaction
     */
    synchronized void storeTransaction(Transaction t) {
        if (t.getStatus() == Transaction.STATUS_PREPARED || t.getName() != null) {
            Object[] v = { t.getStatus(), t.getName() };
            preparedTransactions.put(t.getId(), v);
        }
    }

    /**
     * Log an entry.
     *
     * @param t the transaction
     * @param logId the log id
     * @param opType the operation type
     * @param mapId the map id
     * @param key the key
     * @param oldValue the old value
     */
    void log(Transaction t, long logId, int opType, int mapId,
            Object key, Object oldValue) {
        commitIfNeeded();
        long[] undoKey = { t.getId(), logId };
        Object[] log = new Object[] { opType, mapId, key, oldValue };
        synchronized (undoLog) {
            if (logId == 0) {
                if (undoLog.containsKey(undoKey)) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_TRANSACTION_STILL_OPEN, 
                            "An old transaction with the same id is still open: {0}", 
                            t.getId());
                }
            }
            undoLog.put(undoKey, log);
        }
    }

    /**
     * Remove a log entry.
     *
     * @param t the transaction
     * @param logId the log id
     */
    public void logUndo(Transaction t, long logId) {
        long[] undoKey = { t.getId(), logId };
        synchronized (undoLog) {
            undoLog.remove(undoKey);
        }
    }
    
    <K, V> void removeMap(TransactionMap<K, V> map) {
        maps.remove(map.mapId);
        store.removeMap(map.map);
    }

    /**
     * Commit a transaction.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     */
    void commit(Transaction t, long maxLogId) {
        if (store.isClosed()) {
            return;
        }
        synchronized (undoLog) {
            t.setStatus(Transaction.STATUS_COMMITTING);
            for (long logId = 0; logId < maxLogId; logId++) {
                commitIfNeeded();
                long[] undoKey = new long[] { t.getId(), logId };
                Object[] op = undoLog.get(undoKey);
                if (op == null) {
                    // partially committed: load next
                    undoKey = undoLog.ceilingKey(undoKey);
                    if (undoKey == null || undoKey[0] != t.getId()) {
                        break;
                    }
                    logId = undoKey[1] - 1;
                    continue;
                }
                
                ;
                // TODO undoLog: do we need the opType?
                
                int mapId = (Integer) op[1];
                MVMap<Object, VersionedValue> map = openMap(mapId);
                Object key = op[2];
                VersionedValue value = map.get(key);
                if (value == null) {
                    // nothing to do
                } else if (value.value == null) {
                    // remove the value
                    map.remove(key);
                } else {
                    VersionedValue v2 = new VersionedValue();
                    v2.value = value.value;
                    map.put(key, v2);
                }
                undoLog.remove(undoKey);
            }
        }
        endTransaction(t);
    }

    private synchronized MVMap<Object, VersionedValue> openMap(int mapId) {
        MVMap<Object, VersionedValue> map = maps.get(mapId);
        if (map != null) {
            return map;
        }
        // TODO open map by id if possible
        Map<String, String> meta = store.getMetaMap();
        String m = meta.get("map." + mapId);
        if (m == null) {
            // the map was removed later on
            return null;
        }
        String mapName = DataUtils.parseMap(m).get("name");
        VersionedValueType vt = new VersionedValueType(dataType);
        MVMap.Builder<Object, VersionedValue> mapBuilder =
                new MVMap.Builder<Object, VersionedValue>().
                keyType(dataType).valueType(vt);
        map = store.openMap(mapName, mapBuilder);
        maps.put(mapId, map);
        return map;
    }

    /**
     * Check whether the given transaction id is still open and contains log
     * entries.
     *
     * @param transactionId the transaction id
     * @return true if it is open
     */
    boolean isTransactionOpen(long transactionId) {
        ;
        // TODO probably not needed at all
        synchronized (undoLog) {
            long[] key = { transactionId, 0 };
            key = undoLog.ceilingKey(key);
            return key != null && key[0] == transactionId;
        }
    }

    /**
     * End this transaction
     *
     * @param t the transaction
     */
    synchronized void endTransaction(Transaction t) {
        if (t.getStatus() == Transaction.STATUS_PREPARED) {
            preparedTransactions.remove(t.getId());
        }
        t.setStatus(Transaction.STATUS_CLOSED);
        if (store.getAutoCommitDelay() == 0) {
            store.commit();
            return;
        }
        // to avoid having to store the transaction log,
        // if there is no open transaction,
        // and if there have been many changes, store them now
        if (undoLog.isEmpty()) {
            int unsaved = store.getUnsavedPageCount();
            int max = store.getAutoCommitPageCount();
            // save at 3/4 capacity
            if (unsaved * 4 > max * 3) {
                store.commit();
            }
        }
    }

    /**
     * Rollback to an old savepoint.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     * @param toLogId the log id to roll back to
     */
    void rollbackTo(Transaction t, long maxLogId, long toLogId) {
        synchronized (undoLog) {
            for (long logId = maxLogId - 1; logId >= toLogId; logId--) {
                commitIfNeeded();
                long[] undoKey = new long[] { t.getId(), logId };
                Object[] op = undoLog.get(undoKey);
                if (op == null) {
                    // partially rolled back: load previous
                    undoKey = undoLog.floorKey(undoKey);
                    if (undoKey == null || undoKey[0] != t.getId()) {
                        break;
                    }
                    logId = undoKey[1] + 1;
                    continue;
                }
                int mapId = ((Integer) op[1]).intValue();
                MVMap<Object, VersionedValue> map = openMap(mapId);
                if (map != null) {
                    Object key = op[2];
                    VersionedValue oldValue = (VersionedValue) op[3];
                    if (oldValue == null) {
                        // this transaction added the value
                        map.remove(key);
                    } else {
                        // this transaction updated the value
                        map.put(key, oldValue);
                    }
                }
                undoLog.remove(undoKey);
            }
        }
    }

    /**
     * Get the changes of the given transaction, starting from the latest log id
     * back to the given log id.
     *
     * @param t the transaction
     * @param maxLogId the maximum log id
     * @param toLogId the minimum log id
     * @return the changes
     */
    Iterator<Change> getChanges(final Transaction t, final long maxLogId, final long toLogId) {
        return new Iterator<Change>() {

            private long logId = maxLogId - 1;
            private Change current;

            {
                fetchNext();
            }

            private void fetchNext() {
                synchronized (undoLog) {
                    while (logId >= toLogId) {
                        long[] undoKey = new long[] { t.getId(), logId };
                        Object[] op = undoLog.get(undoKey);
                        logId--;
                        if (op == null) {
                            // partially rolled back: load previous
                            undoKey = undoLog.floorKey(undoKey);
                            if (undoKey == null || undoKey[0] != t.getId()) {
                                break;
                            }
                            logId = undoKey[1];
                            continue;
                        }
                        int mapId = ((Integer) op[1]).intValue();
                        // TODO open map by id if possible
                        Map<String, String> meta = store.getMetaMap();
                        String m = meta.get("map." + mapId);
                        if (m == null) {
                            // map was removed later on
                        } else {
                            current = new Change();
                            current.mapName = DataUtils.parseMap(m).get("name");
                            current.key = op[2];
                            VersionedValue oldValue = (VersionedValue) op[3];
                            current.value = oldValue == null ? null : oldValue.value;
                            return;
                        }
                    }
                }
                current = null;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public Change next() {
                if (current == null) {
                    throw DataUtils.newUnsupportedOperationException("no data");
                }
                Change result = current;
                fetchNext();
                return result;
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("remove");
            }

        };
    }

    /**
     * A change in a map.
     */
    public static class Change {

        /**
         * The name of the map where the change occurred.
         */
        public String mapName;

        /**
         * The key.
         */
        public Object key;

        /**
         * The value.
         */
        public Object value;
    }

    /**
     * A transaction.
     */
    public static class Transaction {

        /**
         * The status of a closed transaction (committed or rolled back).
         */
        public static final int STATUS_CLOSED = 0;

        /**
         * The status of an open transaction.
         */
        public static final int STATUS_OPEN = 1;

        /**
         * The status of a prepared transaction.
         */
        public static final int STATUS_PREPARED = 2;

        /**
         * The status of a transaction that is being committed, but possibly not
         * yet finished. A transactions can go into this state when the store is
         * closed while the transaction is committing. When opening a store,
         * such transactions should be committed.
         */
        public static final int STATUS_COMMITTING = 3;

        /**
         * The operation type for changes in a map.
         */
        static final int OP_REMOVE = 0, OP_ADD = 1, OP_SET = 2;

        /**
         * The transaction store.
         */
        final TransactionStore store;

        /**
         * The transaction id.
         */
        final int transactionId;

        /**
         * The log id of the last entry in the undo log map.
         */
        long logId;

        private int status;

        private String name;

        Transaction(TransactionStore store, int transactionId, int status, String name, long logId) {
            this.store = store;
            this.transactionId = transactionId;
            this.status = status;
            this.name = name;
            this.logId = logId;
        }

        public int getId() {
            return transactionId;
        }

        public int getStatus() {
            return status;
        }

        void setStatus(int status) {
            this.status = status;
        }

        public void setName(String name) {
            checkNotClosed();
            this.name = name;
            store.storeTransaction(this);
        }

        public String getName() {
            return name;
        }

        /**
         * Create a new savepoint.
         *
         * @return the savepoint id
         */
        public long setSavepoint() {
            checkNotClosed();
            return logId;
        }

        /**
         * Add a log entry.
         *
         * @param opType the operation type
         * @param mapId the map id
         * @param key the key
         * @param oldValue the old value
         */
        void log(int opType, int mapId, Object key, Object oldValue) {
            store.log(this, logId, opType, mapId, key, oldValue);
            // only increment the log id if logging was successful
            logId++;
        }

        /**
         * Remove the last log entry.
         */
        void logUndo() {
            store.logUndo(this, --logId);
        }

        /**
         * Open a data map.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param name the name of the map
         * @return the transaction map
         */
        public <K, V> TransactionMap<K, V> openMap(String name) {
            return openMap(name, null, null);
        }

        /**
         * Open the map to store the data.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param name the name of the map
         * @param keyType the key data type
         * @param valueType the value data type
         * @return the transaction map
         */
        public <K, V> TransactionMap<K, V> openMap(String name, DataType keyType, DataType valueType) {
            checkNotClosed();
            if (keyType == null) {
                keyType = new ObjectDataType();
            }
            if (valueType == null) {
                valueType = new ObjectDataType();
            }
            VersionedValueType vt = new VersionedValueType(valueType);
            MVMap.Builder<K, VersionedValue> builder = new MVMap.Builder<K, VersionedValue>()
                    .keyType(keyType).valueType(vt);
            MVMap<K, VersionedValue> map = store.store.openMap(name, builder);
            int mapId = map.getId();
            return new TransactionMap<K, V>(this, map, mapId);
        }

        /**
         * Open the transactional version of the given map.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param map the base map
         * @return the transactional map
         */
        public <K, V> TransactionMap<K, V> openMap(MVMap<K, VersionedValue> map) {
            checkNotClosed();
            int mapId = map.getId();
            return new TransactionMap<K, V>(this, map, mapId);
        }

        /**
         * Prepare the transaction. Afterwards, the transaction can only be
         * committed or rolled back.
         */
        public void prepare() {
            checkNotClosed();
            status = STATUS_PREPARED;
            store.storeTransaction(this);
        }

        /**
         * Commit the transaction. Afterwards, this transaction is closed.
         */
        public void commit() {
            checkNotClosed();
            store.commit(this, logId);
        }

        /**
         * Roll back to the given savepoint. This is only allowed if the
         * transaction is open.
         *
         * @param savepointId the savepoint id
         */
        public void rollbackToSavepoint(long savepointId) {
            checkNotClosed();
            store.rollbackTo(this, logId, savepointId);
            logId = savepointId;
        }

        /**
         * Roll the transaction back. Afterwards, this transaction is closed.
         */
        public void rollback() {
            checkNotClosed();
            store.rollbackTo(this, logId, 0);
            store.endTransaction(this);
        }

        /**
         * Get the list of changes, starting with the latest change, up to the
         * given savepoint (in reverse order than they occurred). The value of
         * the change is the value before the change was applied.
         *
         * @param savepointId the savepoint id, 0 meaning the beginning of the
         *            transaction
         * @return the changes
         */
        public Iterator<Change> getChanges(long savepointId) {
            return store.getChanges(this, logId, savepointId);
        }

        /**
         * Check whether this transaction is open or prepared.
         */
        void checkNotClosed() {
            if (status == STATUS_CLOSED) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_CLOSED, "Transaction is closed");
            }
        }

        /**
         * Remove the map.
         *
         * @param map the map
         */
        public <K, V> void removeMap(TransactionMap<K, V> map) {
            store.removeMap(map);
        }
        
        @Override
        public String toString() {
            return "" + transactionId;
        }

    }

    /**
     * A map that supports transactions.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static class TransactionMap<K, V> {

        /**
         * The map used for writing (the latest version).
         * <p>
         * Key: key the key of the data.
         * Value: { transactionId, oldVersion, value }
         */
        final MVMap<K, VersionedValue> map;

        /**
         * The map id.
         */
        final int mapId;
        
        private Transaction transaction;

        /**
         * If a record was read that was updated by this transaction, and the
         * update occurred before this log id, the older version is read. This
         * is so that changes are not immediately visible, to support statement
         * processing (for example "update test set id = id + 1").
         */
        private long readLogId = Long.MAX_VALUE;

        TransactionMap(Transaction transaction, MVMap<K, VersionedValue> map, int mapId) {
            this.transaction = transaction;
            this.map = map;
            this.mapId = mapId;
        }

        /**
         * Set the savepoint. Afterwards, reads are based on the specified
         * savepoint.
         *
         * @param savepoint the savepoint
         */
        public void setSavepoint(long savepoint) {
            this.readLogId = savepoint;
        }

        /**
         * Get a clone of this map for the given transaction.
         *
         * @param transaction the transaction
         * @param savepoint the savepoint
         * @return the map
         */
        public TransactionMap<K, V> getInstance(Transaction transaction, long savepoint) {
            TransactionMap<K, V> m = new TransactionMap<K, V>(transaction, map, mapId);
            m.setSavepoint(savepoint);
            return m;
        }

        /**
         * Get the size of the map as seen by this transaction.
         *
         * @return the size
         */
        public long sizeAsLong() {
            // TODO this method is very slow
            long size = 0;
            Cursor<K> cursor = map.keyIterator(null);
            while (cursor.hasNext()) {
                K key = cursor.next();
                if (get(key) != null) {
                    size++;
                }
            }
            return size;
        }

        /**
         * Remove an entry.
         * <p>
         * If the row is locked, this method will retry until the row could be
         * updated or until a lock timeout.
         *
         * @param key the key
         * @throws IllegalStateException if a lock timeout occurs
         */
        public V remove(K key) {
            return set(key, null);
        }

        /**
         * Update the value for the given key.
         * <p>
         * If the row is locked, this method will retry until the row could be
         * updated or until a lock timeout.
         *
         * @param key the key
         * @param value the new value (not null)
         * @return the old value
         * @throws IllegalStateException if a lock timeout occurs
         */
        public V put(K key, V value) {
            DataUtils.checkArgument(value != null, "The value may not be null");
            return set(key, value);
        }

        private V set(K key, V value) {
            transaction.checkNotClosed();
            long start = 0;
            while (true) {
                V old = get(key);
                boolean ok = trySet(key, value, false);
                if (ok) {
                    return old;
                }
                // an uncommitted transaction:
                // wait until it is committed, or until the lock timeout
                long timeout = transaction.store.lockTimeout;
                if (timeout == 0) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_TRANSACTION_LOCK_TIMEOUT, "Lock timeout");
                }
                if (start == 0) {
                    start = System.currentTimeMillis();
                } else {
                    long t = System.currentTimeMillis() - start;
                    if (t > timeout) {
                        throw DataUtils.newIllegalStateException(
                                DataUtils.ERROR_TRANSACTION_LOCK_TIMEOUT, "Lock timeout");
                    }
                    // TODO use wait/notify instead, or remove the feature
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        }

        /**
         * Try to remove the value for the given key.
         * <p>
         * This will fail if the row is locked by another transaction (that
         * means, if another open transaction changed the row).
         *
         * @param key the key
         * @return whether the entry could be removed
         */
        public boolean tryRemove(K key) {
            return trySet(key, null, false);
        }

        /**
         * Try to update the value for the given key.
         * <p>
         * This will fail if the row is locked by another transaction (that
         * means, if another open transaction changed the row).
         *
         * @param key the key
         * @param value the new value
         * @return whether the entry could be updated
         */
        public boolean tryPut(K key, V value) {
            DataUtils.checkArgument(value != null, "The value may not be null");
            return trySet(key, value, false);
        }

        /**
         * Try to set or remove the value. When updating only unchanged entries,
         * then the value is only changed if it was not changed after opening
         * the map.
         *
         * @param key the key
         * @param value the new value (null to remove the value)
         * @param onlyIfUnchanged only set the value if it was not changed (by
         *            this or another transaction) since the map was opened
         * @return true if the value was set, false if there was a concurrent update
         */
        public boolean trySet(K key, V value, boolean onlyIfUnchanged) {
            VersionedValue current = map.get(key);
            if (onlyIfUnchanged) {
                VersionedValue old = getValue(key, readLogId);
                if (!map.areValuesEqual(old, current)) {
                    long tx = current.transactionId;
                    if (tx == transaction.transactionId) {
                        if (value == null) {
                            // ignore removing an entry
                            // if it was added or changed
                            // in the same statement
                            return true;
                        } else if (current.value == null) {
                            // add an entry that was removed
                            // in the same statement
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
            int opType;
            if (current == null || current.value == null) {
                if (value == null) {
                    // remove a removed value
                    opType = Transaction.OP_SET;
                } else {
                    opType = Transaction.OP_ADD;
                }
            } else {
                if (value == null) {
                    opType = Transaction.OP_REMOVE;
                } else {
                    opType = Transaction.OP_SET;
                }
            }
            VersionedValue newValue = new VersionedValue();
            newValue.transactionId = transaction.transactionId;
            newValue.logId = transaction.logId;
            newValue.value = value;
            if (current == null) {
                // a new value
                transaction.log(opType, mapId, key, current);
                VersionedValue old = map.putIfAbsent(key, newValue);
                if (old != null) {
                    transaction.logUndo();
                    return false;
                }
                return true;
            }
            long tx = current.transactionId;
            if (tx == transaction.transactionId) {
                // added or updated by this transaction
                transaction.log(opType, mapId, key, current);
                if (!map.replace(key, current, newValue)) {
                    // strange, somebody overwrite the value
                    // even thought the change was not committed
                    transaction.logUndo();
                    return false;
                }
                return true;
            }
            // added or updated by another transaction
            boolean open = transaction.store.isTransactionOpen(tx);
            if (!open) {
                transaction.log(opType, mapId, key, current);
                // the transaction is committed:
                // overwrite the value
                if (!map.replace(key, current, newValue)) {
                    // somebody else was faster
                    transaction.logUndo();
                    return false;
                }
                return true;
            }
            // the transaction is not yet committed
            return false;
        }

        /**
         * Get the value for the given key at the time when this map was opened.
         *
         * @param key the key
         * @return the value or null
         */
        public V get(K key) {
            return get(key, readLogId);
        }

        /**
         * Get the most recent value for the given key.
         *
         * @param key the key
         * @return the value or null
         */
        public V getLatest(K key) {
            return get(key, Long.MAX_VALUE);
        }

        /**
         * Whether the map contains the key.
         *
         * @param key the key
         * @return true if the map contains an entry for this key
         */
        public boolean containsKey(K key) {
            return get(key) != null;
        }

        /**
         * Get the value for the given key.
         *
         * @param key the key
         * @param maxLogId the maximum log id
         * @return the value or null
         */
        @SuppressWarnings("unchecked")
        public V get(K key, long maxLogId) {
            transaction.checkNotClosed();
            VersionedValue data = getValue(key, maxLogId);
            return data == null ? null : (V) data.value;
        }

        /**
         * Whether the entry for this key was added or removed from this session.
         *
         * @param key the key
         * @return true if yes
         */
        public boolean isSameTransaction(K key) {
            VersionedValue data = map.get(key);
            if (data == null) {
                // doesn't exist or deleted by a committed transaction
                return false;
            }
            long tx = data.transactionId;
            return tx == transaction.transactionId;
        }

        private VersionedValue getValue(K key, long maxLog) {
            VersionedValue data = map.get(key);
            for (int i = 0; i < 10; i++) {
                long tx;
                if (data == null) {
                    // doesn't exist or deleted by a committed transaction
                    return null;
                }
                tx = data.transactionId;
                long logId = data.logId;
                if (tx == transaction.transactionId) {
                    // added by this transaction
                    if (logId < maxLog) {
                        return data;
                    }
                }
                // added, updated, or removed by another transaction
                boolean open = transaction.store.isTransactionOpen(tx);
                if (!open) {
                    // it is committed
                    return data;
                }
                // get the value before the uncommitted transaction
                long[] x = new long[] { tx, logId };
                Object[] d;
                synchronized (transaction.store.undoLog) {
                    d = transaction.store.undoLog.get(x);
                }
                if (d == null) {
                    // this entry was committed or rolled back 
                    // in the meantime (the transaction might still be open)
                    data = map.get(key);
                } else {
                    data = (VersionedValue) d[3];
                }
            }
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_TRANSACTION_CORRUPT, 
                    "The transaction log might be corrupt for key {0}", key);
        }

        /**
         * Check whether this map is closed.
         *
         * @return true if closed
         */
        public boolean isClosed() {
            return map.isClosed();
        }

        /**
         * Clear the map.
         */
        public void clear() {
            // TODO truncate transactionally
            map.clear();
        }

        /**
         * Get the first key.
         *
         * @return the first key, or null if empty
         */
        public K firstKey() {
            Iterator<K> it = keyIterator(null);
            return it.hasNext() ? it.next() : null;
        }

        /**
         * Get the last key.
         *
         * @return the last key, or null if empty
         */
        public K lastKey() {
            K k = map.lastKey();
            while (true) {
                if (k == null) {
                    return null;
                }
                if (get(k) != null) {
                    return k;
                }
                k = map.lowerKey(k);
            }
        }

        /**
         * Get the most recent smallest key that is larger or equal to this key.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K getLatestCeilingKey(K key) {
            Cursor<K> cursor = map.keyIterator(key);
            while (cursor.hasNext()) {
                key = cursor.next();
                if (get(key, Long.MAX_VALUE) != null) {
                    return key;
                }
            }
            return null;
        }

        /**
         * Get the smallest key that is larger or equal to this key.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K ceilingKey(K key) {
            // TODO this method is slow
            Cursor<K> cursor = map.keyIterator(key);
            while (cursor.hasNext()) {
                key = cursor.next();
                if (get(key) != null) {
                    return key;
                }
            }
            return null;
        }

        /**
         * Get the smallest key that is larger than the given key, or null if no
         * such key exists.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K higherKey(K key) {
            // TODO transactional higherKey
            return map.higherKey(key);
        }

        /**
         * Get the largest key that is smaller than the given key, or null if no
         * such key exists.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K lowerKey(K key) {
            // TODO transactional lowerKey
            return map.lowerKey(key);
        }

        /**
         * Iterate over keys.
         *
         * @param from the first key to return
         * @return the iterator
         */
        public Iterator<K> keyIterator(K from) {
            return keyIterator(from, false);
        }

        /**
         * Iterate over keys.
         *
         * @param from the first key to return
         * @param includeUncommitted whether uncommitted entries should be included
         * @return the iterator
         */
        public Iterator<K> keyIterator(K from, boolean includeUncommitted) {
            Cursor<K> it = map.keyIterator(from);
            return wrapIterator(it, includeUncommitted);
        }

        /**
         * Iterate over keys.
         *
         * @param iterator the iterator to wrap
         * @param includeUncommitted whether uncommitted entries should be included
         * @return the iterator
         */
        public Iterator<K> wrapIterator(final Iterator<K> iterator, final boolean includeUncommitted) {
            return new Iterator<K>() {
                private K current;

                {
                    fetchNext();
                }

                private void fetchNext() {
                    while (iterator.hasNext()) {
                        current = iterator.next();
                        if (includeUncommitted) {
                            return;
                        }
                        if (containsKey(current)) {
                            return;
                        }
                    }
                    current = null;
                }

                @Override
                public boolean hasNext() {
                    return current != null;
                }

                @Override
                public K next() {
                    K result = current;
                    fetchNext();
                    return result;
                }

                @Override
                public void remove() {
                    throw DataUtils.newUnsupportedOperationException(
                            "Removing is not supported");
                }
            };
        }

        public Transaction getTransaction() {
            return transaction;
        }

    }

    /**
     * A versioned value (possibly null). It contains a pointer to the old
     * value, and the value itself.
     */
    static class VersionedValue {

        /**
         * The transaction id.
         */
        public long transactionId;

        /**
         * The log id.
         */
        public long logId;

        /**
         * The value.
         */
        public Object value;
        
        @Override
        public String toString() {
            return "{" + transactionId + "/" + logId + "}: " + value;
        }

    }

    /**
     * The value type for a versioned value.
     */
    public static class VersionedValueType implements DataType {

        private final DataType valueType;

        VersionedValueType(DataType valueType) {
            this.valueType = valueType;
        }

        @Override
        public int getMemory(Object obj) {
            VersionedValue v = (VersionedValue) obj;
            return valueType.getMemory(v.value) + 16;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == bObj) {
                return 0;
            }
            VersionedValue a = (VersionedValue) aObj;
            VersionedValue b = (VersionedValue) bObj;
            long comp = a.transactionId - b.transactionId;
            if (comp == 0) {
                comp = a.logId - b.logId;
                if (comp == 0) {
                    return valueType.compare(a.value, b.value);
                }
            }
            return Long.signum(comp);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            VersionedValue v = (VersionedValue) obj;
            buff.putVarLong(v.transactionId);
            buff.putVarLong(v.logId);
            if (v.value == null) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                valueType.write(buff, v.value);
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            VersionedValue v = new VersionedValue();
            v.transactionId = DataUtils.readVarLong(buff);
            v.logId = DataUtils.readVarLong(buff);
            if (buff.get() == 1) {
                v.value = valueType.read(buff);
            }
            return v;
        }

    }

    /**
     * A data type that contains an array of objects with the specified data
     * types.
     */
    public static class ArrayType implements DataType {

        private final int arrayLength;
        private final DataType[] elementTypes;

        ArrayType(DataType[] elementTypes) {
            this.arrayLength = elementTypes.length;
            this.elementTypes = elementTypes;
        }

        @Override
        public int getMemory(Object obj) {
            Object[] array = (Object[]) obj;
            int size = 0;
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                Object o = array[i];
                if (o != null) {
                    size += t.getMemory(o);
                }
            }
            return size;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == bObj) {
                return 0;
            }
            Object[] a = (Object[]) aObj;
            Object[] b = (Object[]) bObj;
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                int comp = t.compare(a[i], b[i]);
                if (comp != 0) {
                    return comp;
                }
            }
            return 0;
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            Object[] array = (Object[]) obj;
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                Object o = array[i];
                if (o == null) {
                    buff.put((byte) 0);
                } else {
                    buff.put((byte) 1);
                    t.write(buff, o);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            Object[] array = new Object[arrayLength];
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                if (buff.get() == 1) {
                    array[i] = t.read(buff);
                }
            }
            return array;
        }

    }

}

