/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;

/**
 * A store that supports concurrent MVCC read-committed transactions.
 */
public class TransactionStore {

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
     * Key: opId, value: [ mapId, key, oldValue ].
     */
    final MVMap<Long, Object[]> undoLog;

    /**
     * the reader/writer lock for the undo-log. Allows us to process multiple
     * selects in parallel.
     */
    final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * The map of maps.
     */
    private final HashMap<Integer, MVMap<Object, VersionedValue>> maps =
            new HashMap<>();

    private final DataType dataType;

    /**
     * This BitSet is used as vacancy indicator for transaction slots in transactions[].
     * It provides easy way to find first unoccupied slot, and also allows for copy-on-write
     * non-blocking updates.
     */
    final AtomicReference<VersionedBitSet> openTransactions = new AtomicReference<>(new VersionedBitSet());

    /**
     * This is intended to be the source of ultimate truth about transaction being committed.
     * Once bit is set, corresponding transaction is logically committed,
     * although it might be plenty of "uncommitted" entries in various maps
     * and undo record are still around.
     * Nevertheless, all of those should be considered by other transactions as committed.
     */
    final AtomicReference<BitSet> committingTransactions = new AtomicReference<>(new BitSet());

    private boolean init;

    /**
     * Soft limit on the number of concurrently opened transactions.
     * Not really needed but used by some test.
     */
    private int maxTransactionId = MAX_OPEN_TRANSACTIONS;

    /**
     * Array holding all open transaction objects.
     * Position in array is "transaction id".
     * VolatileReferenceArray would do the job here, but there is no such thing in Java yet
     */
    private final AtomicReferenceArray<Transaction> transactions =
                                                        new AtomicReferenceArray<>(MAX_OPEN_TRANSACTIONS + 1);

    /**
     * The next id of a temporary map.
     */
    private int nextTempMapId;


    /**
     * Hard limit on the number of concurrently opened transactions
     */
    // TODO: introduce constructor parameter instead of a static field, driven by URL parameter
    private static final int MAX_OPEN_TRANSACTIONS = 65535;



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
        DataType oldValueType = new VersionedValue.Type(dataType);
        ArrayType undoLogValueType = new ArrayType(new DataType[]{
                new ObjectDataType(), dataType, oldValueType
        });
        MVMap.Builder<Long, Object[]> builder =
                new MVMap.Builder<Long, Object[]>().
                valueType(undoLogValueType);
        undoLog = store.openMap("undoLog", builder);
        if (undoLog.getValueType() != undoLogValueType) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_TRANSACTION_CORRUPT,
                    "Undo map open with a different value type");
        }
    }

    /**
     * Initialize the store. This is needed before a transaction can be opened.
     * If the transaction store is corrupt, this method can throw an exception,
     * in which case the store can only be used for reading.
     */
    public void init() {
        init(RollbackListener.NONE);
    }

    public synchronized void init(RollbackListener listener) {
        if (!init) {
            // remove all temporary maps
            for (String mapName : store.getMapNames()) {
                if (mapName.startsWith("temp.")) {
                    MVMap<Object, Integer> temp = openTempMap(mapName);
                    store.removeMap(temp);
                }
            }
            rwLock.writeLock().lock();
            try {
                if (!undoLog.isEmpty()) {
                    Long key = undoLog.firstKey();
                    while (key != null) {
                        int transactionId = getTransactionId(key);
                        if (!openTransactions.get().get(transactionId)) {
                            Object[] data = preparedTransactions.get(transactionId);
                            int status;
                            String name;
                            if (data == null) {
                                if (undoLog.containsKey(getOperationId(transactionId, 0))) {
                                    status = Transaction.STATUS_OPEN;
                                } else {
                                    status = Transaction.STATUS_COMMITTING;
                                }
                                name = null;
                            } else {
                                status = (Integer) data[0];
                                name = (String) data[1];
                            }
                            long nextTxUndoKey = getOperationId(transactionId + 1, 0);
                            Long lastUndoKey = undoLog.lowerKey(nextTxUndoKey);
                            assert lastUndoKey != null;
                            assert getTransactionId(lastUndoKey) == transactionId;
                            long logId = getLogId(lastUndoKey) + 1;
                            registerTransaction(transactionId, status, name, logId, listener);
                            key = undoLog.ceilingKey(nextTxUndoKey);
                        }
                    }
                }
            } finally {
                rwLock.writeLock().unlock();
            }
            init = true;
        }
    }

    /**
     * Set the maximum transaction id, after which ids are re-used. If the old
     * transaction is still in use when re-using an old id, the new transaction
     * fails.
     *
     * @param max the maximum id
     */
    public void setMaxTransactionId(int max) {
        DataUtils.checkArgument(max <= MAX_OPEN_TRANSACTIONS,
                "Concurrent transactions limit is too high: {0}", max);
        this.maxTransactionId = max;
    }

    /**
     * Check whether a given map exists.
     *
     * @param name the map name
     * @return true if it exists
     */
    public boolean hasMap(String name) {
        return store.hasMap(name);
    }

    private static final int LOG_ID_BITS = Transaction.LOG_ID_BITS;
    private static final long LOG_ID_MASK = (1L << LOG_ID_BITS) - 1;

    /**
     * Combine the transaction id and the log id to an operation id.
     *
     * @param transactionId the transaction id
     * @param logId the log id
     * @return the operation id
     */
    static long getOperationId(int transactionId, long logId) {
        DataUtils.checkArgument(transactionId >= 0 && transactionId < (1 << (64 - LOG_ID_BITS)),
                "Transaction id out of range: {0}", transactionId);
        DataUtils.checkArgument(logId >= 0 && logId <= LOG_ID_MASK,
                "Transaction log id out of range: {0}", logId);
        return ((long) transactionId << LOG_ID_BITS) | logId;
    }

    /**
     * Get the transaction id for the given operation id.
     *
     * @param operationId the operation id
     * @return the transaction id
     */
    static int getTransactionId(long operationId) {
        return (int) (operationId >>> LOG_ID_BITS);
    }

    /**
     * Get the log id for the given operation id.
     *
     * @param operationId the operation id
     * @return the log id
     */
    static long getLogId(long operationId) {
        return operationId & LOG_ID_MASK;
    }

    /**
     * Get the list of unclosed transactions that have pending writes.
     *
     * @return the list of transactions (sorted by id)
     */
    public List<Transaction> getOpenTransactions() {
        if(!init) {
            init();
        }
        rwLock.readLock().lock();
        try {
            ArrayList<Transaction> list = new ArrayList<>();
            int transactionId = 0;
            BitSet bitSet = openTransactions.get();
            while((transactionId = bitSet.nextSetBit(transactionId + 1)) > 0) {
                Transaction transaction = getTransaction(transactionId);
                if(transaction != null) {
                    if(transaction.getStatus() != Transaction.STATUS_CLOSED) {
                        list.add(transaction);
                    }
                }
            }
            return list;
        } finally {
            rwLock.readLock().unlock();
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
    public Transaction begin() {
        return begin(RollbackListener.NONE);
    }

    /**
     * Begin a new transaction.
     * @param listener to be notified in case of a rollback
     *
     * @return the transaction
     */
    public Transaction begin(RollbackListener listener) {
        Transaction transaction = registerTransaction(0, Transaction.STATUS_OPEN, null, 0, listener);
        return transaction;
    }

    private Transaction registerTransaction(int txId, int status, String name, long logId,
                                            RollbackListener listener) {

        int transactionId;
        long sequenceNo;
        boolean success;
        do {
            VersionedBitSet original = openTransactions.get();
            if (txId == 0) {
                transactionId = original.nextClearBit(1);
            } else {
                transactionId = txId;
                assert !original.get(transactionId);
            }
            if (transactionId > maxTransactionId) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_TOO_MANY_OPEN_TRANSACTIONS,
                        "There are {0} open transactions",
                        transactionId - 1);
            }
            VersionedBitSet clone = original.clone();
            clone.set(transactionId);
            sequenceNo = clone.getVersion() + 1;
            clone.setVersion(sequenceNo);
            success = openTransactions.compareAndSet(original, clone);
        } while(!success);

        Transaction transaction = new Transaction(this, transactionId, sequenceNo, status, name, logId, listener);

        assert transactions.get(transactionId) == null;
        transactions.set(transactionId, transaction);

        return transaction;
    }

    /**
     * Store a transaction.
     *
     * @param t the transaction
     */
    synchronized void storeTransaction(Transaction t) {
        if (t.getStatus() == Transaction.STATUS_PREPARED ||
                t.getName() != null) {
            Object[] v = { t.getStatus(), t.getName() };
            preparedTransactions.put(t.getId(), v);
            t.wasStored = true;
        }
    }

    /**
     * Log an entry.
     *
     * @param t the transaction
     * @param logId the log id
     * @param mapId the map id
     * @param key the key
     * @param oldValue the old value
     */
    long log(Transaction t, long logId, int mapId,
            Object key, Object oldValue) {
        Long undoKey = getOperationId(t.getId(), logId);
        Object[] log = { mapId, key, oldValue };
        rwLock.writeLock().lock();
        try {
            if (logId == 0) {
                if (undoLog.containsKey(undoKey)) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_TOO_MANY_OPEN_TRANSACTIONS,
                            "An old transaction with the same id " +
                            "is still open: {0}",
                            t.getId());
                }
            }
            undoLog.put(undoKey, log);
        } finally {
            rwLock.writeLock().unlock();
        }
        return undoKey;
    }

    /**
     * Remove a log entry.
     *
     * @param t the transaction
     * @param logId the log id
     */
    public void logUndo(Transaction t, long logId) {
        Long undoKey = getOperationId(t.getId(), logId);
        rwLock.writeLock().lock();
        try {
            Object[] old = undoLog.remove(undoKey);
            if (old == null) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                        "Transaction {0} was concurrently rolled back",
                        t.getId());
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Remove the given map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param map the map
     */
    synchronized <K, V> void removeMap(TransactionMap<K, V> map) {
        maps.remove(map.mapId);
        store.removeMap(map.map);
    }

    /**
     * Commit a transaction.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     * @param hasChanges true if there were updates within specified
     *                   transaction (even fully rolled back),
     *                   false if just data access
     */
    void commit(Transaction t, long maxLogId, boolean hasChanges) {
        if (store.isClosed()) {
            return;
        }
        int transactionId = t.transactionId;
        // this is an atomic action that causes all changes
        // made by this transaction, to be considered as "committed"
        flipCommittingTransactionsBit(transactionId, true);

        // TODO could synchronize on blocks (100 at a time or so)
        rwLock.writeLock().lock();
        try {
            for (long logId = 0; logId < maxLogId; logId++) {
                Long undoKey = getOperationId(transactionId, logId);
                Object[] op = undoLog.get(undoKey);
                if (op == null) {
                    // partially committed: load next
                    undoKey = undoLog.ceilingKey(undoKey);
                    if (undoKey == null ||
                            getTransactionId(undoKey) != transactionId) {
                        break;
                    }
                    logId = getLogId(undoKey) - 1;
                    continue;
                }
                int mapId = (Integer) op[0];
                MVMap<Object, VersionedValue> map = openMap(mapId);
                if (map != null) { // might be null if map was removed later
                    Object key = op[1];
                    VersionedValue value = map.get(key);
                    if (value != null) {
                        // only commit (remove/update) value if we've reached
                        // last undoLog entry for a given key
                        if (value.operationId == undoKey) {
                            if (value.value == null) {
                                map.remove(key);
                            } else {
                                map.put(key, new VersionedValue(0L, value.value));
                            }
                        }
                    }
                }
                undoLog.remove(undoKey);
            }
        } finally {
            rwLock.writeLock().unlock();
            flipCommittingTransactionsBit(transactionId, false);
        }
        endTransaction(t, hasChanges);
    }

    private void flipCommittingTransactionsBit(int transactionId, boolean flag) {
        boolean success;
        do {
            BitSet original = committingTransactions.get();
            assert original.get(transactionId) != flag : flag ? "Double commit" : "Mysterious bit's disappearance";
            BitSet clone = (BitSet) original.clone();
            clone.set(transactionId, flag);
            success = committingTransactions.compareAndSet(original, clone);
        } while(!success);
    }

    /**
     * Open the map with the given name.
     *
     * @param <K> the key type
     * @param name the map name
     * @param keyType the key type
     * @param valueType the value type
     * @return the map
     */
    synchronized <K> MVMap<K, VersionedValue> openMap(String name,
            DataType keyType, DataType valueType) {
        if (keyType == null) {
            keyType = new ObjectDataType();
        }
        if (valueType == null) {
            valueType = new ObjectDataType();
        }
        VersionedValue.Type vt = new VersionedValue.Type(valueType);
        MVMap<K, VersionedValue> map;
        MVMap.Builder<K, VersionedValue> builder =
                new MVMap.Builder<K, VersionedValue>().
                keyType(keyType).valueType(vt);
        map = store.openMap(name, builder);
        @SuppressWarnings("unchecked")
        MVMap<Object, VersionedValue> m = (MVMap<Object, VersionedValue>) map;
        maps.put(map.getId(), m);
        return map;
    }

    /**
     * Open the map with the given id.
     *
     * @param mapId the id
     * @return the map
     */
    synchronized MVMap<Object, VersionedValue> openMap(int mapId) {
        MVMap<Object, VersionedValue> map = maps.get(mapId);
        if (map != null) {
            return map;
        }
        String mapName = store.getMapName(mapId);
        if (mapName == null) {
            // the map was removed later on
            return null;
        }
        DataType vt = new VersionedValue.Type(dataType);
        MVMap.Builder<Object, VersionedValue> mapBuilder =
                new MVMap.Builder<Object, VersionedValue>().
                keyType(dataType).valueType(vt);
        map = store.openMap(mapName, mapBuilder);
        maps.put(mapId, map);
        return map;
    }

    /**
     * Create a temporary map. Such maps are removed when opening the store.
     *
     * @return the map
     */
    synchronized MVMap<Object, Integer> createTempMap() {
        String mapName = "temp." + nextTempMapId++;
        return openTempMap(mapName);
    }

    /**
     * Open a temporary map.
     *
     * @param mapName the map name
     * @return the map
     */
    private MVMap<Object, Integer> openTempMap(String mapName) {
        MVMap.Builder<Object, Integer> mapBuilder =
                new MVMap.Builder<Object, Integer>().
                keyType(dataType);
        return store.openMap(mapName, mapBuilder);
    }

    /**
     * End this transaction. Change status to CLOSED and vacate transaction slot.
     * Will try to commit MVStore if autocommitDelay is 0 or if database is idle
     * and amount of unsaved changes is sizable.
     *
     * @param t the transaction
     * @param hasChanges false for R/O tx
     */
    synchronized void endTransaction(Transaction t, boolean hasChanges) {
        int txId = t.transactionId;
        t.setStatus(Transaction.STATUS_CLOSED);

        assert transactions.get(txId) == t : transactions.get(txId) + " != " + t;
        transactions.set(txId, null);

        boolean success;
        do {
            VersionedBitSet original = openTransactions.get();
            assert original.get(txId);
            VersionedBitSet clone = original.clone();
            clone.clear(txId);
            success = openTransactions.compareAndSet(original, clone);
        } while(!success);

        if (hasChanges) {
            boolean wasStored = t.wasStored;
            if (wasStored && !preparedTransactions.isClosed()) {
                preparedTransactions.remove(txId);
            }
            if (wasStored || store.getAutoCommitDelay() == 0) {
                store.tryCommit();
            } else {
                // to avoid having to store the transaction log,
                // if there is no open transaction,
                // and if there have been many changes, store them now
                if (undoLog.isEmpty()) {
                    int unsaved = store.getUnsavedMemory();
                    int max = store.getAutoCommitMemory();
                    // save at 3/4 capacity
                    if (unsaved * 4 > max * 3) {
                        store.tryCommit();
                    }
                }
            }
        }
    }

    Transaction getTransaction(int transactionId) {
        return transactions.get(transactionId);
    }

    /**
     * Rollback to an old savepoint.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     * @param toLogId the log id to roll back to
     */
    void rollbackTo(Transaction t, long maxLogId, long toLogId) {
        // TODO could synchronize on blocks (100 at a time or so)
        rwLock.writeLock().lock();
        try {
            for (long logId = maxLogId - 1; logId >= toLogId; logId--) {
                Long undoKey = getOperationId(t.getId(), logId);
                Object[] op = undoLog.get(undoKey);
                if (op == null) {
                    // partially rolled back: load previous
                    undoKey = undoLog.floorKey(undoKey);
                    if (undoKey == null ||
                            getTransactionId(undoKey) != t.getId()) {
                        break;
                    }
                    logId = getLogId(undoKey) + 1;
                    continue;
                }
                int mapId = ((Integer) op[0]).intValue();
                MVMap<Object, VersionedValue> map = openMap(mapId);
                if (map != null) {
                    Object key = op[1];
                    VersionedValue oldValue = (VersionedValue) op[2];
                    VersionedValue currentValue;
                    if (oldValue == null) {
                        // this transaction added the value
                        currentValue = map.remove(key);
                    } else {
                        // this transaction updated the value
                        currentValue = map.put(key, oldValue);
                    }
                    t.listener.onRollback(map, key, currentValue, oldValue);
                }
                undoLog.remove(undoKey);
            }
        } finally {
            rwLock.writeLock().unlock();
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
    Iterator<Change> getChanges(final Transaction t, final long maxLogId,
            final long toLogId) {
        return new Iterator<Change>() {

            private long logId = maxLogId - 1;
            private Change current;

            private void fetchNext() {
                rwLock.writeLock().lock();
                try {
                    int transactionId = t.getId();
                    while (logId >= toLogId) {
                        Long undoKey = getOperationId(transactionId, logId);
                        Object[] op = undoLog.get(undoKey);
                        logId--;
                        if (op == null) {
                            // partially rolled back: load previous
                            undoKey = undoLog.floorKey(undoKey);
                            if (undoKey == null ||
                                    getTransactionId(undoKey) != transactionId) {
                                break;
                            }
                            logId = getLogId(undoKey);
                            continue;
                        }
                        int mapId = ((Integer) op[0]).intValue();
                        MVMap<Object, VersionedValue> m = openMap(mapId);
                        if (m != null) { // could be null if map was removed later on
                            VersionedValue oldValue = (VersionedValue) op[2];
                            current = new Change(m.getName(), op[1], oldValue == null ? null : oldValue.value);
                            return;
                        }
                    }
                } finally {
                    rwLock.writeLock().unlock();
                }
                current = null;
            }

            @Override
            public boolean hasNext() {
                if(current == null) {
                    fetchNext();
                }
                return current != null;
            }

            @Override
            public Change next() {
                if(!hasNext()) {
                    throw DataUtils.newUnsupportedOperationException("no data");
                }
                Change result = current;
                current = null;
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
        public final String mapName;

        /**
         * The key.
         */
        public final Object key;

        /**
         * The value.
         */
        public final Object value;

        public Change(String mapName, Object key, Object value) {
            this.mapName = mapName;
            this.key = key;
            this.value = value;
        }
    }

    /**
     * This listener can be registered with the transaction to be notified of
     * every compensating change during transaction rollback.
     * Normally this is not required, if no external resources were modified,
     * because state of all transactional maps will be restored automatically.
     * Only state of external resources, possibly modified by triggers
     * need to be restored.
     */
    public interface RollbackListener {

        RollbackListener NONE = new RollbackListener() {
            @Override
            public void onRollback(MVMap<Object, VersionedValue> map, Object key,
                                    VersionedValue existingValue, VersionedValue restoredValue) {
                // do nothing
            }
        };

        /**
         * Notified of a single map change (add/update/remove)
         * @param map modified
         * @param key of the modified entry
         * @param existingValue value in the map (null if delete is rolled back)
         * @param restoredValue value to be restored (null if add is rolled back)
         */
        void onRollback(MVMap<Object,VersionedValue> map, Object key,
                        VersionedValue existingValue, VersionedValue restoredValue);
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
        public void read(ByteBuffer buff, Object[] obj,
                int len, boolean key) {
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
            }
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj,
                int len, boolean key) {
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
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
